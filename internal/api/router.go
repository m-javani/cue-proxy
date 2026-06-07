// Copyright 2026 M. Javani
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/m-javani/cue-proxy/internal"
	"github.com/m-javani/cue-proxy/internal/model"
	"github.com/m-javani/cue-proxy/pkg"
	"go.uber.org/zap"
)

type Router interface {
	AddConsumer(topic string, consumer *Consumer)
	RemoveConsumer(topic, consumerID string)
	EnqueueJobs(topic string, jobs []*model.Job) int
	GetNextConsumer(topic string) *Consumer
	BuildHeartbeatReport() []model.TopicCapacity
}

const (
	jobTTL = 5 * time.Second

	perTopicBufferSize = 25_000

	dispatchBatchSize = 8
)

type topicPressure struct {
	saturated   bool
	lastUpdated time.Time
}

type queuedJob struct {
	*model.Job
	firstSeen time.Time
}

type DefaultRouter struct {
	mu sync.RWMutex

	topicConsumers map[string]map[string]*Consumer
	mainChans      map[string]chan *queuedJob
	dispatchCancel map[string]context.CancelFunc

	topicPressure map[string]*topicPressure // for fast backpressure signaling

	proxyID string
	logger  *zap.Logger
	metrics *internal.ApiMetrics
}

func NewDefaultRouter(proxyID string, logger *zap.Logger, metrics *internal.ApiMetrics) Router {
	return &DefaultRouter{
		topicConsumers: make(map[string]map[string]*Consumer),
		mainChans:      make(map[string]chan *queuedJob),
		dispatchCancel: make(map[string]context.CancelFunc),
		topicPressure:  make(map[string]*topicPressure),
		proxyID:        proxyID,
		logger:         logger,
		metrics:        metrics,
	}
}

func (r *DefaultRouter) AddConsumer(topic string, consumer *Consumer) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.topicConsumers[topic]; !ok {
		r.topicConsumers[topic] = make(map[string]*Consumer)
		r.mainChans[topic] = make(chan *queuedJob, perTopicBufferSize)
		r.topicPressure[topic] = &topicPressure{lastUpdated: time.Now()}
		r.startDispatcher(topic)
	}

	r.topicConsumers[topic][consumer.ID] = consumer
}

func (r *DefaultRouter) RemoveConsumer(topic, consumerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if consumers, ok := r.topicConsumers[topic]; ok {
		delete(consumers, consumerID)
		if len(consumers) == 0 {
			r.drainAndStopTopic(topic)
		}
	}
}

func (r *DefaultRouter) drainAndStopTopic(topic string) {
	r.stopDispatcher(topic)

	// Drain main queue
	for len(r.mainChans[topic]) > 0 {
		<-r.mainChans[topic]
	}

	delete(r.topicConsumers, topic)
	delete(r.mainChans, topic)
	delete(r.topicPressure, topic)
}

func (r *DefaultRouter) markSaturated(topic string) {
	r.mu.Lock()
	if p, ok := r.topicPressure[topic]; ok {
		p.saturated = true
		p.lastUpdated = time.Now()
	}
	r.mu.Unlock()
}

// Returns number of rejected jobs
func (r *DefaultRouter) EnqueueJobs(topic string, jobs []*model.Job) int {
	r.mu.RLock()
	ch, ok := r.mainChans[topic]
	r.mu.RUnlock()

	if !ok {
		return len(jobs)
	}

	rejected := 0
	for _, j := range jobs {
		qj := &queuedJob{Job: j, firstSeen: time.Now()}

		select {
		case ch <- qj:
			// success
		default:
			rejected++
			r.metrics.JobsDropped(topic)
			r.markSaturated(topic)
		}
	}

	if rejected > 0 {
		r.markSaturated(topic)
	}

	return rejected
}

func (r *DefaultRouter) GetNextConsumer(topic string) *Consumer {
	r.mu.RLock()
	defer r.mu.RUnlock()

	consumersMap := r.topicConsumers[topic]
	if len(consumersMap) == 0 {
		return nil
	}

	var best *Consumer
	bestFree := -1
	for _, c := range consumersMap {
		free := c.GetFreeSlots()
		if free > bestFree {
			best = c
			bestFree = free
		}
	}
	return best
}

func (r *DefaultRouter) BuildHeartbeatReport() []model.TopicCapacity {
	r.mu.RLock()
	defer r.mu.RUnlock()

	capacities := make([]model.TopicCapacity, 0, len(r.topicConsumers))

	for topic, consumers := range r.topicConsumers {
		if len(consumers) == 0 {
			continue
		}

		// Simple auto-reset of saturated flag
		if pressure := r.topicPressure[topic]; pressure != nil {
			if pressure.saturated && time.Since(pressure.lastUpdated) > 2*time.Second {
				pressure.saturated = false
			}
		}

		if p := r.topicPressure[topic]; p != nil && p.saturated {
			capacities = append(capacities, model.TopicCapacity{
				Topic:            topic,
				ConsumptionScore: 0,
			})
			continue
		}

		activeCount := 0
		for _, c := range consumers {
			if c.GetFreeSlots() > 0 {
				activeCount++
			}
		}

		capacities = append(capacities, model.TopicCapacity{
			Topic:            topic,
			ConsumptionScore: activeCount, // number of available consumers
		})
	}

	return capacities
}

func (r *DefaultRouter) startDispatcher(topic string) {
	ctx, cancel := context.WithCancel(context.Background())
	r.dispatchCancel[topic] = cancel
	go r.dispatchLoop(topic, ctx)
}

func (r *DefaultRouter) stopDispatcher(topic string) {
	if cancel, ok := r.dispatchCancel[topic]; ok {
		cancel()
		delete(r.dispatchCancel, topic)
	}
}

func (r *DefaultRouter) dispatchLoop(topic string, ctx context.Context) {
	ch := r.mainChans[topic]

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Block until we have a consumer with free slots
		consumer := r.GetNextConsumer(topic)
		for consumer == nil || consumer.GetFreeSlots() <= 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
				consumer = r.GetNextConsumer(topic)
				continue
			}
		}

		// Send up to dispatchBatchSize jobs, checking free slots each time
		done := false
		for i := 0; i < dispatchBatchSize && !done; i++ {
			// Check if consumer still has free slots before each send
			if consumer.GetFreeSlots() <= 0 {
				break
			}

			select {
			case qj, ok := <-ch:
				if !ok || qj == nil {
					done = true
					continue
				}

				// TTL check
				if time.Since(qj.firstSeen) > jobTTL {
					r.metrics.JobsDroppedPerTopic.WithLabelValues(topic).Inc()
					continue
				}

				seqID := consumer.OnJobSent()

				msg := pkg.ToConsumerDelivery{
					Action: "job",
					Topic:  topic,
					JobID:  qj.ID,
					SeqID:  seqID,
					Data:   qj.Data,
				}

				msgJSON, err := json.Marshal(msg)
				if err != nil {
					r.logger.Error("json marshal error", zap.Error(err))
					continue
				}

				select {
				case consumer.writeCh <- msgJSON:
					r.metrics.JobsPushed(topic)
				default:
					// Consumer became full mid-batch — stop trying
					r.metrics.JobsDroppedPerTopic.WithLabelValues(topic).Inc()
					done = true
				}
			default:
				// No more jobs in queue — done
				done = true
			}
		}

		// Consumer now has fewer slots (or queue empty)
		// Loop restarts and blocks until consumer has slots again
	}
}
