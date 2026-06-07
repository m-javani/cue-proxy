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
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/m-javani/cue-proxy/internal"
	"github.com/m-javani/cue-proxy/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// ----- Helpers -----

type routerTester struct {
	router  *DefaultRouter
	topic   string
	ctx     context.Context
	cancel  context.CancelFunc
	logger  *zap.Logger
	metrics *internal.ApiMetrics
}

func setupTest(t *testing.T) *routerTester {
	logger, _ := zap.NewDevelopment()
	metrics := internal.GetApiMetrics()
	ctx, cancel := context.WithCancel(context.Background())

	router := NewDefaultRouter("test-proxy", logger, metrics)
	r, ok := router.(*DefaultRouter)
	assert.True(t, ok)

	return &routerTester{
		router:  r,
		topic:   "test-topic",
		ctx:     ctx,
		cancel:  cancel,
		logger:  logger,
		metrics: metrics,
	}

}

func (env *routerTester) addConsumer(id string, maxInflights int) *Consumer {
	conn := &websocket.Conn{} // mock
	consumer := NewConsumer(env.ctx, id, "uuid-"+id, env.topic, conn, maxInflights, env.logger)
	env.router.AddConsumer(env.topic, consumer)
	return consumer
}

func (env *routerTester) addConsumerWithFreeSlots(id string, maxInflights, sent, ack int) *Consumer {
	consumer := env.addConsumer(id, maxInflights)
	consumer.LastSentID = int64(sent)
	consumer.LastDeliveryAckID = int64(ack)
	return consumer
}

func (env *routerTester) enqueueJobs(count int) {
	jobs := make([]*model.Job, count)
	for i := range jobs {
		jobs[i] = &model.Job{
			ID:   "job-" + string(rune('a'+i)),
			Data: json.RawMessage(`"valid-json-string"`),
		}
	}
	env.router.EnqueueJobs(env.topic, jobs)
}

// ----- Tests -----

func TestRouter_AddConsumer(t *testing.T) {
	env := setupTest(t)
	defer env.cancel()

	consumer := env.addConsumer("c1", 10)

	env.router.mu.RLock()
	consumers, ok := env.router.topicConsumers[env.topic]
	env.router.mu.RUnlock()

	assert.True(t, ok)
	assert.Len(t, consumers, 1)
	assert.Equal(t, consumer.ID, consumers["c1"].ID)
	assert.NotNil(t, env.router.mainChans[env.topic])
	assert.NotNil(t, env.router.topicPressure[env.topic])
}

func TestRouter_RemoveConsumer(t *testing.T) {
	env := setupTest(t)
	defer env.cancel()

	env.addConsumer("c1", 10)
	env.addConsumer("c2", 10)

	env.router.RemoveConsumer(env.topic, "c1")

	env.router.mu.RLock()
	consumers := env.router.topicConsumers[env.topic]
	env.router.mu.RUnlock()

	assert.Len(t, consumers, 1)
	_, ok := consumers["c1"]
	assert.False(t, ok)

	// Remove last consumer - topic should be cleaned up
	env.router.RemoveConsumer(env.topic, "c2")

	env.router.mu.RLock()
	_, ok = env.router.topicConsumers[env.topic]
	env.router.mu.RUnlock()
	assert.False(t, ok)
	_, ok = env.router.mainChans[env.topic]
	assert.False(t, ok)
}

func TestRouter_EnqueueJobs(t *testing.T) {
	env := setupTest(t)
	defer env.cancel()

	env.addConsumer("c1", 10)

	// Test successful enqueue
	jobs := []*model.Job{{ID: "job1", Data: json.RawMessage(`"test-data"`)}}
	rejected := env.router.EnqueueJobs(env.topic, jobs)
	assert.Equal(t, 0, rejected)

	env.router.mu.RLock()
	ch := env.router.mainChans[env.topic]
	env.router.mu.RUnlock()
	assert.Len(t, ch, 1)

	// Test enqueue to non-existent topic
	rejected = env.router.EnqueueJobs("unknown", jobs)
	assert.Equal(t, 1, rejected)
}

func TestRouter_EnqueueJobs_Backpressure(t *testing.T) {
	env := setupTest(t)
	defer env.cancel()

	// Don't start dispatcher
	env.addConsumer("c1", 10)

	// Keep adding until we get a rejection
	var rejected int
	var lastRejected int
	for i := 0; i < 30000; i++ { // more than capacity
		jobs := []*model.Job{{ID: "job", Data: json.RawMessage(`"test-data"`)}}
		rejected = env.router.EnqueueJobs(env.topic, jobs)
		if rejected > 0 {
			lastRejected = rejected
			break
		}
	}

	// Should have rejected at least one
	assert.Greater(t, lastRejected, 0)

	// Check saturated flag
	env.router.mu.RLock()
	pressure := env.router.topicPressure[env.topic]
	env.router.mu.RUnlock()
	require.NotNil(t, pressure)
	assert.True(t, pressure.saturated)
}

func TestRouter_GetNextConsumer(t *testing.T) {
	env := setupTest(t)
	defer env.cancel()

	// No consumers
	consumer := env.router.GetNextConsumer(env.topic)
	assert.Nil(t, consumer)

	// Add consumers with different free slots
	env.addConsumerWithFreeSlots("c1", 10, 0, 0) // 10 free
	env.addConsumerWithFreeSlots("c2", 10, 5, 0) // 5 free
	env.addConsumerWithFreeSlots("c3", 10, 9, 0) // 1 free

	consumer = env.router.GetNextConsumer(env.topic)
	require.NotNil(t, consumer)
	assert.Equal(t, "c1", consumer.ID) // Should pick c1 with most free slots
}

func TestRouter_BuildHeartbeatReport(t *testing.T) {
	env := setupTest(t)
	defer env.cancel()

	// No consumers
	report := env.router.BuildHeartbeatReport()
	assert.Empty(t, report)

	// Add consumers
	env.addConsumerWithFreeSlots("c1", 10, 0, 0)
	env.addConsumerWithFreeSlots("c2", 10, 5, 0)

	report = env.router.BuildHeartbeatReport()
	require.Len(t, report, 1)
	assert.Equal(t, env.topic, report[0].Topic)
	assert.Equal(t, 2, report[0].ConsumptionScore) // both consumers available

	// Saturated topic
	env.router.markSaturated(env.topic)
	report = env.router.BuildHeartbeatReport()
	require.Len(t, report, 1)
	assert.Equal(t, 0, report[0].ConsumptionScore)

	// Saturation auto-reset after 2 seconds
	time.Sleep(2500 * time.Millisecond)
	report = env.router.BuildHeartbeatReport()
	require.Len(t, report, 1)
	assert.Equal(t, 2, report[0].ConsumptionScore)
}

func TestRouter_DispatchLoop_Blocking(t *testing.T) {
	env := setupTest(t)
	defer env.cancel()

	consumer := env.addConsumer("c1", 2)

	// Pre-set ack to 0, sent to 0
	consumer.LastDeliveryAckID = 0
	consumer.LastSentID = 0

	// Add 3 jobs
	jobs := make([]*model.Job, 3)
	for i := range jobs {
		jobs[i] = &model.Job{
			ID:   "job",
			Data: json.RawMessage(`"data"`),
		}
	}
	env.router.EnqueueJobs(env.topic, jobs)

	// Start dispatcher
	env.router.startDispatcher(env.topic)
	defer env.router.stopDispatcher(env.topic)

	// Wait for first batch to be sent
	time.Sleep(50 * time.Millisecond)

	// Consumer should have sent 2 jobs (max inflights = 2)
	assert.Equal(t, int64(2), consumer.LastSentID)
	assert.Equal(t, 0, consumer.GetFreeSlots()) // Should be full

	// The 3rd job should still be in the queue
	env.router.mu.RLock()
	ch := env.router.mainChans[env.topic]
	env.router.mu.RUnlock()
	assert.Len(t, ch, 1)
}

func TestRouter_DispatchLoop_RespectsMaxInflights(t *testing.T) {
	env := setupTest(t)
	defer env.cancel()

	// Consumer with max 2 inflights
	consumer := env.addConsumer("c1", 2)
	consumer.LastDeliveryAckID = 0
	consumer.LastSentID = 0

	// Add 5 jobs
	for i := 0; i < 5; i++ {
		jobs := []*model.Job{{
			ID:   "job",
			Data: json.RawMessage(`"data"`),
		}}
		env.router.EnqueueJobs(env.topic, jobs)
	}

	// Start dispatcher
	env.router.startDispatcher(env.topic)
	defer env.router.stopDispatcher(env.topic)

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	// Should only send 2 jobs (max inflights)
	assert.Equal(t, int64(2), consumer.LastSentID)
	assert.Equal(t, 0, consumer.GetFreeSlots())

	// Queue should have 3 jobs remaining
	env.router.mu.RLock()
	ch := env.router.mainChans[env.topic]
	env.router.mu.RUnlock()
	assert.Len(t, ch, 3)
}

func TestRouter_DispatchLoop_ResumesAfterAck(t *testing.T) {
	env := setupTest(t)
	defer env.cancel()

	consumer := env.addConsumer("c1", 2)
	consumer.LastDeliveryAckID = 0
	consumer.LastSentID = 0

	// Add 5 jobs
	for i := 0; i < 5; i++ {
		jobs := []*model.Job{{
			ID:   "job",
			Data: json.RawMessage(`"data"`),
		}}
		env.router.EnqueueJobs(env.topic, jobs)
	}

	env.router.startDispatcher(env.topic)
	defer env.router.stopDispatcher(env.topic)

	// Wait for first batch
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int64(2), consumer.LastSentID)

	// Send acks for 2 jobs
	consumer.UpdateDeliveryAck(2)

	// Wait for dispatcher to send more
	time.Sleep(50 * time.Millisecond)

	// Should have sent 2 more jobs
	assert.Equal(t, int64(4), consumer.LastSentID)

	// Queue should have 1 job remaining
	env.router.mu.RLock()
	ch := env.router.mainChans[env.topic]
	env.router.mu.RUnlock()
	assert.Len(t, ch, 1)
}

func TestRouter_DispatchLoop_MultipleConsumers(t *testing.T) {
	env := setupTest(t)
	defer env.cancel()

	// Two consumers with different capacities
	c1 := env.addConsumer("c1", 5)
	c2 := env.addConsumer("c2", 3)
	c1.LastDeliveryAckID = 0
	c1.LastSentID = 0
	c2.LastDeliveryAckID = 0
	c2.LastSentID = 0

	// Add 10 jobs
	for i := 0; i < 10; i++ {
		jobs := []*model.Job{{
			ID:   "job",
			Data: json.RawMessage(`"data"`),
		}}
		env.router.EnqueueJobs(env.topic, jobs)
	}

	env.router.startDispatcher(env.topic)
	defer env.router.stopDispatcher(env.topic)

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	// Both consumers should have received jobs
	// The one with more capacity should get more
	totalSent := c1.LastSentID + c2.LastSentID
	assert.Equal(t, int64(8), totalSent) // 5 + 3 = 8 max inflights total

	// Queue should have 2 jobs remaining
	env.router.mu.RLock()
	ch := env.router.mainChans[env.topic]
	env.router.mu.RUnlock()
	assert.Len(t, ch, 2)
}

func TestRouter_DispatchLoop_BatchCap(t *testing.T) {
	env := setupTest(t)
	defer env.cancel()

	// Consumer with many slots
	consumer := env.addConsumer("c1", 100)
	consumer.LastDeliveryAckID = 0
	consumer.LastSentID = 0

	// Add 20 jobs
	for i := 0; i < 20; i++ {
		jobs := []*model.Job{{
			ID:   "job",
			Data: json.RawMessage(`"data"`),
		}}
		env.router.EnqueueJobs(env.topic, jobs)
	}

	env.router.startDispatcher(env.topic)
	defer env.router.stopDispatcher(env.topic)

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	// With one consumer and 100 capacity, all 20 jobs should be sent
	assert.Equal(t, int64(20), consumer.LastSentID)

	// Queue should be empty
	env.router.mu.RLock()
	ch := env.router.mainChans[env.topic]
	env.router.mu.RUnlock()
	assert.Len(t, ch, 0)
}

func TestRouter_DispatchLoop_ConsumerRemovalDuringSend(t *testing.T) {
	env := setupTest(t)
	defer env.cancel()

	_ = env.addConsumer("c1", 10)

	// Add many jobs
	for i := 0; i < 20; i++ {
		jobs := []*model.Job{{
			ID:   "job",
			Data: json.RawMessage(`"data"`),
		}}
		env.router.EnqueueJobs(env.topic, jobs)
	}

	env.router.startDispatcher(env.topic)
	defer env.router.stopDispatcher(env.topic)

	// Let dispatcher start sending
	time.Sleep(30 * time.Millisecond)

	// Remove consumer while dispatcher is running
	env.router.RemoveConsumer(env.topic, "c1")

	// Dispatcher should handle gracefully (no panic)
	time.Sleep(100 * time.Millisecond)

	// Consumer should be removed
	env.router.mu.RLock()
	consumers := env.router.topicConsumers[env.topic]
	env.router.mu.RUnlock()
	assert.Empty(t, consumers)
}

func TestRouter_DispatchLoop_BatchCapFairness(t *testing.T) {
	env := setupTest(t)
	defer env.cancel()

	// Two consumers with same capacity
	c1 := env.addConsumer("c1", 100)
	c2 := env.addConsumer("c2", 100)
	c1.LastDeliveryAckID = 0
	c1.LastSentID = 0
	c2.LastDeliveryAckID = 0
	c2.LastSentID = 0

	// Add 20 jobs
	for i := 0; i < 20; i++ {
		jobs := []*model.Job{{
			ID:   "job",
			Data: json.RawMessage(`"data"`),
		}}
		env.router.EnqueueJobs(env.topic, jobs)
	}

	env.router.startDispatcher(env.topic)
	defer env.router.stopDispatcher(env.topic)

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	// With 2 consumers, they should each get about half
	totalSent := c1.LastSentID + c2.LastSentID
	assert.Equal(t, int64(20), totalSent)

	// Each should have sent roughly the same amount (within batch size of each other)
	assert.Less(t, c1.LastSentID-c2.LastSentID, int64(5))
	assert.Greater(t, c1.LastSentID-c2.LastSentID, int64(-5))
}

func TestRouter_DispatchLoop_TTL(t *testing.T) {
	env := setupTest(t)
	defer env.cancel()

	consumer := env.addConsumer("c1", 10)

	// Add an old job that should expire
	qj := &queuedJob{
		Job:       &model.Job{ID: "old", Data: []byte("data")},
		firstSeen: time.Now().Add(-jobTTL - time.Second),
	}
	env.router.mainChans[env.topic] <- qj

	// Add a fresh job
	env.enqueueJobs(1)

	env.router.startDispatcher(env.topic)
	defer env.router.stopDispatcher(env.topic)

	time.Sleep(100 * time.Millisecond)

	// Only fresh job should be processed
	assert.Equal(t, int64(1), consumer.LastSentID)
}

func TestRouter_DispatchLoop_ConsumerRemoval(t *testing.T) {
	env := setupTest(t)
	defer env.cancel()

	_ = env.addConsumer("c1", 10)
	env.enqueueJobs(5)

	env.router.startDispatcher(env.topic)
	defer env.router.stopDispatcher(env.topic)

	// Let dispatcher start
	time.Sleep(50 * time.Millisecond)

	// Remove consumer while dispatcher is running
	env.router.RemoveConsumer(env.topic, "c1")

	// Dispatcher should handle this gracefully (no panic)
	time.Sleep(100 * time.Millisecond)
}

func TestRouter_ConcurrentOperations(t *testing.T) {
	env := setupTest(t)
	defer env.cancel()

	var wg sync.WaitGroup

	// Concurrent adds and removes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			_ = env.addConsumer(id, 10)
			time.Sleep(time.Millisecond)
			env.router.RemoveConsumer(env.topic, id)
		}(string(rune('a' + i)))
	}

	// Concurrent enqueue
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			env.enqueueJobs(10)
		}()
	}

	// Concurrent GetNextConsumer
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			env.router.GetNextConsumer(env.topic)
		}()
	}

	wg.Wait()
}
