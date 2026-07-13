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

// dispatch_buffer.go
package cluster

import (
	"context"
	"hash/fnv"
	"slices"
	"sync/atomic"
	"time"

	"github.com/m-javani/cue-proxy/internal/model"
)

const JOBS_BUFFER_WINDOW time.Duration = 20 * time.Millisecond

type ApiWaiting struct {
	ApiReqID  uint32
	RespCh    chan<- *model.ToProducerResponse
	JobHashes []uint64
}

type JobBuffer struct {
	topic              string
	store              chan model.ApiRequestWithRespCh
	jobsRequestCounter *atomic.Uint32
	bufferedCount      atomic.Int32
	bufferedJobsCount  atomic.Int32
	waitingMap         map[uint32][]ApiWaiting // key: requestID
	apiInputCh         chan model.ApiRequestWithRespCh
	responseInputCh    chan *model.ToProducerResponse
	sendJobsCh         chan model.ProxyRequest
	ticker             *time.Ticker
	done               chan struct{}
	ctx                context.Context
}

func NewJobBuffer(ctx context.Context, topic string, jobsRequestCounter *atomic.Uint32, sendJobsCh chan model.ProxyRequest) *JobBuffer {
	return &JobBuffer{
		topic:              topic,
		apiInputCh:         make(chan model.ApiRequestWithRespCh, 1024),
		responseInputCh:    make(chan *model.ToProducerResponse, 1024),
		sendJobsCh:         sendJobsCh,
		ticker:             time.NewTicker(JOBS_BUFFER_WINDOW),
		done:               make(chan struct{}),
		ctx:                ctx,
		store:              make(chan model.ApiRequestWithRespCh, 1024),
		jobsRequestCounter: jobsRequestCounter,
		waitingMap:         map[uint32][]ApiWaiting{},
		bufferedCount:      atomic.Int32{},
		bufferedJobsCount:  atomic.Int32{},
	}
}

func (b *JobBuffer) Run() {
	defer b.ticker.Stop()

	for {
		select {
		case <-b.done:
			return

		case <-b.ctx.Done():
			return

		case req := <-b.apiInputCh:
			b.buffer(req)

		case resp := <-b.responseInputCh:
			b.dispatchResponse(resp)

		case <-b.ticker.C:
			b.drain()
		}
	}
}

func (b *JobBuffer) buffer(request model.ApiRequestWithRespCh) {
	b.store <- request
	b.bufferedCount.Add(1)
	jobCount := len(request.ProxyRequest.AddJobs.Jobs)
	b.bufferedJobsCount.Add(int32(jobCount))
}

func (b *JobBuffer) drain() {
	if len(b.store) == 0 {
		return
	}
	bufferCount := b.bufferedCount.Load()
	allJobs := make([]*model.Job, 0, b.bufferedJobsCount.Load())
	waitingList := make([]ApiWaiting, 0, bufferCount)

	for i := 0; i < int(bufferCount); i++ {
		req := <-b.store
		payload := req.ProxyRequest.AddJobs
		jobHashes := make([]uint64, 0, len(payload.Jobs))
		for _, job := range payload.Jobs {
			allJobs = append(allJobs, job)
			h := fnv.New64a()
			h.Write([]byte(job.ID))
			hash := h.Sum64()
			jobHashes = append(jobHashes, hash)
		}

		waiting := ApiWaiting{
			ApiReqID:  req.ProxyRequest.RequestID,
			RespCh:    req.ToProducerRespCh,
			JobHashes: jobHashes,
		}
		waitingList = append(waitingList, waiting)
	}

	b.bufferedCount.Store(0)
	b.bufferedJobsCount.Store(0)

	requestID := b.jobsRequestCounter.Add(1)
	b.waitingMap[requestID] = waitingList

	// BLOCKING send - creates backpressure
	b.sendJobsCh <- model.ProxyRequest{
		RequestID: requestID,
		Type:      model.ReqAddJobs,
		AddJobs: &model.AddJobsPayload{
			Topic: b.topic,
			Jobs:  allJobs,
		},
	}
}

func (b *JobBuffer) dispatchResponse(resp *model.ToProducerResponse) {
	waitingList, ok := b.waitingMap[resp.RequestID]
	if !ok {
		return
	}
	if resp.Status == model.ToProxyRespStatusSuccess {
		for _, call := range waitingList {
			if call.RespCh != nil {
				select {
				case call.RespCh <- &model.ToProducerResponse{
					RequestID: call.ApiReqID,
					Status:    model.ToProxyRespStatusSuccess,
				}:
				default:
				}
			}
		}
		delete(b.waitingMap, resp.RequestID)
		return
	}

	// hits all api requests. its a top level error not add job error
	if resp.Status == model.ToProxyRespStatusError && len(resp.Failures) == 0 {
		for _, call := range waitingList {
			if call.RespCh != nil {
				select {
				case call.RespCh <- &model.ToProducerResponse{
					RequestID: call.ApiReqID,
					Status:    model.ToProxyRespStatusError,
					Error:     resp.Error,
				}:
				default:
				}
			}
		}
		delete(b.waitingMap, resp.RequestID)
		return
	}

	// Error path - partial failures
	for _, call := range waitingList {
		if call.RespCh == nil {
			continue
		}
		var clientFailures []model.JobFailure
		for _, f := range resp.Failures {
			if b.jobBelongsToClient(f.JobID, call.JobHashes) {
				clientFailures = append(clientFailures, f)
			}
		}
		if len(clientFailures) > 0 {
			select {
			case call.RespCh <- &model.ToProducerResponse{
				RequestID: call.ApiReqID,
				Status:    model.ToProxyRespStatusError,
				Failures:  clientFailures,
				Error:     resp.Error,
			}:
			default:
			}
		} else {
			select {
			case call.RespCh <- &model.ToProducerResponse{
				RequestID: call.ApiReqID,
				Status:    model.ToProxyRespStatusSuccess,
			}:
			default:
			}
		}
	}

	// Clean up waitingMap on partial failure path
	delete(b.waitingMap, resp.RequestID)
}

func (b *JobBuffer) jobBelongsToClient(jobID string, clientHashes []uint64) bool {
	h := fnv.New64a()
	h.Write([]byte(jobID))
	hash := h.Sum64()
	return slices.Contains(clientHashes, hash)
}
