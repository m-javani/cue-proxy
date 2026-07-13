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

package cluster

import (
	"context"
	"hash/fnv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m-javani/cue-proxy/internal/model"
	"github.com/stretchr/testify/assert"
)

func TestNewJobBuffer(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Uint32
	sendJobsCh := make(chan model.ProxyRequest)

	buffer := NewJobBuffer(ctx, "test-topic", &counter, sendJobsCh)

	assert.NotNil(t, buffer)
	assert.Equal(t, "test-topic", buffer.topic)
	assert.NotNil(t, buffer.apiInputCh)
	assert.NotNil(t, buffer.responseInputCh)
	assert.NotNil(t, buffer.sendJobsCh)
	assert.NotNil(t, buffer.ticker)
	assert.NotNil(t, buffer.done)
	assert.NotNil(t, buffer.ctx)
	assert.NotNil(t, buffer.store)
	assert.NotNil(t, buffer.waitingMap)
	assert.Equal(t, int32(0), buffer.bufferedCount.Load())
	assert.Equal(t, int32(0), buffer.bufferedJobsCount.Load())
}

func TestJobBuffer_RunAndBuffer(t *testing.T) {
	ctx := t.Context()

	var counter atomic.Uint32
	sendJobsCh := make(chan model.ProxyRequest, 10)
	buffer := NewJobBuffer(ctx, "test-topic", &counter, sendJobsCh)

	// Start the buffer
	go buffer.Run()
	defer close(buffer.done)

	// Create test request
	req := model.ApiRequestWithRespCh{
		ProxyRequest: model.ProxyRequest{
			RequestID: 1,
			Type:      model.ReqAddJobs,
			AddJobs: &model.AddJobsPayload{
				Topic: "test-topic",
				Jobs: []*model.Job{
					{ID: "job1", Data: []byte("payload1")},
					{ID: "job2", Data: []byte("payload2")},
				},
			},
		},
		ToProducerRespCh: make(chan *model.ToProducerResponse, 1),
	}

	// Send request to buffer
	buffer.apiInputCh <- req

	// Wait for ticker to fire (JOBS_BUFFER_WINDOW + small buffer)
	time.Sleep(JOBS_BUFFER_WINDOW + 10*time.Millisecond)

	// Verify drain happened
	select {
	case sentReq := <-sendJobsCh:
		assert.Equal(t, uint32(1), sentReq.RequestID)
		assert.Equal(t, model.ReqAddJobs, sentReq.Type)
		assert.NotNil(t, sentReq.AddJobs)
		assert.Equal(t, "test-topic", sentReq.AddJobs.Topic)
		assert.Len(t, sentReq.AddJobs.Jobs, 2)
		assert.Equal(t, "job1", sentReq.AddJobs.Jobs[0].ID)
		assert.Equal(t, "job2", sentReq.AddJobs.Jobs[1].ID)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("No request sent to sendJobsCh")
	}
}

func TestJobBuffer_DrainWithMultipleRequests(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Uint32
	sendJobsCh := make(chan model.ProxyRequest, 10)
	buffer := NewJobBuffer(ctx, "test-topic", &counter, sendJobsCh)

	// Start the buffer
	go buffer.Run()
	defer close(buffer.done)

	// Send multiple requests
	for i := 0; i < 3; i++ {
		req := model.ApiRequestWithRespCh{
			ProxyRequest: model.ProxyRequest{
				RequestID: uint32(i + 1),
				Type:      model.ReqAddJobs,
				AddJobs: &model.AddJobsPayload{
					Topic: "test-topic",
					Jobs: []*model.Job{
						{ID: "job1", Data: []byte("payload1")},
					},
				},
			},
			ToProducerRespCh: make(chan *model.ToProducerResponse, 1),
		}
		buffer.apiInputCh <- req
	}

	// Wait for ticker
	time.Sleep(JOBS_BUFFER_WINDOW + 10*time.Millisecond)

	// Verify all requests were drained into one
	select {
	case sentReq := <-sendJobsCh:
		assert.Len(t, sentReq.AddJobs.Jobs, 3)
		// Verify waiting map has 3 waiting entries
		assert.Len(t, buffer.waitingMap[sentReq.RequestID], 3)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("No request sent to sendJobsCh")
	}
}

func TestJobBuffer_DispatchSuccess(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Uint32
	sendJobsCh := make(chan model.ProxyRequest, 10)
	buffer := NewJobBuffer(ctx, "test-topic", &counter, sendJobsCh)

	// Prepare waiting map
	respCh1 := make(chan *model.ToProducerResponse, 1)
	respCh2 := make(chan *model.ToProducerResponse, 1)

	requestID := uint32(100)
	buffer.waitingMap[requestID] = []ApiWaiting{
		{
			ApiReqID:  1,
			RespCh:    respCh1,
			JobHashes: []uint64{123, 456},
		},
		{
			ApiReqID:  2,
			RespCh:    respCh2,
			JobHashes: []uint64{789},
		},
	}

	// Dispatch success response
	response := &model.ToProducerResponse{
		RequestID: requestID,
		Status:    model.ToProxyRespStatusSuccess,
	}

	buffer.dispatchResponse(response)

	// Verify responses sent
	select {
	case resp := <-respCh1:
		assert.Equal(t, uint32(1), resp.RequestID)
		assert.Equal(t, model.ToProxyRespStatusSuccess, resp.Status)
	default:
		t.Fatal("Response not sent to respCh1")
	}

	select {
	case resp := <-respCh2:
		assert.Equal(t, uint32(2), resp.RequestID)
		assert.Equal(t, model.ToProxyRespStatusSuccess, resp.Status)
	default:
		t.Fatal("Response not sent to respCh2")
	}

	// Verify waiting map cleaned up
	_, ok := buffer.waitingMap[requestID]
	assert.False(t, ok)
}

func TestJobBuffer_DispatchTopLevelError(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Uint32
	sendJobsCh := make(chan model.ProxyRequest, 10)
	buffer := NewJobBuffer(ctx, "test-topic", &counter, sendJobsCh)

	respCh1 := make(chan *model.ToProducerResponse, 1)
	respCh2 := make(chan *model.ToProducerResponse, 1)

	requestID := uint32(200)
	buffer.waitingMap[requestID] = []ApiWaiting{
		{
			ApiReqID:  1,
			RespCh:    respCh1,
			JobHashes: []uint64{123},
		},
		{
			ApiReqID:  2,
			RespCh:    respCh2,
			JobHashes: []uint64{456},
		},
	}

	response := &model.ToProducerResponse{
		RequestID: requestID,
		Status:    model.ToProxyRespStatusError,
		Error:     "top level error",
		Failures:  []model.JobFailure{}, // Empty failures indicates top-level error
	}

	buffer.dispatchResponse(response)

	// Both should receive error
	select {
	case resp := <-respCh1:
		assert.Equal(t, model.ToProxyRespStatusError, resp.Status)
		assert.Equal(t, "top level error", resp.Error)
	default:
		t.Fatal("Response not sent")
	}

	select {
	case resp := <-respCh2:
		assert.Equal(t, model.ToProxyRespStatusError, resp.Status)
		assert.Equal(t, "top level error", resp.Error)
	default:
		t.Fatal("Response not sent")
	}
}

func TestJobBuffer_DispatchPartialFailures(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Uint32
	sendJobsCh := make(chan model.ProxyRequest, 10)
	buffer := NewJobBuffer(ctx, "test-topic", &counter, sendJobsCh)

	respCh1 := make(chan *model.ToProducerResponse, 1)
	respCh2 := make(chan *model.ToProducerResponse, 1)

	// Create hashes for job IDs
	// job1 -> hash, job2 -> hash
	h1 := fnv.New64a()
	h1.Write([]byte("job1"))
	hash1 := h1.Sum64()

	h2 := fnv.New64a()
	h2.Write([]byte("job2"))
	hash2 := h2.Sum64()

	requestID := uint32(300)
	buffer.waitingMap[requestID] = []ApiWaiting{
		{
			ApiReqID:  1,
			RespCh:    respCh1,
			JobHashes: []uint64{hash1, hash2},
		},
		{
			ApiReqID:  2,
			RespCh:    respCh2,
			JobHashes: []uint64{hash2},
		},
	}

	response := &model.ToProducerResponse{
		RequestID: requestID,
		Status:    model.ToProxyRespStatusError,
		Error:     "partial failure",
		Failures: []model.JobFailure{
			{JobID: "job1", Reason: 3},
		},
	}

	buffer.dispatchResponse(response)

	// Client 1 has job1 and job2 -> should get error
	select {
	case resp := <-respCh1:
		assert.Equal(t, model.ToProxyRespStatusError, resp.Status)
		assert.Len(t, resp.Failures, 1)
		assert.Equal(t, "job1", resp.Failures[0].JobID)
	default:
		t.Fatal("Response not sent to respCh1")
	}

	// Client 2 only has job2 -> should get success
	select {
	case resp := <-respCh2:
		assert.Equal(t, model.ToProxyRespStatusSuccess, resp.Status)
		assert.Empty(t, resp.Failures)
	default:
		t.Fatal("Response not sent to respCh2")
	}
}

func TestJobBuffer_DispatchUnknownRequestID(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Uint32
	sendJobsCh := make(chan model.ProxyRequest, 10)
	buffer := NewJobBuffer(ctx, "test-topic", &counter, sendJobsCh)

	// Dispatch response for unknown request ID - should not panic
	response := &model.ToProducerResponse{
		RequestID: 999,
		Status:    model.ToProxyRespStatusSuccess,
	}

	assert.NotPanics(t, func() {
		buffer.dispatchResponse(response)
	})
}

func TestJobBuffer_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var counter atomic.Uint32
	sendJobsCh := make(chan model.ProxyRequest, 10)
	buffer := NewJobBuffer(ctx, "test-topic", &counter, sendJobsCh)

	done := make(chan bool)
	go func() {
		buffer.Run()
		done <- true
	}()

	// Cancel context
	cancel()

	select {
	case <-done:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Buffer did not stop on context cancellation")
	}
}

func TestJobBuffer_StopViaDone(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Uint32
	sendJobsCh := make(chan model.ProxyRequest, 10)
	buffer := NewJobBuffer(ctx, "test-topic", &counter, sendJobsCh)

	done := make(chan bool)
	go func() {
		buffer.Run()
		done <- true
	}()

	// Close done channel
	close(buffer.done)

	select {
	case <-done:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Buffer did not stop on done close")
	}
}

func TestJobBuffer_DrainWithEmptyStore(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Uint32
	sendJobsCh := make(chan model.ProxyRequest, 10)
	buffer := NewJobBuffer(ctx, "test-topic", &counter, sendJobsCh)

	// Drain with empty store should not block or panic
	assert.NotPanics(t, func() {
		buffer.drain()
	})
}

func TestJobBuffer_JobBelongsToClient(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Uint32
	sendJobsCh := make(chan model.ProxyRequest, 10)
	buffer := NewJobBuffer(ctx, "test-topic", &counter, sendJobsCh)

	// Create hashes
	h1 := fnv.New64a()
	h1.Write([]byte("job1"))
	hash1 := h1.Sum64()

	h2 := fnv.New64a()
	h2.Write([]byte("job2"))
	hash2 := h2.Sum64()

	clientHashes := []uint64{hash1, hash2}

	assert.True(t, buffer.jobBelongsToClient("job1", clientHashes))
	assert.True(t, buffer.jobBelongsToClient("job2", clientHashes))
	assert.False(t, buffer.jobBelongsToClient("job3", clientHashes))
}

func TestJobBuffer_NilResponseChannel(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Uint32
	sendJobsCh := make(chan model.ProxyRequest, 10)
	buffer := NewJobBuffer(ctx, "test-topic", &counter, sendJobsCh)

	requestID := uint32(400)
	buffer.waitingMap[requestID] = []ApiWaiting{
		{
			ApiReqID:  1,
			RespCh:    nil, // Nil channel should be handled gracefully
			JobHashes: []uint64{123},
		},
	}

	response := &model.ToProducerResponse{
		RequestID: requestID,
		Status:    model.ToProxyRespStatusSuccess,
	}

	// Should not panic with nil channel
	assert.NotPanics(t, func() {
		buffer.dispatchResponse(response)
	})
}

func TestJobBuffer_ResponseChannelFull(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Uint32
	sendJobsCh := make(chan model.ProxyRequest, 10)
	buffer := NewJobBuffer(ctx, "test-topic", &counter, sendJobsCh)

	respCh := make(chan *model.ToProducerResponse, 1)
	// Fill the channel
	respCh <- &model.ToProducerResponse{RequestID: 999}

	requestID := uint32(500)
	buffer.waitingMap[requestID] = []ApiWaiting{
		{
			ApiReqID:  1,
			RespCh:    respCh,
			JobHashes: []uint64{123},
		},
	}

	response := &model.ToProducerResponse{
		RequestID: requestID,
		Status:    model.ToProxyRespStatusSuccess,
	}

	// Should not block when channel is full (uses default case)
	assert.NotPanics(t, func() {
		buffer.dispatchResponse(response)
	})
}

func TestJobBuffer_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Uint32
	sendJobsCh := make(chan model.ProxyRequest, 100)
	buffer := NewJobBuffer(ctx, "test-topic", &counter, sendJobsCh)

	go buffer.Run()
	defer close(buffer.done)

	// Concurrent sends
	const numRequests = 10
	for i := 0; i < numRequests; i++ {
		go func(id int) {
			req := model.ApiRequestWithRespCh{
				ProxyRequest: model.ProxyRequest{
					RequestID: uint32(id),
					Type:      model.ReqAddJobs,
					AddJobs: &model.AddJobsPayload{
						Topic: "test-topic",
						Jobs: []*model.Job{
							{ID: "job1", Data: []byte("payload1")},
						},
					},
				},
				ToProducerRespCh: make(chan *model.ToProducerResponse, 1),
			}
			buffer.apiInputCh <- req
		}(i)
	}

	time.Sleep(JOBS_BUFFER_WINDOW + 50*time.Millisecond)

	// Should have drained at least once
	select {
	case <-sendJobsCh:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("No drain occurred")
	}
}

// dispatch_buffer_test.go

func TestJobBuffer_WaitingMapCleanupOnPartialFailure(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Uint32
	sendJobsCh := make(chan model.ProxyRequest, 10)
	buffer := NewJobBuffer(ctx, "test-topic", &counter, sendJobsCh)

	// Create hashes for job IDs
	h1 := fnv.New64a()
	h1.Write([]byte("job1"))
	hash1 := h1.Sum64()

	h2 := fnv.New64a()
	h2.Write([]byte("job2"))
	hash2 := h2.Sum64()

	// Setup waitingMap with a test entry
	requestID := uint32(300)
	respCh := make(chan *model.ToProducerResponse, 1)
	buffer.waitingMap[requestID] = []ApiWaiting{
		{
			ApiReqID:  1,
			RespCh:    respCh,
			JobHashes: []uint64{hash1, hash2},
		},
	}

	// Verify entry exists
	_, exists := buffer.waitingMap[requestID]
	assert.True(t, exists, "WaitingMap entry should exist before partial failure")

	// Send partial failure response
	response := &model.ToProducerResponse{
		RequestID: requestID,
		Status:    model.ToProxyRespStatusError,
		Error:     "partial failure",
		Failures: []model.JobFailure{
			{JobID: "job1", Reason: 3},
		},
	}

	buffer.dispatchResponse(response)

	// Verify waitingMap entry was cleaned up
	_, exists = buffer.waitingMap[requestID]
	assert.False(t, exists, "WaitingMap entry should be cleaned up after partial failure")

	// Verify response was sent
	select {
	case resp := <-respCh:
		assert.Equal(t, uint32(1), resp.RequestID)
		assert.Equal(t, model.ToProxyRespStatusError, resp.Status)
		assert.Len(t, resp.Failures, 1)
		assert.Equal(t, "job1", resp.Failures[0].JobID)
		assert.Equal(t, 3, resp.Failures[0].Reason)
	default:
		t.Fatal("Response not sent to channel")
	}
}

func TestJobBuffer_WaitingMapCleanupOnMultipleClientsPartialFailure(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Uint32
	sendJobsCh := make(chan model.ProxyRequest, 10)
	buffer := NewJobBuffer(ctx, "test-topic", &counter, sendJobsCh)

	// Create hashes for job IDs
	h1 := fnv.New64a()
	h1.Write([]byte("job1"))
	hash1 := h1.Sum64()

	h2 := fnv.New64a()
	h2.Write([]byte("job2"))
	hash2 := h2.Sum64()

	h3 := fnv.New64a()
	h3.Write([]byte("job3"))
	hash3 := h3.Sum64()

	// Setup waitingMap with multiple clients
	requestID := uint32(400)
	respCh1 := make(chan *model.ToProducerResponse, 1)
	respCh2 := make(chan *model.ToProducerResponse, 1)
	respCh3 := make(chan *model.ToProducerResponse, 1)

	buffer.waitingMap[requestID] = []ApiWaiting{
		{
			ApiReqID:  1,
			RespCh:    respCh1,
			JobHashes: []uint64{hash1, hash2},
		},
		{
			ApiReqID:  2,
			RespCh:    respCh2,
			JobHashes: []uint64{hash2, hash3},
		},
		{
			ApiReqID:  3,
			RespCh:    respCh3,
			JobHashes: []uint64{hash1, hash3},
		},
	}

	// Send partial failure response - only job1 fails
	response := &model.ToProducerResponse{
		RequestID: requestID,
		Status:    model.ToProxyRespStatusError,
		Error:     "some jobs failed",
		Failures: []model.JobFailure{
			{JobID: "job1", Reason: 3},
		},
	}

	buffer.dispatchResponse(response)

	// Verify waitingMap entry was cleaned up
	_, exists := buffer.waitingMap[requestID]
	assert.False(t, exists, "WaitingMap entry should be cleaned up after partial failure")

	// Client 1 has job1 and job2 -> should get error
	select {
	case resp := <-respCh1:
		assert.Equal(t, uint32(1), resp.RequestID)
		assert.Equal(t, model.ToProxyRespStatusError, resp.Status)
		assert.Len(t, resp.Failures, 1)
		assert.Equal(t, "job1", resp.Failures[0].JobID)
	default:
		t.Fatal("Response not sent to respCh1")
	}

	// Client 2 has job2 and job3 -> should get success
	select {
	case resp := <-respCh2:
		assert.Equal(t, uint32(2), resp.RequestID)
		assert.Equal(t, model.ToProxyRespStatusSuccess, resp.Status)
		assert.Empty(t, resp.Failures)
	default:
		t.Fatal("Response not sent to respCh2")
	}

	// Client 3 has job1 and job3 -> should get error
	select {
	case resp := <-respCh3:
		assert.Equal(t, uint32(3), resp.RequestID)
		assert.Equal(t, model.ToProxyRespStatusError, resp.Status)
		assert.Len(t, resp.Failures, 1)
		assert.Equal(t, "job1", resp.Failures[0].JobID)
	default:
		t.Fatal("Response not sent to respCh3")
	}
}

func TestJobBuffer_WaitingMapCleanupOnAllFailurePaths(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Uint32
	sendJobsCh := make(chan model.ProxyRequest, 10)
	buffer := NewJobBuffer(ctx, "test-topic", &counter, sendJobsCh)

	testCases := []struct {
		name     string
		response *model.ToProducerResponse
	}{
		{
			name: "success",
			response: &model.ToProducerResponse{
				RequestID: 100,
				Status:    model.ToProxyRespStatusSuccess,
			},
		},
		{
			name: "top level error",
			response: &model.ToProducerResponse{
				RequestID: 200,
				Status:    model.ToProxyRespStatusError,
				Error:     "top level error",
				Failures:  []model.JobFailure{},
			},
		},
		{
			name: "partial failure",
			response: &model.ToProducerResponse{
				RequestID: 300,
				Status:    model.ToProxyRespStatusError,
				Error:     "partial failure",
				Failures: []model.JobFailure{
					{JobID: "job1", Reason: 3},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup waitingMap with entry
			respCh := make(chan *model.ToProducerResponse, 1)
			h := fnv.New64a()
			h.Write([]byte("job1"))
			hash := h.Sum64()

			buffer.waitingMap[tc.response.RequestID] = []ApiWaiting{
				{
					ApiReqID:  1,
					RespCh:    respCh,
					JobHashes: []uint64{hash},
				},
			}

			// Verify entry exists
			_, exists := buffer.waitingMap[tc.response.RequestID]
			assert.True(t, exists, "Entry should exist before dispatch")

			// Dispatch response
			buffer.dispatchResponse(tc.response)

			// Verify entry was cleaned up
			_, exists = buffer.waitingMap[tc.response.RequestID]
			assert.False(t, exists, "Entry should be cleaned up after %s", tc.name)
		})
	}
}
