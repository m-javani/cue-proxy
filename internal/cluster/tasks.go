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
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/m-javani/cue-proxy/internal/model"
	"go.uber.org/zap"
)

func (a *ClusterAgent) requestReceiverTask(awg *sync.WaitGroup) {
	defer awg.Done()

	for {
		select {
		case <-a.ctx.Done():
			return
		case request, ok := <-a.producerCh:
			if !ok {
				return
			}
			// Handle Done requests with batching
			// done signals dont need response back
			if request.ProxyRequest.Type == model.ReqDone && request.ProxyRequest.Done != nil {
				a.doneCmdBatchMu.Lock()
				topic := request.ProxyRequest.Done.Topic
				// Append jobIDs to the batch
				if _, exists := a.doneCmdBatchBuffer[topic]; !exists {
					a.doneCmdBatchBuffer[topic] = make([]string, 1024)
				}
				a.doneCmdBatchBuffer[topic] = append(a.doneCmdBatchBuffer[topic], request.ProxyRequest.Done.JobIDs...)
				a.doneCmdBatchMu.Unlock()
				continue
			}

			// for add job
			if request.ToProducerRespCh != nil {
				a.requestMapMu.Lock()
				rid := a.requestCounter.Add(1)
				request.ProxyRequest.RequestID = fmt.Sprintf("%d", rid)
				a.requestMap[request.ProxyRequest.RequestID] = request.ToProducerRespCh
				a.requestMapMu.Unlock()
			}

			if err := a.sendToLeader(&request.ProxyRequest); err != nil {
				a.requestMapMu.Lock()
				delete(a.requestMap, request.ProxyRequest.RequestID)
				select {
				case request.ToProducerRespCh <- &model.ToProducerResponse{
					RequestID: request.ProxyRequest.RequestID,
					Status:    model.ToProxyRespStatusError,
					Error:     "leader unavailable",
				}:
				default:
				}
				a.requestMapMu.Unlock()
			}
		}
	}
}

// Flush all pending Done requests
func (a *ClusterAgent) flushDoneCmdsTasks(awg *sync.WaitGroup, tick time.Duration) {
	defer awg.Done()

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			// Context cancelled - flush all remaining
			a.flushAllTopics()
			return
		case <-ticker.C:
			a.flushAllTopics()
		}
	}
}

// Flush all topics
func (a *ClusterAgent) flushAllTopics() {
	a.doneCmdBatchMu.Lock()
	// Copy topics to flush
	topics := make([]string, 0, len(a.doneCmdBatchBuffer))
	for topic := range a.doneCmdBatchBuffer {
		topics = append(topics, topic)
	}
	a.doneCmdBatchMu.Unlock()

	// Flush each topic
	for _, topic := range topics {
		a.flushTopic(topic)
	}
}

// Flush a specific topic
func (a *ClusterAgent) flushTopic(topic string) {
	a.doneCmdBatchMu.Lock()

	// Check if topic still exists in batch
	jobIDs, exists := a.doneCmdBatchBuffer[topic]
	if !exists || len(jobIDs) == 0 {
		a.doneCmdBatchMu.Unlock()
		return
	}

	// Make a copy of the jobIDs
	jobsCopy := make([]string, len(jobIDs))
	copy(jobsCopy, jobIDs)

	// Create the batched Done request
	request := model.ProxyRequest{
		RequestID: a.nextRequestID(),
		Type:      model.ReqDone,
		Done: &model.DonePayload{
			Topic:  topic,
			JobIDs: jobsCopy,
		},
	}

	// Remove the topic from batch before sending
	a.doneCmdBatchBuffer[topic] = a.doneCmdBatchBuffer[topic][:0]
	a.doneCmdBatchMu.Unlock()

	// Send the batched request to leader
	if err := a.sendToLeader(&request); err != nil {
		a.logger.Warn("couldnt send the batch done command to leader")
		// add back to batch if you want retry
		a.doneCmdBatchMu.Lock()
		if len(a.doneCmdBatchBuffer[topic]) == 0 {
			a.doneCmdBatchBuffer[topic] = append(a.doneCmdBatchBuffer[topic], jobsCopy...)
		}
		a.doneCmdBatchMu.Unlock()
	}
}

func (a *ClusterAgent) syncConnectionsTask(awg *sync.WaitGroup, tick time.Duration) {
	defer awg.Done()

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return

		case <-ticker.C:
			nodesConInfo := a.ListPeersAddrServerName()
			var curVoterIDs []string
			for _, node := range nodesConInfo {
				curVoterIDs = append(curVoterIDs, node.NodeID)
			}

			// Step 1: Remove nodes that are no longer in the desired list
			a.mu.Lock()

			for nodeID := range a.sendConns {
				if !slices.Contains(curVoterIDs, nodeID) {
					if conn := a.sendConns[nodeID]; conn != nil {
						_ = conn.CloseWithError(0, "node removed from cluster")
					}
					delete(a.sendConns, nodeID)
					delete(a.nodeToAddr, nodeID)
					delete(a.addressToNode, nodeID)
				}
			}

			for nodeID := range a.recvConns {
				if !slices.Contains(curVoterIDs, nodeID) {
					if conn := a.recvConns[nodeID]; conn != nil {
						_ = conn.CloseWithError(0, "node removed from cluster")
					}
					delete(a.recvConns, nodeID)
					delete(a.nodeToAddr, nodeID)
					delete(a.addressToNode, nodeID)
				}
			}

			a.mu.Unlock()

			var wg sync.WaitGroup
			// Step 2: Connect nodes that need to connect and not already connected in parallel
			for _, ni := range nodesConInfo {
				wg.Add(1)
				go func(ni PeerResolvedInfo) {
					defer wg.Done()
					if con, ok := a.sendConns[ni.NodeID]; !ok || con.Context().Err() != nil {
						if err := a.dialSendConnection(ni); err != nil {
							a.logger.Debug("dial send connection failed", zap.Error(err))
						}
					}
					if con, ok := a.recvConns[ni.NodeID]; !ok || con.Context().Err() != nil {
						if err := a.dialRecvConnection(ni); err != nil {
							a.logger.Debug("dial recv connection failed", zap.Error(err))
						}
					}
				}(ni)
			}
			wg.Wait()

		}
	}
}

func (a *ClusterAgent) heartbeatTask(awg *sync.WaitGroup, tick time.Duration) {
	defer awg.Done()

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			// Build heartbeat report from Router (single source of truth)
			capacities := a.router.BuildHeartbeatReport()

			report := &model.HeartbeatReport{
				ProxyID:    a.proxyID,
				Timestamp:  time.Now().Unix(),
				Capacities: capacities,
			}

			// Fire and forget
			_ = a.sendToLeader(&model.ProxyRequest{
				RequestID:       a.nextRequestID(),
				Type:            model.ReqHeartbeatReport,
				HeartbeatReport: report,
			})
		}
	}
}
