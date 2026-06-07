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
	"time"

	"github.com/m-javani/cue-proxy/internal/model"
	"go.uber.org/zap"
)

func (a *ClusterAgent) handleResponse(resp *model.ToProducerResponse) {
	a.requestMapMu.Lock()
	toProducerResponse, ok := a.requestMap[resp.RequestID]
	if !ok {
		// done signals do not have response
		return
	}
	delete(a.requestMap, resp.RequestID)
	a.requestMapMu.Unlock()

	select {
	case toProducerResponse <- resp:
	default:
	}
}

func (a *ClusterAgent) handleHeartbeat(senderID string, hb *model.ToProxyHeartbeat) {
	a.topologyMu.Lock()
	defer a.topologyMu.Unlock()

	// Init heartbeat map if nil
	if a.cueTopology.NodeHeartbeats == nil {
		a.cueTopology.NodeHeartbeats = make(map[string]*NodeHeartbeatInfo)
	}

	// Update or create heartbeat entry for sender
	info, exists := a.cueTopology.NodeHeartbeats[senderID]
	if !exists {
		info = &NodeHeartbeatInfo{}
		a.cueTopology.NodeHeartbeats[senderID] = info
	}
	info.ClaimedLeader = hb.Leader
	info.Term = hb.Term
	info.LastSeen = time.Now()
	info.Status = hb.NodeStatus

	// Clean stale heartbeats (> 5 seconds old)
	now := time.Now()
	for id, nodeInfo := range a.cueTopology.NodeHeartbeats {
		if now.Sub(nodeInfo.LastSeen) > 5*time.Second {
			delete(a.cueTopology.NodeHeartbeats, id)
		}
	}

	switch hb.NodeStatus {
	case model.NodeStatusLeaderActive.String():
		a.evaluateLeadership(senderID, hb)

	case model.NodeStatusUnavailable.String():
		if senderID == a.currentLeader.Load() {
			a.logger.Warn("Current leader unavailable",
				zap.String("leader", senderID))
			a.leaderAvailable.Store(false)
		}
		delete(a.cueTopology.NodeHeartbeats, senderID)

	case model.NodeStatusFollowerActive.String():
		// Followers confirm who they think the leader is
		if hb.Leader != "" && hb.Leader != senderID {
			a.evaluateLeadership(hb.Leader, hb)
		}
	}
}

func (a *ClusterAgent) evaluateLeadership(claimedLeader string, hb *model.ToProxyHeartbeat) {
	now := time.Now()
	activeNodes := 0
	agreementCount := 0

	for _, info := range a.cueTopology.NodeHeartbeats {
		if now.Sub(info.LastSeen) > 2*time.Second {
			continue
		}
		activeNodes++
		if info.ClaimedLeader == claimedLeader && info.Term == hb.Term {
			agreementCount++
		}
	}

	if activeNodes == 0 {
		return
	}

	// Quorum = majority of voters
	quorum := len(a.cueTopology.Voters)/2 + 1
	if quorum == 0 {
		quorum = activeNodes/2 + 1
	}

	curLeader := a.currentLeader.Load()
	currentLeader := ""
	if curLeader != nil {
		currentLeader = curLeader.(string)
	}
	currentTerm := a.cueTopology.Term

	// Only accept leadership with quorum agreement
	if agreementCount >= quorum {
		if hb.Term > currentTerm ||
			(hb.Term == currentTerm && claimedLeader != currentLeader) {

			a.logger.Info("Leadership confirmed",
				zap.String("leader", claimedLeader),
				zap.Uint64("term", hb.Term))

			a.cueTopology.Leader = claimedLeader
			a.cueTopology.Term = hb.Term
			a.cueTopology.Voters = hb.Voters
			a.cueTopology.Learners = hb.Learners
			a.cueTopology.LastUpdate = now

			a.currentLeader.Store(claimedLeader)
			a.leaderAvailable.Store(true)
		}
	} else {
		// Only log if current leader lost quorum (important event)
		if claimedLeader == currentLeader && agreementCount < quorum {
			a.logger.Warn("Leader lost quorum",
				zap.String("leader", currentLeader),
				zap.Int("agreement", agreementCount),
				zap.Int("quorum", quorum))
			a.leaderAvailable.Store(false)
		}
	}
}

// Helper to get current status for debugging
func (a *ClusterAgent) GetStatus() map[string]interface{} {
	a.topologyMu.RLock()
	defer a.topologyMu.RUnlock()

	activeNodes := make(map[string]map[string]interface{})
	for nodeID, info := range a.cueTopology.NodeHeartbeats {
		activeNodes[nodeID] = map[string]interface{}{
			"claimed_leader": info.ClaimedLeader,
			"term":           info.Term,
			"status":         info.Status,
			"last_seen":      info.LastSeen,
		}
	}

	return map[string]interface{}{
		"leader":          a.currentLeader.Load(),
		"available":       a.leaderAvailable.Load(),
		"term":            a.cueTopology.Term,
		"voters":          a.cueTopology.Voters,
		"learners":        a.cueTopology.Learners,
		"active_nodes":    len(a.cueTopology.NodeHeartbeats),
		"node_heartbeats": activeNodes,
		"last_update":     a.cueTopology.LastUpdate,
	}
}
