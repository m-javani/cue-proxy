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

import "time"

// Handshake models (for proxy → Cue)
type ConnectionType string

const (
	ConnectionTypeInbound  ConnectionType = "inbound"
	ConnectionTypeOutbound ConnectionType = "outbound"
)

type Handshake struct {
	ProxyID         string         `msgpack:"proxy_id"`
	ConnectionType  ConnectionType `msgpack:"connection_type"`
	ProtocolVersion int            `msgpack:"protocol_version"`
}

type HandshakeResponse struct {
	Status  string `msgpack:"status"`
	Message string `msgpack:"message,omitempty"`
	NodeID  string `msgpack:"node_id"`
}

// ============

type CueTopology struct {
	Leader         string
	Term           uint64
	Voters         []string
	Learners       []string
	LastUpdate     time.Time
	NodeHeartbeats map[string]*NodeHeartbeatInfo
}

type NodeHeartbeatInfo struct {
	ClaimedLeader string
	Term          uint64
	LastSeen      time.Time
	Status        string
}

type PeerResolvedInfo struct {
	NodeID     string `msgpack:"node_id"`
	Addr       string `msgpack:"addr"`
	ServerName string `msgpack:"serverName"`
}
