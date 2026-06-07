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

package model

type ProxyRequestType uint8

const (
	ReqHeartbeatReport ProxyRequestType = iota
	ReqAddTopic
	ReqAddJob
	ReqDone
)

type ProxyRequest struct {
	RequestID string           `msgpack:"request_id"`
	Type      ProxyRequestType `msgpack:"type"`

	AddTopic        *AddTopicPayload `msgpack:"add_topic,omitempty"`
	HeartbeatReport *HeartbeatReport `msgpack:"heartbeat_report,omitempty"`
	AddJob          *AddJobPayload   `msgpack:"add_job,omitempty"`
	Done            *DonePayload     `msgpack:"done,omitempty"`
}

type AddTopicPayload struct {
	Topic string `msgpack:"topic"`
}

type HeartbeatReport struct {
	ProxyID    string          `msgpack:"proxy_id"`
	Timestamp  int64           `msgpack:"timestamp"`
	Capacities []TopicCapacity `msgpack:"capacities"`
}

type TopicCapacity struct {
	Topic            string `msgpack:"topic"`
	ConsumptionScore int    `msgpack:"consumption_score"`
}

type AddJobPayload struct {
	Job Job `msgpack:"job"`
}

type DonePayload struct {
	Topic  string   `msgpack:"topic"`
	JobIDs []string `msgpack:"job_ids"`
}

type Job struct {
	ID    string `msgpack:"id"`
	Topic string `msgpack:"topic"`
	Data  []byte `msgpack:"data"`
}

// ToProxyMessage (from Cue to Proxy)
type ToProxyMessageType string

const (
	ProxyMessageResponse  ToProxyMessageType = "response"
	ProxyMessageOutbound  ToProxyMessageType = "outbound"
	ProxyMessageHeartbeat ToProxyMessageType = "heartbeat"
)

type ToProxyMessage struct {
	Type      ToProxyMessageType  `msgpack:"type"`
	Response  *ToProducerResponse `msgpack:"response,omitempty"`
	Outbound  *ToConsumerMessage  `msgpack:"outbound,omitempty"`
	Heartbeat *ToProxyHeartbeat   `msgpack:"heartbeat,omitempty"`
}

type ToProducerRespStatus string

const (
	ToProxyRespStatusSuccess ToProducerRespStatus = "success"
	ToProxyRespStatusError   ToProducerRespStatus = "error"
	ToProxyRespStatusExist   ToProducerRespStatus = "exist"
)

type ToProducerResponse struct {
	// cluster agent adds this not top level
	RequestID string               `msgpack:"request_id"`
	Status    ToProducerRespStatus `msgpack:"status"`
	Error     string               `msgpack:"error,omitempty"`
}

type ToConsumerMessage struct {
	Topic   string `msgpack:"topic"`
	ProxyID string `msgpack:"proxy_id"`
	Jobs    []*Job `msgpack:"jobs"`
}

type NodeStatus string

const (
	NodeStatusFollowerActive NodeStatus = "follower"
	NodeStatusLeaderActive   NodeStatus = "leader"
	NodeStatusUnavailable    NodeStatus = "unavailable"
)

func (s NodeStatus) String() string {
	switch s {
	case NodeStatusFollowerActive:
		return "follower"
	case NodeStatusLeaderActive:
		return "leader"
	case NodeStatusUnavailable:
		return "unavailable"
	default:
		return "unknown"
	}
}

type ToProxyHeartbeat struct {
	NodeStatus string   `msgpack:"node_status"`
	Voters     []string `msgpack:"voters"`
	Learners   []string `msgpack:"learners"`
	Leader     string   `msgpack:"leader"`
	Term       uint64   `msgpack:"term"`
}

type ProxyRequestWithRespCh struct {
	ProxyRequest     ProxyRequest
	ToProducerRespCh chan<- *ToProducerResponse
}
