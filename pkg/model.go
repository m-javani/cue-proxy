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

package pkg

import "encoding/json"

type ToConsumerDelivery struct {
	Action string          `json:"action"`
	Topic  string          `json:"topic"`
	JobID  string          `json:"jobId"`
	SeqID  int64           `json:"seqId"`
	Data   json.RawMessage `json:"data"`
}

type WebSocketMessage struct {
	Action string `json:"action"`
	UUID   string `json:"uuid,omitempty"`
	Topic  string `json:"topic,omitempty"`
	LastID int64  `json:"last_id,omitempty"`
	JobID  string
	Data   json.RawMessage `json:"data,omitempty"`
}
