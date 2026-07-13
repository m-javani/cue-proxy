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
	"os"

	"github.com/stretchr/testify/assert/yaml"
)

// LoadDiscoveryFile loads and validates peers from YAML
func LoadDiscoveryFile(pathStr string) (map[string]PeerInfo, error) {
	data, err := os.ReadFile(pathStr)
	if err != nil {
		return nil, fmt.Errorf("failed to read discovery file %s: %w", pathStr, err)
	}

	var config struct {
		Nodes []PeerInfo `yaml:"nodes"`
	}

	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse discovery.yml: %w", err)
	}

	if len(config.Nodes) == 0 {
		return nil, fmt.Errorf("no nodes defined in discovery file")
	}

	infos := make(map[string]PeerInfo)

	for i, node := range config.Nodes {
		if err := node.Validate(); err != nil {
			return nil, fmt.Errorf("validation failed for node at index %d (%s): %w", i, node.NodeID, err)
		}

		if _, ok := infos[node.NodeID]; ok {
			return nil, fmt.Errorf("duplicate node_id: %s", node.NodeID)
		}
		infos[node.NodeID] = node
	}

	return infos, nil
}
