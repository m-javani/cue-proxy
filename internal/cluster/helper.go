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
