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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadDiscoveryFile(t *testing.T) {
	tests := []struct {
		name          string
		content       string
		expectedPeers map[string]PeerInfo
		expectError   bool
		errorContains string
	}{
		{
			name: "valid discovery file",
			content: `
nodes:
  - node_id: node1
    ip: 192.168.1.10
    identity:
      kind: dns
      value: node1.example.com
  - node_id: node2
    ip: 192.168.1.11
    identity:
      kind: ip
      value: 192.168.1.11
  - node_id: node3
    ip: 192.168.1.12
    identity:
      kind: spiffe
      value: spiffe://example.org/node3
`,
			expectedPeers: map[string]PeerInfo{
				"node1": {
					NodeID: "node1",
					IP:     "192.168.1.10",
					Identity: TLSIdentity{
						Kind:  IdentityDNS,
						Value: "node1.example.com",
					},
				},
				"node2": {
					NodeID: "node2",
					IP:     "192.168.1.11",
					Identity: TLSIdentity{
						Kind:  IdentityIP,
						Value: "192.168.1.11",
					},
				},
				"node3": {
					NodeID: "node3",
					IP:     "192.168.1.12",
					Identity: TLSIdentity{
						Kind:  IdentitySPIFFE,
						Value: "spiffe://example.org/node3",
					},
				},
			},
			expectError: false,
		},
		{
			name: "file with no nodes",
			content: `
nodes: []
`,
			expectedPeers: map[string]PeerInfo{},
			expectError:   true,
			errorContains: "no nodes defined",
		},
		{
			name: "duplicate node_id",
			content: `
nodes:
  - node_id: node1
    ip: 192.168.1.10
    identity:
      kind: dns
      value: node1.example.com
  - node_id: node1
    ip: 192.168.1.11
    identity:
      kind: ip
      value: 192.168.1.11
`,
			expectError:   true,
			errorContains: "duplicate node_id",
		},
		{
			name: "missing required node_id",
			content: `
nodes:
  - ip: 192.168.1.10
    identity:
      kind: dns
      value: node1.example.com
`,
			expectError:   true,
			errorContains: "node_id is required",
		},
		{
			name: "missing required ip",
			content: `
nodes:
  - node_id: node1
    identity:
      kind: dns
      value: node1.example.com
`,
			expectError:   true,
			errorContains: "ip is required",
		},
		{
			name: "invalid ip address",
			content: `
nodes:
  - node_id: node1
    ip: invalid-ip
    identity:
      kind: dns
      value: node1.example.com
`,
			expectError:   true,
			errorContains: "invalid IP address",
		},
		{
			name: "invalid identity kind",
			content: `
nodes:
  - node_id: node1
    ip: 192.168.1.10
    identity:
      kind: invalid
      value: node1.example.com
`,
			expectError:   true,
			errorContains: "unknown identity kind",
		},
		{
			name: "empty identity value",
			content: `
nodes:
  - node_id: node1
    ip: 192.168.1.10
    identity:
      kind: dns
      value: ""
`,
			expectError:   true,
			errorContains: "identity value is required",
		},
		{
			name: "SPIFFE identity without spiffe:// prefix",
			content: `
nodes:
  - node_id: node1
    ip: 192.168.1.10
    identity:
      kind: spiffe
      value: example.org/node1
`,
			expectError:   true,
			errorContains: "SPIFFE identity must start with spiffe://",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp file
			tmpFile, err := os.CreateTemp("", "discovery-*.yaml")
			require.NoError(t, err)
			defer os.Remove(tmpFile.Name())

			_, err = tmpFile.WriteString(tt.content)
			require.NoError(t, err)
			err = tmpFile.Close()
			require.NoError(t, err)

			peers, err := LoadDiscoveryFile(tmpFile.Name())

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				assert.Nil(t, peers)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedPeers, peers)
			}
		})
	}
}

func TestLoadDiscoveryFile_FileNotFound(t *testing.T) {
	peers, err := LoadDiscoveryFile("/nonexistent/file.yaml")
	assert.Error(t, err)
	assert.Nil(t, peers)
	assert.Contains(t, err.Error(), "failed to read discovery file")
}

func TestLoadDiscoveryFile_InvalidYAML(t *testing.T) {
	// Create temp file with invalid YAML
	tmpFile, err := os.CreateTemp("", "discovery-*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString("invalid: yaml: [")
	require.NoError(t, err)
	err = tmpFile.Close()
	require.NoError(t, err)

	peers, err := LoadDiscoveryFile(tmpFile.Name())
	assert.Error(t, err)
	assert.Nil(t, peers)
	assert.Contains(t, err.Error(), "failed to parse discovery.yml")
}
