// Copyright 2026 M. Javani
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type ClusterInfoResponse struct {
	Status   string `json:"status"`
	LeaderID string `json:"leader_id"`
	Members  struct {
		Voters   []string `json:"voters"`
		Learners []string `json:"learners"`
	} `json:"members"`
}

type NodeProbeResult struct {
	HealthOK bool
	Cluster  ClusterInfoResponse
}

type TestCluster struct {
	Nodes   []TestNode
	CACert  []byte
	TempDir string
}

type TestNode struct {
	Name      string
	APIPort   string // host port
	ProxyPort string // host port
}

func SetupTestCluster(t *testing.T, ctx context.Context, caCertDir, domain string) (*TestCluster, map[string]string, *zap.Logger) {
	t.Helper()

	_, err := CreateCA(caCertDir, "ca", 1, "")
	require.NoError(t, err)

	testCluster, err := NewTestCluster(ctx, caCertDir, domain)
	require.NoError(t, err)

	// Automatic cleanup
	t.Cleanup(func() {
		_ = testCluster.Terminate(ctx)
	})

	WaitForClusterReady(t, testCluster, 45*time.Second)

	portMap := testCluster.GetProxyMappedPorts()
	addrs := make(map[string]string, len(portMap))
	for name, port := range portMap {
		addrs[name] = fmt.Sprintf("%s:%s", domain, port)
	}

	logger, _ := zap.NewDevelopment()
	return testCluster, addrs, logger
}

// SetupFullTestSystem sets up both the cluster (via Docker Compose) and the proxy.
func SetupFullTestSystem(t *testing.T, ctx context.Context, caCertDir, domain string) (*zap.Logger, *TestCluster, *Client, map[string]string) {
	t.Helper()

	testCluster, addrsWithPortedMap, logger := SetupTestCluster(t, ctx, caCertDir, domain)

	// Start proxy connected to the cluster
	_, proxyURL, wsURL, stopProxy := StartProxy(t, ctx, logger, 1, testCluster, caCertDir, domain)
	t.Cleanup(stopProxy)

	client := NewClient(proxyURL, wsURL, logger)
	t.Cleanup(client.Stop)

	// Wait for proxy readiness
	WaitForProxyReady(t, proxyURL, 10*time.Second)

	logger.Info("Waiting for heartbeats to propagate...")
	time.Sleep(3 * time.Second)

	return logger, testCluster, client, addrsWithPortedMap
}

func NewTestCluster(ctx context.Context, caCertDir, domain string) (*TestCluster, error) {
	tmpDir, err := os.MkdirTemp("", "cue-test-*")
	if err != nil {
		return nil, err
	}

	// Load CA
	caInfo, err := LoadCA(caCertDir, "ca")
	if err != nil {
		return nil, fmt.Errorf("load CA: %w", err)
	}
	caCert, _ := os.ReadFile(caInfo.CertPath)

	// Generate certificates into the compose-mounted certs directory
	for _, name := range []string{"node1", "node2", "node3"} {
		_, _, err = CreateNodeCert(caCertDir, caInfo, NodeCert{
			NodeIdentity: name,
			ServerNames:  []string{name, name + "." + domain},
		}, 1)
		if err != nil {
			return nil, fmt.Errorf("create cert for %s: %w", name, err)
		}
	}

	composeDir := getComposeDir()
	if err := runCompose(composeDir, "up", "-d"); err != nil {
		return nil, fmt.Errorf("docker compose up failed: %w", err)
	}

	cluster := &TestCluster{
		Nodes: []TestNode{
			{Name: "node1", APIPort: "9321", ProxyPort: "9322"},
			{Name: "node2", APIPort: "9421", ProxyPort: "9422"},
			{Name: "node3", APIPort: "9521", ProxyPort: "9522"},
		},
		CACert:  caCert,
		TempDir: tmpDir,
	}

	return cluster, nil
}

func getComposeDir() string {
	return "."
}

func runCompose(dir string, args ...string) error {
	cmd := exec.Command("docker", append([]string{"compose"}, args...)...)
	cmd.Dir = dir
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	if err != nil {
		fmt.Println(out.String())
		return err
	}
	return nil
}

func (c *TestCluster) GetProxyMappedPorts() map[string]string {
	m := make(map[string]string, len(c.Nodes))
	for _, n := range c.Nodes {
		m[n.Name] = n.ProxyPort
	}
	return m
}

func (c *TestCluster) Terminate(ctx context.Context) error {
	if dir := getComposeDir(); dir != "" {
		_ = runCompose(dir, "down", "--volumes")
	}
	if c.TempDir != "" {
		_ = os.RemoveAll(c.TempDir)
	}
	return nil
}

// WaitForClusterReady and ProbeNode remain unchanged (or use the previous version)
func WaitForClusterReady(t *testing.T, testCluster *TestCluster, timeout time.Duration) {
	t.Logf("Waiting for cluster to be ready (timeout: %v)", timeout)

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		<-ticker.C // Simple channel receive, no select needed

		// Check all nodes
		allReady := true
		var leaderID string

		for _, node := range testCluster.Nodes {
			addr := fmt.Sprintf("127.0.0.1:%s", node.APIPort)
			result, err := ProbeNode(context.Background(), addr)
			if err != nil || !result.HealthOK {
				t.Logf("  Node %s not ready: %v", node.Name, err)
				allReady = false
				break
			}

			// Track leader
			if result.Cluster.LeaderID != "" {
				leaderID = result.Cluster.LeaderID
			}
		}

		if allReady && leaderID != "" {
			t.Logf("✅ Cluster ready! Leader: %s", leaderID)
			return
		}

	}

	t.Fatalf("❌ Cluster not ready after %v", timeout)
}

func ProbeNode(ctx context.Context, addr string) (*NodeProbeResult, error) {
	client := &http.Client{
		Timeout: 3 * time.Second,
	}

	result := &NodeProbeResult{}

	// -------------------
	// 1. /health
	// -------------------
	healthURL := fmt.Sprintf("http://%s/health", addr)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create health request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("health request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var body map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return nil, fmt.Errorf("decode health response: %w", err)
		}
		result.HealthOK = body["status"] == "ok"
	} else {
		return nil, fmt.Errorf("health endpoint returned %d", resp.StatusCode)
	}

	// -------------------
	// 2. /cluster/info
	// -------------------
	infoURL := fmt.Sprintf("http://%s/cluster/info", addr)

	req2, err := http.NewRequestWithContext(ctx, http.MethodGet, infoURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create cluster info request: %w", err)
	}

	resp2, err := client.Do(req2)
	if err != nil {
		return nil, fmt.Errorf("cluster info request failed: %w", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cluster info returned %d", resp2.StatusCode)
	}

	if err := json.NewDecoder(resp2.Body).Decode(&result.Cluster); err != nil {
		return nil, fmt.Errorf("decode cluster info: %w", err)
	}

	return result, nil
}

func WaitForProxyReady(t *testing.T, proxyURL string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		<-ticker.C // Simple channel receive, no select needed

		resp, err := http.Get(proxyURL + "/health")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			t.Log("✅ Proxy ready!")
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
	}
	t.Fatalf("❌ Proxy not ready after %v", timeout)
}
