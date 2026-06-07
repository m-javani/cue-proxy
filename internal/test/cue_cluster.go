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

package test

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
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

type Option func(*options)

type options struct {
	image      string
	nodeCount  int
	configData []byte
	authData   []byte
	network    *testcontainers.DockerNetwork
	certsDir   string
	domain     string
}

func defaultOptions() *options {
	return &options{
		image:     "cue:latest",
		nodeCount: 3,
	}
}

func WithImage(image string) Option { return func(o *options) { o.image = image } }
func WithNodeCount(n int) Option    { return func(o *options) { o.nodeCount = n } }
func WithConfigYAML(data []byte) Option {
	return func(o *options) { o.configData = data }
}
func WithAuthYAML(data []byte) Option {
	return func(o *options) { o.authData = data }
}
func WithNetwork(net *testcontainers.DockerNetwork) Option {
	return func(o *options) { o.network = net }
}
func WithCertsDir(dir string) Option {
	return func(o *options) { o.certsDir = dir }
}

//go:embed configs/cluster/config.yml
var defaultConfigTpl []byte

//go:embed configs/cluster/auth.yml
var defaultAuthYAML []byte

type Cluster struct {
	Network *testcontainers.DockerNetwork
	Nodes   []*TestNode
	CACert  []byte
	TempDir string
}

type TestNode struct {
	Name        string
	Container   testcontainers.Container
	Hostname    string
	APIPort     string
	ProxyPort   string
	ClusterPort string
}

func SetupTestCluster(t *testing.T, ctx context.Context, caCertDir, domain string) (*Cluster, map[string]string, *zap.Logger) {
	_, err := CreateCA(caCertDir, "ca", 1, "")
	require.NoError(t, err)

	cluster, err := NewTestCluster(ctx, WithCertsDir(caCertDir))
	require.NoError(t, err)

	WaitForClusterReady(t, cluster, 30*time.Second)

	portMap := cluster.GetProxyMappedPorts()
	addrsWithPortedMap := make(map[string]string, 0)
	for name, port := range portMap {
		addrsWithPortedMap[name] = fmt.Sprintf("%s:%s", domain, port)
	}
	logger, _ := zap.NewDevelopment()

	return cluster, addrsWithPortedMap, logger
}

func SetupFullTestSystem(t *testing.T, ctx context.Context, caCertDir, domain string) (*zap.Logger, *Cluster, *Client, map[string]string) {
	cluster, addrsWithPortedMap, logger := SetupTestCluster(t, ctx, caCertDir, domain)

	// Start proxy connected to cluster
	_, proxyURL, wsURL, stopProxy := StartProxy(t, ctx, logger, 1, cluster, caCertDir, domain)
	defer stopProxy()

	client := NewClient(proxyURL, wsURL, logger)
	defer client.Stop()

	// Wait for proxy to be ready instead
	WaitForProxyReady(t, proxyURL, 10*time.Second)

	logger.Info("Waiting for heartbeats to propagate...")
	time.Sleep(3 * time.Second)

	return logger, cluster, client, addrsWithPortedMap

}

// Add this to setup.go
func WaitForClusterReady(t *testing.T, cluster *Cluster, timeout time.Duration) {
	t.Logf("Waiting for cluster to be ready (timeout: %v)", timeout)

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		<-ticker.C // Simple channel receive, no select needed

		// Check all nodes
		allReady := true
		var leaderID string

		for _, node := range cluster.Nodes {
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

		if allReady {
			// t.Log("  All nodes healthy, waiting for leader election...")
		}
	}

	t.Fatalf("❌ Cluster not ready after %v", timeout)
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

func NewTestCluster(ctx context.Context, opts ...Option) (*Cluster, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	if o.certsDir == "" {
		return nil, fmt.Errorf("certsDir is required: use WithCertsDir()")
	}

	if o.domain == "" {
		o.domain = "localhost"
	}
	// Debug embed
	if len(defaultConfigTpl) == 0 {
		return nil, fmt.Errorf("defaultConfigTpl is empty. Make sure configs/cluster/config.yml exists")
	}

	tmpDir, err := os.MkdirTemp("", "cue-testcluster-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(tmpDir)
		}
	}()

	// Load CA (shared with proxy)
	caInfo, err := LoadCA(o.certsDir, "ca")
	if err != nil {
		return nil, fmt.Errorf("load CA from %s: %w", o.certsDir, err)
	}

	// copy ca to tempdir
	caCertBytes, err := os.ReadFile(caInfo.CertPath)
	if err != nil {
		return nil, err
	}
	err = os.WriteFile(
		filepath.Join(tmpDir, "ca_cert.pem"),
		caCertBytes,
		0644,
	)
	if err != nil {
		return nil, err
	}
	caKeyBytes, err := os.ReadFile(caInfo.KeyPath)
	if err != nil {
		return nil, err
	}
	err = os.WriteFile(
		filepath.Join(tmpDir, "ca_key.pem"),
		caKeyBytes,
		0644,
	)
	if err != nil {
		return nil, err
	}

	nodeNames := make([]string, o.nodeCount)
	for i := range nodeNames {
		nodeNames[i] = fmt.Sprintf("node%d", i+1)
	}

	// Config template
	var configTpl *template.Template
	if len(o.configData) == 0 {
		configTpl, err = template.New("config").Parse(string(defaultConfigTpl))
	} else {
		configTpl, err = template.New("config").Parse(string(o.configData))
	}
	if err != nil {
		return nil, fmt.Errorf("parse config template: %w", err)
	}

	authData := o.authData
	if len(authData) == 0 {
		authData = defaultAuthYAML
	}

	// Network
	var net *testcontainers.DockerNetwork
	if o.network != nil {
		net = o.network
	} else {
		net, err = network.New(ctx, network.WithAttachable())
		if err != nil {
			return nil, fmt.Errorf("create network: %w", err)
		}
	}

	// Create node certs
	for _, name := range nodeNames {
		_, _, err = CreateNodeCert(tmpDir, caInfo, NodeCert{
			NodeIdentity: name,
			ServerNames:  []string{name + "." + o.domain},
		}, 1)
		if err != nil {
			return nil, fmt.Errorf("create cert %s: %w", name, err)
		}
	}

	caCertPEM, err := os.ReadFile(caInfo.CertPath)
	if err != nil {
		return nil, fmt.Errorf("read CA cert: %w", err)
	}

	// Start nodes in parallel
	var (
		wg      sync.WaitGroup
		nodes   = make([]*TestNode, 0, len(nodeNames))
		mu      sync.Mutex
		errOnce error
	)

	for _, name := range nodeNames {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			node, startErr := startNode(ctx, name, nodeNames, tmpDir, o.image, configTpl, authData, net)
			if startErr != nil {
				errOnce = fmt.Errorf("node %s: %w", name, startErr)
				return
			}
			mu.Lock()
			nodes = append(nodes, node)
			mu.Unlock()
		}(name)
	}

	wg.Wait()
	if errOnce != nil {
		return nil, errOnce
	}

	return &Cluster{
		Network: net,
		Nodes:   nodes,
		CACert:  caCertPEM,
		TempDir: tmpDir,
	}, nil
}

func startNode(
	ctx context.Context,
	name string,
	allNodes []string,
	certsDir string,
	image string,
	configTpl *template.Template,
	authData []byte,
	net *testcontainers.DockerNetwork,
) (*TestNode, error) {
	voters := `["` + strings.Join(allNodes, `","`) + `"]`
	authPath := "/etc/cue/auth.yml"

	var configBuf bytes.Buffer
	if err := configTpl.Execute(&configBuf, struct {
		NodeName      string
		InitialVoters string
		Peers         string
		AuthPath      string
	}{
		NodeName:      name,
		InitialVoters: voters,
		Peers:         voters,
		AuthPath:      authPath,
	}); err != nil {
		return nil, err
	}

	// Copy files (configs + certs)
	files := []testcontainers.ContainerFile{
		{Reader: bytes.NewReader(configBuf.Bytes()), ContainerFilePath: "/etc/cue/config.yml", FileMode: 0644},
		{Reader: bytes.NewReader(authData), ContainerFilePath: authPath, FileMode: 0644},
	}

	certEntries, _ := os.ReadDir(certsDir)
	for _, f := range certEntries {
		if f.IsDir() {
			continue
		}
		data, _ := os.ReadFile(filepath.Join(certsDir, f.Name()))
		files = append(files, testcontainers.ContainerFile{
			Reader:            bytes.NewReader(data),
			ContainerFilePath: "/etc/cue/certs/" + f.Name(),
			FileMode:          0644,
		})
	}

	req := testcontainers.ContainerRequest{
		Image: image,
		ExposedPorts: []string{
			"8321/tcp",
			"8322/udp",
			"8323/udp",
		},
		Networks: []string{net.Name},
		NetworkAliases: map[string][]string{
			net.Name: {name},
		},
		Files: files,
		// IMPORTANT: Do NOT include "/cue" here because of ENTRYPOINT
		Cmd: []string{
			"serve",
			"--config", "/etc/cue/config.yml",
			"--node-id", name,
			"--data-dir", "/etc/cue/data",
			"--cluster-cert", fmt.Sprintf("/etc/cue/certs/%s.pem", name),
			"--cluster-key", fmt.Sprintf("/etc/cue/certs/%s_key.pem", name),
			"--cluster-ca", "/etc/cue/certs/ca_cert.pem",
			"--proxy-cert", fmt.Sprintf("/etc/cue/certs/%s.pem", name),
			"--proxy-key", fmt.Sprintf("/etc/cue/certs/%s_key.pem", name),
			"--proxy-ca", "/etc/cue/certs/ca_cert.pem",
			"--initial-voters", strings.Join(allNodes, ","),
			"--peers", strings.Join(allNodes, ","),
			"--log-level", "debug",
			"--log-output", "stdout",
		},
		WaitingFor: wait.ForHTTP("/health").
			WithPort("8321/tcp").
			WithStartupTimeout(45 * time.Second),
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Consumers: []testcontainers.LogConsumer{&testcontainers.StdoutLogConsumer{}},
		},
	}

	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("create container for %s: %w", name, err)
	}

	apiPort, err := ctr.MappedPort(ctx, "8321/tcp")
	if err != nil {
		return nil, err
	}

	proxyPort, err := ctr.MappedPort(ctx, "8322/udp")
	if err != nil {
		return nil, err
	}

	quicPort, err := ctr.MappedPort(ctx, "8323/udp")
	if err != nil {
		return nil, err
	}

	return &TestNode{
		Name:        name,
		Container:   ctr,
		Hostname:    "127.0.0.1",
		APIPort:     apiPort.Port(),
		ClusterPort: quicPort.Port(),
		ProxyPort:   proxyPort.Port(),
	}, nil
}

func (c *Cluster) GetProxyMappedPorts() map[string]string {
	portMap := make(map[string]string, 0)
	for _, n := range c.Nodes {
		portMap[n.Name] = n.ProxyPort
	}
	return portMap
}

func (c *Cluster) Terminate(ctx context.Context) error {
	var errs []error

	for _, n := range c.Nodes {
		if n.Container != nil {
			if err := n.Container.Terminate(ctx); err != nil {
				errs = append(errs, err)
			}
		}
	}

	if c.Network != nil {
		if err := c.Network.Remove(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	_ = os.RemoveAll(c.TempDir)

	if len(errs) > 0 {
		return fmt.Errorf("terminate errors: %v", errs)
	}

	return nil
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
