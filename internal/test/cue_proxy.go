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
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m-javani/cue-proxy/internal/app"
	"github.com/m-javani/cue-proxy/internal/cluster"
	"github.com/m-javani/cue-proxy/internal/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// StartProxy starts a real proxy instance connected to the provided test cluster
func StartProxy(t *testing.T, ctx context.Context, logger *zap.Logger, proxyIdUint8 uint8, testCluster *TestCluster, caDir, domain string) (proxyID, proxyURL, wsURL string, stop func()) {
	t.Helper()

	proxyID = fmt.Sprintf("proxy%d", proxyIdUint8)

	// Load proxy config (separate from cluster)
	cfg, err := config.LoadConfig("./configs/config.yml") // your proxy config
	require.NoError(t, err)
	cfg.ProxyID = proxyID

	cfg.Cluster.QUICPort = 8322
	cfg.API.Port = getRandomPort(18080, 19080)
	cfg.API.Host = "localhost"
	addr := cfg.GetAPIAddress()
	proxyURL = fmt.Sprintf("http://%s", addr)
	wsURL = fmt.Sprintf("ws://%s/ws", addr)

	// === Use same CA from cluster ===
	// Write CA cert temporarily so CreateNodeCertWithCAFile can find it
	caCertPath := filepath.Join(caDir, "ca_cert.pem")
	err = os.WriteFile(caCertPath, testCluster.CACert, 0644)
	require.NoError(t, err)
	t.Cleanup(func() { os.Remove(caCertPath) })

	// Generate proxy cert signed by cluster CA
	d := proxyID
	if domain != "" {
		d = fmt.Sprintf("%s:%s", proxyID, domain)
	}
	proxyNode := NodeCert{
		NodeIdentity: proxyID,
		ServerNames:  []string{d},
	}

	certPath, keyPath, err := CreateNodeCertWithCAFile(caDir, "ca", proxyNode, 1)
	require.NoError(t, err)

	// TLS config for proxy
	cfg.Cluster.CAPath = caCertPath
	cfg.Cluster.CertPath = certPath
	cfg.Cluster.KeyPath = keyPath

	portMap := testCluster.GetProxyMappedPorts()
	addrsWithPortedMap := make(map[string]string, 0)
	for name, port := range portMap {
		addrsWithPortedMap[name] = fmt.Sprintf("localhost:%s", port)
	}

	leaderAvailable := atomic.Bool{}
	leaderAvailable.Store(false)

	discovery, err := cluster.LoadDiscoveryFile("./configs/discovery.yml")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load initial discovery file: %v\n", err)
		os.Exit(1)
	}

	// start fale api
	go runFakeServer(t, discovery)

	// Start proxy
	go func() {
		if err := app.RunProxy(ctx, cfg, logger, &leaderAvailable, discovery); err != nil && ctx.Err() == nil {
			logger.Error("proxy stopped with error", zap.Error(err))
		}
	}()

	// check health
	logger.Sugar().Infof("probing health at %s/health", proxyURL)
	require.Eventually(t, func() bool {
		resp, err := http.Get(proxyURL + "/health")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 100*time.Millisecond)
	logger.Info("api health check done")

	stop = func() {
		os.Remove(cfg.Cluster.CertPath)
		os.Remove(cfg.Cluster.KeyPath)
		// CA file cleanup is handled by t.Cleanup above
	}

	return proxyID, proxyURL, wsURL, stop
}

// getRandomPort returns a random port in the specified range
func getRandomPort(min, max int) int {
	port := rand.Intn(max-min) + min

	// Verify port is available (optional but recommended)
	for range 10 {
		if isPortAvailable(port) {
			return port
		}
		port = rand.Intn(max-min) + min
	}
	return port // Return anyway, will fail fast if in use
}

// isPortAvailable checks if a TCP port is available
func isPortAvailable(port int) bool {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return false
	}
	ln.Close()
	return true
}

func runFakeServer(t *testing.T, discovery map[string]cluster.PeerInfo) {
	// Pre-marshal the data
	jsonData, _ := json.Marshal(discovery)

	srv := &http.Server{
		Addr: "127.0.0.1:8321",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				http.Error(w, `{"error": "method not allowed"}`, http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(jsonData)
		}),
	}

	go srv.ListenAndServe()
	t.Cleanup(func() { srv.Shutdown(context.Background()) })
}
