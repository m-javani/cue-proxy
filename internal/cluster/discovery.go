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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/m-javani/cue-proxy/internal/config"
	"go.uber.org/zap"
)

// syncPeers starts continuous background polling for HTTP discovery.
// It returns immediately if the discovery kind is not HTTP.
func (a *ClusterAgent) syncPeers(awg *sync.WaitGroup, tick time.Duration) {
	defer awg.Done()
	if a.discoveryKind != config.DiscoveryKindHttp {
		return
	}

	// Immediate first sync
	a.updateDiscovery()

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			a.updateDiscovery()
		}
	}
}

// updateDiscovery refreshes peer information from the external HTTP endpoint.
func (a *ClusterAgent) updateDiscovery() {
	if !a.discovering.CompareAndSwap(false, true) {
		return
	}
	defer a.discovering.Store(false)

	peers, err := a.refreshFromHTTP()
	if err != nil {
		a.logger.Error("failed to refresh peers from HTTP", zap.Error(err))
		return
	}
	if len(peers) == 0 {
		a.logger.Warn("received empty peers list from HTTP")
		return
	}

	a.discoveryMu.Lock()
	a.discovery = peers
	a.discoveryMu.Unlock()

	a.logger.Info("discovery updated from HTTP", zap.Int("peers", len(peers)))
}

// refreshFromHTTP fetches the latest peers from the external HTTP discovery endpoint.
func (a *ClusterAgent) refreshFromHTTP() (map[string]PeerInfo, error) {
	if a.discoveryHTTPHost == "" {
		return nil, fmt.Errorf("no HTTP endpoint configured")
	}
	addr := fmt.Sprintf("%s?client=cueproxy", a.discoveryHTTPHost)
	req, err := http.NewRequest(http.MethodGet, addr, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http status not OK: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Versioned response first
	var httpResp httpPeersResponse
	if err := json.Unmarshal(body, &httpResp); err == nil && httpResp.Version > 0 {
		if httpResp.Version != 1 {
			return nil, fmt.Errorf("unsupported version: %d", httpResp.Version)
		}
		peers := make(map[string]PeerInfo, len(httpResp.Peers))
		for _, p := range httpResp.Peers {
			peers[p.NodeID] = p
		}
		return peers, nil
	}

	return nil, fmt.Errorf("received empty peer list")
}
