package cluster

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"
)

func (a *ClusterAgent) updateDiscovery() {
	if !a.discovering.CompareAndSwap(false, true) {
		return
	}
	defer a.discovering.Store(false)

	// Try leader first if available
	if a.leaderAvailable.Load() {
		if val := a.currentLeader.Load(); val != nil {
			if leaderID, ok := val.(string); ok {
				a.discoveryMu.RLock()
				peer, exist := a.discovery[leaderID]
				a.discoveryMu.RUnlock()
				if exist {
					if err := a.fetchDiscovery([]string{peer.IP}); err == nil {
						a.logger.Info("discovery updated from leader", zap.String("leader", leaderID))
						return
					}
				}
			}
		}
	}

	// Try all available nodes in discovery
	a.discoveryMu.RLock()
	targets := make([]string, 0, len(a.discovery))
	for _, peer := range a.discovery {
		targets = append(targets, peer.IP)
	}
	a.discoveryMu.RUnlock()

	if len(targets) == 0 {
		a.logger.Warn("no discovery targets available to fetch cluster peers")
		return
	}

	if err := a.fetchDiscovery(targets); err != nil {
		a.logger.Sugar().Warnf("failed to upadte discovery. %s", err.Error())
	}
}

func (a *ClusterAgent) fetchDiscovery(ips []string) error {
	for _, ip := range ips {
		peers, err := a.getPeersFromClusterAPI(ip, a.clusterApiPort)
		if err == nil && len(peers) > 0 {
			a.discoveryMu.Lock()
			a.discovery = peers
			a.discoveryMu.Unlock()
			a.logger.Info("discovery updated from cluster", zap.String("node", ip), zap.Int("peers", len(peers)))
			return nil
		}
	}
	return fmt.Errorf("failed to update discovery from all targets")
}

func (a *ClusterAgent) getPeersFromClusterAPI(ip string, port int) (map[string]PeerInfo, error) {
	url := fmt.Sprintf("http://%s:%d/discovery/peers", ip, port)

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %s", resp.Status)
	}

	var peers map[string]PeerInfo
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if len(peers) == 0 {
		return nil, fmt.Errorf("received empty peer list")
	}

	return peers, nil
}
