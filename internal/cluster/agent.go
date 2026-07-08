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
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m-javani/cue-proxy/internal"
	"github.com/m-javani/cue-proxy/internal/api"
	"github.com/m-javani/cue-proxy/internal/config"
	"github.com/m-javani/cue-proxy/internal/model"
	"github.com/quic-go/quic-go"
	"go.uber.org/zap"
)

// ClusterAgent is the main flat struct managing QUIC connections to all Cue nodes
type ClusterAgent struct {
	proxyID string

	quicAddr string
	quicPort uint16

	certPath   string
	keyPath    string
	caCertPath string

	transportConfig *quic.Config
	clientTLSConfig *tls.Config

	// Connection maps
	sendConns     map[string]*quic.Conn // nodeID -> send (Proxy→Cue)
	recvConns     map[string]*quic.Conn // nodeID -> recv (Cue→Proxy)
	addressToNode map[string]string
	nodeToAddr    map[string]string

	mu sync.RWMutex

	// Leader & availability
	currentLeader   atomic.Value // string
	leaderAvailable *atomic.Bool
	cueTopology     CueTopology
	topologyMu      sync.RWMutex

	discovery         map[string]PeerInfo
	discoveryKind     config.DiscoveryKind
	discoveryYMLPath  string
	discoveryHTTPHost string
	discoveryMu       sync.RWMutex
	discovering       atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc

	logger  *zap.Logger
	metrics *internal.ClusterMetrics

	// Upper layer integration
	producerCh <-chan model.ProxyRequestWithRespCh

	// In-flight producer requests
	requestMap     map[string]chan<- *model.ToProducerResponse
	requestMapMu   sync.RWMutex
	requestCounter atomic.Uint32

	router api.Router

	doneCmdBatchMu     sync.Mutex
	doneCmdBatchBuffer map[string][]string // topic -> jobIDs

	reqIDCounter atomic.Uint64
}

const (
	protocolVersion = 1
)

// NewClusterAgent creates the agent
func NewClusterAgent(
	proxyID string,
	quicAddr string,
	quicPort uint16,
	certPath, keyPath, caCertPath string,
	producerCh <-chan model.ProxyRequestWithRespCh,
	router api.Router,
	discovery map[string]PeerInfo,
	logger *zap.Logger,
	leaderAvailable *atomic.Bool,
	discoveryKind config.DiscoveryKind,
	discoveryYMLPath string,
	discoveryHTTPHost string,
) (*ClusterAgent, error) {
	tlsConfig, err := loadClientTLSConfig(certPath, keyPath, caCertPath)
	if err != nil {
		return nil, fmt.Errorf("load TLS config: %w", err)
	}

	transportConfig := createTransportConfig()

	ctx, cancel := context.WithCancel(context.Background())

	agent := &ClusterAgent{
		proxyID:         proxyID,
		quicAddr:        quicAddr,
		quicPort:        quicPort,
		certPath:        certPath,
		keyPath:         keyPath,
		caCertPath:      caCertPath,
		clientTLSConfig: tlsConfig,
		transportConfig: transportConfig,
		sendConns:       make(map[string]*quic.Conn),
		recvConns:       make(map[string]*quic.Conn),
		addressToNode:   make(map[string]string),
		nodeToAddr:      make(map[string]string),
		requestMap:      make(map[string]chan<- *model.ToProducerResponse),
		producerCh:      producerCh,
		metrics:         internal.GetClusterMetrics(),
		router:          router,
		cueTopology: CueTopology{
			Voters:   []string{},
			Learners: []string{},
		},
		discovery:          discovery,
		logger:             logger,
		ctx:                ctx,
		cancel:             cancel,
		mu:                 sync.RWMutex{},
		currentLeader:      atomic.Value{},
		leaderAvailable:    leaderAvailable,
		topologyMu:         sync.RWMutex{},
		requestMapMu:       sync.RWMutex{},
		requestCounter:     atomic.Uint32{},
		doneCmdBatchMu:     sync.Mutex{},
		doneCmdBatchBuffer: make(map[string][]string, 0),
		reqIDCounter:       atomic.Uint64{},
		discoveryMu:        sync.RWMutex{},
		discoveryKind:      discoveryKind,
		discoveryYMLPath:   discoveryYMLPath,
		discoveryHTTPHost:  discoveryHTTPHost,
		discovering:        atomic.Bool{},
	}

	agent.currentLeader.Store("")

	return agent, nil
}

func (a *ClusterAgent) nextRequestID() string {
	return strconv.FormatUint(a.reqIDCounter.Add(1), 36)
}

// Run starts background tasks (call from main)
func (a *ClusterAgent) Run() {
	var wg sync.WaitGroup

	wg.Add(1)
	go a.syncConnectionsTask(&wg, 1000*time.Millisecond)

	wg.Add(1)
	go a.heartbeatTask(&wg, 500*time.Millisecond)

	wg.Add(1)
	go a.requestReceiverTask(&wg)

	wg.Add(1)
	go a.flushDoneCmdsTasks(&wg, 500*time.Millisecond)

	wg.Add(1)
	go a.syncPeers(&wg, 2*time.Second)

	a.logger.Info("ClusterAgent started", zap.String("proxy_id", a.proxyID))

	<-a.ctx.Done()

	wg.Wait()
	a.logger.Info("shutting down cluster agent", zap.String("proxy_id", a.proxyID))
}

// Close shuts down everything
func (a *ClusterAgent) Close() error {
	a.cancel()

	a.mu.Lock()
	defer a.mu.Unlock()

	for _, conn := range a.sendConns {
		_ = conn.CloseWithError(0, "shutting down")
	}
	for _, conn := range a.recvConns {
		_ = conn.CloseWithError(0, "shutting down")
	}

	a.logger.Info("ClusterAgent closed")
	return nil
}
