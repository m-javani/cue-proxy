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

package app

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/m-javani/cue-proxy/internal"
	"github.com/m-javani/cue-proxy/internal/api"
	"github.com/m-javani/cue-proxy/internal/cluster"
	"github.com/m-javani/cue-proxy/internal/config"
	"github.com/m-javani/cue-proxy/internal/model"
	"github.com/m-javani/cue/pkg/discovery"
	"github.com/m-javani/cue/pkg/verifier"
	"go.uber.org/zap"
)

const ProducerReqBufferSize int = 1000

// RunProxy starts the proxy with the given configuration and logger.
// It blocks until the context is cancelled or a fatal error occurs.
func RunProxy(ctx context.Context, cfg *config.Config, logger *zap.Logger, addressResolver discovery.AddressResolver, tlsVerifier verifier.TLSVerifier, leaderAvailable *atomic.Bool) error {

	// Create communication channels
	producerCh := make(chan model.ProxyRequestWithRespCh, ProducerReqBufferSize)

	// Create authenticator
	auth, err := api.NewAuthenticator(cfg.API.AuthPath, logger)
	if err != nil {
		return fmt.Errorf("failed to create authenticator: %w", err)
	}
	defer auth.Close()

	metrics := internal.GetApiMetrics()
	router := api.NewDefaultRouter(cfg.ProxyID, logger, metrics)

	// Create Proxy API
	proxyAPI := api.NewProxyApi(
		cfg.ProxyID,
		logger,
		auth,
		router,
		producerCh,
		&cfg.API,
		fmt.Sprintf("%s:%d", cfg.API.Host, cfg.API.Port),
		ctx,
		leaderAvailable,
		metrics,
	)

	// Create Cluster Agent
	clusterAgent, err := cluster.NewClusterAgent(
		cfg.ProxyID,
		cfg.Cluster.QUICAddr,
		uint16(cfg.Cluster.QUICPort),
		cfg.Cluster.CertPath,
		cfg.Cluster.KeyPath,
		cfg.Cluster.CAPath,
		producerCh,
		router,
		cfg.Cluster.ClusterSeeds,
		logger,
		addressResolver,
		tlsVerifier,
		leaderAvailable,
	)
	if err != nil {
		return fmt.Errorf("failed to create cluster agent: %w", err)
	}

	// Start components
	go clusterAgent.Run()

	// Start proxy and run HTTP server
	go func() {
		if err := proxyAPI.Start(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server failed", zap.Error(err))
		}
	}()

	// Wait for shutdown signal
	<-ctx.Done()
	logger.Info("shutdown signal received, gracefully stopping...")

	// Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	proxyAPI.Stop(shutdownCtx)

	if err := clusterAgent.Close(); err != nil {
		logger.Error("cluster agent close error", zap.Error(err))
	}

	logger.Info("proxy stopped")
	return nil
}
