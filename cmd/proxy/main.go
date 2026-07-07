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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/m-javani/cue-proxy/internal/app"
	"github.com/m-javani/cue-proxy/internal/cluster"
	"github.com/m-javani/cue-proxy/internal/config"
	"go.uber.org/zap"
)

var version = "0.2.0"

func main() {
	showVersion := flag.Bool("version", false, "Show version")
	showVersionShort := flag.Bool("v", false, "Show version (short)")
	configPath := flag.String("config", "", "Path to config file")
	apiHost := flag.String("api-host", "", "API server host (overrides config)")
	apiPort := flag.Int("api-port", 0, "API server port (overrides config)")
	authFile := flag.String("auth-file", "", "Auth file path (overrides config)")
	quicAddr := flag.String("quic-addr", "", "QUIC server address (overrides config)")
	quicPort := flag.Int("quic-port", 0, "QUIC server port (overrides config)")
	apiCertPath := flag.String("api-cert", "", "API TLS certificate path (overrides config)")
	apiKeyPath := flag.String("api-key", "", "API TLS key path (overrides config)")
	clusterCertPath := flag.String("cluster-cert", "", "Cluster TLS certificate path (overrides config)")
	clusterKeyPath := flag.String("cluster-key", "", "Cluster TLS key path (overrides config)")
	clusterCaPath := flag.String("cluster-ca", "", "Cluster CA certificate path (overrides config)")
	clusterSeeds := flag.String("seed", "", "Comma-separated cluster seed nodes (e.g., node1,node2,node3)")
	clusterApiPort := flag.Int("cluster-api-port", 0, "Cluster api port (overrides config)")
	proxyID := flag.String("proxy-id", "", "Proxy ID (REQUIRED, must match certificate identity)")
	discoveryYMLPath := flag.String("discovery-yml", "./discovery.yml", "Path to discovery.yml file containing initial cluster peer info")
	logLevel := flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Proxy Server v%s\n\n", version)
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s -proxy-id proxy1 -config config.yml\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -proxy-id node1 -api-port 8080 -quic-port 8322 -seed node2,node3\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -version\n", os.Args[0])
	}
	flag.Parse()

	if *proxyID == "" {
		fmt.Fprintf(os.Stderr, "ERROR: -proxy-id is required and must match the certificate identity\n")
		flag.Usage()
		os.Exit(1)
	}

	if *showVersion || *showVersionShort {
		fmt.Printf("cueproxy version %s\n", version)
		os.Exit(0)
	}

	cfg, logger, err := buildConfig(
		*configPath, *apiHost, *apiPort, *authFile,
		*quicAddr, *quicPort,
		*apiCertPath, *apiKeyPath,
		*clusterCertPath, *clusterKeyPath, *clusterCaPath,
		*clusterSeeds, *proxyID, *logLevel, *discoveryYMLPath, *clusterApiPort,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to build config: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		_ = logger.Sync()
	}()

	// --- Run Proxy (Blocking) ---
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	leaderAvailable := atomic.Bool{}
	leaderAvailable.Store(false)

	discovery, err := cluster.LoadDiscoveryFile(cfg.Cluster.DiscoveryYMLPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load initial discovery file: %v\n", err)
		os.Exit(1)
	}

	if err := app.RunProxy(ctx, cfg, logger, &leaderAvailable, discovery); err != nil {
		logger.Fatal("proxy runtime error", zap.Error(err))
	}
}

// buildConfig loads and merges configuration from file, CLI flags, and defaults.
// It returns the final Config and a Logger.
func buildConfig(
	configPath, apiHost string, apiPort int, authFile string,
	quicAddr string, quicPort int,
	apiCertPath, apiKeyPath,
	clusterCertPath, clusterKeyPath, clusterCaPath string,
	clusterSeeds string, proxyID string, logLevel string,
	discoveryYMLPath string, clusterApiPort int,
) (*config.Config, *zap.Logger, error) {
	// Setup logger
	var logger *zap.Logger
	var err error
	switch logLevel {
	case "debug":
		logger, _ = zap.NewDevelopment()
	default:
		logger, _ = zap.NewProduction()
	}

	// Load configuration
	var cfg *config.Config
	if configPath != "" {
		cfg, err = config.LoadConfig(configPath)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to load config: %w", err)
		}
	} else {
		cfg = config.DefaultConfig()
		logger.Info("using default configuration")
	}

	cfg.ProxyID = proxyID
	cfg.Cluster.DiscoveryYMLPath = discoveryYMLPath
	cfg.Cluster.ClusterApiPort = clusterApiPort

	if apiHost != "" {
		cfg.API.Host = apiHost
	}
	if apiPort > 0 {
		cfg.API.Port = apiPort
	}
	if authFile != "" {
		cfg.API.AuthPath = authFile
	}
	if quicAddr != "" {
		cfg.Cluster.QUICAddr = quicAddr
	}
	if quicPort > 0 {
		cfg.Cluster.QUICPort = quicPort
	}
	// API TLS overrides
	if apiCertPath != "" {
		cfg.API.CertPath = apiCertPath
		cfg.API.TLSEnabled = true // Enable TLS if cert is provided
	}
	if apiKeyPath != "" {
		cfg.API.KeyPath = apiKeyPath
		cfg.API.TLSEnabled = true // Enable TLS if key is provided
	}
	// Cluster TLS overrides
	if clusterCertPath != "" {
		cfg.Cluster.CertPath = clusterCertPath
	}
	if clusterKeyPath != "" {
		cfg.Cluster.KeyPath = clusterKeyPath
	}
	if clusterCaPath != "" {
		cfg.Cluster.CAPath = clusterCaPath
	}
	if clusterSeeds != "" {
		seeds := strings.Split(clusterSeeds, ",")
		cfg.Cluster.ClusterSeeds = seeds
	}

	logger.Info("configuration loaded",
		zap.String("proxy_id", cfg.ProxyID),
		zap.String("api_address", cfg.GetAPIAddress()),
		zap.String("quic_address", cfg.GetQUICAddress()),
		zap.Strings("cluster_seeds", cfg.Cluster.ClusterSeeds),
	)

	return cfg, logger, nil
}
