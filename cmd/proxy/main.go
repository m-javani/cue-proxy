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

	"github.com/google/uuid"
	"github.com/m-javani/cue-proxy/internal/app"
	"github.com/m-javani/cue-proxy/internal/config"
	"github.com/m-javani/cue/pkg/discovery"
	"github.com/m-javani/cue/pkg/verifier"
	"go.uber.org/zap"
)

var version = "1.0"

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
	proxyID := flag.String("proxy-id", "", "Proxy ID (auto-generated if empty)")
	logLevel := flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Proxy Server v%s\n\n", version)
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s -config config.yml\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -api-port 8080 -quic-port 8322 -seed node1,node2,node3\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -version\n", os.Args[0])
	}
	flag.Parse()

	if *showVersion || *showVersionShort {
		fmt.Printf("cueproxy version %s\n", version)
		os.Exit(0)
	}

	cfg, logger, err := buildConfig(
		*configPath, *apiHost, *apiPort, *authFile,
		*quicAddr, *quicPort,
		*apiCertPath, *apiKeyPath,
		*clusterCertPath, *clusterKeyPath, *clusterCaPath,
		*clusterSeeds, *proxyID, *logLevel,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to build config: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// --- Run Proxy (Blocking) ---
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	clVerif, err := NewTLSVerifier(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to build tls verifier: %v\n", err)
		os.Exit(1)
	}
	addrResolver, err := NewAddressResolver(*cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to build address resolver: %v\n", err)
		os.Exit(1)
	}

	leaderAvailable := atomic.Bool{}
	leaderAvailable.Store(false)

	if err := app.RunProxy(ctx, cfg, logger, addrResolver, clVerif, &leaderAvailable); err != nil {
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

	// Override with CLI flags
	if proxyID != "" {
		cfg.ProxyID = proxyID
	}
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

	// Generate proxy ID if still empty
	if cfg.ProxyID == "" {
		cfg.ProxyID = uuid.New().String()[:8]
		logger.Info("auto-generated proxy ID", zap.String("proxy_id", cfg.ProxyID))
	}

	logger.Info("configuration loaded",
		zap.String("proxy_id", cfg.ProxyID),
		zap.String("api_address", cfg.GetAPIAddress()),
		zap.String("quic_address", cfg.GetQUICAddress()),
		zap.Strings("cluster_seeds", cfg.Cluster.ClusterSeeds),
	)

	return cfg, logger, nil
}

func NewAddressResolver(cfg config.Config) (discovery.AddressResolver, error) {
	var resolver discovery.AddressResolver

	switch cfg.Cluster.AddressResolver.Type {
	case "service":
		port, ok := cfg.Cluster.AddressResolver.Config["port"].(int)
		if !ok {
			port = int(cfg.Cluster.QUICPort) // fallback to cluster port
		}
		resolver = discovery.NewServiceResolver(port)

	case "dns":
		domain, ok := cfg.Cluster.AddressResolver.Config["domain"].(string)
		if !ok {
			return nil, fmt.Errorf("dns resolver requires 'domain' config field")
		}
		port, ok := cfg.Cluster.AddressResolver.Config["port"].(int)
		if !ok {
			port = int(cfg.Cluster.QUICPort) // fallback to cluster port
		}
		resolver = discovery.NewDNSResolver(port, domain)

	case "static":
		peersRaw, ok := cfg.Cluster.AddressResolver.Config["peers"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("static resolver requires 'peers' config map")
		}
		peers := make(map[string]string)
		for k, v := range peersRaw {
			peers[k] = v.(string)
		}
		resolver = discovery.StaticResolver{
			Addresses: peers,
		}

	default:
		return nil, fmt.Errorf("unknown address resolver type: %q", cfg.Cluster.AddressResolver.Type)
	}

	return resolver, nil
}

func NewTLSVerifier(cfg *config.Config) (verifier.TLSVerifier, error) {
	var tlsVerifier verifier.TLSVerifier

	switch cfg.Cluster.TLSVerifier.Type {
	case "dns":
		domain, ok := cfg.Cluster.TLSVerifier.Config["domain"].(string)
		if !ok {
			return nil, fmt.Errorf("dns verifier requires 'domain' config field")
		}
		tlsVerifier = verifier.DNSVerifier{
			Domain: domain,
		}

	case "cn":
		// No config needed
		tlsVerifier = verifier.CNVerifier{}

	case "spiffe":
		trustDomain, ok := cfg.Cluster.TLSVerifier.Config["trust_domain"].(string)
		if !ok {
			return nil, fmt.Errorf("spiffe verifier requires 'trust_domain' config field")
		}
		namespace, _ := cfg.Cluster.TLSVerifier.Config["namespace"].(string) // optional
		tlsVerifier = verifier.SPIFFEVerifier{
			TrustDomain: trustDomain,
			Namespace:   namespace,
		}

	default:
		return nil, fmt.Errorf("unknown TLS verifier type: %q", cfg.Cluster.TLSVerifier.Type)
	}

	return tlsVerifier, nil
}
