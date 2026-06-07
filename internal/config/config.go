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

package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	// Proxy identification
	ProxyID string `mapstructure:"-"`

	// API Server
	API APIConfig `mapstructure:"api"`

	// Cluster Agent
	Cluster ClusterConfig `mapstructure:"cluster"`
}

type APIConfig struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`

	// Timeouts
	ReadTimeoutSec    int64 `mapstructure:"read_timeout_sec"`
	WriteTimeoutSec   int64 `mapstructure:"write_timeout_sec"`
	IdleTimeoutSec    int64 `mapstructure:"idle_timeout_sec"`
	WSReadTimeoutSec  int64 `mapstructure:"ws_read_timeout_sec"`
	WSWriteTimeoutSec int64 `mapstructure:"ws_write_timeout_sec"`
	WSReadLimitBytes  int64 `mapstructure:"ws_read_limit_bytes"`

	// Default max inflights per consumer
	DefaultMaxInflights int `mapstructure:"default_max_inflights"`

	// Path to auth.yml file
	AuthPath string `mapstructure:"auth_path"`

	// TLS settings for API (HTTP/WebSocket)
	TLSEnabled bool   `mapstructure:"tls_enabled"`
	CertPath   string `mapstructure:"cert_path"`
	KeyPath    string `mapstructure:"key_path"`
}

type ClusterConfig struct {
	// QUIC server (this proxy listens on)
	QUICAddr string `mapstructure:"quic_addr"`
	QUICPort int    `mapstructure:"quic_port"`

	// Cluster seeds (other proxy nodes for discovery)
	ClusterSeeds []string `mapstructure:"cluster_seeds"`

	// TLS settings for Cluster (QUIC)
	CertPath string `mapstructure:"cert_path"`
	KeyPath  string `mapstructure:"key_path"`
	CAPath   string `mapstructure:"ca_path"`

	AddressResolver ResolverConfig `mapstructure:"address_resolver"`
	TLSVerifier     VerifierConfig `mapstructure:"tls_verifier"`
}

type ResolverConfig struct {
	Type   string         `mapstructure:"type"` // "service", "dns", "static"
	Config map[string]any `mapstructure:"config"`
}

type VerifierConfig struct {
	Type   string         `mapstructure:"type"` // "dns", "cn", "spiffe"
	Config map[string]any `mapstructure:"config"`
}

// Default values
func DefaultConfig() *Config {
	return &Config{
		ProxyID: "", // Will be auto-generated if empty
		API: APIConfig{
			Host:                "0.0.0.0",
			Port:                8080,
			ReadTimeoutSec:      45,
			WriteTimeoutSec:     0,
			IdleTimeoutSec:      300,
			DefaultMaxInflights: 10,
			AuthPath:            "./auth.yml",
			TLSEnabled:          false,
			CertPath:            "certs/api-cert.pem",
			KeyPath:             "certs/api-key.pem",
			WSReadTimeoutSec:    0,
			WSWriteTimeoutSec:   30,
			WSReadLimitBytes:    32768, // 32KB
		},
		Cluster: ClusterConfig{
			QUICAddr:     "0.0.0.0",
			QUICPort:     8443,
			ClusterSeeds: []string{},
			CertPath:     "certs/cluster-cert.pem",
			KeyPath:      "certs/cluster-key.pem",
			CAPath:       "certs/cluster-ca.pem",
			AddressResolver: ResolverConfig{
				Type:   "service",
				Config: map[string]any{},
			},
			TLSVerifier: VerifierConfig{
				Type:   "cn",
				Config: map[string]any{},
			},
		},
	}
}

// LoadConfig loads configuration from file and binds CLI flags
func LoadConfig(configPath string) (*Config, error) {
	v := viper.New()

	// Set defaults
	setDefaults(v)

	// Read config file
	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("./config")
		v.AddConfigPath("/etc/proxy/")
	}

	// Environment variable support
	v.SetEnvPrefix("PROXY")
	v.AutomaticEnv()

	// Read config file
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, err
		}
		// Config file not found, use defaults + env + flags
	}

	// Unmarshal config
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	// Validate
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
	// API defaults
	v.SetDefault("api.host", "0.0.0.0")
	v.SetDefault("api.port", 8080)
	v.SetDefault("api.read_timeout_sec", 30)
	v.SetDefault("api.write_timeout_sec", 0)
	v.SetDefault("api.idle_timeout_sec", 300)
	v.SetDefault("api.ws_read_timeout_sec", 0)
	v.SetDefault("api.ws_write_timeout_sec", 30)
	v.SetDefault("api.ws_read_limit_bytes", 32768) // 32KB
	v.SetDefault("api.default_max_inflights", 10)
	v.SetDefault("api.auth_path", "./auth.yml")
	v.SetDefault("api.tls_enabled", false)
	v.SetDefault("api.cert_path", "certs/api-cert.pem")
	v.SetDefault("api.key_path", "certs/api-key.pem")
	v.SetDefault("api.ca_path", "certs/api-ca.pem")

	// Cluster defaults
	v.SetDefault("cluster.quic_addr", "0.0.0.0")
	v.SetDefault("cluster.quic_port", 8443)
	v.SetDefault("cluster.cluster_seeds", []string{})
	v.SetDefault("cluster.cert_path", "certs/cluster-cert.pem")
	v.SetDefault("cluster.key_path", "certs/cluster-key.pem")
	v.SetDefault("cluster.ca_path", "certs/cluster-ca.pem")
	v.SetDefault("cluster.address_resolver.type", "service")
	v.SetDefault("cluster.address_resolver.config", map[string]any{})
	v.SetDefault("cluster.tls_verifier.type", "cn")
	v.SetDefault("cluster.tls_verifier.config", map[string]any{})
}

// Validate returns all validation errors
func (c *Config) Validate() error {
	var errs []string

	if c.API.Port < 1 {
		errs = append(errs, fmt.Sprintf("invalid api.port: %d", c.API.Port))
	}
	if c.Cluster.QUICPort < 1 {
		errs = append(errs, fmt.Sprintf("invalid cluster.quic_port: %d", c.Cluster.QUICPort))
	}
	if c.API.DefaultMaxInflights < 1 {
		errs = append(errs, fmt.Sprintf("invalid default_max_inflights: %d", c.API.DefaultMaxInflights))
	}

	// Validate API TLS if enabled
	if c.API.TLSEnabled {
		if c.API.CertPath == "" {
			errs = append(errs, "api.cert_path is required when tls_enabled is true")
		}
		if c.API.KeyPath == "" {
			errs = append(errs, "api.key_path is required when tls_enabled is true")
		}
	}

	// Validate Cluster TLS (always required for QUIC)
	if c.Cluster.CertPath == "" {
		errs = append(errs, "cluster.cert_path is required")
	}
	if c.Cluster.KeyPath == "" {
		errs = append(errs, "cluster.key_path is required")
	}

	// Resolver validation
	if c.Cluster.AddressResolver.Type == "" {
		errs = append(errs, "cluster.address_resolver.type is required")
	} else {
		validResolvers := map[string]bool{"service": true, "dns": true, "static": true}
		if !validResolvers[c.Cluster.AddressResolver.Type] {
			errs = append(errs, fmt.Sprintf("unknown address_resolver type: %q", c.Cluster.AddressResolver.Type))
		} else {
			// Validate required config for each resolver type
			switch c.Cluster.AddressResolver.Type {
			case "dns":
				if _, ok := c.Cluster.AddressResolver.Config["domain"]; !ok {
					errs = append(errs, "dns resolver requires 'domain' in address_resolver.config")
				}
			case "static":
				if _, ok := c.Cluster.AddressResolver.Config["peers"]; !ok {
					errs = append(errs, "static resolver requires 'peers' in address_resolver.config")
				}
			case "service":
				// No required config, port is optional
			}
		}
	}

	// TLS Verifier validation
	if c.Cluster.TLSVerifier.Type == "" {
		errs = append(errs, "cluster.tls_verifier.type is required")
	} else {
		validVerifiers := map[string]bool{"dns": true, "cn": true, "spiffe": true}
		if !validVerifiers[c.Cluster.TLSVerifier.Type] {
			errs = append(errs, fmt.Sprintf("unknown tls_verifier type: %q", c.Cluster.TLSVerifier.Type))
		} else {
			// Validate required config for each verifier type
			switch c.Cluster.TLSVerifier.Type {
			case "dns":
				if _, ok := c.Cluster.TLSVerifier.Config["domain"]; !ok {
					errs = append(errs, "dns verifier requires 'domain' in tls_verifier.config")
				}
			case "spiffe":
				if _, ok := c.Cluster.TLSVerifier.Config["trust_domain"]; !ok {
					errs = append(errs, "spiffe verifier requires 'trust_domain' in tls_verifier.config")
				}
			case "cn":
				// No config needed
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("config validation failed:\n- %s", strings.Join(errs, "\n- "))
	}
	return nil
}

// GetAPIAddress returns the full API server address
func (c *Config) GetAPIAddress() string {
	return fmt.Sprintf("%s:%d", c.API.Host, c.API.Port)
}

// GetQUICAddress returns the full QUIC server address
func (c *Config) GetQUICAddress() string {
	return fmt.Sprintf("%s:%d", c.Cluster.QUICAddr, c.Cluster.QUICPort)
}
