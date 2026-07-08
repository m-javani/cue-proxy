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

type DiscoveryKind uint8

const (
	DiscoveryKindStatic DiscoveryKind = iota
	DiscoveryKindHttp
)

// String returns the string representation of DiscoveryKind
func (d DiscoveryKind) String() string {
	switch d {
	case DiscoveryKindStatic:
		return "static"
	case DiscoveryKindHttp:
		return "http"
	default:
		return "unknown"
	}
}

// ParseDiscoveryKind converts a string to DiscoveryKind
func ParseDiscoveryKind(s string) (DiscoveryKind, error) {
	switch strings.ToLower(s) {
	case "static":
		return DiscoveryKindStatic, nil
	case "http":
		return DiscoveryKindHttp, nil
	default:
		return DiscoveryKindStatic, fmt.Errorf("unknown discovery kind: %s", s)
	}
}

type ClusterConfig struct {
	// QUIC server (this proxy listens on)
	QUICAddr string `mapstructure:"quic_addr"`
	QUICPort int    `mapstructure:"quic_port"`

	// TLS settings for Cluster (QUIC)
	CertPath string `mapstructure:"cert_path"`
	KeyPath  string `mapstructure:"key_path"`
	CAPath   string `mapstructure:"ca_path"`

	DiscoveryKind     string `mapstructure:"discovery_kind"`
	DiscoveryYMLPath  string `mapstructure:"discovery_yml_path"`  // Required for DiscoveryKindStatic
	DiscoveryHTTPHost string `mapstructure:"discovery_http_host"` // Required for DiscoveryKindHttp

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
			QUICAddr:         "0.0.0.0",
			QUICPort:         8323,
			CertPath:         "certs/cluster-cert.pem",
			KeyPath:          "certs/cluster-key.pem",
			CAPath:           "certs/cluster-ca.pem",
			DiscoveryKind:    "static",
			DiscoveryYMLPath: "./discovery.yml",
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
	v.SetDefault("cluster.quic_port", 8323)
	v.SetDefault("cluster.cluster_seeds", []string{})
	v.SetDefault("cluster.cert_path", "certs/cluster-cert.pem")
	v.SetDefault("cluster.key_path", "certs/cluster-key.pem")
	v.SetDefault("cluster.ca_path", "certs/cluster-ca.pem")
	v.SetDefault("cluster.discovery_yml_path", "./discovery.yml")
	v.SetDefault("cluster.discovery_kind", "static")
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

	kind, _ := ParseDiscoveryKind(c.Cluster.DiscoveryKind)
	if kind == DiscoveryKindStatic && c.Cluster.DiscoveryYMLPath == "" {
		errs = append(errs, "cluster.discovery_yml_path is required for static discovery")
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

	// Validate discovery configuration
	if err := validateDiscoveryConfig(&c.Cluster); err != nil {
		errs = append(errs, err.Error())
	}

	if len(errs) > 0 {
		return fmt.Errorf("config validation failed:\n- %s", strings.Join(errs, "\n- "))
	}
	return nil
}

// validateDiscoveryConfig validates the discovery configuration
func validateDiscoveryConfig(cfg *ClusterConfig) error {
	kind, err := ParseDiscoveryKind(cfg.DiscoveryKind)
	if err != nil {
		return fmt.Errorf("invalid discovery_kind '%s': %w", cfg.DiscoveryKind, err)
	}

	switch kind {
	case DiscoveryKindStatic:
		if strings.TrimSpace(cfg.DiscoveryYMLPath) == "" {
			return fmt.Errorf("discovery_yml_path is required when discovery_kind=static")
		}

	case DiscoveryKindHttp:
		if strings.TrimSpace(cfg.DiscoveryHTTPHost) == "" {
			return fmt.Errorf("discovery_http_host is required when discovery_kind=http")
		}

	default:
		return fmt.Errorf("unsupported discovery kind: %s", cfg.DiscoveryKind)
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
