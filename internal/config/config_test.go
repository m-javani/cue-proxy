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
	"os"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	t.Run("happy_path_with_yaml", func(t *testing.T) {
		// Create a valid YAML config
		yamlContent := `
api:
  host: "127.0.0.1"
  port: 9090
  read_timeout_sec: 45
  write_timeout_sec: 0
  idle_timeout_sec: 300
  ws_read_timeout_sec: 0
  ws_write_timeout_sec: 30
  ws_read_limit_bytes: 32768
  default_max_inflights: 20
  auth_path: "custom/auth.yml"
  tls_enabled: true
  cert_path: "custom/api-cert.pem"
  key_path: "custom/api-key.pem"
  ca_path: "custom/api-ca.pem"

cluster:
  quic_addr: "127.0.0.1"
  quic_port: 9443
  cluster_seeds:
    - "seed1:8323"
    - "seed2:8323"
  cert_path: "custom/cluster-cert.pem"
  key_path: "custom/cluster-key.pem"
  ca_path: "custom/cluster-ca.pem"
  discovery_yml_path: "./discovery.yml"
`

		tmpfile, err := os.CreateTemp("", "config-*.yaml")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tmpfile.Name())

		if _, err := tmpfile.Write([]byte(yamlContent)); err != nil {
			t.Fatal(err)
		}
		tmpfile.Close()

		cfg, err := LoadConfig(tmpfile.Name())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify API values
		if cfg.API.Host != "127.0.0.1" {
			t.Errorf("expected API.Host '127.0.0.1', got '%s'", cfg.API.Host)
		}
		if cfg.API.Port != 9090 {
			t.Errorf("expected API.Port 9090, got %d", cfg.API.Port)
		}
		if cfg.API.ReadTimeoutSec != 45 {
			t.Errorf("expected ReadTimeoutSec 60, got %v", cfg.API.ReadTimeoutSec)
		}
		if cfg.API.DefaultMaxInflights != 20 {
			t.Errorf("expected DefaultMaxInflights 20, got %d", cfg.API.DefaultMaxInflights)
		}
		if cfg.API.AuthPath != "custom/auth.yml" {
			t.Errorf("expected AuthPath 'custom/auth.yml', got '%s'", cfg.API.AuthPath)
		}
		if !cfg.API.TLSEnabled {
			t.Error("expected API TLS to be enabled")
		}
		if cfg.API.CertPath != "custom/api-cert.pem" {
			t.Errorf("expected API CertPath 'custom/api-cert.pem', got '%s'", cfg.API.CertPath)
		}
		if cfg.API.KeyPath != "custom/api-key.pem" {
			t.Errorf("expected API KeyPath 'custom/api-key.pem', got '%s'", cfg.API.KeyPath)
		}

		// Verify Cluster values
		if cfg.Cluster.QUICPort != 9443 {
			t.Errorf("expected QUICPort 9443, got %d", cfg.Cluster.QUICPort)
		}
		if len(cfg.Cluster.ClusterSeeds) != 2 {
			t.Errorf("expected 2 cluster seeds, got %d", len(cfg.Cluster.ClusterSeeds))
		}
		if cfg.Cluster.CertPath != "custom/cluster-cert.pem" {
			t.Errorf("expected Cluster CertPath 'custom/cluster-cert.pem', got '%s'", cfg.Cluster.CertPath)
		}
		if cfg.Cluster.KeyPath != "custom/cluster-key.pem" {
			t.Errorf("expected Cluster KeyPath 'custom/cluster-key.pem', got '%s'", cfg.Cluster.KeyPath)
		}
		if cfg.Cluster.CAPath != "custom/cluster-ca.pem" {
			t.Errorf("expected Cluster CAPath 'custom/cluster-ca.pem', got '%s'", cfg.Cluster.CAPath)
		}

	})

	t.Run("config_file_not_found_uses_defaults", func(t *testing.T) {
		cfg, err := LoadConfig("")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg == nil {
			t.Fatal("expected non-nil config")
		}

		// Should have default values
		if cfg.API.Port != 8080 {
			t.Errorf("expected default API.Port 8080, got %d", cfg.API.Port)
		}
		if cfg.API.TLSEnabled != false {
			t.Error("expected API TLS to be disabled by default")
		}
		if cfg.Cluster.QUICPort != 8323 {
			t.Errorf("expected default QUICPort 8323, got %d", cfg.Cluster.QUICPort)
		}
		if cfg.Cluster.CertPath != "certs/cluster-cert.pem" {
			t.Errorf("expected default cluster cert path 'certs/cluster-cert.pem', got '%s'", cfg.Cluster.CertPath)
		}
	})

	t.Run("invalid_yaml_returns_error", func(t *testing.T) {
		tmpfile, err := os.CreateTemp("", "config-*.yaml")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tmpfile.Name())

		invalidYAML := `
api:
  port: "not-an-int"  # This should cause unmarshal error
`
		if _, err := tmpfile.Write([]byte(invalidYAML)); err != nil {
			t.Fatal(err)
		}
		tmpfile.Close()

		_, err = LoadConfig(tmpfile.Name())
		if err == nil {
			t.Error("expected error for invalid YAML, got nil")
		}
	})
}

func TestValidate(t *testing.T) {
	t.Run("all_validation_errors_detected", func(t *testing.T) {
		c := &Config{
			API: APIConfig{
				Port:                0,
				DefaultMaxInflights: 0,
				TLSEnabled:          true,
				CertPath:            "", // Missing cert when enabled
				KeyPath:             "", // Missing key when enabled
			},
			Cluster: ClusterConfig{
				QUICPort:         0,
				CertPath:         "", // Missing cert
				KeyPath:          "", // Missing key
				DiscoveryYMLPath: "",
			},
		}

		err := c.Validate()
		if err == nil {
			t.Fatal("expected validation errors, got nil")
		}

		errMsg := err.Error()
		expectedErrors := []string{
			"invalid api.port: 0",
			"invalid cluster.quic_port: 0",
			"invalid default_max_inflights: 0",
			"api.cert_path is required when tls_enabled is true",
			"api.key_path is required when tls_enabled is true",
			"cluster.cert_path is required",
			"cluster.key_path is required",
			"cluster.discovery_yml_path is required",
		}

		for _, expected := range expectedErrors {
			if !contains(errMsg, expected) {
				t.Errorf("expected error message to contain %q, got %q", expected, errMsg)
			}
		}
	})

	t.Run("valid_config_passes_validation", func(t *testing.T) {
		c := DefaultConfig()
		if err := c.Validate(); err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("api_tls_disabled_skips_validation", func(t *testing.T) {
		c := DefaultConfig()
		c.API.TLSEnabled = false
		c.API.CertPath = "" // Empty but TLS disabled, so should pass
		if err := c.Validate(); err != nil {
			t.Errorf("expected no error when TLS disabled, got %v", err)
		}
	})

	t.Run("validation_individual_cases", func(t *testing.T) {
		testCases := []struct {
			name   string
			modify func(*Config)
		}{
			{"invalid_api_port", func(c *Config) { c.API.Port = 0 }},
			{"invalid_quic_port", func(c *Config) { c.Cluster.QUICPort = 0 }},
			{"invalid_max_inflights", func(c *Config) { c.API.DefaultMaxInflights = 0 }},
			{"api_tls_enabled_missing_cert", func(c *Config) {
				c.API.TLSEnabled = true
				c.API.CertPath = ""
			}},
			{"api_tls_enabled_missing_key", func(c *Config) {
				c.API.TLSEnabled = true
				c.API.KeyPath = ""
			}},
			{"cluster_missing_cert", func(c *Config) {
				c.Cluster.CertPath = ""
			}},
			{"cluster_missing_key", func(c *Config) {
				c.Cluster.KeyPath = ""
			}},
			{"cluster_missing_discovery", func(c *Config) {
				c.Cluster.DiscoveryYMLPath = ""
			}},
			{"cluster_missing_discovery", func(c *Config) {
				c.Cluster.DiscoveryYMLPath = ""
			}},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				c := DefaultConfig()
				tc.modify(c)
				if err := c.Validate(); err == nil {
					t.Error("expected validation error")
				}
			})
		}
	})

}

func TestGetAddressFunctions(t *testing.T) {
	c := &Config{
		API: APIConfig{
			Host: "192.168.1.100",
			Port: 9999,
		},
		Cluster: ClusterConfig{
			QUICAddr: "192.168.1.101",
			QUICPort: 8888,
		},
	}

	if addr := c.GetAPIAddress(); addr != "192.168.1.100:9999" {
		t.Errorf("expected '192.168.1.100:9999', got '%s'", addr)
	}

	if addr := c.GetQUICAddress(); addr != "192.168.1.101:8888" {
		t.Errorf("expected '192.168.1.101:8888', got '%s'", addr)
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	// Spot check some defaults
	if cfg.API.Host != "0.0.0.0" {
		t.Errorf("expected API.Host '0.0.0.0', got '%s'", cfg.API.Host)
	}
	if cfg.API.DefaultMaxInflights != 10 {
		t.Errorf("expected DefaultMaxInflights 10, got %d", cfg.API.DefaultMaxInflights)
	}
	if cfg.API.TLSEnabled != false {
		t.Error("expected API TLS to be disabled by default")
	}
	if cfg.API.CertPath != "certs/api-cert.pem" {
		t.Errorf("expected API CertPath 'certs/api-cert.pem', got '%s'", cfg.API.CertPath)
	}
	if cfg.Cluster.QUICPort != 8323 {
		t.Errorf("expected QUICPort 8323, got %d", cfg.Cluster.QUICPort)
	}
	if cfg.Cluster.CertPath != "certs/cluster-cert.pem" {
		t.Errorf("expected cluster cert path 'certs/cluster-cert.pem', got '%s'", cfg.Cluster.CertPath)
	}
	if cfg.Cluster.DiscoveryYMLPath != "./discovery.yml" {
		t.Errorf("expected discovery path './discovery.yml', got '%s'", cfg.Cluster.DiscoveryYMLPath)
	}

	if cfg.API.AuthPath != "./auth.yml" {
		t.Errorf("expected AuthPath './auth.yml', got '%s'", cfg.API.AuthPath)
	}
}

// Helper function
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
