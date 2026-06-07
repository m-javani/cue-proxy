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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/quic-go/quic-go"
)

// createTransportConfig creates the QUIC transport configuration
func createTransportConfig() *quic.Config {
	// Heartbeat every 5s, idle timeout 30s — generous but not wasteful
	heartbeatInterval := 5 * time.Second
	idleTimeout := 30 * time.Second

	return &quic.Config{
		// Packet size: 1350 is standard and safe across networks
		InitialPacketSize: 1350,

		// ---- Flow Control (THE CRITICAL FIX) ----
		// Per-stream windows: 2MB initial, 8MB max
		// Connection-wide: 8MB initial, 32MB max
		// These numbers prevent stalls under burst load
		InitialStreamReceiveWindow:     2_000_000,  // 2 MB
		MaxStreamReceiveWindow:         8_000_000,  // 8 MB
		InitialConnectionReceiveWindow: 8_000_000,  // 8 MB
		MaxConnectionReceiveWindow:     32_000_000, // 32 MB

		// ---- Connection Limits ----
		// High TPS means many concurrent streams.
		// If each request maps to a stream, set this to your expected concurrency.
		MaxIncomingStreams:    10_000,
		MaxIncomingUniStreams: 0, // Set only if you use unidirectional streams

		// ---- Timeouts ----
		MaxIdleTimeout:       idleTimeout,
		HandshakeIdleTimeout: 10 * time.Second,
		KeepAlivePeriod:      heartbeatInterval,
		// Note: KeepAlivePeriod != 0 here because transport-level keepalive
		// is more reliable than application-level for connection liveness.

		// ---- 0-RTT ----
		// Enable if you have replay-safe idempotent operations
		Allow0RTT: false,

		// ---- Datagrams ----
		// Only enable if you actually use them
		EnableDatagrams: false,
	}
}

// loadClientTLSConfig loads and configures TLS with optional client CA verification
func loadClientTLSConfig(certPath, keyPath, caCertPath string) (*tls.Config, error) {
	// Load server certificate
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load keypair: %w", err)
	}

	// Load CA certificate (for verifying peers)
	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA cert: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	tlsConfig := &tls.Config{
		Certificates:           []tls.Certificate{cert},
		RootCAs:                caCertPool,
		MinVersion:             tls.VersionTLS13,
		SessionTicketsDisabled: true,
	}

	return tlsConfig, nil
}
