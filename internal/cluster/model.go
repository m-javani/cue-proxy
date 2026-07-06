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
	"fmt"
	"net"
	"strings"
	"time"
)

// Handshake models (for proxy → Cue)
type ConnectionType string

const (
	ConnectionTypeInbound  ConnectionType = "inbound"
	ConnectionTypeOutbound ConnectionType = "outbound"
)

type Handshake struct {
	ProxyID         string         `msgpack:"proxy_id"`
	ConnectionType  ConnectionType `msgpack:"connection_type"`
	ProtocolVersion int            `msgpack:"protocol_version"`
}

type HandshakeResponse struct {
	Status  string `msgpack:"status"`
	Message string `msgpack:"message,omitempty"`
	NodeID  string `msgpack:"node_id"`
}

// ============

type CueTopology struct {
	Leader         string
	Term           uint64
	Voters         []string
	Learners       []string
	LastUpdate     time.Time
	NodeHeartbeats map[string]*NodeHeartbeatInfo
}

type NodeHeartbeatInfo struct {
	ClaimedLeader string
	Term          uint64
	LastSeen      time.Time
	Status        string
}

// --------------------------------
// Discovery
// --------------------------------

type IdentityKind uint8

const (
	IdentityDNS IdentityKind = iota
	IdentityIP
	IdentitySPIFFE
)

var identityKindStrings = map[IdentityKind]string{
	IdentityDNS:    "dns",
	IdentityIP:     "ip",
	IdentitySPIFFE: "spiffe",
}

var stringToIdentityKind = map[string]IdentityKind{
	"dns":    IdentityDNS,
	"ip":     IdentityIP,
	"spiffe": IdentitySPIFFE,
}

func (k IdentityKind) String() string {
	if s, ok := identityKindStrings[k]; ok {
		return s
	}
	return fmt.Sprintf("unknown(%d)", k)
}

func (k IdentityKind) MarshalText() ([]byte, error) {
	return []byte(k.String()), nil
}

func (k *IdentityKind) UnmarshalText(text []byte) error {
	s := strings.ToLower(strings.TrimSpace(string(text)))
	if kind, ok := stringToIdentityKind[s]; ok {
		*k = kind
		return nil
	}
	return fmt.Errorf("unknown identity kind: %s", s)
}

type TLSIdentity struct {
	Kind  IdentityKind `msgpack:"kind" json:"kind" yaml:"kind"`
	Value string       `msgpack:"value" json:"value" yaml:"value"`
}

func (i TLSIdentity) String() string {
	switch i.Kind {
	case IdentityDNS:
		return fmt.Sprintf("DNS:%s", i.Value)
	case IdentityIP:
		return fmt.Sprintf("IP:%s", i.Value)
	case IdentitySPIFFE:
		return fmt.Sprintf("SPIFFE:%s", i.Value)
	default:
		return fmt.Sprintf("Unknown:%s", i.Value)
	}
}

func (i TLSIdentity) Validate() error {
	if strings.TrimSpace(i.Value) == "" {
		return fmt.Errorf("identity value is required")
	}

	switch i.Kind {
	case IdentityDNS, IdentityIP, IdentitySPIFFE:
		// valid
	default:
		return fmt.Errorf("unknown identity kind: %s (valid: dns, ip, spiffe)", i.Kind)
	}

	// Kind-specific validation
	switch i.Kind {
	case IdentityDNS:
		if len(i.Value) > 253 {
			return fmt.Errorf("DNS name too long (max 253 characters)")
		}
	case IdentityIP:
		if net.ParseIP(i.Value) == nil {
			return fmt.Errorf("invalid IP address in identity: %s", i.Value)
		}
	case IdentitySPIFFE:
		if !strings.HasPrefix(strings.ToLower(i.Value), "spiffe://") {
			return fmt.Errorf("SPIFFE identity must start with spiffe://")
		}
	}

	return nil
}

type PeerInfo struct {
	NodeID   string      `msgpack:"node_id" json:"node_id" yaml:"node_id"`
	IP       string      `msgpack:"ip" json:"ip" yaml:"ip"`
	Identity TLSIdentity `msgpack:"identity" json:"identity" yaml:"identity"`
	Port     uint16      `msgpack:"port" json:"port" yaml:"port"`
}

func (p PeerInfo) String() string {
	return fmt.Sprintf("NodeID:%s, IP:%s, Port:%d, Identity:%s", p.NodeID, p.IP, p.Port, p.Identity.String())
}

func (p PeerInfo) Validate() error {
	// NodeID
	if strings.TrimSpace(p.NodeID) == "" {
		return fmt.Errorf("node_id is required")
	}
	if len(p.NodeID) > 64 {
		return fmt.Errorf("node_id too long (max 64 characters)")
	}

	// IP address (using stdlib net.ParseIP)
	if strings.TrimSpace(p.IP) == "" {
		return fmt.Errorf("ip is required")
	}
	if net.ParseIP(p.IP) == nil {
		return fmt.Errorf("invalid IP address: %s", p.IP)
	}

	// Identity
	if err := p.Identity.Validate(); err != nil {
		return fmt.Errorf("invalid identity: %w", err)
	}

	return nil
}
