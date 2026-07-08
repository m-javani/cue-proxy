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
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"time"

	"github.com/m-javani/cue-proxy/internal/model"
	"github.com/quic-go/quic-go"
	"github.com/vmihailenco/msgpack/v5"
	"go.uber.org/zap"
)

func (a *ClusterAgent) dialSendConnection(peer PeerInfo) error {

	conn, err := a.dialWithHandshake(ConnectionTypeInbound, peer)
	if err != nil {
		return fmt.Errorf("dial to %s failed: %w", peer.NodeID, err)
	}

	a.mu.Lock()
	a.sendConns[peer.NodeID] = conn
	a.mu.Unlock()

	return nil
}

func (a *ClusterAgent) dialRecvConnection(peer PeerInfo) error {

	conn, err := a.dialWithHandshake(ConnectionTypeOutbound, peer)
	if err != nil {
		return fmt.Errorf("dial to %s failed: %w", peer.NodeID, err)
	}

	a.mu.Lock()
	a.recvConns[peer.NodeID] = conn
	a.mu.Unlock()

	go a.handleRecvConnection(peer.NodeID, conn)
	return nil
}

func (s *ClusterAgent) VerifyTLSIdentity(cert *x509.Certificate, expected TLSIdentity) error {
	switch expected.Kind {
	case IdentityDNS:
		if slices.Contains(cert.DNSNames, expected.Value) {
			return nil
		}
		return fmt.Errorf("certificate does not contain expected DNS SAN: %s. cert.DNSNames: %+v", expected.Value, cert.DNSNames)

	case IdentityIP:
		expectedIP := net.ParseIP(expected.Value)
		if expectedIP == nil {
			return fmt.Errorf("invalid IP in expected identity: %s", expected.Value)
		}
		for _, ip := range cert.IPAddresses {
			if ip.Equal(expectedIP) {
				return nil
			}
		}
		return fmt.Errorf("certificate does not contain expected IP SAN: %s", expected.Value)

	case IdentitySPIFFE:
		for _, uri := range cert.URIs {
			if uri.Scheme == "spiffe" && uri.String() == expected.Value {
				return nil
			}
		}
		return fmt.Errorf("certificate does not contain expected SPIFFE URI: %s", expected.Value)

	default:
		return fmt.Errorf("unsupported identity kind: %d", expected.Kind)
	}
}

func (a *ClusterAgent) dialWithHandshake(connType ConnectionType, peer PeerInfo) (*quic.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tlsConfig := a.clientTLSConfig.Clone()
	host := peer.Host
	if host == "" {
		host = peer.NodeID
	}
	remoteAddr := net.JoinHostPort(host, strconv.Itoa(int(peer.Port)))

	tlsConfig.VerifyConnection = func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return fmt.Errorf("no peer certificate")
		}
		leaf := cs.PeerCertificates[0]

		intermediates := x509.NewCertPool()
		for _, cert := range cs.PeerCertificates[1:] {
			intermediates.AddCert(cert)
		}

		// 1. Full chain validation
		if _, err := leaf.Verify(x509.VerifyOptions{
			Roots:         tlsConfig.RootCAs,
			Intermediates: intermediates,
		}); err != nil {
			return fmt.Errorf("certificate chain verification failed: %w", err)
		}

		return nil
	}

	conn, err := quic.DialAddr(ctx, remoteAddr, tlsConfig, a.transportConfig)
	if err != nil {
		return nil, err
	}

	cs := conn.ConnectionState()
	leaf := cs.TLS.PeerCertificates[0]
	if err := a.VerifyTLSIdentity(leaf, peer.Identity); err != nil {
		return nil, err
	}

	// Handshake
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		_ = conn.CloseWithError(0, "handshake stream failed")
		return nil, err
	}
	defer stream.Close()

	hs := Handshake{
		ProxyID:         a.proxyID,
		ConnectionType:  connType,
		ProtocolVersion: protocolVersion,
	}

	if err := msgpack.NewEncoder(stream).Encode(hs); err != nil {
		return nil, err
	}

	var resp HandshakeResponse
	if err := msgpack.NewDecoder(stream).Decode(&resp); err != nil {
		return nil, err
	}

	if resp.NodeID == "" {
		return nil, fmt.Errorf("handshake failed: %s", resp.Message)
	}
	// if resp.Status != "ok" {
	// 	return nil, fmt.Errorf("handshake failed: %s", resp.Message)
	// }

	return conn, nil
}

func (a *ClusterAgent) sendRequest(conn *quic.Conn, req *model.ProxyRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	defer stream.Close() // important

	data, err := msgpack.Marshal(req)
	if err != nil {
		return err
	}

	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))

	if _, err := stream.Write(lenBuf); err != nil {
		return err
	}
	if _, err := stream.Write(data); err != nil {
		return err
	}

	// Optional: Give a tiny bit of time for the receiver
	// stream.Close() is already called by defer

	a.metrics.MessageSent()
	a.metrics.AddBytesSent(uint64(len(data)))
	return nil
}

func (a *ClusterAgent) handleRecvConnection(senderID string, conn *quic.Conn) {
	defer func() { _ = conn.CloseWithError(0, "recv handler done") }()

	for {
		stream, err := conn.AcceptStream(a.ctx)
		if err != nil {
			if a.ctx.Err() != nil {
				return
			}
			a.logger.Debug("accept stream error", zap.Error(err))
			return
		}

		go a.handleSingleStream(senderID, stream)
	}
}

func (a *ClusterAgent) handleSingleStream(senderID string, stream *quic.Stream) {
	defer func() {
		(*stream).Close()
	}()

	msg, err := a.readRequest(stream)
	if err != nil {
		a.logger.Error("FAILED TO PROCESS STREAM", zap.Error(err))
		return
	}

	// route message
	switch msg.Type {
	case model.ProxyMessageResponse:
		if msg.Response != nil {
			a.handleResponse(msg.Response)
		}
	case model.ProxyMessageOutbound:
		if msg.Outbound != nil {
			rejected := a.router.EnqueueJobs(msg.Outbound.Topic, msg.Outbound.Jobs)
			if rejected > 0 {
				// Send backpressure signal to leader
				// rejected Ids will be retried later
				a.logger.Warn("jobs rejected because of slow consumption", zap.Int("count", rejected), zap.String("topic", msg.Outbound.Topic))
				report := &model.HeartbeatReport{
					ProxyID:   a.proxyID,
					Timestamp: time.Now().Unix(),
					Capacities: []model.TopicCapacity{
						{
							Topic:            msg.Outbound.Topic,
							ConsumptionScore: 0,
						},
					},
				}

				// Fire and forget
				_ = a.sendToLeader(&model.ProxyRequest{
					RequestID:       a.nextRequestID(),
					Type:            model.ReqHeartbeatReport,
					HeartbeatReport: report,
				})
			}
		}
	case model.ProxyMessageHeartbeat:
		if msg.Heartbeat != nil {
			a.handleHeartbeat(senderID, msg.Heartbeat)
		}
	}
}

// ReadRequest reads a framed request from a stream
func (a *ClusterAgent) readRequest(stream *quic.Stream) (*model.ToProxyMessage, error) {
	// Read length prefix
	var lenBuf [4]byte
	if _, err := io.ReadFull(stream, lenBuf[:]); err != nil {
		a.logger.Error("read length prefix FAILED", zap.Error(err))
		a.metrics.ReceiveError()
		return nil, err
	}

	length := binary.LittleEndian.Uint32(lenBuf[:])
	if length == 0 || length > 10*1024*1024 {
		return nil, fmt.Errorf("invalid message size: %d", length)
	}

	// Read exact payload — use io.ReadFull for reliability
	data := make([]byte, length)
	if _, err := io.ReadFull(stream, data); err != nil {
		a.logger.Error("read payload FAILED",
			zap.Error(err),
			zap.Uint32("expected", length))
		a.metrics.ReceiveError()
		return nil, err
	}

	var req model.ToProxyMessage
	if err := msgpack.Unmarshal(data, &req); err != nil {
		a.logger.Error("msgpack unmarshal FAILED", zap.Error(err))
		a.metrics.ReceiveError()
		return nil, err
	}

	a.metrics.MessageReceived()
	a.metrics.AddBytesReceived(uint64(length))
	return &req, nil
}

func (a *ClusterAgent) sendToLeader(request *model.ProxyRequest) error {
	leaderID := a.currentLeader.Load().(string)
	if leaderID == "" || !a.leaderAvailable.Load() {
		// a.logger.Sugar().Debugf("no leader to send request id: %s", request.RequestID)
		return ErrLeaderUnavailable
	}
	return a.sendRequestByNodeID(leaderID, request)

}

// sendRequest sends a request to a specific target node with retries and backoff
func (a *ClusterAgent) sendRequestByNodeID(targetNodeID string, request *model.ProxyRequest) error {
	backoff := 5 * time.Millisecond
	deadline := time.Now().Add(400 * time.Millisecond)
	maxRetries := 3

	for attempt := range maxRetries {
		// Check deadline
		if time.Now().After(deadline) {
			return ErrDeadlineExceeded
		}

		// Get connection to target node
		conn, ok := a.sendConns[targetNodeID]
		if !ok {
			if attempt < maxRetries-1 {
				time.Sleep(backoff)
				backoff = min(backoff*2, 100*time.Millisecond)
				continue
			}
			a.logger.Sugar().Debugf("no connection found to send request id: %s", request.RequestID)

			return ErrConnNotFound
		}

		// Send request on connection
		err := a.sendRequest(conn, request)
		if err != nil {
			a.metrics.SendError()
			if attempt < maxRetries-1 {
				time.Sleep(backoff)
				backoff = min(backoff*2, 100*time.Millisecond)
				continue
			}
			// a.logger.Sugar().Debugf("send failed request id: %s", request.RequestID)

			return err
		}

		return nil
	}

	return ErrMaxRetriesExceeded
}
