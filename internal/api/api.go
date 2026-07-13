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

package api

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/m-javani/cue-proxy/internal"
	"github.com/m-javani/cue-proxy/internal/config"
	"github.com/m-javani/cue-proxy/internal/model"
	"github.com/m-javani/cue-proxy/pkg"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		// In production, we might want to check origins more strictly
		// For now, allow all origins
		return true
	},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// Subprotocols are automatically handled
	// TLS is automatically detected by the underlying connection
}

type ProxyApi struct {
	proxyID       string
	logger        *zap.Logger
	metrics       *internal.ApiMetrics
	authenticator *Authenticator

	router Router // Injected from top layer

	// Channels to/from ClusterAgent
	producerCh chan<- model.ApiRequestWithRespCh

	consumers       map[string]*Consumer
	consumersByUUID map[string]*Consumer
	mu              sync.RWMutex

	cfg             *config.APIConfig
	httpServer      *http.Server
	httpAddr        string
	leaderAvailable *atomic.Bool
	ctx             context.Context

	reqIDCounter atomic.Uint32
}

func NewProxyApi(
	proxyID string,
	logger *zap.Logger,
	authenticator *Authenticator,
	router Router,
	producerCh chan<- model.ApiRequestWithRespCh,
	cfg *config.APIConfig,
	httpAddr string,
	ctx context.Context,
	leaderAvailable *atomic.Bool,
	metrics *internal.ApiMetrics,
) *ProxyApi {
	return &ProxyApi{
		proxyID:         proxyID,
		logger:          logger,
		metrics:         metrics,
		authenticator:   authenticator,
		router:          router,
		producerCh:      producerCh,
		consumers:       make(map[string]*Consumer),
		consumersByUUID: make(map[string]*Consumer),
		cfg:             cfg,
		httpAddr:        httpAddr,
		ctx:             ctx,
		leaderAvailable: leaderAvailable,
		httpServer:      &http.Server{},
		reqIDCounter:    atomic.Uint32{},
	}
}

func (p *ProxyApi) nextRequestID() uint32 {
	return p.reqIDCounter.Add(1)
}

func (p *ProxyApi) Start() error {
	p.httpServer = p.SetupHTTPServer(p.httpAddr)
	p.logger.Info("HTTP server listening",
		zap.String("address", p.httpAddr),
		zap.Bool("tls_enabled", p.cfg.TLSEnabled),
	)

	var err error
	if p.cfg.TLSEnabled {
		// Configure TLS with secure defaults for production
		p.httpServer.TLSConfig = p.GetTLSConfig()
		p.logger.Info("TLS enabled, using certificates",
			zap.String("cert", p.cfg.CertPath),
			zap.String("key", p.cfg.KeyPath),
		)
		err = p.httpServer.ListenAndServeTLS(p.cfg.CertPath, p.cfg.KeyPath)
	} else {
		p.logger.Info("TLS is disabled - API will serve over HTTP")
		err = p.httpServer.ListenAndServe()
	}

	if err != nil && err != http.ErrServerClosed {
		p.logger.Error("HTTP server failed", zap.Error(err))
		return err
	}
	return nil
}

// GetTLSConfig returns a secure TLS configuration for production use
func (p *ProxyApi) GetTLSConfig() *tls.Config {
	return &tls.Config{
		// Enforce TLS 1.2 minimum (TLS 1.3 supported in Go 1.21+)
		MinVersion: tls.VersionTLS12,

		// Modern, secure cipher suites
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		},

		// Preferred elliptic curves
		CurvePreferences: []tls.CurveID{
			tls.X25519,    // Modern, fast, secure
			tls.CurveP256, // NIST P-256
			tls.CurveP384, // NIST P-384 (for compatibility)
		},

		// Let server choose cipher suite based on client capabilities
		PreferServerCipherSuites: true,

		// Disable session resumption for better security (optional)
		SessionTicketsDisabled: false,

		// Enable HTTP/2 support
		NextProtos: []string{"h2", "http/1.1"},
	}
}

func (p *ProxyApi) Stop(ctx context.Context) {
	if p.httpServer != nil {
		_ = p.httpServer.Shutdown(ctx)
	}
	// Dispatchers are stopped via router when consumers are removed
}

func (p *ProxyApi) SetupHTTPServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", p.HealthHandler)
	mux.Handle("/metrics", p.MetricsHandler())
	mux.HandleFunc("/producer/topic", p.withLeaderCheck(p.AddTopicHandler)) // only admin
	mux.HandleFunc("/producer/jobs", p.withLeaderCheck(p.AddJobHandler))
	mux.HandleFunc("/ws", p.WebSocketHandler)

	return &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  time.Duration(p.cfg.ReadTimeoutSec) * time.Second,
		WriteTimeout: 0, // Should be 0 when using WebSockets
		IdleTimeout:  time.Duration(p.cfg.IdleTimeoutSec) * time.Second,
	}
}

// Middleware, handlers, and helper functions
func (p *ProxyApi) withLeaderCheck(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !p.leaderAvailable.Load() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			if err := json.NewEncoder(w).Encode(map[string]string{
				"error":   "service_unavailable",
				"message": "The service is temporarily unavailable. Please try again later.",
			}); err != nil {
				p.logger.Sugar().Debugf("failed to encode error response: %v", err)
			}
			return
		}
		handler(w, r)
	}
}

func (p *ProxyApi) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (p *ProxyApi) MetricsHandler() http.Handler {
	return promhttp.Handler()
}

func (p *ProxyApi) AddTopicHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		p.metrics.HTTPRequestDuration.WithLabelValues("POST", "/producer/topic").Observe(time.Since(start).Seconds())
	}()

	// Auth check
	token := extractToken(r)
	if !p.authenticator.ValidateRole(token, RoleAdmin) {
		p.metrics.AuthFailure()
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var req model.AddTopicPayload
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil || req.Topic == "" {
		p.metrics.HTTPRequestsTotal.WithLabelValues("POST", "/producer/topic", "400").Inc()
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create response channel
	respCh := make(chan *model.ToProducerResponse, 1)

	// Send to ClusterAgent
	select {
	case p.producerCh <- model.ApiRequestWithRespCh{
		ProxyRequest: model.ProxyRequest{
			RequestID: p.nextRequestID(),
			Type:      model.ReqAddTopic,
			AddTopic:  &req,
		},
		ToProducerRespCh: respCh,
	}:
	case <-time.After(5 * time.Second):
		http.Error(w, "proxy busy", http.StatusServiceUnavailable)
		return
	}

	// Wait for response with timeout
	select {
	case <-p.ctx.Done():
		http.Error(
			w,
			"proxy shutting down",
			http.StatusServiceUnavailable,
		)
		return
	case resp := <-respCh:

		if resp.Status == model.ToProxyRespStatusSuccess {
			p.metrics.HTTPRequestsTotal.WithLabelValues("POST", "/producer/topic", "200").Inc()
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "success"})
		} else {
			p.metrics.HTTPRequestsTotal.WithLabelValues("POST", "/producer/topic", "400").Inc()
			http.Error(w, resp.Error, http.StatusBadRequest)
		}
	case <-time.After(5 * time.Second):
		p.metrics.HTTPRequestsTotal.WithLabelValues("POST", "/producer/topic", "504").Inc()
		http.Error(w, "timeout", http.StatusGatewayTimeout)
	}
}

func (p *ProxyApi) AddJobHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		p.metrics.HTTPRequestDuration.WithLabelValues("POST", "/producer/jobs").Observe(time.Since(start).Seconds())
	}()

	// Auth check
	token := extractToken(r)
	if !p.authenticator.ValidateRole(token, RoleProducer) {
		p.metrics.AuthFailure()
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var req model.AddJobsPayload
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil || req.Topic == "" || len(req.Jobs) == 0 {
		p.metrics.HTTPRequestsTotal.WithLabelValues("POST", "/producer/jobs", "400").Inc()
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create response channel
	respCh := make(chan *model.ToProducerResponse, 1)

	// Send to ClusterAgent
	select {
	case p.producerCh <- model.ApiRequestWithRespCh{
		ProxyRequest: model.ProxyRequest{
			RequestID: p.nextRequestID(),
			Type:      model.ReqAddJobs,
			AddJobs:   &req,
		},
		ToProducerRespCh: respCh,
	}:
	case <-time.After(5 * time.Second):
		http.Error(w, "proxy busy", http.StatusServiceUnavailable)
		return
	}

	// Wait for response with timeout
	select {
	case <-p.ctx.Done():
		return
	case resp := <-respCh:
		if resp.Status == model.ToProxyRespStatusSuccess {
			p.metrics.HTTPRequestsTotal.WithLabelValues("POST", "/producer/jobs", "200").Inc()
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "success", "topic": req.Topic})
		} else {
			p.metrics.HTTPRequestsTotal.WithLabelValues("POST", "/producer/jobs", "400").Inc()
			http.Error(w, resp.Error, http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "error", "message": resp.Error})
		}
	case <-time.After(30 * time.Second):
		p.metrics.HTTPRequestsTotal.WithLabelValues("POST", "/producer/jobs", "504").Inc()
		http.Error(w, "timeout", http.StatusGatewayTimeout)
	}
}

func (p *ProxyApi) WebSocketHandler(w http.ResponseWriter, r *http.Request) {
	// p.logger.Info("ws_connection_attempt", zap.String("remote_addr", r.RemoteAddr))
	token := extractToken(r)
	if !p.authenticator.ValidateRole(token, RoleConsumer) {
		p.logger.Debug("ws_auth_failed", zap.String("remote_addr", r.RemoteAddr))
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		p.logger.Error("ws_upgrade_failed", zap.Error(err))
		return
	}
	conn.SetReadLimit(p.cfg.WSReadLimitBytes)
	if p.cfg.WSReadTimeoutSec > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(time.Duration(p.cfg.WSReadTimeoutSec)))
	}
	p.metrics.WebSocketConnections.Inc()
	defer p.metrics.WebSocketConnections.Dec()

	var initMsg pkg.WebSocketMessage
	if err := conn.ReadJSON(&initMsg); err != nil {
		p.logger.Error("ws_init_read_failed", zap.Error(err))
		conn.Close()
		return
	}

	if initMsg.Action != "init" || initMsg.UUID == "" || initMsg.Topic == "" {
		_ = conn.WriteJSON(pkg.ToConsumerDelivery{Action: "rejected", Data: json.RawMessage(`{"error":"first message action must be init and include uuid and topic"}`)})
		conn.Close()
		return
	}

	consumer := NewConsumer(
		p.ctx,
		uuid.New().String(),
		initMsg.UUID,
		initMsg.Topic,
		conn,
		p.cfg.DefaultMaxInflights,
		p.logger,
	)

	p.mu.Lock()
	p.consumers[consumer.ID] = consumer
	p.consumersByUUID[consumer.UUID] = consumer
	p.mu.Unlock()

	p.router.AddConsumer(initMsg.Topic, consumer)

	p.logger.Info("new consumer connected",
		zap.String("uuid", initMsg.UUID),
		zap.String("consumer_id", consumer.ID),
		zap.String("topic", initMsg.Topic),
		zap.Bool("secure", r.TLS != nil))

	go consumer.StartWriteLoop()

	msg, _ := json.Marshal(pkg.ToConsumerDelivery{Action: "accepted"})
	_ = consumer.WriteMessage(msg)

	// Read loop
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		var msg pkg.WebSocketMessage
		if err := conn.ReadJSON(&msg); err != nil {
			// p.logger.Debug("websocket_closed_normally", zap.Error(err), zap.String("uuid", consumer.UUID))
			break
		}

		// Reset read deadline after successful read
		if p.cfg.WSReadTimeoutSec > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(time.Duration(p.cfg.WSReadTimeoutSec)))
		}

		if msg.Action == "ack" {
			consumer.UpdateDeliveryAck(msg.LastID)
			if msg.Topic != "" && msg.JobID != "" {
				select {
				case p.producerCh <- model.ApiRequestWithRespCh{
					ProxyRequest: model.ProxyRequest{
						RequestID: p.nextRequestID(),
						Type:      model.ReqDone,
						Done: &model.DonePayload{
							Topic:  msg.Topic,
							JobIDs: []string{msg.JobID},
						},
					},
				}:
				case <-time.After(1 * time.Second):
				}
			}
		}
	}

	// Cleanup on disconnect
	p.mu.Lock()
	delete(p.consumers, consumer.ID)
	delete(p.consumersByUUID, consumer.UUID)
	p.mu.Unlock()

	p.router.RemoveConsumer(consumer.Topic, consumer.ID)
	consumer.Close()
}

func extractToken(r *http.Request) string {
	if auth := r.Header.Get("Authorization"); auth != "" {
		var token string
		if _, err := fmt.Sscanf(auth, "Bearer %s", &token); err == nil && token != "" {
			return token
		}
	}
	return r.URL.Query().Get("token")
}
