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

package api_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/m-javani/cue-proxy/internal"
	"github.com/m-javani/cue-proxy/internal/api"
	"github.com/m-javani/cue-proxy/internal/config"
	"github.com/m-javani/cue-proxy/internal/model"
	"github.com/m-javani/cue-proxy/pkg"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// ============================================
// FAKE ROUTER - Minimal implementation
// ============================================

// FakeRouter implements api.Router with minimal functionality for testing
type FakeRouter struct {
	mu        sync.RWMutex
	consumers map[string]map[string]*api.Consumer // topic -> consumerID -> consumer
}

func NewFakeRouter() *FakeRouter {
	return &FakeRouter{
		consumers: make(map[string]map[string]*api.Consumer),
	}
}

func (r *FakeRouter) AddConsumer(topic string, consumer *api.Consumer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.consumers[topic] == nil {
		r.consumers[topic] = make(map[string]*api.Consumer)
	}
	r.consumers[topic][consumer.ID] = consumer
}

func (r *FakeRouter) RemoveConsumer(topic, consumerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if consumers, ok := r.consumers[topic]; ok {
		delete(consumers, consumerID)
	}
}

func (r *FakeRouter) EnqueueJobs(topic string, jobs []*model.Job) int {
	// Not used in API tests - just return count
	return len(jobs)
}

func (r *FakeRouter) GetNextConsumer(topic string) *api.Consumer {
	// Not used in API tests
	r.mu.RLock()
	defer r.mu.RUnlock()
	if consumers, ok := r.consumers[topic]; ok {
		for _, c := range consumers {
			return c
		}
	}
	return nil
}

func (r *FakeRouter) BuildHeartbeatReport() []model.TopicCapacity {
	// Not used in API tests
	return []model.TopicCapacity{}
}

// Helper method for test assertions
func (r *FakeRouter) GetConsumerCount(topic string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if consumers, ok := r.consumers[topic]; ok {
		return len(consumers)
	}
	return 0
}

// ============================================
// SIMPLE FAKE CLUSTER AGENT
// ============================================
type FakeClusterAgent struct {
	producerCh    <-chan model.ApiRequestWithRespCh
	mu            sync.Mutex
	stopCh        chan struct{}
	wg            sync.WaitGroup
	defaultResp   *model.ToProducerResponse
	mode          atomic.Int32 // 0=success, 1=error, 2=timeout
	errorMsg      string
	receivedCount atomic.Int32
}

const (
	ModeSuccess = 0
	ModeError   = 1
	ModeTimeout = 2
)

func NewFakeClusterAgent(producerCh <-chan model.ApiRequestWithRespCh) *FakeClusterAgent {
	agent := &FakeClusterAgent{
		producerCh:  producerCh,
		stopCh:      make(chan struct{}),
		defaultResp: &model.ToProducerResponse{Status: model.ToProxyRespStatusSuccess},
		mode:        atomic.Int32{},
	}
	agent.mode.Store(0)
	return agent
}

func (f *FakeClusterAgent) Start() {
	f.wg.Add(1)
	go f.run()
}

func (f *FakeClusterAgent) Stop() {
	select {
	case <-f.stopCh:
	default:
		close(f.stopCh)
	}
	f.wg.Wait()
}

func (f *FakeClusterAgent) run() {
	defer f.wg.Done()
	for {
		select {
		case <-f.stopCh:
			return
		case req, ok := <-f.producerCh:
			if !ok {
				return
			}

			f.mu.Lock()
			mode := f.mode.Load()
			f.mu.Unlock()

			switch mode {
			case ModeTimeout:
				// Do nothing → handler will hit timeout
				return

			case ModeError:
				resp := &model.ToProducerResponse{
					Status: model.ToProxyRespStatusError,
					Error:  f.errorMsg,
				}
				select {
				case req.ToProducerRespCh <- resp:
				case <-f.stopCh:
					return
				}

			default: // Success
				select {
				case req.ToProducerRespCh <- f.defaultResp:
				case <-f.stopCh:
					return
				}
			}
		}
	}
}

func (f *FakeClusterAgent) SetMode(mode int, errMsg string) {
	f.mu.Lock()
	f.mode.Store(int32(mode))
	f.errorMsg = errMsg
	f.mu.Unlock()
}

func (f *FakeClusterAgent) SetDefaultResponse(resp *model.ToProducerResponse) {
	f.mu.Lock()
	f.defaultResp = resp
	f.mu.Unlock()
}

func (f *FakeClusterAgent) GetReceivedCount() int {
	return int(f.receivedCount.Load())
}

// ============================================
// AUTH HELPER - Create temp auth file
// ============================================

func createTempAuthFile(t *testing.T) string {
	t.Helper()

	content := `tokens:
  - token: test-producer-token
    role: producer
  - token: producer-token-123
    role: producer
  - token: test-consumer-token
    role: consumer
  - token: consumer-token-456
    role: consumer
  - token: admin-token
    role: admin
  - token: mon-token
    role: monitoring
`
	tmpfile, err := os.CreateTemp("", "auth-*.yml")
	require.NoError(t, err)

	_, err = tmpfile.Write([]byte(content))
	require.NoError(t, err)

	err = tmpfile.Close()
	require.NoError(t, err)

	t.Cleanup(func() {
		os.Remove(tmpfile.Name())
	})

	return tmpfile.Name()
}

// ============================================
// API TESTER
// ============================================

// APITester provides a test harness for API testing
type APITester struct {
	t               *testing.T
	ctx             context.Context
	cancel          context.CancelFunc
	api             *api.ProxyApi
	router          *FakeRouter
	clusterAgent    *FakeClusterAgent
	producerCh      chan model.ApiRequestWithRespCh
	auth            *api.Authenticator
	leaderAvailable *atomic.Bool
	logger          *zap.Logger
	server          *httptest.Server
	wsURL           string
	httpURL         string
	metrics         *internal.ApiMetrics
	cfg             *config.APIConfig
	testTokens      map[string]string
	cleanupFuncs    []func()
}

// APITesterOption configures the APITester
type APITesterOption func(*APITester)

// WithAuthPath sets a custom auth path
func WithAuthPath(path string) APITesterOption {
	return func(t *APITester) {
		t.cfg.AuthPath = path
	}
}

// WithTestToken adds a test token with a role
func WithTestToken(user, token string) APITesterOption {
	return func(t *APITester) {
		t.testTokens[user] = token
	}
}

// NewAPITester creates a new API test harness
func NewAPITester(t *testing.T, opts ...APITesterOption) *APITester {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	// Setup logger
	logger := zaptest.NewLogger(t)

	// Setup metrics
	metrics := internal.GetApiMetrics()

	// Create temp auth file
	authPath := createTempAuthFile(t)

	// Setup config
	cfg := &config.APIConfig{
		Host:                "127.0.0.1",
		Port:                0,
		ReadTimeoutSec:      1,
		IdleTimeoutSec:      2,
		DefaultMaxInflights: 10,
		AuthPath:            authPath,
		TLSEnabled:          false,
		CertPath:            "../testdata/cert.pem",
		KeyPath:             "../testdata/key.pem",
	}

	// Create test harness
	tester := &APITester{
		t:               t,
		ctx:             ctx,
		cancel:          cancel,
		router:          NewFakeRouter(),
		producerCh:      make(chan model.ApiRequestWithRespCh, 10240),
		leaderAvailable: &atomic.Bool{},
		logger:          logger,
		metrics:         metrics,
		cfg:             cfg,
		testTokens:      make(map[string]string),
		cleanupFuncs:    []func(){},
	}

	// Apply options
	for _, opt := range opts {
		opt(tester)
	}

	// Setup authenticator
	auth, err := api.NewAuthenticator(tester.cfg.AuthPath, logger)
	require.NoError(t, err)
	tester.auth = auth
	tester.cleanupFuncs = append(tester.cleanupFuncs, func() { auth.Close() })

	// Setup cluster agent
	tester.clusterAgent = NewFakeClusterAgent(tester.producerCh)
	tester.clusterAgent.SetDefaultResponse(&model.ToProducerResponse{
		Status: model.ToProxyRespStatusSuccess,
	})
	tester.clusterAgent.Start()
	tester.cleanupFuncs = append(tester.cleanupFuncs, tester.clusterAgent.Stop)

	// Set leader available
	tester.leaderAvailable.Store(true)

	// Create proxy API
	tester.api = api.NewProxyApi(
		"test-proxy",
		logger,
		auth,
		tester.router,
		tester.producerCh,
		tester.cfg,
		"127.0.0.1:0",
		ctx,
		tester.leaderAvailable,
		metrics,
	)

	// Setup HTTP server for testing
	tester.setupTestServer()

	return tester
}

func (t *APITester) setupTestServer() {
	// Create test server
	server := httptest.NewServer(t.api.SetupHTTPServer("").Handler)
	t.server = server
	t.httpURL = server.URL

	// WebSocket URL (convert http:// to ws://)
	t.wsURL = "ws" + server.URL[4:] + "/ws"

	t.cleanupFuncs = append(t.cleanupFuncs, server.Close)
}

// Close cleans up the test harness
func (t *APITester) Close() {
	t.cancel()
	for _, f := range t.cleanupFuncs {
		f()
	}
}

// ============================================
// HTTP HELPERS
// ============================================
func (t *APITester) DoRequest(method, path string, body interface{}, token string) *http.Response {
	t.t.Helper()

	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		require.NoError(t.t, err)
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, t.httpURL+path, reqBody)
	require.NoError(t.t, err)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Do(req)
	require.NoError(t.t, err)
	return resp
}

func (t *APITester) DoRequestWithToken(method, path string, body interface{}, user string) *http.Response {
	t.t.Helper()
	token, ok := t.testTokens[user]
	if !ok {
		token = "test-token-" + user
	}
	return t.DoRequest(method, path, body, token)
}

// AddTopic helper - uses real model
func (t *APITester) AddTopic(topic string, user string) *http.Response {
	t.t.Helper()
	payload := model.AddTopicPayload{Topic: topic}
	return t.DoRequestWithToken("POST", "/producer/topic", payload, user)
}

// AddJob helper - uses real model
func (t *APITester) AddJob(job model.Job, user string) *http.Response {
	t.t.Helper()
	payload := model.AddJobsPayload{
		Topic: job.Topic,
		Jobs:  []*model.Job{&job},
	}
	return t.DoRequestWithToken("POST", "/producer/jobs", payload, user)
}

// ============================================
// WEBSOCKET HELPERS
// ============================================

func (t *APITester) ConnectWebSocket(uuid, topic, token string) (*websocket.Conn, *pkg.ToConsumerDelivery) {
	t.t.Helper()

	headers := http.Header{}
	if token != "" {
		headers.Set("Authorization", "Bearer "+token)
	}

	conn, _, err := websocket.DefaultDialer.Dial(t.wsURL, headers)
	require.NoError(t.t, err)

	// Send init message
	initMsg := pkg.WebSocketMessage{
		Action: "init",
		UUID:   uuid,
		Topic:  topic,
	}
	err = conn.WriteJSON(initMsg)
	require.NoError(t.t, err)

	// Read response
	var resp pkg.ToConsumerDelivery
	err = conn.ReadJSON(&resp)
	require.NoError(t.t, err)

	return conn, &resp
}

func (t *APITester) SendAck(conn *websocket.Conn, lastID int64, topic, jobID string) error {
	msg := pkg.WebSocketMessage{
		Action: "ack",
		LastID: lastID,
		Topic:  topic,
		JobID:  jobID,
	}
	return conn.WriteJSON(msg)
}

// ============================================
// ASSERTION HELPERS
// ============================================

func (t *APITester) AssertStatus(resp *http.Response, expectedStatus int) {
	t.t.Helper()
	assert.Equal(t.t, expectedStatus, resp.StatusCode)
}

func (t *APITester) AssertResponseBody(resp *http.Response, expected interface{}) {
	t.t.Helper()
	var actual map[string]interface{}
	err := json.NewDecoder(resp.Body).Decode(&actual)
	require.NoError(t.t, err)

	expectedBytes, err := json.Marshal(expected)
	require.NoError(t.t, err)
	var expectedMap map[string]interface{}
	err = json.Unmarshal(expectedBytes, &expectedMap)
	require.NoError(t.t, err)

	assert.Equal(t.t, expectedMap, actual)
}

// ============================================
// TEST FUNCTIONS
// ============================================

func TestProxyAPI(t *testing.T) {
	t.Run("HealthCheck", testHealthCheck)
	t.Run("Metrics", testMetrics)
	t.Run("AddTopicSuccess", testAddTopicSuccess)
	t.Run("AddTopicUnauthorized", testAddTopicUnauthorized)
	t.Run("AddTopicError", testAddTopicError)
	t.Run("AddJobSuccess", testAddJobSuccess)
	t.Run("AddJobUnauthorized", testAddJobUnauthorized)
	t.Run("AddJobError", testAddJobError)
	t.Run("WebSocketConnection", testWebSocketConnection)
	t.Run("WebSocketUnauthorized", testWebSocketUnauthorized)
	t.Run("WebSocketAck", testWebSocketAck)
	t.Run("LeaderCheck", testLeaderCheck)
	t.Run("ClusterBusy", testClusterBusy)
	t.Run("Timeout", testTimeout)
}

func testHealthCheck(t *testing.T) {
	tester := NewAPITester(t)
	defer tester.Close()

	resp := tester.DoRequest("GET", "/health", nil, "")
	defer resp.Body.Close()

	tester.AssertStatus(resp, http.StatusOK)
	tester.AssertResponseBody(resp, map[string]string{"status": "ok"})
}

func testMetrics(t *testing.T) {
	tester := NewAPITester(t)
	defer tester.Close()

	resp := tester.DoRequest("GET", "/metrics", nil, "")
	defer resp.Body.Close()

	tester.AssertStatus(resp, http.StatusOK)
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/plain")
}

func testAddTopicSuccess(t *testing.T) {
	tester := NewAPITester(t, WithTestToken("producer", "test-producer-token"))
	defer tester.Close()

	resp := tester.AddTopic("test-topic", "producer")
	defer resp.Body.Close()

	tester.AssertStatus(resp, http.StatusOK)
	tester.AssertResponseBody(resp, map[string]string{"status": "success"})
}

func testAddTopicUnauthorized(t *testing.T) {
	tester := NewAPITester(t)
	defer tester.Close()

	resp := tester.AddTopic("test-topic", "unknown-user")
	defer resp.Body.Close()

	tester.AssertStatus(resp, http.StatusUnauthorized)
}

func testAddTopicError(t *testing.T) {
	tester := NewAPITester(t, WithTestToken("producer", "test-producer-token"))
	defer tester.Close()

	// Configure cluster agent to return error
	errorResp := &model.ToProducerResponse{
		Status: model.ToProxyRespStatusError,
		Error:  "topic already exists",
	}
	tester.clusterAgent.SetDefaultResponse(errorResp)

	resp := tester.AddTopic("existing-topic", "producer")
	defer resp.Body.Close()

	tester.AssertStatus(resp, http.StatusBadRequest)
}

func testAddJobSuccess(t *testing.T) {
	tester := NewAPITester(t, WithTestToken("producer", "test-producer-token"))
	defer tester.Close()

	job := model.Job{
		ID:    "job-1",
		Topic: "test-topic",
		Data:  []byte(`{"test":"data"}`),
	}

	resp := tester.AddJob(job, "producer")
	defer resp.Body.Close()

	tester.AssertStatus(resp, http.StatusOK)

	var result map[string]string
	err := json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, "success", result["status"])
	assert.Equal(t, "test-topic", result["topic"])
}

func testAddJobUnauthorized(t *testing.T) {
	tester := NewAPITester(t)
	defer tester.Close()

	job := model.Job{
		ID:    "job-1",
		Topic: "test-topic",
		Data:  []byte(`{"test":"data"}`),
	}

	resp := tester.AddJob(job, "unknown-user")
	defer resp.Body.Close()

	tester.AssertStatus(resp, http.StatusUnauthorized)
}

func testAddJobError(t *testing.T) {
	tester := NewAPITester(t, WithTestToken("producer", "test-producer-token"))
	defer tester.Close()

	// Configure cluster agent to return error
	errorResp := &model.ToProducerResponse{
		Status: model.ToProxyRespStatusError,
		Error:  "job validation failed",
	}
	tester.clusterAgent.SetDefaultResponse(errorResp)

	job := model.Job{
		ID:    "invalid-job",
		Topic: "test-topic",
		Data:  []byte(`{}`),
	}

	resp := tester.AddJob(job, "producer")
	defer resp.Body.Close()

	tester.AssertStatus(resp, http.StatusBadRequest)
}

func testWebSocketConnection(t *testing.T) {
	tester := NewAPITester(t, WithTestToken("consumer", "test-consumer-token"))
	defer tester.Close()

	conn, resp := tester.ConnectWebSocket("user-123", "test-topic", "test-consumer-token")
	defer conn.Close()

	assert.Equal(t, "accepted", resp.Action)
	assert.Equal(t, 1, tester.router.GetConsumerCount("test-topic"))
}

func testWebSocketUnauthorized(t *testing.T) {
	tester := NewAPITester(t)
	defer tester.Close()

	// Try without token - this should fail at the HTTP handshake level
	headers := http.Header{}
	conn, resp, err := websocket.DefaultDialer.Dial(tester.wsURL, headers)

	// The dial should fail with an error
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad handshake")

	// The response should be 401 Unauthorized
	if resp != nil {
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		defer resp.Body.Close()
	}

	// We should not have a connection
	assert.Nil(t, conn)

	// Now test with an invalid token
	headers = http.Header{}
	headers.Set("Authorization", "Bearer invalid-token")
	conn, resp, err = websocket.DefaultDialer.Dial(tester.wsURL, headers)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad handshake")
	if resp != nil {
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		defer resp.Body.Close()
	}
	assert.Nil(t, conn)
}

func testWebSocketAck(t *testing.T) {
	tester := NewAPITester(t, WithTestToken("consumer", "test-consumer-token"))
	defer tester.Close()

	conn, _ := tester.ConnectWebSocket("user-123", "test-topic", "test-consumer-token")
	defer conn.Close()

	// Send ACK with int64 LastID
	err := tester.SendAck(conn, 12345, "test-topic", "job-1")
	require.NoError(t, err)

	// Give it time to process
	time.Sleep(100 * time.Millisecond)
}

func testLeaderCheck(t *testing.T) {
	tester := NewAPITester(t, WithTestToken("producer", "test-producer-token"))
	defer tester.Close()

	// Make leader unavailable
	tester.leaderAvailable.Store(false)

	// Test AddTopic
	resp := tester.AddTopic("test-topic", "producer")
	defer resp.Body.Close()

	tester.AssertStatus(resp, http.StatusServiceUnavailable)

	var result map[string]string
	err := json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, "service_unavailable", result["error"])
	assert.Contains(t, result["message"], "temporarily unavailable")

	// Reset leader available
	tester.leaderAvailable.Store(true)

	// Should work now
	resp = tester.AddTopic("test-topic", "producer")
	defer resp.Body.Close()
	tester.AssertStatus(resp, http.StatusOK)
}

func testClusterBusy(t *testing.T) {
	tester := NewAPITester(t, WithTestToken("producer", "test-producer-token"))
	defer tester.Close()

	// Simulate cluster returning busy/error
	tester.clusterAgent.SetMode(ModeError, "cluster is busy / overloaded")

	resp := tester.AddTopic("test-topic", "producer")
	defer resp.Body.Close()

	tester.AssertStatus(resp, http.StatusBadRequest) // 400
}

func testTimeout(t *testing.T) {
	tester := NewAPITester(t, WithTestToken("producer", "test-producer-token"))
	defer tester.Close()

	// Simulate cluster returning busy/error
	tester.clusterAgent.SetMode(ModeTimeout, "")

	resp := tester.AddTopic("test-topic", "producer")
	defer resp.Body.Close()

	tester.AssertStatus(resp, http.StatusGatewayTimeout) // 504
}

// ============================================
// TEST MAIN (optional setup/teardown)
// ============================================

func TestMain(m *testing.M) {
	// Reset metrics for testing
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	m.Run()
}

func TestGetTLSConfig(t *testing.T) {
	// Use the test helper to create a fully configured API tester
	tester := NewAPITester(t)
	defer tester.Close()

	// Call the exported method on the api instance
	tlsConfig := tester.api.GetTLSConfig()

	// Assert basic configuration
	assert.NotNil(t, tlsConfig, "TLS config should not be nil")
	assert.Equal(t, uint16(tls.VersionTLS12), tlsConfig.MinVersion, "Should enforce TLS 1.2 minimum")

	// Check cipher suites
	expectedCiphers := []uint16{
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
		tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	}
	assert.Equal(t, expectedCiphers, tlsConfig.CipherSuites, "Cipher suites should match expected list")

	// Check curve preferences
	expectedCurves := []tls.CurveID{
		tls.X25519,
		tls.CurveP256,
		tls.CurveP384,
	}
	assert.Equal(t, expectedCurves, tlsConfig.CurvePreferences, "Curve preferences should match expected list")

	// Check other settings
	assert.False(t, tlsConfig.SessionTicketsDisabled, "Session tickets should be enabled")
	assert.Equal(t, []string{"h2", "http/1.1"}, tlsConfig.NextProtos, "Should support HTTP/2 and HTTP/1.1")
}
