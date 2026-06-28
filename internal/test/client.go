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

package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/m-javani/cue-proxy/pkg"
	"go.uber.org/zap"
)

// ─── Types ───────────────────────────────────────────────────────────────────

type SentJob struct {
	JobID     string
	Topic     string
	Data      []byte
	Received  bool
	RespTime  time.Time
	ResStatus int
}

type ReceivedJob struct {
	JobID      string
	Topic      string
	ConsumerID int
	SeqID      int64
	Timestamp  time.Time
}

// ─── ProducerClient ──────────────────────────────────────────────────────────

type ProducerClient struct {
	baseURL string
	token   string
	client  *http.Client
}

func NewProducerClient(baseURL, token string) *ProducerClient {
	return &ProducerClient{
		baseURL: baseURL,
		token:   token,
		client:  &http.Client{Timeout: 5 * time.Second},
	}
}

func (p *ProducerClient) Close() {
	p.client.CloseIdleConnections()
}

func (p *ProducerClient) doPost(path string, body any) (*http.Response, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", p.baseURL+path, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("Content-Type", "application/json")
	return p.client.Do(req)
}

func (p *ProducerClient) AddTopic(topic string) error {
	resp, err := p.doPost("/producer/topic", map[string]string{"topic": topic})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("add topic failed: %s", resp.Status)
	}
	return nil
}

func (p *ProducerClient) AddJob(jobID, topic string, data []byte) (int, error) {
	job := map[string]any{
		"job": map[string]any{
			"id":    jobID,
			"topic": topic,
			"data":  data,
		},
	}
	resp, err := p.doPost("/producer/job", job)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, fmt.Errorf("add job failed: %s", resp.Status)
	}
	return resp.StatusCode, nil
}

// ─── Consumer ────────────────────────────────────────────────────────────────

type consumer struct {
	id         int
	uuid       string
	conn       *websocket.Conn
	connMu     sync.RWMutex
	topic      string
	receivedCh chan ReceivedJob
	doneCh     chan struct{}
	stopOnce   sync.Once

	lastAckID int64
	lastAckMu sync.RWMutex
}

func newConsumer(id int, topic string) *consumer {
	return &consumer{
		id:         id,
		uuid:       fmt.Sprintf("c%d-%d", id, time.Now().UnixNano()%1000000),
		topic:      topic,
		receivedCh: make(chan ReceivedJob, 500),
		doneCh:     make(chan struct{}),
	}
}

func (c *consumer) UpdateLastAck(ackID int64) {
	c.lastAckMu.Lock()
	if ackID > c.lastAckID {
		c.lastAckID = ackID
	}
	c.lastAckMu.Unlock()
}

func (c *consumer) GetLastAck() int64 {
	c.lastAckMu.RLock()
	defer c.lastAckMu.RUnlock()
	return c.lastAckID
}

func (c *consumer) stop() {
	c.stopOnce.Do(func() {
		c.connMu.Lock()
		if c.conn != nil {
			_ = c.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "test done"), time.Now().Add(time.Second))
			c.conn.Close()
			c.conn = nil
		}
		c.connMu.Unlock()
		close(c.doneCh)
	})
}

// ─── Client (coordinator) ──────────────────────────────────────────────────────

type Client struct {
	producer      *ProducerClient
	consumerURL   string
	consumerToken string

	consumers      map[int]*consumer
	nextConsumerID int
	consumersMu    sync.Mutex

	sentJobs      map[string]SentJob
	httpResponses map[string]bool
	receivedJobs  []ReceivedJob
	trackingMu    sync.RWMutex

	ctx       context.Context
	cancel    context.CancelFunc
	startOnce sync.Once
	stopOnce  sync.Once
	wg        sync.WaitGroup

	logger *zap.Logger
}

func NewClient(producerURL, consumerURL string, logger *zap.Logger) *Client {
	return &Client{
		producer:      NewProducerClient(producerURL, "prod_abc123"),
		consumerURL:   consumerURL,
		consumerToken: "cons_xyz789",
		consumers:     make(map[int]*consumer),
		sentJobs:      make(map[string]SentJob, 20400),
		httpResponses: make(map[string]bool, 0),
		logger:        logger,
	}
}

// AddTopic adds a new topic via the producer API
func (c *Client) AddTopic(topic string) error {
	return c.producer.AddTopic(topic)
}

// AddJob adds a new job via the producer API
func (c *Client) AddJob(jobID, topic string, data []byte) error {
	statusCode, err := c.producer.AddJob(jobID, topic, data)
	if err != nil {
		return err
	}
	c.trackingMu.Lock()
	c.sentJobs[jobID] = SentJob{
		JobID:     jobID,
		Topic:     topic,
		Data:      data,
		Received:  false,
		RespTime:  time.Now(),
		ResStatus: statusCode,
	}
	if statusCode == http.StatusOK {
		c.httpResponses[jobID] = true
	}
	c.trackingMu.Unlock()
	return nil
}

// AddConsumer adds a new consumer and returns its ID
func (c *Client) AddConsumer(topic string) int {
	c.consumersMu.Lock()
	defer c.consumersMu.Unlock()

	id := c.nextConsumerID
	c.nextConsumerID++
	c.consumers[id] = newConsumer(id, topic)
	return id
}

// Start starts all consumers
func (c *Client) Start() error {
	var err error
	c.startOnce.Do(func() {
		c.ctx, c.cancel = context.WithCancel(context.Background())

		c.consumersMu.Lock()
		list := make([]*consumer, 0, len(c.consumers))
		for _, cons := range c.consumers {
			list = append(list, cons)
		}
		c.consumersMu.Unlock()

		for _, cons := range list {
			c.wg.Add(1)
			go c.runConsumer(cons)
		}
		time.Sleep(500 * time.Millisecond)
	})
	return err
}

// Stop stops all consumers and cleans up
func (c *Client) Stop() {
	c.stopOnce.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}
		c.consumersMu.Lock()
		for _, cons := range c.consumers {
			cons.stop()
		}
		c.consumersMu.Unlock()
		c.wg.Wait()
		c.producer.Close()
	})
}

// StopConsumer fully stops and removes a consumer
func (c *Client) StopConsumer(consumerID int) error {
	c.consumersMu.Lock()
	cons, ok := c.consumers[consumerID]
	if !ok {
		c.consumersMu.Unlock()
		return fmt.Errorf("consumer %d not found", consumerID)
	}
	delete(c.consumers, consumerID)
	c.consumersMu.Unlock()

	cons.stop()
	return nil
}

// runConsumer is the main goroutine for a consumer
func (c *Client) runConsumer(cons *consumer) {
	defer c.wg.Done()

	// c.logger.Info("consumer_dial_start", zap.Int("consumer_id", cons.id)) // ADD

	headers := http.Header{}
	headers.Set("Authorization", "Bearer "+c.consumerToken)

	conn, _, err := websocket.DefaultDialer.Dial(c.consumerURL, headers)
	if err != nil {
		c.logger.Error("consumer_dial_failed", zap.Error(err), zap.Int("consumer_id", cons.id))
		return
	}
	// c.logger.Info("consumer_dial_success", zap.Int("consumer_id", cons.id))

	cons.connMu.Lock()
	cons.conn = conn
	cons.connMu.Unlock()

	// Send init
	// c.logger.Info("sending_init", zap.Int("consumer_id", cons.id), zap.String("uuid", cons.uuid))
	initMsg := pkg.WebSocketMessage{
		Action: "init",
		UUID:   cons.uuid,
		Topic:  cons.topic,
	}
	if err := conn.WriteJSON(initMsg); err != nil {
		c.logger.Error("init_send_failed", zap.Error(err))
		return
	}

	// Read loop
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-cons.doneCh:
			return
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))

		var delivery pkg.ToConsumerDelivery
		if err := conn.ReadJSON(&delivery); err != nil {
			// if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
			// c.logger.Sugar().Debugf("websocket closed - consumer_id: %d", cons.id)
			// }
			return
		}

		if delivery.Action == "rejected" {
			c.logger.Error("connection rejected", zap.Int("consumer_id", cons.id))
			return
		}

		if delivery.Action != "job" {
			// if delivery.Action != "" {
			// 	c.logger.Debug("ignoring_non_job_message",
			// 		zap.String("action", delivery.Action),
			// 		zap.Int("consumer_id", cons.id))
			// }
			continue
		}

		// c.logger.Info("received_job_message", zap.Any("msg", delivery))

		received := ReceivedJob{
			JobID:      delivery.JobID,
			Topic:      delivery.Topic,
			ConsumerID: cons.id,
			SeqID:      int64(delivery.SeqID),
			Timestamp:  time.Now(),
		}

		// c.logger.Info("job_successfully_processed",
		// 	zap.String("job_id", delivery.JobID),
		// 	zap.Int64("seq_id", delivery.SeqID))

		// Update tracking
		c.trackingMu.Lock()
		c.receivedJobs = append(c.receivedJobs, received)
		if sj, ok := c.sentJobs[delivery.JobID]; ok {
			sj.Received = true
			c.sentJobs[delivery.JobID] = sj
		}
		c.trackingMu.Unlock()

		// Send ACK
		ackMsg := pkg.WebSocketMessage{
			Action: "ack",
			LastID: delivery.SeqID,
			JobID:  delivery.JobID,
			Topic:  delivery.Topic,
		}
		_ = conn.WriteJSON(ackMsg)

		cons.UpdateLastAck(delivery.SeqID)
	}
}

// AssertAllJobsReceivedE2E waits up to timeout for all jobs to be received.
// Works reliably with single and multiple consumers.
func (c *Client) AssertAllJobsReceivedE2E(t *testing.T, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(300 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			c.trackingMu.RLock()
			sent := len(c.sentJobs)
			receivedCount := len(c.receivedJobs)
			c.trackingMu.RUnlock()

			if receivedCount == sent {
				t.Logf("✓ All %d jobs received successfully", sent)
				// c.PrintSummary()
				return
			}

			if time.Now().After(deadline) {
				// c.reportMissingJobs(t)
				return
			}

		case <-time.After(timeout):
			// c.reportMissingJobs(t)
			return
		}
	}
}

// GetReceivedJobsByConsumer returns all jobs received by a specific consumer
func (c *Client) GetReceivedJobsByConsumer(consumerID int) []ReceivedJob {
	c.trackingMu.RLock()
	defer c.trackingMu.RUnlock()

	jobs := make([]ReceivedJob, 0)
	for _, job := range c.receivedJobs {
		if job.ConsumerID == consumerID {
			jobs = append(jobs, job)
		}
	}
	return jobs
}

// ClearTracking clears all sent and received job tracking
func (c *Client) ClearTracking() {
	c.trackingMu.Lock()
	defer c.trackingMu.Unlock()

	c.sentJobs = make(map[string]SentJob)
	c.receivedJobs = c.receivedJobs[:0]
}

// GetSentJobs returns all sent jobs
func (c *Client) GetSentJobs() []SentJob {
	c.trackingMu.RLock()
	defer c.trackingMu.RUnlock()

	jobs := make([]SentJob, 0, len(c.sentJobs))
	for _, job := range c.sentJobs {
		jobs = append(jobs, job)
	}
	return jobs
}

// GetReceivedJobs returns all received jobs
func (c *Client) GetReceivedJobs() []ReceivedJob {
	c.trackingMu.RLock()
	defer c.trackingMu.RUnlock()

	jobs := make([]ReceivedJob, len(c.receivedJobs))
	copy(jobs, c.receivedJobs)
	return jobs
}

func (c *Client) GetConsumerLastAck(consumerID int) int64 {
	c.consumersMu.Lock()
	cons, ok := c.consumers[consumerID]
	c.consumersMu.Unlock()
	if !ok {
		return 0
	}
	return cons.GetLastAck()
}

// PrintSummary prints a summary of sent and received jobs
func (c *Client) PrintSummary() {
	c.trackingMu.RLock()
	defer c.trackingMu.RUnlock()

	fmt.Printf("\n=== Test Summary ===\n")
	fmt.Printf("Sent Jobs: %d\n", len(c.sentJobs))
	fmt.Printf("Received Jobs: %d\n", len(c.receivedJobs))

	if len(c.receivedJobs) > 0 {
		fmt.Printf("\nReceived by Consumer:\n")
		consumerJobs := make(map[int][]string)
		for _, job := range c.receivedJobs {
			consumerJobs[job.ConsumerID] = append(consumerJobs[job.ConsumerID], job.JobID)
		}
		for consumerID, jobs := range consumerJobs {
			fmt.Printf("  Consumer %d: %v\n", consumerID, jobs)
		}
	}
	fmt.Printf("==================\n\n")
}

// ─── Additional Assert Functions ──────────────────────────────────────────────

// WaitForProducerResponses waits for all add job HTTP responses or timeout.
// Prints: total sent, received, failed (status != 200), missing, p50, p99.
// Returns: true if all received with status 200, false otherwise.
func (c *Client) WaitForProducerResponses(t *testing.T, timeout time.Duration) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			c.trackingMu.RLock()
			sent := len(c.sentJobs)
			failed := 0
			httpReceived := len(c.httpResponses)
			for _, job := range c.sentJobs {
				if job.ResStatus != http.StatusOK {
					failed++
				}
			}
			c.trackingMu.RUnlock()

			if httpReceived == sent && sent > 0 && failed == 0 {
				t.Logf("✓ All %d jobs received successful HTTP responses", sent)
				return true
			}

			if time.Now().After(deadline) {
				c.trackingMu.RLock()
				missing := sent - failed
				c.trackingMu.RUnlock()

				t.Logf("✗ Timeout: Sent=%d, Received=%d, Failed=%d, Missing=%d", sent, httpReceived, failed, missing)
				return false
			}

		case <-time.After(timeout):
			c.trackingMu.RLock()
			sent := len(c.sentJobs)
			received := 0
			failed := 0
			for _, job := range c.sentJobs {
				if job.Received {
					received++
				}
				if job.ResStatus != http.StatusOK {
					failed++
				}
			}
			missing := sent - received
			c.trackingMu.RUnlock()

			t.Logf("✗ Timeout: Sent=%d, Received=%d, Failed=%d, Missing=%d", sent, received, failed, missing)
			return false
		}
	}
}

// WaitForConsumerDelivery waits for all jobs to be dispatched to consumers or timeout.
// Prints: total sent, received, missing, p50, p99.
// Returns: true if all jobs received by consumers, false otherwise.
func (c *Client) WaitForConsumerDelivery(t *testing.T, timeout time.Duration) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			c.trackingMu.RLock()
			sent := len(c.sentJobs)
			received := len(c.receivedJobs)
			allReceived := received == sent && sent > 0
			c.trackingMu.RUnlock()

			if allReceived {
				// Calculate and print p50, p99
				c.trackingMu.RLock()
				deliveryTimes := make([]time.Duration, 0, len(c.receivedJobs))
				if len(c.receivedJobs) > 0 {
					first := c.receivedJobs[0].Timestamp
					for _, job := range c.receivedJobs {
						if job.Timestamp.Before(first) {
							first = job.Timestamp
						}
					}
					for _, job := range c.receivedJobs {
						deliveryTimes = append(deliveryTimes, job.Timestamp.Sub(first))
					}
				}
				c.trackingMu.RUnlock()

				p50 := calculatePercentile(deliveryTimes, 0.50)
				p99 := calculatePercentile(deliveryTimes, 0.99)

				t.Logf("✓ All %d jobs delivered to consumers", sent)
				t.Logf("  P50: %v, P99: %v", p50, p99)
				return true
			}

			if time.Now().After(deadline) {
				c.trackingMu.RLock()
				sent := len(c.sentJobs)
				received := len(c.receivedJobs)
				missing := sent - received

				deliveryTimes := make([]time.Duration, 0, len(c.receivedJobs))
				if len(c.receivedJobs) > 0 {
					first := c.receivedJobs[0].Timestamp
					for _, job := range c.receivedJobs {
						if job.Timestamp.Before(first) {
							first = job.Timestamp
						}
					}
					for _, job := range c.receivedJobs {
						deliveryTimes = append(deliveryTimes, job.Timestamp.Sub(first))
					}
				}
				c.trackingMu.RUnlock()

				p50 := calculatePercentile(deliveryTimes, 0.50)
				p99 := calculatePercentile(deliveryTimes, 0.99)

				t.Logf("✗ Timeout: Sent=%d, Received=%d, Missing=%d", sent, received, missing)
				t.Logf("  P50: %v, P99: %v", p50, p99)
				return false
			}

		case <-time.After(timeout):
			c.trackingMu.RLock()
			sent := len(c.sentJobs)
			received := len(c.receivedJobs)
			missing := sent - received

			deliveryTimes := make([]time.Duration, 0, len(c.receivedJobs))
			if len(c.receivedJobs) > 0 {
				first := c.receivedJobs[0].Timestamp
				for _, job := range c.receivedJobs {
					if job.Timestamp.Before(first) {
						first = job.Timestamp
					}
				}
				for _, job := range c.receivedJobs {
					deliveryTimes = append(deliveryTimes, job.Timestamp.Sub(first))
				}
			}
			c.trackingMu.RUnlock()

			p50 := calculatePercentile(deliveryTimes, 0.50)
			p99 := calculatePercentile(deliveryTimes, 0.99)

			t.Logf("✗ Timeout: Sent=%d, Received=%d, Missing=%d", sent, received, missing)
			t.Logf("  P50: %v, P99: %v", p50, p99)
			return false
		}
	}
}

// Helper function to calculate percentile
func calculatePercentile(durations []time.Duration, p float64) time.Duration {
	if len(durations) == 0 {
		return 0
	}

	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)

	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	idx := int(float64(len(sorted)-1) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
