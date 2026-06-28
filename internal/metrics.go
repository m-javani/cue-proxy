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

package internal

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// Gateway Metrics
// =============================================================================

// ClusterMetrics caches metric references for cluster-level operations
type ClusterMetrics struct {
	// Connection
	connectionOpenedTotal   prometheus.Counter
	connectionAcceptedTotal prometheus.Counter
	connectionRejectedTotal prometheus.Counter
	connectionErrorTotal    prometheus.Counter
	connectionDroppedTotal  prometheus.Counter

	// Request Response
	messageSentTotal     prometheus.Counter
	messageReceivedTotal prometheus.Counter
	bytesSentTotal       prometheus.Counter
	bytesReceivedTotal   prometheus.Counter
	sendErrorTotal       prometheus.Counter
	receiveErrorTotal    prometheus.Counter

	// Condition
	leaderChangedTotal prometheus.Counter

	// WAL
	walFlushTotal       prometheus.Counter
	lastAppliedWalIndex prometheus.Gauge
}

var (
	clusterMetricsInstance *ClusterMetrics
	clusterMetricsOnce     sync.Once
)

// GetClusterMetrics returns the singleton ClusterMetrics instance
func GetClusterMetrics() *ClusterMetrics {
	clusterMetricsOnce.Do(func() {
		clusterMetricsInstance = &ClusterMetrics{
			// Connection
			connectionOpenedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_connection_opened_total",
				Help: "Total number of connections opened",
			}),
			connectionAcceptedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_connection_accepted_total",
				Help: "Total number of connections accepted",
			}),
			connectionRejectedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_connection_rejected_total",
				Help: "Total number of connections rejected",
			}),
			connectionErrorTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_connection_error_total",
				Help: "Total number of connection errors",
			}),
			connectionDroppedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_connection_dropped_total",
				Help: "Total number of connections dropped",
			}),

			// Request Response
			messageSentTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_message_sent_total",
				Help: "Total number of messages sent",
			}),
			messageReceivedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_message_received_total",
				Help: "Total number of messages received",
			}),
			bytesSentTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_bytes_sent_total",
				Help: "Total bytes sent",
			}),
			bytesReceivedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_bytes_received_total",
				Help: "Total bytes received",
			}),
			sendErrorTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_send_error_total",
				Help: "Total number of send errors",
			}),
			receiveErrorTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_receive_error_total",
				Help: "Total number of receive errors",
			}),

			// Condition
			leaderChangedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_leader_changed_total",
				Help: "Total number of leadership changes",
			}),

			walFlushTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "cluster_wal_flush_total",
				Help: "Total number of wal flushes",
			}),
			lastAppliedWalIndex: promauto.NewGauge(
				prometheus.GaugeOpts{
					Name: "cluster_last_applied_wal_index",
					Help: "Current raft last index applied to state",
				},
			),
		}
	})
	return clusterMetricsInstance
}

// Connection methods
func (m *ClusterMetrics) ConnectionOpened() {
	m.connectionOpenedTotal.Inc()
}

func (m *ClusterMetrics) ConnectionAccepted() {
	m.connectionAcceptedTotal.Inc()
}

func (m *ClusterMetrics) ConnectionRejected() {
	m.connectionRejectedTotal.Inc()
}

func (m *ClusterMetrics) ConnectionError() {
	m.connectionErrorTotal.Inc()
}

func (m *ClusterMetrics) ConnectionDropped() {
	m.connectionDroppedTotal.Inc()
}

// Request/Response methods
func (m *ClusterMetrics) MessageSent() {
	m.messageSentTotal.Inc()
}

func (m *ClusterMetrics) MessageReceived() {
	m.messageReceivedTotal.Inc()
}

func (m *ClusterMetrics) AddBytesSent(bytes uint64) {
	m.bytesSentTotal.Add(float64(bytes))
}

func (m *ClusterMetrics) AddBytesReceived(bytes uint64) {
	m.bytesReceivedTotal.Add(float64(bytes))
}

func (m *ClusterMetrics) SendError() {
	m.sendErrorTotal.Inc()
}

func (m *ClusterMetrics) ReceiveError() {
	m.receiveErrorTotal.Inc()
}

// Condition methods
func (m *ClusterMetrics) LeaderChanged() {
	m.leaderChangedTotal.Inc()
}

// =============================================================================
// ProxyApi Metrics
// =============================================================================
type ApiMetrics struct {
	// HTTP metrics
	HTTPRequestsTotal   *prometheus.CounterVec
	HTTPRequestDuration *prometheus.HistogramVec

	// WebSocket metrics
	WebSocketConnections prometheus.Gauge

	// Job metrics - per topic
	JobsPushedPerTopic  *prometheus.CounterVec
	JobsAckedPerTopic   *prometheus.CounterVec
	JobsDroppedPerTopic *prometheus.CounterVec

	// Auth metrics
	AuthFailuresTotal prometheus.Counter
}

var metricsInstance *ApiMetrics

func GetApiMetrics() *ApiMetrics {
	if metricsInstance == nil {
		metricsInstance = &ApiMetrics{
			HTTPRequestsTotal: promauto.NewCounterVec(
				prometheus.CounterOpts{
					Name: "proxy_http_requests_total",
					Help: "Total HTTP requests",
				},
				[]string{"method", "endpoint", "status"},
			),
			HTTPRequestDuration: promauto.NewHistogramVec(
				prometheus.HistogramOpts{
					Name:    "proxy_http_request_duration_seconds",
					Help:    "HTTP request duration",
					Buckets: prometheus.DefBuckets,
				},
				[]string{"method", "endpoint"},
			),
			WebSocketConnections: promauto.NewGauge(
				prometheus.GaugeOpts{
					Name: "proxy_websocket_connections",
					Help: "Current WebSocket connections",
				},
			),
			JobsPushedPerTopic: promauto.NewCounterVec(
				prometheus.CounterOpts{
					Name: "proxy_jobs_pushed_per_topic",
					Help: "Total jobs pushed to consumers per topic",
				},
				[]string{"topic"},
			),
			JobsAckedPerTopic: promauto.NewCounterVec(
				prometheus.CounterOpts{
					Name: "proxy_jobs_acked_per_topic",
					Help: "Total jobs acknowledged by consumers per topic",
				},
				[]string{"topic"},
			),
			JobsDroppedPerTopic: promauto.NewCounterVec(
				prometheus.CounterOpts{
					Name: "proxy_jobs_dropped_per_topic",
					Help: "Total jobs dropped per topic",
				},
				[]string{"topic"},
			),
			AuthFailuresTotal: promauto.NewCounter(
				prometheus.CounterOpts{
					Name: "proxy_auth_failures_total",
					Help: "Total authentication/authorization failures",
				},
			),
		}
	}
	return metricsInstance
}

// Job metrics methods
func (m *ApiMetrics) JobsPushed(topic string) {
	m.JobsPushedPerTopic.WithLabelValues(topic).Inc()
}

func (m *ApiMetrics) JobsAcked(topic string) {
	m.JobsAckedPerTopic.WithLabelValues(topic).Inc()
}

func (m *ApiMetrics) JobsDropped(topic string) {
	m.JobsDroppedPerTopic.WithLabelValues(topic).Inc()
}

// Auth metrics methods
func (m *ApiMetrics) AuthFailure() {
	m.AuthFailuresTotal.Inc()
}
