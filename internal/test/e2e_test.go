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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ============================================
// Basic Full Flow Test
// ============================================
func TestFullSystemFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	domain := "localhost"
	integrationDir, _ := os.Getwd()
	caCertDir := filepath.Join(integrationDir, "cert")

	logger, cluster, client, _ := SetupFullTestSystem(t, ctx, caCertDir, domain)
	defer cluster.Terminate(ctx)

	// ============================================
	// Topic Management
	// ============================================
	err := client.AddTopic("orders")
	require.NoError(t, err)

	err = client.AddTopic("shipments")
	require.NoError(t, err)

	// Adding duplicate topic should not error (idempotent)
	err = client.AddTopic("orders")
	require.Error(t, err)

	logger.Info("Add topics test passed")

	// ============================================
	// Single Consumer Basic Flow
	// ============================================
	client.ClearTracking()

	topic := "orders"
	consumerID := client.AddConsumer(topic)
	t.Logf("Subscribed consumer %d to topics: %s", consumerID, topic)
	client.Start()

	// Push jobs
	client.AddJob("order-1", "orders", []byte(`{"item":"book"}`))
	client.AddJob("order-2", "orders", []byte(`{"item":"pen"}`))

	// Verify
	client.AssertAllJobsReceivedE2E(t, 5*time.Second)

	received := client.GetReceivedJobsByConsumer(consumerID)
	require.Len(t, received, 2)
	require.Equal(t, "order-1", received[0].JobID)
	require.Equal(t, "order-2", received[1].JobID)

	time.Sleep(500 * time.Millisecond)
	client.StopConsumer(consumerID)
}

// ============================================
// Topic Isolation Test
// ============================================
func TestTopicIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	domain := "localhost"
	integrationDir, _ := os.Getwd()
	caCertDir := filepath.Join(integrationDir, "cert")

	_, cluster, client, _ := SetupFullTestSystem(t, ctx, caCertDir, domain)
	defer cluster.Terminate(ctx)

	// ============================================
	// Add Topics
	// ============================================
	err := client.AddTopic("orders")
	require.NoError(t, err)

	err = client.AddTopic("shipments")
	require.NoError(t, err)

	// ============================================
	// Topic Isolation
	// ============================================
	client.ClearTracking()

	consumer1 := client.AddConsumer("orders")
	consumer2 := client.AddConsumer("shipments")

	client.Start()

	client.AddJob("order-only", "orders", nil)
	client.AddJob("ship-only", "shipments", nil)

	time.Sleep(2 * time.Second)

	ordersConsumer := client.GetReceivedJobsByConsumer(consumer1)
	shipmentsConsumer := client.GetReceivedJobsByConsumer(consumer2)

	require.Len(t, ordersConsumer, 1)
	require.Equal(t, "order-only", ordersConsumer[0].JobID)

	require.Len(t, shipmentsConsumer, 1)
	require.Equal(t, "ship-only", shipmentsConsumer[0].JobID)
}

// ============================================
// Multiple Consumers
// ============================================
func TestMultiConsumers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	domain := "localhost"
	integrationDir, _ := os.Getwd()
	caCertDir := filepath.Join(integrationDir, "cert")

	_, cluster, client, _ := SetupFullTestSystem(t, ctx, caCertDir, domain)
	defer cluster.Terminate(ctx)

	// ============================================
	// Add Topics
	// ============================================
	err := client.AddTopic("orders")
	require.NoError(t, err)

	err = client.AddTopic("shipments")
	require.NoError(t, err)

	// ============================================
	// Multiple Consumers on Same Topic
	// ============================================
	client.ClearTracking()

	// Add 3 consumers
	consumers := make([]int, 3)
	for i := range 3 {
		consumers[i] = client.AddConsumer("orders")
	}
	client.Start()

	// Push 9 jobs
	for i := range 9 {
		client.AddJob(
			fmt.Sprintf("multi-job-%d", i),
			"orders",
			fmt.Appendf(nil, `{"seq":%d}`, i),
		)
	}

	client.AssertAllJobsReceivedE2E(t, 10*time.Second)

	// Cleanup
	for _, consumerID := range consumers {
		client.StopConsumer(consumerID)
	}
}
