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

//go:build integration

package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================
// Probe cluster without proxy
// ============================================
func TestProbeCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	domain := "localhost"
	integrationDir, err := os.Getwd()
	caCertDir := filepath.Join(integrationDir, "cert")

	cluster, _, logger := SetupTestCluster(t, ctx, caCertDir, domain)
	defer cluster.Terminate(ctx)

	node := cluster.Nodes[0]
	targetAddr := fmt.Sprintf("%s:%s", domain, node.APIPort)
	logger.Sugar().Infof("sending prob requests to %s", targetAddr)
	res, err := ProbeNode(ctx, targetAddr)
	require.NoError(t, err)
	require.True(t, res.HealthOK)
	require.NotEmpty(t, res.Cluster.LeaderID)
	require.Contains(t, res.Cluster.Members.Voters, node.Name)

}

// ============================================
// Limited Volume Test
// ============================================
func TestLimitedVolumeEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	domain := "localhost"
	integrationDir, _ := os.Getwd()
	caCertDir := filepath.Join(integrationDir, "cert")

	// ============================================
	// Setup Cluster
	// ============================================
	logger, cluster, client, _ := SetupFullTestSystem(t, ctx, caCertDir, domain)
	defer cluster.Terminate(ctx)

	client.AddTopic("high-volume")

	// ============================================
	// High Volume Throughput
	// ============================================
	client.ClearTracking()

	// Create consumers
	const NUM_CONSUMERS int = 200
	consumers := make([]int, NUM_CONSUMERS)
	for i := range NUM_CONSUMERS {
		consumers[i] = client.AddConsumer("high-volume")
	}
	client.Start()

	const NUM_WORKERS = 50
	const NUM_JOBS = 1000

	var wg sync.WaitGroup
	jobs := make(chan int, NUM_JOBS)

	// Start worker pool
	for range NUM_WORKERS {
		wg.Go(func() {
			for i := range jobs {
				if err := client.AddJob(
					fmt.Sprintf("bulk-%d", i),
					"high-volume",
					fmt.Appendf(nil, `{"id":%d}`, i),
				); err != nil {
					t.Logf("Job %d failed: %v", i, err)
				}
			}
		})
	}

	// Send jobs
	start := time.Now()
	for i := range NUM_JOBS {
		jobs <- i
	}
	close(jobs)

	// Wait for all jobs to be delivered
	client.AssertAllJobsReceivedE2E(t, 60*time.Second)
	wg.Wait()

	elapsed := time.Since(start).Seconds()

	// ============================================
	// Cleanup
	// ============================================
	for _, consumerID := range consumers {
		client.StopConsumer(consumerID)
	}

	logger.Sync()
	logger.Sugar().Infof("Processed %d jobs in %.2fs", NUM_JOBS, elapsed)
	logger.Sugar().Infof("Throughput: %.2f jobs/sec", float64(NUM_JOBS)/elapsed)

}

// ============================================
// Limited Volume Producers Test
// ============================================
func TestLimitedVolumeProducers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	domain := "localhost"
	integrationDir, _ := os.Getwd()
	caCertDir := filepath.Join(integrationDir, "cert")

	logger, cluster, client, _ := SetupFullTestSystem(t, ctx, caCertDir, domain)
	defer cluster.Terminate(ctx)

	client.AddTopic("high-volume")

	// ============================================
	// High Volume Throughput
	// ============================================
	client.ClearTracking()

	start := time.Now()

	const NUM_JOBS int = 4000
	// Send 100 jobs
	var wg sync.WaitGroup
	sem := make(chan struct{}, 20)
	for i := range NUM_JOBS {
		wg.Add(1)
		sem <- struct{}{}

		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			client.AddJob(
				fmt.Sprintf("bulk-%d", i),
				"high-volume",
				fmt.Appendf(nil, `{"id":%d}`, i),
			)
		}(i)
	}

	success := client.WaitForProducerResponses(t, 60*time.Second)
	assert.True(t, success)
	elapsed := time.Since(start).Seconds()

	logger.Sugar().Infof("Processed %d jobs in %v", NUM_JOBS, elapsed)
	logger.Sugar().Infof("Throughput: %.2f jobs/sec", float64(NUM_JOBS)/elapsed)

	wg.Wait()

}
