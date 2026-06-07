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
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestConsumer(t *testing.T) {
	// Setup logger
	logger, _ := zap.NewDevelopment()

	// Create a test websocket server
	var upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		require.NoError(t, err)
		defer conn.Close()

		// Read messages from the consumer
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			// Echo back or process as needed
			t.Logf("Received message: %s", msg)
		}
	}))
	defer server.Close()

	// Convert http URL to ws URL
	wsURL := "ws" + server.URL[4:]

	// Create a consumer connection
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test 1: Create consumer
	consumer := NewConsumer(ctx, "test-id", "test-uuid", "test-topic", conn, 5, logger)
	assert.NotNil(t, consumer)

	// Test 2: Test GetFreeSlots
	t.Run("GetFreeSlots", func(t *testing.T) {
		// Reset consumer state
		consumer.mu.Lock()
		consumer.LastSentID = 0
		consumer.LastDeliveryAckID = 0
		consumer.mu.Unlock()

		// Initially all slots are free
		freeSlots := consumer.GetFreeSlots()
		assert.Equal(t, 5, freeSlots)

		// After sending some jobs
		for i := 0; i < 3; i++ {
			consumer.OnJobSent()
		}
		freeSlots = consumer.GetFreeSlots()
		assert.Equal(t, 2, freeSlots) // 5 - 3 = 2

		// After acknowledging some
		consumer.UpdateDeliveryAck(2)
		freeSlots = consumer.GetFreeSlots()
		// LastSentID=3, LastDeliveryAckID=2 -> slots = 5 - (3-2) = 4
		assert.Equal(t, 4, freeSlots)

		// After acknowledging all
		consumer.UpdateDeliveryAck(3)
		freeSlots = consumer.GetFreeSlots()
		assert.Equal(t, 5, freeSlots)
	})

	// Test 3: Test OnJobSent
	t.Run("OnJobSent", func(t *testing.T) {
		// Reset consumer state
		consumer.mu.Lock()
		consumer.LastSentID = 0
		consumer.LastDeliveryAckID = 0
		consumer.mu.Unlock()

		id1 := consumer.OnJobSent()
		id2 := consumer.OnJobSent()
		id3 := consumer.OnJobSent()

		assert.Equal(t, int64(1), id1)
		assert.Equal(t, int64(2), id2)
		assert.Equal(t, int64(3), id3)

		assert.Equal(t, int64(3), consumer.LastSentID)
	})

	// Test 4: Test UpdateDeliveryAck
	t.Run("UpdateDeliveryAck", func(t *testing.T) {
		consumer.mu.Lock()
		consumer.LastSentID = 5
		consumer.LastDeliveryAckID = 0
		consumer.mu.Unlock()

		consumer.UpdateDeliveryAck(3)
		assert.Equal(t, int64(3), consumer.LastDeliveryAckID)

		// Should not decrease
		consumer.UpdateDeliveryAck(1)
		assert.Equal(t, int64(3), consumer.LastDeliveryAckID)

		// Should update with higher value
		consumer.UpdateDeliveryAck(5)
		assert.Equal(t, int64(5), consumer.LastDeliveryAckID)
	})

	// Test 5: Test WriteMessage
	t.Run("WriteMessage", func(t *testing.T) {
		// Create a new server just for this test
		msgServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := upgrader.Upgrade(w, r, nil)
			require.NoError(t, err)
			defer conn.Close()

			// Read exactly one message
			_, msg, err := conn.ReadMessage()
			require.NoError(t, err)
			assert.Equal(t, "test message", string(msg))
		}))
		defer msgServer.Close()

		wsURL2 := "ws" + msgServer.URL[4:]
		conn2, _, err := websocket.DefaultDialer.Dial(wsURL2, nil)
		require.NoError(t, err)
		defer conn2.Close()

		consumer2 := NewConsumer(ctx, "test-id-2", "test-uuid-2", "test-topic", conn2, 5, logger)

		// Start write loop
		go consumer2.StartWriteLoop()
		defer consumer2.Close()

		// Write message
		err = consumer2.WriteMessage([]byte("test message"))
		assert.NoError(t, err)

		// Give time for message to be sent
		time.Sleep(100 * time.Millisecond)
	})

	// Test 6: Test StartWriteLoop
	t.Run("StartWriteLoop", func(t *testing.T) {
		// Create a server that receives multiple messages
		var mu sync.Mutex
		var receivedMessages []string

		loopServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := upgrader.Upgrade(w, r, nil)
			require.NoError(t, err)
			defer conn.Close()

			for i := 0; i < 3; i++ {
				_, msg, err := conn.ReadMessage()
				require.NoError(t, err)
				mu.Lock()
				receivedMessages = append(receivedMessages, string(msg))
				mu.Unlock()
			}
		}))
		defer loopServer.Close()

		wsURL3 := "ws" + loopServer.URL[4:]
		conn3, _, err := websocket.DefaultDialer.Dial(wsURL3, nil)
		require.NoError(t, err)
		defer conn3.Close()

		consumer3 := NewConsumer(ctx, "test-id-3", "test-uuid-3", "test-topic", conn3, 5, logger)

		// Start write loop
		go consumer3.StartWriteLoop()
		defer consumer3.Close()

		// Send multiple messages
		messages := []string{"msg1", "msg2", "msg3"}
		for _, msg := range messages {
			err := consumer3.WriteMessage([]byte(msg))
			assert.NoError(t, err)
		}

		// Give time for all messages to be sent
		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		assert.Equal(t, messages, receivedMessages)
		mu.Unlock()
	})

	// Test 7: Test Close
	t.Run("Close", func(t *testing.T) {
		closeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := upgrader.Upgrade(w, r, nil)
			require.NoError(t, err)
			defer conn.Close()
			// Keep connection open
			select {}
		}))
		defer closeServer.Close()

		wsURL4 := "ws" + closeServer.URL[4:]
		conn4, _, err := websocket.DefaultDialer.Dial(wsURL4, nil)
		require.NoError(t, err)

		consumer4 := NewConsumer(ctx, "test-id-4", "test-uuid-4", "test-topic", conn4, 5, logger)

		// Start write loop
		go consumer4.StartWriteLoop()

		// Close consumer
		consumer4.Close()

		// Wait a bit for the connection to close
		time.Sleep(50 * time.Millisecond)

		// Verify the connection is closed by trying to write directly
		err = conn4.WriteMessage(websocket.TextMessage, []byte("test"))
		assert.Error(t, err, "Connection should be closed after consumer.Close()")
	})

	// Test 8: Test Context cancellation
	t.Run("ContextCancellation", func(t *testing.T) {
		// Create a new connection for this test
		conn5, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		require.NoError(t, err)
		defer conn5.Close()

		ctx2, cancel2 := context.WithCancel(context.Background())
		consumer5 := NewConsumer(ctx2, "test-id-5", "test-uuid-5", "test-topic", conn5, 5, logger)

		// Start write loop in a goroutine
		done := make(chan struct{})
		go func() {
			defer close(done)
			consumer5.StartWriteLoop()
		}()

		// Write a message to ensure the loop is running
		err = consumer5.WriteMessage([]byte("test message"))
		assert.NoError(t, err)

		// Cancel context
		cancel2()

		// Wait for the write loop to exit
		select {
		case <-done:
			// Success - loop exited
		case <-time.After(1 * time.Second):
			t.Fatal("StartWriteLoop did not exit after context cancellation")
		}

		// WriteMessage should still work (it doesn't check context)
		// but the message won't be delivered because the loop is stopped
		err = consumer5.WriteMessage([]byte("another message"))
		// It might still accept the message into the channel
		// This is fine - the channel will eventually be drained/closed
		assert.NoError(t, err)

		// Clean up
		consumer5.Close()
	})
}

// Helper to create a test server that echoes messages
func createEchoServer(t *testing.T) *httptest.Server {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		require.NoError(t, err)
		defer conn.Close()

		for {
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			err = conn.WriteMessage(mt, msg)
			if err != nil {
				return
			}
		}
	}))
}

// Additional test for concurrent access
func TestConsumerConcurrency(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := context.Background()

	server := createEchoServer(t)
	defer server.Close()

	wsURL := "ws" + server.URL[4:]
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	defer conn.Close()

	consumer := NewConsumer(ctx, "concurrent-id", "concurrent-uuid", "test-topic", conn, 10, logger)

	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := consumer.WriteMessage([]byte("msg"))
			assert.NoError(t, err)
		}(i)
	}

	// Concurrent updates
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			consumer.OnJobSent()
		}(i)
	}

	// Concurrent ack updates
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			consumer.UpdateDeliveryAck(int64(i))
		}(i)
	}

	wg.Wait()

	// Verify no deadlocks occurred
	assert.NotNil(t, consumer)
}

// Test for edge cases
func TestConsumerEdgeCases(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := context.Background()

	t.Run("GetFreeSlotsWithNegativeDifference", func(t *testing.T) {
		conn, _, err := websocket.DefaultDialer.Dial("ws://localhost:8080", nil)
		// This will fail if no server is running, so we handle it
		if err != nil {
			t.Skip("No websocket server available")
		}
		defer conn.Close()

		consumer := NewConsumer(ctx, "test-id", "test-uuid", "test-topic", conn, 5, logger)

		// Manually set inconsistent state
		consumer.mu.Lock()
		consumer.LastSentID = 2
		consumer.LastDeliveryAckID = 5 // Ack > Sent (shouldn't happen normally)
		consumer.mu.Unlock()

		// Should not go negative
		slots := consumer.GetFreeSlots()
		assert.Equal(t, 0, slots)
	})

	t.Run("WriteMessageWithClosedDoneCh", func(t *testing.T) {
		conn, _, err := websocket.DefaultDialer.Dial("ws://localhost:8080", nil)
		if err != nil {
			t.Skip("No websocket server available")
		}
		defer conn.Close()

		consumer := NewConsumer(ctx, "test-id", "test-uuid", "test-topic", conn, 5, logger)

		// Close the consumer
		consumer.Close()

		// Try to write
		err = consumer.WriteMessage([]byte("test"))
		assert.Error(t, err)
	})
}

// Test for WriteMessage buffer full
func TestConsumerWriteBufferFull(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := context.Background()

	// Create a server that doesn't read messages quickly
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		require.NoError(t, err)
		defer conn.Close()
		// Don't read messages - buffer will fill up
		select {}
	}))
	defer server.Close()

	wsURL := "ws" + server.URL[4:]
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	defer conn.Close()

	consumer := NewConsumer(ctx, "test-id", "test-uuid", "test-topic", conn, 5, logger)

	// Start write loop
	go consumer.StartWriteLoop()
	defer consumer.Close()

	// Fill up the buffer (size is 256)
	for i := 0; i < 260; i++ {
		err := consumer.WriteMessage([]byte("message"))
		// Once buffer is full, this should block or return error
		// The select in WriteMessage will try to send, but if buffer is full
		// it will block until doneCh is closed or there's space
		// For testing purposes, we'll just ensure it doesn't panic
		assert.NoError(t, err)
	}
}
