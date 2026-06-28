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
	"sync"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type Consumer struct {
	ID                string
	UUID              string
	Conn              *websocket.Conn
	Topic             string
	LastSentID        int64
	LastDeliveryAckID int64
	MaxInflights      int
	mu                sync.RWMutex
	logger            *zap.Logger
	writeCh           chan []byte
	doneCh            chan struct{}
	ctx               context.Context
	stopOnce          sync.Once
}

func NewConsumer(ctx context.Context, id, uuid, topic string, conn *websocket.Conn, maxInflights int, logger *zap.Logger) *Consumer {
	return &Consumer{
		ID:           id,
		UUID:         uuid,
		Conn:         conn,
		Topic:        topic,
		MaxInflights: maxInflights,
		writeCh:      make(chan []byte, 256),
		doneCh:       make(chan struct{}),
		logger:       logger,
		ctx:          ctx,
	}
}

func (c *Consumer) GetFreeSlots() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	slots := c.MaxInflights - int(c.LastSentID-c.LastDeliveryAckID)
	if slots < 0 {
		return 0
	}
	return slots
}

func (c *Consumer) OnJobSent() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.LastSentID++
	return c.LastSentID
}

func (c *Consumer) UpdateDeliveryAck(lastAckID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if lastAckID > c.LastDeliveryAckID {
		c.LastDeliveryAckID = lastAckID
	}
}

func (c *Consumer) WriteMessage(msg []byte) error {
	select {
	case c.writeCh <- msg:
		return nil
	case <-c.doneCh:
		return websocket.ErrCloseSent
	}
}

func (c *Consumer) StartWriteLoop() {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("panic recovered: %v", zap.Any("panic", r))
		}
	}()

	for {
		select {
		case <-c.ctx.Done():
			return
		case msg, ok := <-c.writeCh:
			if !ok {
				return
			}
			if err := c.Conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					c.logger.Debug("failed to write message", zap.Error(err), zap.String("consumer_id", c.ID))
				}
				return
			}
		}
	}
}

func (c *Consumer) Close() {
	c.stopOnce.Do(func() {
		close(c.doneCh)
		if c.Conn != nil {
			c.Conn.Close()
		}
	})
}
