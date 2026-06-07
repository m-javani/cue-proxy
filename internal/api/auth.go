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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type Role string

const (
	RoleAdmin      Role = "admin"
	RoleProducer   Role = "producer"
	RoleConsumer   Role = "consumer"
	RoleMonitoring Role = "monitoring"
)

type AuthToken struct {
	Token string `yaml:"token"`
	Role  string `yaml:"role"`
}

type AuthConfig struct {
	Tokens []AuthToken `yaml:"tokens"`
}

type Authenticator struct {
	mu          sync.RWMutex
	tokens      map[string]Role // token -> role
	logger      *zap.Logger
	authFile    string
	stopWatcher chan struct{}
	closeOnce   sync.Once
}

func NewAuthenticator(authFile string, logger *zap.Logger) (*Authenticator, error) {
	auth := &Authenticator{
		tokens:      make(map[string]Role),
		logger:      logger,
		authFile:    authFile,
		stopWatcher: make(chan struct{}),
	}

	if err := auth.loadTokens(); err != nil {
		return nil, fmt.Errorf("failed to load auth file: %w", err)
	}

	go auth.watchFile()
	return auth, nil
}

func (a *Authenticator) loadTokens() error {
	data, err := os.ReadFile(a.authFile)
	if err != nil {
		return err
	}

	var config AuthConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return err
	}

	newTokens := make(map[string]Role)
	for _, t := range config.Tokens {
		newTokens[t.Token] = Role(t.Role)
	}

	a.mu.Lock()
	a.tokens = newTokens
	a.mu.Unlock()

	a.logger.Info("auth tokens reloaded", zap.Int("count", len(newTokens)))
	return nil
}

func (a *Authenticator) watchFile() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		a.logger.Error("failed to create file watcher", zap.Error(err))
		return
	}
	defer watcher.Close()

	if err := watcher.Add(a.authFile); err != nil {
		a.logger.Error("failed to watch auth file", zap.Error(err))
		return
	}

	// Also watch the directory for renames
	if err := watcher.Add(getDir(a.authFile)); err != nil {
		a.logger.Warn("failed to watch auth directory", zap.Error(err))
	}

	for {
		select {
		case <-a.stopWatcher:
			return
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			a.logger.Info("fsnotify event",
				zap.String("name", event.Name),
				zap.String("op", event.Op.String()),
			)
			if event.Op&(fsnotify.Write|
				fsnotify.Create|
				fsnotify.Rename|
				fsnotify.Remove) != 0 {
				time.Sleep(100 * time.Millisecond) // debounce
				if err := a.loadTokens(); err != nil {
					a.logger.Error("failed to reload tokens", zap.Error(err))
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			a.logger.Error("file watcher error", zap.Error(err))
		}
	}
}

func getDir(path string) string {
	return filepath.Dir(path)
}

func (a *Authenticator) Validate(token string) (Role, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	role, ok := a.tokens[token]
	return role, ok
}

func (a *Authenticator) ValidateRole(token string, required Role) bool {
	role, ok := a.Validate(token)
	if !ok {
		return false
	}

	// Admin has access to everything
	if role == RoleAdmin {
		return true
	}

	return role == required
}

func (a *Authenticator) Close() {
	a.closeOnce.Do(func() {
		close(a.stopWatcher)
	})
}
