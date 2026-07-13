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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewAuthenticator(t *testing.T) {
	logger := zap.NewNop()

	t.Run("successful load", func(t *testing.T) {
		tmpFile := createTempAuthFile(t, `tokens:
  - token: "admin123"
    role: "admin"
  - token: "prod456"
    role: "producer"`)
		defer os.Remove(tmpFile)

		auth, err := NewAuthenticator(tmpFile, logger)
		require.NoError(t, err)
		defer auth.Close()

		role, ok := auth.GetRole("admin123")
		assert.True(t, ok)
		assert.Equal(t, RoleAdmin, role)
	})

	t.Run("invalid file", func(t *testing.T) {
		auth, err := NewAuthenticator("/nonexistent/file.yaml", logger)
		assert.Error(t, err)
		assert.Nil(t, auth)
	})
}

func TestAuthenticator_Validate(t *testing.T) {
	logger := zap.NewNop()
	tmpFile := createTempAuthFile(t, `tokens:
  - token: "admin-token"
    role: "admin"
  - token: "producer-token"
    role: "producer"
  - token: "consumer-token"
    role: "consumer"`)
	defer os.Remove(tmpFile)

	auth, err := NewAuthenticator(tmpFile, logger)
	require.NoError(t, err)
	defer auth.Close()

	tests := []struct {
		name     string
		token    string
		wantRole Role
		wantOk   bool
	}{
		{"valid admin", "admin-token", RoleAdmin, true},
		{"valid producer", "producer-token", RoleProducer, true},
		{"valid consumer", "consumer-token", RoleConsumer, true},
		{"invalid token", "invalid", "", false},
		{"empty token", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			role, ok := auth.GetRole(tt.token)
			assert.Equal(t, tt.wantOk, ok)
			assert.Equal(t, tt.wantRole, role)
		})
	}
}

func TestAuthenticator_ValidateRole(t *testing.T) {
	logger := zap.NewNop()
	tmpFile := createTempAuthFile(t, `tokens:
  - token: "admin-token"
    role: "admin"
  - token: "producer-token"
    role: "producer"
  - token: "consumer-token"
    role: "consumer"`)
	defer os.Remove(tmpFile)

	auth, err := NewAuthenticator(tmpFile, logger)
	require.NoError(t, err)
	defer auth.Close()

	tests := []struct {
		name     string
		token    string
		required Role
		want     bool
	}{
		{"admin can access admin", "admin-token", RoleAdmin, true},
		{"admin can access producer", "admin-token", RoleProducer, true},
		{"admin can access consumer", "admin-token", RoleConsumer, true},
		{"producer can access producer", "producer-token", RoleProducer, true},
		{"producer cannot access consumer", "producer-token", RoleConsumer, false},
		{"consumer can access consumer", "consumer-token", RoleConsumer, true},
		{"consumer cannot access producer", "consumer-token", RoleProducer, false},
		{"invalid token", "invalid", RoleAdmin, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := auth.ValidateRole(tt.token, tt.required)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestAuthenticator_Reload(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	tmpFile := createTempAuthFile(t, `
tokens:
  - token: old-token
    role: consumer
`)

	auth, err := NewAuthenticator(tmpFile, logger)
	require.NoError(t, err)
	defer auth.Close()

	role, ok := auth.GetRole("old-token")
	require.True(t, ok)
	require.Equal(t, RoleConsumer, role)

	time.Sleep(100 * time.Millisecond)

	err = os.WriteFile(tmpFile, []byte(`
tokens:
  - token: new-token
    role: admin
`), 0644)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		role, ok := auth.GetRole("new-token")
		return ok && role == RoleAdmin
	}, 2*time.Second, 50*time.Millisecond)

	_, ok = auth.GetRole("old-token")
	assert.False(t, ok)
}

func TestAuthenticator_Close(t *testing.T) {
	logger := zap.NewNop()
	tmpFile := createTempAuthFile(t, `tokens:
  - token: "test"
    role: "admin"`)
	defer os.Remove(tmpFile)

	auth, err := NewAuthenticator(tmpFile, logger)
	require.NoError(t, err)

	// Close should not panic
	auth.Close()

	// Double close should not panic
	auth.Close()
}

func TestGetDir(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"/path/to/file.yaml", "/path/to"},
		{"file.yaml", "."},
		{"/file.yaml", "/"},
		{"path/to/file.yaml", "path/to"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := getDir(tt.path)
			assert.Equal(t, tt.want, got)
		})
	}
}

// Helper function to create a temporary auth file
func createTempAuthFile(t *testing.T, content string) string {
	t.Helper()
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "auth.yaml")
	err := os.WriteFile(tmpFile, []byte(content), 0644)
	require.NoError(t, err)
	return tmpFile
}
