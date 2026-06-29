BINARY_NAME=cueproxy
BUILD_DIR=.
VERSION?=1
TAG?=latest

.PHONY: build run clean help docker-build docker-build-local test test-sum

build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
		-ldflags="-s -w -extldflags '-static' -X main.version=$(VERSION)" \
		-trimpath \
		-o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/proxy
	@echo "✅ Static binary built: $(BUILD_DIR)/$(BINARY_NAME)"
	@file $(BUILD_DIR)/$(BINARY_NAME)
	@ls -lh $(BUILD_DIR)/$(BINARY_NAME)


# Build Docker image using Dockerfile (full build inside container)
docker-build:
	@echo "Building Docker image with tag: $(TAG)..."
	docker build -t $(BINARY_NAME):$(TAG) .
	@echo "✅ Docker image built: $(BINARY_NAME):$(TAG)"
	@docker images $(BINARY_NAME):$(TAG) --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}"

# Original test target with gotestsum
test-sum:
	@echo "🧪 Running unit tests with gotestsum..."
	@gotestsum --format testname -- -p=1 -coverprofile=coverage.unit.cov -coverpkg=./internal/api,./internal/app,./internal/cluster,./internal/config,./internal/model ./...
	@echo ""
	@echo "✅ Unit tests complete"

# Standard test target without gotestsum
test:
	@echo "🧪 Running unit tests with coverage..."
	@go test -v -p=1 -coverprofile=coverage.unit.cov -coverpkg=./internal/api,./internal/app,./internal/cluster,./internal/config,./internal/model ./... 2>&1 | while IFS= read -r line; do \
		if echo "$$line" | grep -q "^=== RUN"; then \
			test_name=$$(echo "$$line" | sed 's/^=== RUN[[:space:]]*//'); \
			printf "\r⏳ %-60s" "$$test_name"; \
		elif echo "$$line" | grep -q "^--- PASS"; then \
			test_name=$$(echo "$$line" | sed 's/^--- PASS:[[:space:]]*//'); \
			printf "\r\033[32m%-60s\033[0m\n" "$$test_name"; \
		elif echo "$$line" | grep -q "^--- FAIL"; then \
			test_name=$$(echo "$$line" | sed 's/^--- FAIL:[[:space:]]*//'); \
			printf "\r\033[31m❌ %-60s\033[0m\n" "$$test_name"; \
		fi; \
	done
	@echo ""
	@echo "✅ Unit tests complete"

certs:
	@echo "Generating TLS certificates..."
	@chmod +x scripts/generate-tls.sh
	@scripts/generate-tls.sh
	@echo "Certificates generated in ./certs/"

# Clean build artifacts and test data
clean:
	@echo "Cleaning..."
	@rm -rf ./certs
	@rm -f coverage.unit.cov coverage.html
	@rm -f $(BINARY_NAME)
	@go clean -testcache
	@echo "Clean complete"

.PHONY: license
license: ## Add license headers to all Go files
	@echo "Adding license headers..."
	@addlicense -c "M. Javani" -l apache .

.PHONY: license-check
license-check: ## Check if all files have license headers
	@echo "Checking license headers..."
	@addlicense -c "M. Javani" -l apache -check . || (echo "Some files are missing license headers. Run 'make license' to fix." && exit 1)

.PHONY: license-update
license-update: ## Update license headers (with current year)
	@echo "Updating license headers..."
	@addlicense -c "M. Javani" -l apache -y `date +%Y` .

help:
	@echo "Available targets:"
	@echo "  make build                    - Build static Linux binary"
	@echo "  make docker-build             - Build Docker image (full build in container)"
	@echo "  make test                     - Run unit tests with coverage (standard go test)"
	@echo "  make test-sum                 - Run unit tests with gotestsum"
	@echo "  make certs                    - Generate TLS certificates"
	@echo "  make clean                    - Clean build artifacts"
	@echo "  make license                  - Add license headers"
	@echo "  make license-check            - Check license headers"
	@echo "  make license-update           - Update license headers"
	@echo "  make help                     - Show this help"