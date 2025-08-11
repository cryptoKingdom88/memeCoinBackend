# Backend WebSocket Service Makefile

.PHONY: help build run test clean docker-build docker-run docker-stop docs start stop status health

# Default target
help: ## Show this help message
	@echo "Backend WebSocket Service - Available commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

# Development commands
build: ## Build the application
	@echo "Building application..."
	@go mod tidy
	@swag init -g main.go --output ./docs || echo "Warning: swag not found, skipping docs generation"
	@go build -o backendService .
	@echo "Build completed successfully"

run: build ## Build and run the application
	@echo "Starting application..."
	@./backendService

test: ## Run tests
	@echo "Running tests..."
	@go test -v ./...

test-coverage: ## Run tests with coverage
	@echo "Running tests with coverage..."
	@go test -v -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

clean: ## Clean build artifacts
	@echo "Cleaning build artifacts..."
	@rm -f backendService
	@rm -f coverage.out coverage.html
	@rm -rf docs/docs.go docs/swagger.json docs/swagger.yaml
	@echo "Clean completed"

# Script-based commands
start: ## Start the service using startup script
	@./scripts/start.sh

stop: ## Stop the service using shutdown script
	@./scripts/stop.sh

status: ## Check service status
	@./scripts/stop.sh --status

health: ## Perform health check
	@./scripts/stop.sh --health

# Docker commands
docker-build: ## Build Docker image
	@echo "Building Docker image..."
	@docker build -t backend-websocket-service .
	@echo "Docker image built successfully"

docker-run: ## Run with Docker Compose (development)
	@echo "Starting services with Docker Compose..."
	@docker-compose up -d
	@echo "Services started. Access at http://localhost:8080"

docker-run-prod: ## Run with Docker Compose (production)
	@echo "Starting services with Docker Compose (production)..."
	@docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
	@echo "Production services started"

docker-stop: ## Stop Docker Compose services
	@echo "Stopping Docker Compose services..."
	@docker-compose down
	@echo "Services stopped"

docker-logs: ## Show Docker Compose logs
	@docker-compose logs -f backend-websocket

docker-clean: ## Clean Docker resources
	@echo "Cleaning Docker resources..."
	@docker-compose down -v --remove-orphans
	@docker system prune -f
	@echo "Docker cleanup completed"

# Documentation
docs: ## Generate Swagger documentation
	@echo "Generating Swagger documentation..."
	@swag init -g main.go --output ./docs
	@echo "Documentation generated successfully"

docs-serve: docs ## Generate and serve documentation
	@echo "Starting documentation server..."
	@echo "Documentation will be available at http://localhost:8080/docs"
	@make run

# Development helpers
dev-setup: ## Set up development environment
	@echo "Setting up development environment..."
	@go mod tidy
	@go install github.com/swaggo/swag/cmd/swag@latest
	@cp .env.example .env
	@echo "Development environment setup completed"
	@echo "Please edit .env file with your configuration"

lint: ## Run linter
	@echo "Running linter..."
	@golangci-lint run || echo "Warning: golangci-lint not found"

format: ## Format code
	@echo "Formatting code..."
	@go fmt ./...
	@echo "Code formatted"

# Kafka helpers
kafka-topics: ## List Kafka topics (requires running Kafka)
	@echo "Listing Kafka topics..."
	@docker exec -it $$(docker-compose ps -q kafka) kafka-topics --bootstrap-server localhost:9092 --list

kafka-create-topics: ## Create required Kafka topics
	@echo "Creating Kafka topics..."
	@docker exec -it $$(docker-compose ps -q kafka) kafka-topics --bootstrap-server localhost:9092 --create --topic token-info --partitions 3 --replication-factor 1 --if-not-exists
	@docker exec -it $$(docker-compose ps -q kafka) kafka-topics --bootstrap-server localhost:9092 --create --topic trade-info --partitions 3 --replication-factor 1 --if-not-exists
	@docker exec -it $$(docker-compose ps -q kafka) kafka-topics --bootstrap-server localhost:9092 --create --topic aggregate-info --partitions 3 --replication-factor 1 --if-not-exists
	@echo "Kafka topics created"

# Monitoring
monitor: ## Show service metrics and logs
	@echo "Monitoring service..."
	@echo "=== Service Status ==="
	@make status || true
	@echo ""
	@echo "=== Health Check ==="
	@make health || true
	@echo ""
	@echo "=== Recent Logs (if running with Docker) ==="
	@docker-compose logs --tail=20 backend-websocket || echo "Not running with Docker Compose"