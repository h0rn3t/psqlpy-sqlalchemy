.PHONY: help docker-up docker-down docker-logs test test-db test-no-db clean install lint format benchmark

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	python -m pip install --upgrade pip wheel
	pip install -e ".[dev]"

docker-up: ## Start PostgreSQL Docker container
	@echo "Starting PostgreSQL Docker container..."
	docker-compose up -d
	@echo "Waiting for PostgreSQL to be ready..."
	@for i in $$(seq 1 30); do \
		if docker exec psqlpy-postgres pg_isready -U postgres >/dev/null 2>&1; then \
			echo "PostgreSQL is ready!"; \
			exit 0; \
		fi; \
		echo "Waiting for PostgreSQL... ($$i/30)"; \
		sleep 1; \
	done; \
	echo "PostgreSQL failed to start within 30 seconds" && exit 1

docker-down: ## Stop PostgreSQL Docker container
	@echo "Stopping PostgreSQL Docker container..."
	docker-compose down

docker-logs: ## Show PostgreSQL Docker container logs
	docker-compose logs -f postgres

test: ## Run all tests (will auto-detect Docker PostgreSQL)
	@echo "Checking if PostgreSQL is available..."
	@if ! docker exec psqlpy-postgres pg_isready -U postgres >/dev/null 2>&1; then \
		echo "PostgreSQL not running, starting Docker container..."; \
		$(MAKE) docker-up; \
		DOCKER_STARTED=1; \
	else \
		echo "PostgreSQL is already running"; \
		DOCKER_STARTED=0; \
	fi; \
	python -m pytest tests/ -v; \
	TEST_EXIT_CODE=$$?; \
	if [ "$$DOCKER_STARTED" = "1" ]; then \
		echo "Stopping Docker container that was started for tests..."; \
		$(MAKE) docker-down; \
	fi; \
	exit $$TEST_EXIT_CODE

test-db: ## Run database tests with Docker PostgreSQL
	@$(MAKE) docker-up
	python -m pytest tests/ -v -k "test_uuid" || ($(MAKE) docker-down && exit 1)
	@$(MAKE) docker-down

test-no-db: ## Run tests without database (skip database tests)
	python -m pytest tests/ -v

test-coverage: ## Run tests with coverage report
	@$(MAKE) docker-up
	python -m pytest tests/ --cov=psqlpy_sqlalchemy --cov-report=html --cov-report=term || ($(MAKE) docker-down && exit 1)
	@$(MAKE) docker-down
	@echo "Coverage report generated in htmlcov/"

lint: ## Run linting
	ruff check psqlpy_sqlalchemy tests

format: ## Format code
	ruff format psqlpy_sqlalchemy tests

format-check: ## Check code formatting
	ruff format --check psqlpy_sqlalchemy tests

typecheck: ## Run type checking
	mypy psqlpy_sqlalchemy

clean: ## Clean up Docker containers and volumes
	docker-compose down -v
	docker system prune -f

dev-setup: install docker-up ## Complete development setup
	@echo "Development environment is ready!"
	@echo "Run 'make test' to run tests with PostgreSQL"
	@echo "Run 'make docker-down' to stop PostgreSQL when done"

benchmark: ## Run performance comparison between psqlpy-sqlalchemy and asyncpg
	@echo "🚀 Starting performance benchmark..."
	@echo "Checking if PostgreSQL is available..."
	@if ! docker exec psqlpy-postgres pg_isready -U postgres >/dev/null 2>&1; then \
		echo "PostgreSQL not running, starting Docker container..."; \
		$(MAKE) docker-up; \
		DOCKER_STARTED=1; \
	else \
		echo "PostgreSQL is already running"; \
		DOCKER_STARTED=0; \
	fi; \
	echo "Ensuring dependencies are installed..."; \
	pip install -e ".[dev]" >/dev/null 2>&1 || echo "Dependencies already installed"; \
	echo "Running performance comparison test..."; \
	python performance_comparison.py; \
	BENCHMARK_EXIT_CODE=$$?; \
	if [ "$$DOCKER_STARTED" = "1" ]; then \
		echo "Stopping Docker container that was started for benchmark..."; \
		$(MAKE) docker-down; \
	fi; \
	if [ $$BENCHMARK_EXIT_CODE -eq 0 ]; then \
		echo "✅ Benchmark completed successfully!"; \
	else \
		echo "❌ Benchmark failed with exit code $$BENCHMARK_EXIT_CODE"; \
	fi; \
	exit $$BENCHMARK_EXIT_CODE
