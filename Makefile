.PHONY: install test lint format clean dev setup help

# Default target
help:
	@echo "Available commands:"
	@echo "  install     - Install project dependencies"
	@echo "  dev         - Install development dependencies"
	@echo "  test        - Run tests"
	@echo "  test-cov    - Run tests with coverage"
	@echo "  lint        - Run linting (ruff + mypy)"
	@echo "  format      - Format code (black + ruff fix)"
	@echo "  clean       - Clean temporary files"
	@echo "  setup       - Setup development environment"
	@echo "  start-spark - Start Spark History Server"
	@echo "  start-mcp   - Start MCP Server"
	@echo "  start-inspector - Start MCP Inspector"

install:
	uv sync

dev: install
	uv sync --group dev

test:
	uv run pytest

test-cov:
	uv run pytest --cov=. --cov-report=html --cov-report=term-missing

lint:
	@echo "Running ruff..."
	uv run ruff check .
	@echo "Running mypy..."
	uv run mypy *.py --ignore-missing-imports

format:
	@echo "Running black..."
	uv run black .
	@echo "Running ruff fix..."
	uv run ruff check --fix .

clean:
	rm -rf .pytest_cache/ .coverage htmlcov/ dist/ build/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

setup: dev
	chmod +x *.sh

# MCP Development shortcuts
start-spark:
	@echo "Starting Spark History Server..."
	./start_local_spark_history.sh

start-mcp:
	@echo "Starting MCP Server..."
	uv run main.py

start-inspector:
	@echo "Starting MCP Inspector..."
	DANGEROUSLY_OMIT_AUTH=true npx @modelcontextprotocol/inspector

# Validation
validate: lint test
	@echo "✅ All validations passed!"