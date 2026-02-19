.PHONY: help install install-dev test lint format clean

help:  ## Show this help message
	@echo 'Usage: make [target]'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

install:  ## Install package
	pip install -e .

install-dev:  ## Install package with dev dependencies
	pip install -e ".[dev]"
	pre-commit install

test:  ## Run tests with coverage
	pytest

test-unit:  ## Run unit tests only
	pytest -m "not integration"

test-integration:  ## Run integration tests only
	pytest -m integration

lint:  ## Run linters
	black --check .
	ruff check .
	mypy dr_sync

format:  ## Format code with black and ruff
	black .
	ruff check --fix .

clean:  ## Clean build artifacts
	rm -rf build dist *.egg-info .pytest_cache .coverage htmlcov .mypy_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
