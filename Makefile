.PHONY: help install test test-cov test-contract-fake test-contract-emulator lint format typecheck check all clean build publish bump-patch bump-minor bump-major

PYTHON := poetry run python
PYTEST := poetry run pytest
RUFF := poetry run ruff
MYPY := poetry run mypy

# Default target
help:
	@echo "Available targets:"
	@echo "  install              Install dependencies"
	@echo "  test                 Run unit tests"
	@echo "  test-cov             Run tests with coverage"
	@echo "  test-contract-fake   Run contract tests against fake"
	@echo "  test-contract-emulator Run contract tests against emulator"
	@echo "  lint                 Run ruff linter"
	@echo "  format               Run ruff formatter"
	@echo "  typecheck            Run mypy type checker"
	@echo "  check                Run all checks (lint + typecheck)"
	@echo "  all                  Run all checks and tests"
	@echo "  clean                Remove cache files"
	@echo "  build                Build package"
	@echo "  publish              Publish to PyPI"
	@echo "  bump-patch           Bump patch version (0.0.x)"
	@echo "  bump-minor           Bump minor version (0.x.0)"
	@echo "  bump-major           Bump major version (x.0.0)"

# Dependencies
install:
	poetry install --with dev,async

# Testing
test:
	$(PYTEST) tests/ -v --ignore=tests/contract

test-cov:
	$(PYTEST) tests/ -v --ignore=tests/contract --cov=fake_firestore --cov-report=term-missing --cov-report=html

test-contract-fake:
	FIRESTORE_BACKEND=fake $(PYTEST) tests/contract -v

test-contract-emulator:
	./scripts/run_contract_emulator.sh

# Linting & Formatting
lint:
	$(RUFF) check fake_firestore

format:
	$(RUFF) format fake_firestore tests
	$(RUFF) check --fix fake_firestore tests

typecheck:
	$(MYPY) fake_firestore

check: lint typecheck

# Run everything
all: format typecheck test test-contract-fake

# Cleanup
clean:
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .ruff_cache
	rm -rf htmlcov
	rm -rf dist
	rm -rf .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

# Build & Publish
build: clean
	poetry build

publish: build
	poetry publish

# Version bumping
bump-patch:
	poetry version patch

bump-minor:
	poetry version minor

bump-major:
	poetry version major
