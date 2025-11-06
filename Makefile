.PHONY: help build run clean check clippy fmt test test-coverage test-coverage-html install-tools

# Default target
help:
	@echo "SpinelDB Makefile - Available commands:"
	@echo ""
	@echo "  make build              - Build the project in release mode"
	@echo "  make build-dev          - Build the project in debug mode"
	@echo "  make run                - Run the project"
	@echo "  make clean              - Clean build artifacts"
	@echo "  make check              - Check the project without building"
	@echo "  make clippy             - Run clippy linter"
	@echo "  make fmt                - Format code with rustfmt"
	@echo "  make fmt-check          - Check if code is formatted"
	@echo "  make test               - Run tests"
	@echo "  make test-coverage      - Run tests with coverage (tarpaulin)"
	@echo "  make test-coverage-html - Run tests and generate HTML coverage report"
	@echo "  make install-tools      - Install required tools (tarpaulin, clippy, rustfmt)"
	@echo "  make all                - Run check, clippy, fmt-check, and test"

# Build commands
build:
	cargo build --release

build-dev:
	cargo build

run:
	cargo run

# Clean
clean:
	cargo clean
	rm -rf coverage_report/
	rm -f cobertura.xml
	rm -f build_rs_cov.profraw

# Check and lint
check:
	cargo check

clippy:
	cargo clippy -- -D warnings

fmt:
	cargo fmt

fmt-check:
	cargo fmt -- --check

# Test commands
test:
	cargo test

test-coverage:
	cargo tarpaulin --out Xml --output-dir coverage_report --exclude-files '*/tests/*' '*/examples/*'

test-coverage-html:
	@echo "Running tests with coverage and generating HTML report..."
	cargo tarpaulin --out Html --out Xml --output-dir coverage_report --exclude-files '*/tests/*' '*/examples/*'
	@echo "Coverage report generated at: coverage_report/tarpaulin-report.html"

# Install development tools
install-tools:
	@echo "Installing cargo-tarpaulin..."
	cargo install cargo-tarpaulin
	@echo "Installing rustfmt..."
	rustup component add rustfmt
	@echo "Installing clippy..."
	rustup component add clippy
	@echo "All tools installed!"

# Run all checks
all: check clippy fmt-check test
	@echo "All checks passed!"

