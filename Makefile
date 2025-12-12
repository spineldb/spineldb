.PHONY: help build run clean check clippy fmt test test-coverage test-coverage-html bench bench-command bench-concurrent bench-memory install-tools

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
	@echo "  make bench              - Run all performance benchmarks"
	@echo "  make bench-command      - Run command execution benchmarks"
	@echo "  make bench-concurrent   - Run concurrent access benchmarks"
	@echo "  make bench-memory       - Run memory usage benchmarks"
	@echo "  make test-coverage      - Run tests with coverage (llvm-cov)"
	@echo "  make test-coverage-html - Run tests and generate HTML coverage report"
	@echo "  make install-tools      - Install required tools (llvm-cov, clippy, rustfmt)"
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
	rm -f coverage.lcov
	rm -f build_rs_cov.profraw

# Check and lint
check:
	cargo check

clippy:
	cargo clippy

fmt:
	cargo fmt

fmt-check:
	cargo fmt -- --check

# Test commands
test:
	RUST_MIN_STACK=8388608 cargo test

bench:
	RUST_MIN_STACK=8388608 cargo bench

bench-command:
	@echo "Running command execution benchmarks..."
	RUST_MIN_STACK=8388608 cargo bench --bench command_bench

bench-concurrent:
	@echo "Running concurrent access benchmarks..."
	RUST_MIN_STACK=8388608 cargo bench --bench concurrent_bench

bench-memory:
	@echo "Running memory usage benchmarks..."
	RUST_MIN_STACK=8388608 cargo bench --bench memory_bench

test-coverage:
	@echo "Running tests with coverage (llvm-cov)..."
	RUST_MIN_STACK=8388608 cargo llvm-cov --workspace --lcov --output-path coverage.lcov --ignore-filename-regex '(.*/tests/|.*/examples/)'
	@echo "Coverage report generated at: coverage.lcov"

test-coverage-html:
	@echo "Running tests with coverage and generating HTML report..."
	RUST_MIN_STACK=8388608 cargo llvm-cov --workspace --html --output-dir coverage_report --ignore-filename-regex '(.*/tests/|.*/examples/)'
	@echo "Coverage report generated at: coverage_report/index.html"

# Install development tools
install-tools:
	@echo "Installing llvm-tools-preview..."
	rustup component add llvm-tools-preview
	@echo "Installing cargo-llvm-cov..."
	cargo install cargo-llvm-cov || true
	@echo "Installing rustfmt..."
	rustup component add rustfmt
	@echo "Installing clippy..."
	rustup component add clippy
	@echo "All tools installed!"

# Run all checks
all: check clippy fmt-check test
	@echo "All checks passed!"

