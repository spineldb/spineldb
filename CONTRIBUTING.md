# Contributing to SpinelDB

First off, thank you for considering contributing to SpinelDB! We welcome any help, from reporting a bug to submitting a feature. This project thrives on community contributions.

This document provides a guide for contributing to SpinelDB.

## Code of Conduct

This project and everyone participating in it is governed by a Code of Conduct. By participating, you are expected to uphold this code. Please take a moment to read it. (A `CODE_OF_CONDUCT.md` file can be added later).

---

## Why Contribute to SpinelDB?

SpinelDB is an ideal project for open-source contributors looking to work on high-quality, impactful software. Here’s why you should consider contributing:

### 1. **Pristine, Modular, and Idiomatic Rust Codebase**
The project's architecture is a masterclass in separation of concerns, making it incredibly easy to navigate and contribute to.
- **Zero-Warning Policy:** The codebase is kept "Clippy-clean" (`cargo clippy -- -D warnings`), ensuring a high standard of code hygiene and adherence to Rust idioms.
- **Clear Module Boundaries:** Logic is cleanly separated into intuitive directories:
  - `src/core/protocol`: All RESP parsing logic lives here.
  - `src/core/persistence`: AOF, snapshotting, and rewriting are self-contained.
  - `src/core/cluster`: Gossip, failover, and slot management are neatly organized.
- **Perfect Entry Point for Newcomers:** The `src/core/commands/` directory is exceptionally well-structured. **Each command is its own self-contained module**. This allows a new contributor to easily fix a bug in a single command or implement a new one without needing to understand the entire system.

### 2. **Modern and Attractive Tech Stack**
You will be working with the best of the modern Rust ecosystem. The project leverages:
- **`tokio`** for its high-performance, asynchronous I/O foundation.
- **`axum`** for the clean, modern web server that powers the metrics endpoint.
- **`async_trait`** for clean, readable asynchronous trait implementations.
This is a great opportunity to hone your skills with cutting-edge, in-demand technologies.

### 3. **A Culture of Quality: Extensive Test Coverage**
The `tests/` directory is filled with dozens of `unit_*_test.rs` files, demonstrating a strong commitment to code quality and stability. This robust test suite means:
- You can refactor and add new features with confidence.
- You have a clear blueprint for how to write tests for your own contributions.

### 4. **Clear Path to Contribution**
The `ROADMAP.md` file provides a clear and curated list of features that are planned for the future (marked with `[ ]`). This serves as a ready-made list of "good first issues" and more challenging tasks, allowing you to choose a contribution that matches your skill level and interests.

### 5. **Smooth Developer Experience**
The project includes a `Dockerfile` for easy containerized setup and a full suite of CI/CD workflows in `.github/workflows` for automated testing. This focus on tooling ensures that you can get up and running quickly and that your contributions are integrated smoothly.

---

## How to Contribute

### Reporting Bugs
- Ensure the bug was not already reported by searching on the GitHub repository under "Issues".
- If you're unable to find an open issue addressing the problem, open a new one. Be sure to include a **title and clear description**, as much relevant information as possible, and a **code sample** or an **executable test case** demonstrating the expected behavior that is not occurring.

### Setting up the Development Environment
1. Fork the repository.
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/spineldb.git`
3. Ensure you have a recent version of the Rust toolchain installed.
4. Build the project: `cargo build`
5. Run the tests to ensure everything is set up correctly: `cargo test`

### Submitting a Pull Request
1. Create a new branch for your changes (`git checkout -b my-feature-branch`).
2. Add your changes and commit them with a descriptive message.
3. Ensure your code is formatted with `cargo fmt` and passes `cargo clippy -- -D warnings`.
4. Push your branch to your fork and open a pull request to the `main` branch of the original repository.
5. Provide a clear description of the changes in the pull request.

We look forward to your contributions!
