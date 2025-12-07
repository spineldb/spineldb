# Chapter 1: Installation & Setup

Welcome to SpinelDB! This guide will walk you through the essential steps to install, configure, and run your SpinelDB server for the first time. In just a few minutes, you'll have a running in-memory database instance ready to go.

## Prerequisites

Before you begin, ensure your system has the following prerequisites:

1.  **Rust Toolchain:** SpinelDB is written in Rust. You will need `rustc` and `cargo` (the Rust package manager) to build the project. If you haven't installed them yet, follow the official instructions at [rustup.rs](https://rustup.rs/).
2.  **Git:** You'll need Git to clone the SpinelDB repository from its source code.
3.  **redis-cli (Optional, but Highly Recommended):** To interact with the SpinelDB server, `redis-cli` is the easiest tool to use due to its API compatibility.

## Quick Install Script

For a rapid setup, you can use our convenient one-liner installation script. This script will download and compile SpinelDB, placing the executable in your system's PATH.

```bash
# Using curl
sh -c "$(curl -fsSL https://raw.githubusercontent.com/spineldb/spineldb/main/install.sh)"

# Using wget
sh -c "$(wget -qO- https://raw.githubusercontent.com/spineldb/spineldb/main/install.sh)"
```

## Docker Installation

For a containerized environment, you can use the official SpinelDB Docker image. This is the recommended method for production deployments and easy integration into existing container workflows.

1.  **Pull the Docker Image:**
    First, pull the latest SpinelDB image from Docker Hub:

    ```bash
    docker pull spineldb/spineldb:latest
    ```

2.  **Run the Container:**
    You can run SpinelDB in a Docker container, mapping the default port (7878) to your host machine:

    ```bash
    docker run -d -p 7878:7878 --name spineldb_instance spineldb/spineldb:latest
    ```
    - `-d`: Runs the container in detached mode (in the background).
    - `-p 7878:7878`: Maps port 7878 on your host to port 7878 inside the container.
    - `--name spineldb_instance`: Assigns a name to your container for easy reference.

3.  **Connect to SpinelDB:**
    Once the container is running, you can connect to it using `redis-cli` on your host machine:

    ```bash
    redis-cli -p 7878
    ```

4.  **Configuration with Docker:**
    To use a custom `config.toml` with Docker, you can mount a volume:

    ```bash
    docker run -d -p 7878:7878 \
      -v /path/to/your/config.toml:/etc/spineldb/config.toml \
      --name spineldb_instance spineldb/spineldb:latest
    ```
    Replace `/path/to/your/config.toml` with the actual path to your configuration file on your host machine.


## Step 1: Building from Source

The primary installation method is compiling SpinelDB from its source code. This ensures you get the latest version with all optimizations for your system.

1.  **Clone the Repository:**
    Open your terminal and clone the official SpinelDB repository.

    ```bash
    git clone https://github.com/spineldb/spineldb.git
    cd spineldb
    ```

2.  **Build the Project:**
    Use `cargo` to build the project. Using the `--release` flag is crucial as it will enable all compiler optimizations, resulting in a much faster binary for production use.

    ```bash
    cargo build --release
    ```

    This process might take a few minutes the first time it runs, as `cargo` will download and compile all dependencies.

3.  **Locate the Binary:**
    Once the compilation is complete, the executable binary will be located in the `target/release/` directory.

    ```bash
    # The binary will be named 'spineldb'
    ls -l ./target/release/spineldb
    ```

    You can run the server directly from this location or copy it to a directory in your system's `PATH` (e.g., `/usr/local/bin`) for easier access.

## Step 2: Understanding the Configuration File (`config.toml`)

SpinelDB is configured using a file named `config.toml`, which should be located in the same directory from which you run the server.

Create a new `config.toml` file with the following minimal content as a starting point:

```toml
# The IP address and port for the server to listen on.
# '127.0.0.1' allows only local connections. Use '0.0.0.0' to allow external connections.
host = "127.0.0.1"
port = 7878

# The maximum number of simultaneous client connections.
# This is a safeguard against resource exhaustion (Denial of Service).
max_clients = 10000

# The maximum memory limit. Highly recommended for production.
# Format: Number (bytes), string with unit ("512mb", "2gb"), or percentage ("75%").
maxmemory = "512mb"

# The eviction policy to use when 'maxmemory' is reached.
# Options: no-eviction, allkeys-lru, volatile-lru, allkeys-random, etc.
maxmemory_policy = "allkeys-lru"

# Set a password to secure the server.
# Leave blank or remove this line to disable authentication.
# password = "your-secret-password"

[persistence]
# Enable snapshot-based persistence (similar to Redis RDB).
spldb_enabled = true
# Save rules: save after 900 seconds if 1 key changed, etc.
save_rules = [
  { seconds = 900, changes = 1 },
  { seconds = 300, changes = 10 },
  { seconds = 60, changes = 10000 },
]
```

SpinelDB has many other configuration options for clustering, caching, security, and more, which will be covered in later chapters.

## Step 3: Running the SpinelDB Server

With the binary built and `config.toml` created, you are now ready to run the server.

From your project's root directory, execute the following command:

```bash
./target/release/spineldb
```

If successful, you will see log output similar to this in your terminal:

```text
INFO spineldb::config: Resolved maxmemory '512mb' to 536870912 bytes (100.00% of total available 536870912 bytes).
INFO spineldb::config: Server configured with 16 databases.
INFO spineldb::server::initialization: Server state initialized.
INFO spineldb::server::initialization: Server starting in STANDALONE mode.
INFO spineldb::core::persistence::spldb_loader: SPLDB file not found at dump.spldb. Starting with an empty database.
INFO spineldb::core::persistence::spldb_loader: Persistence data loaded successfully.
INFO spineldb::server::initialization: SpinelDB server listening on 127.0.0.1:7878
INFO spineldb::core::background_tasks: All background tasks have been spawned.
```

Your server is now running and ready to accept connections!

## Step 4: Connecting with `redis-cli`

Since SpinelDB is compatible with the Redis API, you can use `redis-cli` to interact with it. Open a new terminal and connect to your server.

```bash
# Change the port if you modified it in config.toml
redis-cli -p 7878
```

Now, try a few basic commands:

```text
127.0.0.1:7878> PING
PONG

127.0.0.1:7878> SET mykey "Hello, SpinelDB!"
OK

127.0.0.1:7878> GET mykey
"Hello, SpinelDB!"

127.0.0.1:7878> INFO server
# Server
spineldb_version:0.1.0
tcp_port:7878
```

If you set a `password` in `config.toml`, you will need to authenticate after connecting:

```text
127.0.0.1:7878> AUTH your-secret-password
OK
```

---

<div className="doc-nav-links">
  <span>⬅️ <strong>Previous Chapter: <a href="./command-reference">0. Command Reference</a></strong></span>
  <span>➡️ <strong>Next Chapter: <a href="./core-data-types">2. Core Data Types & Commands</a></strong></span>
</div>

