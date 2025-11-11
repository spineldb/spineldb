# Stage 1: Build the application in a full Rust environment
FROM rust:1.91.1-bookworm AS builder
WORKDIR /usr/src/spineldb

# Copy dependency manifests and build script
COPY Cargo.toml Cargo.lock ./ 
COPY build.rs ./ 

# Create a dummy project to cache dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release

# Remove dummy source and copy the actual project source
RUN rm -f src/main.rs
COPY src ./src

# Build the actual project, leveraging the cached dependencies
RUN cargo build --release

# Stage 2: Create a minimal runtime image
FROM debian:bookworm-slim AS runtime

# Create a non-root user and group for security
RUN groupadd --system --gid 1001 spineldb && \
    useradd --system --uid 1001 --gid spineldb spineldb

# Create directories for config and data
RUN mkdir -p /etc/spineldb /var/lib/spineldb && \
    chown -R spineldb:spineldb /etc/spineldb /var/lib/spineldb
WORKDIR /etc/spineldb

# Copy the compiled binary from the builder stage
COPY --from=builder /usr/src/spineldb/target/release/spineldb /usr/local/bin/spineldb

# Copy the example configuration file
COPY examples/config.toml.example /etc/spineldb/config.toml

# Modify the config.toml to listen on all interfaces
RUN sed -i 's/host = "127.0.0.1"/host = "0.0.0.0"/' /etc/spineldb/config.toml
# Modify the config.toml to set log_level to info
RUN sed -i 's/log_level = "debug"/log_level = "info"/' /etc/spineldb/config.toml

# Set the user to run the application
USER spineldb

# Expose the default port used by SpinelDB (adjust if necessary)
EXPOSE 7890

# Set the default command to run the server
CMD ["/usr/local/bin/spineldb", "--config", "/etc/spineldb/config.toml"]
