// src/core/warden/listener.rs

//! Implements the TCP listener for the Warden, allowing clients and other
//! Wardens to query its state using a subset of the SpinelDB Sentinel command API.

use crate::core::protocol::{RespFrame, RespFrameCodec, RespValue};
use crate::core::warden::state::GlobalWardenState;
use anyhow::Result;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use tracing::{info, warn};

/// Spawns the main TCP listener loop for the Warden.
pub async fn run_listener(port: u16, state: Arc<GlobalWardenState>) -> Result<()> {
    let listener = TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    info!("Warden listening for commands on port {}", port);

    loop {
        // Accept new incoming connections.
        match listener.accept().await {
            Ok((socket, addr)) => {
                let state_clone = state.clone();
                // Spawn a new task to handle each connection concurrently.
                tokio::spawn(async move {
                    info!("Accepted Warden connection from: {}", addr);
                    if let Err(e) = handle_connection(socket, state_clone).await {
                        warn!("Error handling Warden connection from {}: {}", addr, e);
                    }
                });
            }
            Err(e) => {
                warn!("Failed to accept Warden connection: {}", e);
            }
        }
    }
}

/// Handles a single client connection, reading commands and sending responses.
async fn handle_connection(socket: TcpStream, state: Arc<GlobalWardenState>) -> Result<()> {
    // Wrap the TCP stream with our RESP codec to handle frame encoding/decoding.
    let mut framed = Framed::new(socket, RespFrameCodec);

    // Process frames from the client in a loop.
    // The `StreamExt::next` method is used to asynchronously get the next item.
    while let Some(result) = framed.next().await {
        let frame = match result {
            Ok(frame) => frame,
            Err(e) => {
                warn!("Error decoding frame from warden client: {}", e);
                break;
            }
        };

        // We expect commands to be in the form of a RESP Array.
        if let RespFrame::Array(args) = frame {
            let response = process_warden_command(&args, &state);
            // The `SinkExt::send` method is used to asynchronously send a response.
            if let Err(e) = framed.send(response).await {
                warn!("Error sending response to warden client: {}", e);
                break;
            }
        } else {
            let error_response = RespFrame::Error(
                "ERR invalid command format. Commands must be RESP arrays.".to_string(),
            );
            if let Err(e) = framed.send(error_response).await {
                warn!("Error sending error response to warden client: {}", e);
                break;
            }
        }
    }
    Ok(())
}

/// Parses and processes a single command received by the Warden.
fn process_warden_command(args: &[RespFrame], state: &Arc<GlobalWardenState>) -> RespFrame {
    // Use .first() for a more idiomatic and safer way to get the first element.
    let Some(RespFrame::BulkString(cmd_bytes)) = args.first() else {
        return RespFrame::Error("ERR invalid command format".to_string());
    };

    // PING is a simple health check.
    if cmd_bytes.eq_ignore_ascii_case(b"ping") {
        return RespValue::SimpleString("PONG".into()).into();
    }

    // SENTINEL commands are the main API. We keep the "SENTINEL" name for client compatibility.
    if cmd_bytes.eq_ignore_ascii_case(b"sentinel") {
        let Some(RespFrame::BulkString(subcmd_bytes)) = args.get(1) else {
            return RespFrame::Error("ERR unknown sentinel subcommand".to_string());
        };

        // The most important command for clients: get the current master's address.
        if subcmd_bytes.eq_ignore_ascii_case(b"get-master-addr-by-name") {
            let Some(RespFrame::BulkString(master_name_bytes)) = args.get(2) else {
                return RespFrame::Error(
                    "ERR wrong number of arguments for 'sentinel get-master-addr-by-name'"
                        .to_string(),
                );
            };

            let master_name = String::from_utf8_lossy(master_name_bytes);

            // Look up the master's state in the global map.
            return if let Some(master_state_entry) = state.masters.get(&*master_name) {
                // Lock the specific master's state to read its current address.
                let master_state = master_state_entry.value().lock();

                // Return the address as a [ip, port] array, which is the standard SpinelDB format.
                RespValue::Array(vec![
                    RespValue::BulkString(master_state.addr.ip().to_string().into()),
                    RespValue::Integer(master_state.addr.port() as i64),
                ])
                .into()
            } else {
                // If the master name is unknown, return Null.
                RespFrame::Null
            };
        }
    }

    // If the command is not recognized, return an error.
    RespFrame::Error(format!(
        "ERR Unknown command '{}'",
        String::from_utf8_lossy(cmd_bytes)
    ))
}
