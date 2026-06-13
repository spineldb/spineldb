use bytes::Bytes;
use spineldb::config::Config;
use spineldb::connection::SessionState;
use spineldb::core::Command;
use spineldb::core::commands::generic::Eval;
use spineldb::core::handler::command_router::Router;
use spineldb::core::state::ServerState;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::reload;

#[tokio::test]
async fn test_eval_in_transaction_panic_repro() {
    let config = Config::default();
    let (_filter, reload_handle) =
        reload::Layer::<EnvFilter, tracing_subscriber::Registry>::new(EnvFilter::default());
    let reload_handle = Arc::new(reload_handle);
    let state_init = ServerState::initialize(config, reload_handle).unwrap();
    let state = state_init.state;

    let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
    let mut session = SessionState {
        is_authenticated: true,
        is_asking: false,
        is_in_transaction: false,
        is_subscribed: false,
        is_pattern_subscribed: false,
        subscribed_channels: HashSet::new(),
        subscribed_patterns: HashSet::new(),
        pubsub_receivers: Vec::new(),
        current_db_index: 0,
        authenticated_user: None,
        protocol_version: 3,
    };

    let mut router = Router::new(state.clone(), 1, addr, &mut session);

    // 1. MULTI
    let _ = router.route(Command::Multi).await.unwrap();

    // 2. QUEUE EVAL
    let eval_cmd = Command::Eval(Eval {
        script: Bytes::from_static(b"return 1"),
        num_keys: 0,
        keys: vec![],
        args: vec![],
    });
    let _ = router.route(eval_cmd).await.unwrap();

    // 3. EXEC - This is where it should panic
    let result = router.route(Command::Exec).await;

    assert!(
        result.is_ok(),
        "EXEC failed or panicked: {:?}",
        result.err()
    );
}
