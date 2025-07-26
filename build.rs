// build.rs

use std::env;

fn main() {
    let version = env::var("SPINELDB_VERSION")
        .unwrap_or_else(|_| env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "dev".to_string()));

    println!("cargo:rustc-env=CARGO_PKG_VERSION={version}");
    println!("cargo:rerun-if-env-changed=SPINELDB_VERSION");
}
