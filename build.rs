// build.rs

use std::env;

fn main() {
    let version = env::var("SPINELDB_VERSION")
        .unwrap_or_else(|_| env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "dev".to_string()));

    println!("cargo:rustc-env=CARGO_PKG_VERSION={version}");
    println!("cargo:rerun-if-env-changed=SPINELDB_VERSION");

    // Only for MSVC targets
    #[cfg(all(windows, target_env = "msvc"))]
    println!("cargo:rustc-link-arg=/STACK:0x1000000"); // 16MB stack size
}
