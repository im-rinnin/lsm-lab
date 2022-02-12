set -e
cargo fmt --all -- --check
cargo clippy --all --tests -- -D clippy::all

cargo test