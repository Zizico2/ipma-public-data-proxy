set positional-arguments

default:
    @just --list

run *args:
    rtk cargo run --locked -- "$@"

build:
    rtk cargo build --locked --workspace

release:
    rtk cargo build --locked --release --workspace

fmt:
    rtk cargo fmt --all

verify:
    rtk cargo fmt --all --check
    rtk cargo check --locked --workspace --all-targets
    rtk cargo clippy --locked --workspace --all-targets -- -D warnings
    rtk cargo test --locked --workspace --all-targets
    rtk cargo test --locked --workspace --doc

ping address='127.0.0.1:50053':
    rtk buf curl --protocol grpc --http2-prior-knowledge --data '{}' "http://$1/ipma.public_data.v1.PublicDataService/Ping"
