//! Transparent gRPC access to IPMA website feeds, with bounded HTTP and conversion work.
mod convert;
mod proxy;
pub use proxy::{Proxy, ProxyOptions};
