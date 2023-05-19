use std::net::UdpSocket;

use anyhow::{Context, Result};
use cadence::{NopMetricSink, QueuingMetricSink, StatsdClient, UdpMetricSink};

/// Initialize and register a global StatsD logger for this application.
///
/// This function must be run before any of the `statsd_[...]!` macros, or they
/// will panic.
pub fn try_init(enabled: bool) -> Result<()> {
    let client = if enabled {
        create_udp_client().context("config error: failed to initialize local StatsD client")?
    } else {
        create_nop_client()
    };
    cadence_macros::set_global_default(client);
    Ok(())
}

fn create_udp_client() -> Result<StatsdClient> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.set_nonblocking(true)?;

    let host = ("127.0.0.1", cadence::DEFAULT_PORT);
    let udp_sink = UdpMetricSink::from(host, socket)?;
    let queuing_sink = QueuingMetricSink::from(udp_sink);
    let client_builder = StatsdClient::builder("blobnet", queuing_sink)
        .with_error_handler(|err| eprintln!("failed to send metric to StatsD: {err}"));

    Ok(client_builder.build())
}

fn create_nop_client() -> StatsdClient {
    StatsdClient::from_sink("blobnet", NopMetricSink)
}
