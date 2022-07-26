use std::net::{Ipv6Addr, SocketAddr};
use std::path::PathBuf;

use anyhow::{ensure, Context, Result};
use blobnet::Config;
use clap::Parser;
use hyper::server::conn::AddrIncoming;
use shutdown::Shutdown;
use tokio::process::Command;

/// Low-latency file server that is backed by a non-volatile cache.
///
/// This file server uses two directories on disk: an NFS mount, which is a
/// durable but slower store for files, and a local storage directory, which
/// acts as a non-volatile cache. Both directories must be writable by the user
/// runnning this server.
///
/// Files are keyed by their content hashes. At any given time, all files must
/// be present in their corresopnding locations in the NFS directory, but they
/// might not be present in the local storage directory.
#[derive(Parser, Debug)]
#[clap(version, about, long_about = None)]
pub struct Cli {
    /// Path to the local disk storage.
    #[clap(short, long, parse(from_os_str))]
    pub storage_path: PathBuf,

    /// Path to the network file system mount.
    #[clap(short, long, parse(from_os_str))]
    pub nfs_path: PathBuf,

    /// Secret used to authorize users to access the service.
    #[clap(long, env = "BLOBNET_SECRET")]
    pub secret: String,

    /// Enable an additional check for the expected NFS device.
    #[clap(long)]
    pub expected_nfs: Option<String>,

    /// HTTP port to listen on.
    #[clap(short, long, default_value_t = 7609)]
    pub port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    if let Some(expected_nfs) = args.expected_nfs {
        let output = Command::new("stat")
            .args(["-f", "-c", "%T"])
            .arg(&args.nfs_path)
            .output()
            .await
            .context("failed to stat nfs path")?;
        let fs_type = std::str::from_utf8(&output.stdout)?.trim();
        ensure!(
            fs_type == expected_nfs,
            "expected imagefs root to be {}, but found {fs_type} instead",
            expected_nfs,
        );
    }

    let config = Config {
        storage_path: args.storage_path,
        nfs_path: args.nfs_path,
        secret: args.secret,
    };
    let addr = SocketAddr::from((Ipv6Addr::UNSPECIFIED, args.port));
    let incoming = AddrIncoming::bind(&addr).context("failed to listen on address")?;
    println!("listening on http://{addr}");
    let mut shutdown = Shutdown::new()?;
    blobnet::listen_with_shutdown(config, incoming, shutdown.recv()).await?;
    Ok(())
}
