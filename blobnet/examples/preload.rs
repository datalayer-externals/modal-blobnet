use std::io;
use std::io::BufRead;

use anyhow::{Context, Error, Result};
use blobnet::client::FileClient;
use clap::Parser;
use tikv_jemallocator::Jemalloc;

/// Preload a list of blobs from stdin. Each line should have one blob hash as
/// hex and the blob size in bytes separated by a single space. Empty lines and
/// lines starting with `#` are ignored.
#[derive(Parser)]
struct Args {
    /// Address of the blobnet server (for example: `http://localhost:7609`).
    origin: String,

    /// Authentication secret.
    secret: String,
}

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let blobs: Vec<_> = io::stdin()
        .lock()
        .lines()
        .flat_map(|l| {
            let l = l.map_err(anyhow::Error::from);
            let line = l?;
            let (hash, size) = line
                .split_once(' ')
                .context("expected hash and size tuple")?;
            let hash = hash.to_string();
            let size = size.parse()?;
            Ok::<_, Error>(blobnet::proto::preload::Blob { hash, size })
        })
        .collect();

    let preload = blobnet::proto::Preload { blobs };
    let client = FileClient::new_http(&args.origin, &args.secret);

    client.preload(preload).await?;

    Ok(())
}
