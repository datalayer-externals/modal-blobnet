use std::io;
use std::io::Read;

use anyhow::Result;
use blobnet::client::FileClient;
use clap::Parser;
use tikv_jemallocator::Jemalloc;

/// Put a blob from stdin
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

    let mut data = Vec::new();
    let _ = io::stdin().read_to_end(&mut data)?;
    let size = data.len();

    let client = FileClient::new_http(&args.origin, &args.secret);

    let hash = client.put(|| async { Ok(data.clone()) }).await?;

    println!("{hash} {size}");

    Ok(())
}
