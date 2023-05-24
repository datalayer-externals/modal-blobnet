use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use blobnet::client::FileClient;
use blobnet::provider::Provider;
use blobnet::server::{listen, Config};
use blobnet::test_provider::{MockProvider, Request};
use blobnet::{provider, read_to_bytes};
use blobnet::{statsd, test_provider};
use bytes::Bytes;
use hyper::client::HttpConnector;
use hyper::server::conn::AddrIncoming;
use rand::{self, RngCore};
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::{task, time};

type TrackingProvider = test_provider::Tracking<provider::Remote<HttpConnector>>;

/// Spawn a temporary file server on localhost, only used for testing.
async fn spawn_temp_server() -> Result<Arc<TrackingProvider>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let mut incoming = AddrIncoming::from_listener(listener)?;
    incoming.set_nodelay(true);
    tokio::spawn(async {
        let dir = tempdir().unwrap();
        let provider = provider::LocalDir::new(dir.path().join("nfs"));
        let config = Config {
            provider: Box::new(provider),
            secret: "secret".into(),
        };
        listen(config, incoming).await.unwrap();
    });

    let client = FileClient::new_http(&format!("http://{addr}"), "secret");
    let remote = provider::Remote::new(client);
    let tracking = test_provider::Tracking::new(remote);
    Ok(Arc::new(tracking))
}

#[tokio::test]
async fn concurrent_cacheable_reads() -> Result<()> {
    let tracking_client = spawn_temp_server().await?;
    let dir = tempdir().unwrap();
    // Create a caching provider that should only forward one GET request to the
    // underlying tracking test provider.
    let cached_provider = Arc::new(provider::Cached::new(
        tracking_client.clone(),
        dir.path().join("cache"),
        1 << 21,
    ));

    let s1 = "hello world!";
    let data1 = Box::pin(s1.as_bytes());
    let h1 = tracking_client.put(data1).await?;

    // Create hundreds of concurrent reads on the same hash.
    let num_concurrent = 1 << 10;
    let mut set = task::JoinSet::new();
    for _ in 0..num_concurrent {
        let h1 = h1.clone();
        let client = cached_provider.clone();
        set.spawn(async move {
            let stream = (client.get(&h1, None).await).unwrap();
            let _ = read_to_bytes(stream).await.unwrap();
        });
    }
    // Wait for all get requests to finish.
    while set.join_next().await.is_some() {}
    let total_net_out_bytes = tracking_client.get_net_bytes_served.load(Ordering::SeqCst);
    assert_eq!(total_net_out_bytes, s1.len());
    Ok(())
}

#[tokio::test]
async fn request_cancellation() -> Result<()> {
    // Create a mock provider that captures requests
    let mock_provider = Arc::new(MockProvider::default());

    // Create a cached provider wrapping the mock provider
    let dir = tempdir().unwrap();
    let cached_provider = Arc::new(provider::Cached::new(
        mock_provider.clone(),
        dir.path().join("cache"),
        1 << 21,
    ));

    // Create a dummy blob
    let data = "hello world!";
    let hash = format!("{:x}", Sha256::new().chain_update(data).finalize());
    let range = (0, data.len() as u64);

    let client = cached_provider.clone();

    // Submit the first request
    let mut f1 = {
        let client = client.clone();
        let hash = hash.clone();
        task::spawn(async move { client.get(&hash, Some(range)).await })
    };

    // Get the resulting inner request
    let inner_request = match mock_provider.requests.recv().await? {
        Request::Get(_, response) => response,
        _ => bail!("Unexpected inner request type!"),
    };

    // Submit another request
    let mut f2 = {
        let client = client.clone();
        let hash = hash.clone();
        task::spawn(async move { client.get(&hash, Some(range)).await })
    };

    // Check that we did not get another inner request and that neither request
    // completes
    let sleep = time::sleep(Duration::from_millis(200));
    tokio::select! {
        _ = &mut f1 => {
            bail!("Unexpected f1 completion!");
        },
        _ = &mut f2 => {
            bail!("Unexpected f2 completion!");
        },
        _ = mock_provider.requests.recv() => {
            bail!("Unexpected additional request to inner provider!");
        },
        _ = sleep => {
        }
    }

    // Cancel the first request
    f1.abort();

    // Complete the inner request
    inner_request.send(Ok(data.as_bytes().into())).unwrap();

    // Check that the second request completes
    let r2 = read_to_bytes(f2.await??).await?;
    assert_eq!(r2, Bytes::from(data));

    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn test_cleaner() -> Result<()> {
    if let Err(e) = Command::new("sudo").arg("-v").output().await {
        eprintln!("Unable to sudo, skipping test: {e}");
        return Ok(());
    }

    statsd::try_init(false)?;

    const FILE_SIZE: u64 = 4096;
    const DISK_SIZE: u64 = 1024 * 1024;
    const CLEANING_THRESHOLD: u64 = (0.8 * DISK_SIZE as f64) as u64;

    // Mount a 1MB tmpfs to use as cache dir
    let cachedir = scopeguard::guard(tempdir()?, |cachedir| {
        task::spawn(async move {
            Command::new("sudo")
                .args(["umount", "-f"])
                .arg(cachedir.path())
                .output()
                .await
                .expect(&format!("failed to umount tmpfs at {:?}", cachedir.path()));
        });
    });
    Command::new("sudo")
        .args(["mount", "-t", "tmpfs", "-o", "size=1M", "tmp"])
        .arg(cachedir.path())
        .output()
        .await
        .expect(&format!("failed to mount tmpfs at {:?}", cachedir.path()));

    // Set up a cached provider and configure rapid cleaning to speed up test
    std::env::set_var("BLOBNET_CACHE_CLEAN_INTERVAL_MS", "1");
    let provider = provider::Memory::new();
    let cached = scopeguard::guard(
        provider::Cached::new(provider, cachedir.path(), 4096),
        move |_| {
            std::env::remove_var("BLOBNET_CACHE_CLEAN_INTERVAL_MS");
        },
    );

    let stats = cached.stats().await;
    assert_eq!(stats.disk_used_bytes, 0);
    assert_eq!(stats.disk_total_bytes, 1024 * 1024);

    // Fill disk until just below cleaning threshold
    while cached.stats().await.disk_used_bytes < (CLEANING_THRESHOLD - 8 * FILE_SIZE) {
        let mut blob = vec![0u8; FILE_SIZE as usize];
        rand::thread_rng().fill_bytes(&mut blob);
        let hash = cached.put(Box::pin(&*blob)).await?;
        let _ = cached.get(&hash, None).await?;
    }
    while cached.stats().await.pending_disk_write_bytes > 0 {
        time::sleep(Duration::from_millis(10)).await;
    }

    let stats = cached.stats().await;
    let disk_used_bytes_before_cleaner_start = stats.disk_used_bytes;
    assert!(disk_used_bytes_before_cleaner_start < CLEANING_THRESHOLD);

    // Start cleaner and see that it does not delete any files
    let cleaner = task::spawn(cached.cleaner());

    time::sleep(Duration::from_millis(100)).await;
    let stats = cached.stats().await;
    assert_eq!(stats.disk_used_bytes, disk_used_bytes_before_cleaner_start);

    // Stop cleaner
    cleaner.abort();

    // Fill the disk above cleaning threshold
    while cached.stats().await.disk_used_bytes < CLEANING_THRESHOLD {
        let mut blob = vec![0u8; FILE_SIZE as usize];
        rand::thread_rng().fill_bytes(&mut blob);
        let hash = cached.put(Box::pin(&*blob)).await?;
        let _ = cached.get(&hash, None).await?;
        while cached.stats().await.pending_disk_write_bytes > 0 {
            time::sleep(Duration::from_millis(10)).await;
        }
    }

    // Start cleaner again and observe disk usage dropping below threshold
    task::spawn(cached.cleaner());

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut done = false;
    while Instant::now() < deadline {
        let stats = cached.stats().await;
        if stats.disk_used_bytes <= CLEANING_THRESHOLD {
            done = true;
            break;
        }
        time::sleep(Duration::from_millis(1000)).await;
    }
    assert!(done, "disk usage did not drop below 80% within 10 seconds");

    Ok(())
}
