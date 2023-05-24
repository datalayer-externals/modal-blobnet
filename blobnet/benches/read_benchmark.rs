use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Error;
use blobnet::{
    provider::{self, Provider},
    read_to_bytes,
    test_provider::Delayed,
    ReadStream,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode};
use hyper::body::Bytes;
use tikv_jemallocator::Jemalloc;
use tokio::runtime::Runtime;
use tokio::time;
use tokio_stream::StreamExt;
use tokio_util::io::{ReaderStream, StreamReader};

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// Insert 100 blobs of 1KB each, then read each 10 times.
async fn insert_read_1k(provider: impl Provider) -> anyhow::Result<()> {
    let mut hashes = Vec::new();
    for i in 0..100 {
        let mut data = b"asdf".repeat(256);
        data.extend(u32::to_le_bytes(i));
        let hash = provider.put(Box::pin(&*data)).await?;
        hashes.push(hash);
    }
    for _ in 0..10 {
        for hash in &hashes {
            let stream = provider.get(hash, None).await?;
            black_box(read_to_bytes(stream).await?);
        }
    }
    Ok(())
}

fn bench_insert_read_1k(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();

    let mut g = c.benchmark_group("insert_read_1k");
    g.sample_size(20);
    g.measurement_time(Duration::from_secs(10));

    g.bench_function("memory", |b| {
        let provider = provider::Memory::new();
        runtime.block_on(insert_read_1k(&provider)).unwrap();
        b.to_async(&runtime).iter(|| async {
            insert_read_1k(&provider).await.unwrap();
        });
    });

    g.bench_function("localdir", |b| {
        let dir = tempfile::tempdir().unwrap();
        let provider = provider::LocalDir::new(dir.path());
        runtime.block_on(insert_read_1k(&provider)).unwrap();
        b.to_async(&runtime).iter(|| async {
            insert_read_1k(&provider).await.unwrap();
        });
    });

    drop(runtime);

    g.bench_function("cached_localdir", |b| {
        let runtime = Runtime::new().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let provider = provider::Cached::new(
            provider::LocalDir::new(dir.path().join("data")),
            dir.path().join("cache"),
            1 << 21,
        );
        runtime.block_on(insert_read_1k(&provider)).unwrap();
        b.to_async(&runtime).iter(|| async {
            insert_read_1k(&provider).await.unwrap();
        });
        // Stop async cache population
        runtime.shutdown_timeout(Duration::from_secs(30));
    });

    g.finish();
}

/// Setup to create a simulated FS image for other benchmarks.
async fn image_setup(provider: impl Provider) -> anyhow::Result<Vec<String>> {
    let mut hashes = Vec::new();

    // Create 128 files of size 0-1KB.
    for size in 0..128 {
        let data = b"12345678".repeat(size);
        hashes.push(provider.put(Box::pin(&*data)).await?);
    }

    // Create a couple files of size 1-10 MB.
    for size in 1..=10 {
        let data = b"a".repeat(1048576 * size);
        hashes.push(provider.put(Box::pin(&*data)).await?);
    }

    // Create a big file of size 1 GB.
    let chunk_1mb = Bytes::from(b"a".repeat(1048576));
    let byte_stream = tokio_stream::iter(std::iter::repeat(chunk_1mb).take(1024));
    let data = StreamReader::new(byte_stream.map(Ok::<_, std::io::Error>));
    hashes.push(provider.put(Box::pin(data)).await?);

    Ok(hashes)
}

/// Load an image on a simulated network provider with 400 µs mean latency.
async fn image_delayed(
    cache: &Path,
    provider: impl Provider + 'static,
    hashes: &[String],
    await_cache_population: bool,
) -> anyhow::Result<()> {
    let provider = Delayed::new(provider, 400e-6, 12.5);
    image(cache, provider, hashes, await_cache_population).await
}

async fn image(
    cache: &Path,
    provider: impl Provider + 'static,
    hashes: &[String],
    await_cache_population: bool,
) -> Result<(), Error> {
    let provider = provider::Cached::new(provider, cache, 1 << 21);

    for hash in hashes {
        let stream: ReadStream = provider.get(hash, None).await?.into();
        let mut stream = ReaderStream::with_capacity(stream, 1 << 21);
        while let Some(bytes) = stream.next().await {
            black_box(bytes?);
        }
    }

    if await_cache_population {
        while provider.stats().await.pending_disk_write_pages > 0 {
            time::sleep(Duration::from_millis(100)).await;
        }
    }

    Ok(())
}

fn bench_image_delayed(c: &mut Criterion) {
    let mut g = c.benchmark_group("image_delayed");
    g.sampling_mode(SamplingMode::Flat);
    g.sample_size(10);
    g.measurement_time(Duration::from_secs(20));
    g.warm_up_time(Duration::from_secs(2));

    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(provider::LocalDir::new(dir.path()));
    let hashes = Runtime::new()
        .unwrap()
        .block_on(image_setup(&provider))
        .unwrap();

    g.bench_function("cold", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let runtime = Runtime::new().unwrap();
                let cache_dir = tempfile::tempdir().unwrap();
                let start = Instant::now();
                let fut = image_delayed(cache_dir.path(), Arc::clone(&provider), &hashes, false);
                runtime.block_on(fut).unwrap();
                total += start.elapsed();
                // Stop async cache population
                runtime.shutdown_timeout(Duration::from_secs(30));
            }
            total
        })
    });

    let cache_dir = tempfile::tempdir().unwrap();

    // Populate the cache for the next test.
    let runtime = Runtime::new().unwrap();
    runtime.block_on(async {
        image(cache_dir.path(), Arc::clone(&provider), &hashes, true)
            .await
            .unwrap();
    });

    g.bench_function("warm", |b| {
        b.to_async(&runtime).iter(|| async {
            image_delayed(cache_dir.path(), Arc::clone(&provider), &hashes, false)
                .await
                .unwrap();
        });
    });

    g.finish();
}

criterion_group!(benches, bench_insert_read_1k, bench_image_delayed);
criterion_main!(benches);
