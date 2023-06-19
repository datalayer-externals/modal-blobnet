use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use rand::Rng;
use sha2::{Digest, Sha256};
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

pub fn bench_sha256(c: &mut Criterion) {
    let mut g = c.benchmark_group("sha256");
    g.sample_size(20);
    g.measurement_time(Duration::from_secs(10));

    let sizes = [("4KiB", 4096), ("128MiB", 128 * 1024 * 1024)];

    for (name, size) in sizes {
        g.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let mut buf = vec![0_u8; size];
                    rand::thread_rng().fill(buf.as_mut_slice());
                    buf
                },
                |buf| {
                    let mut hasher = Sha256::new();
                    hasher.update(buf);
                    hasher.finalize()
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }

    g.finish();
}

criterion_group!(benches, bench_sha256);
criterion_main!(benches);
