// benches/vector_bench.rs

//! SpinelVector benchmarks
//!
//! Measures performance of vector add, search, and serialization operations.

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use spineldb::core::storage::vector::{DistanceMetric, SpinelVector};
use std::time::Duration;

fn bench_vector_add(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_add");
    group.measurement_time(Duration::from_secs(5));

    for dim in [16, 128, 768] {
        group.bench_function(format!("dim_{}", dim), |b| {
            b.iter_batched(
                || {
                    let sv = SpinelVector::new(dim, DistanceMetric::L2, 100_000, 16, 200, 10);
                    sv
                },
                |mut sv| {
                    for i in 0..100 {
                        let id = Bytes::from(format!("vec-{}", i));
                        let vector: Vec<f32> = (0..dim).map(|j| (i * dim + j) as f32).collect();
                        sv.add(id, vector, None).unwrap();
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_vector_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_search");
    group.measurement_time(Duration::from_secs(5));

    for (n, dim) in [(100, 32), (1000, 32), (1000, 128)] {
        let mut sv = SpinelVector::new(dim, DistanceMetric::L2, n as u64 * 2, 16, 200, 10);
        for i in 0..n {
            let id = Bytes::from(format!("vec-{}", i));
            let vector: Vec<f32> = (0..dim).map(|j| (i * dim + j) as f32).collect();
            sv.add(id, vector, None).unwrap();
        }

        let query: Vec<f32> = (0..dim).map(|j| j as f32).collect();

        group.bench_function(format!("n{}_d{}_k10", n, dim), |b| {
            b.iter(|| {
                sv.search(&query, 10, None).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_vector_search_cosine(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_search_cosine");
    group.measurement_time(Duration::from_secs(5));

    let n = 1000;
    let dim = 128;
    let mut sv = SpinelVector::new(dim, DistanceMetric::Cosine, n as u64 * 2, 16, 200, 10);
    for i in 0..n {
        let id = Bytes::from(format!("vec-{}", i));
        let vector: Vec<f32> = (0..dim).map(|j| ((i + j) as f32).sin()).collect();
        sv.add(id, vector, None).unwrap();
    }

    let query: Vec<f32> = (0..dim).map(|j| (j as f32).sin()).collect();

    group.bench_function("k10", |b| {
        b.iter(|| {
            sv.search(&query, 10, None).unwrap();
        });
    });
    group.finish();
}

fn bench_vector_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_serialize");
    group.measurement_time(Duration::from_secs(5));

    for n in [100, 1000] {
        let mut sv = SpinelVector::new(128, DistanceMetric::L2, n as u64 * 2, 16, 200, 10);
        for i in 0..n {
            let id = Bytes::from(format!("vec-{}", i));
            let vector: Vec<f32> = (0..128).map(|j| (i * 128 + j) as f32).collect();
            let meta = Some(Bytes::from(format!("meta-{}", i)));
            sv.add(id, vector, meta).unwrap();
        }

        group.bench_function(format!("serialize_{}", n), |b| {
            b.iter(|| {
                sv.serialize();
            });
        });

        let bytes = sv.serialize();
        group.bench_function(format!("deserialize_{}", n), |b| {
            b.iter(|| {
                SpinelVector::deserialize(&bytes).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_vector_madd(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_madd");
    group.measurement_time(Duration::from_secs(5));

    let dim = 128;
    for batch_size in [10, 100, 500] {
        group.bench_function(format!("batch_{}", batch_size), |b| {
            b.iter_batched(
                || {
                    let sv = SpinelVector::new(dim, DistanceMetric::L2, 100_000, 16, 200, 10);
                    sv
                },
                |mut sv| {
                    let entries: Vec<_> = (0..batch_size)
                        .map(|i| {
                            let id = Bytes::from(format!("vec-{}", i));
                            let vector: Vec<f32> = (0..dim).map(|j| (i * dim + j) as f32).collect();
                            (id, vector, None)
                        })
                        .collect();
                    sv.madd(entries);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_vector_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_memory_usage");
    group.measurement_time(Duration::from_secs(3));

    let mut sv = SpinelVector::new(128, DistanceMetric::L2, 100_000, 16, 200, 10);
    for i in 0..1000 {
        let id = Bytes::from(format!("vec-{}", i));
        let vector: Vec<f32> = (0..128).map(|j| (i * 128 + j) as f32).collect();
        sv.add(id, vector, None).unwrap();
    }

    group.bench_function("1000_vectors", |b| {
        b.iter(|| {
            sv.memory_usage();
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_vector_add,
    bench_vector_search,
    bench_vector_search_cosine,
    bench_vector_serialize,
    bench_vector_madd,
    bench_vector_memory_usage,
);
criterion_main!(benches);
