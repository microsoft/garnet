// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//! Standalone Rust benchmark for bf-tree operations.
//! Run with: cargo run --release --manifest-path libs/native/bftree-garnet/Cargo.toml --example bench

use bf_tree::{BfTree, Config, LeafReadResult, StorageBackend};
use std::time::Instant;

const ITERATIONS: usize = 2_000_000;
const WARMUP: usize = 10_000;

fn bench<F: FnMut()>(label: &str, mut f: F) {
    for _ in 0..WARMUP {
        f();
    }
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        f();
    }
    let ns = start.elapsed().as_nanos() / ITERATIONS as u128;
    println!("{label}: {ns} ns/op");
}

fn run_benchmarks(label: &str, tree: &BfTree) {
    let key = b"bench:key:00000";
    let value = [42u8; 128];

    // Insert 64 consecutive keys so total data exceeds the base page size
    // (~4 KB default). This ensures reads are served from the circular
    // buffer cache and disk-backed reads don't hit a cold-page corner case.
    for i in 0..64 {
        let k = format!("bench:key:{i:05}");
        tree.insert(k.as_bytes(), &value);
    }

    // Verify read returns correct data
    let mut buf = [0u8; 256];
    match tree.read(key, &mut buf) {
        LeafReadResult::Found(n) => {
            assert_eq!(n, 128, "Expected 128 bytes, got {n}");
            assert_eq!(&buf[..128], &value, "Read value mismatch");
        }
        other => panic!("Expected Found, got {other:?}"),
    }

    println!("\n--- {label} ---");

    bench(&format!("{label} read"), || {
        let mut buf = [0u8; 256];
        let _ = tree.read(key, &mut buf);
    });

    bench(&format!("{label} insert"), || {
        tree.insert(key, &value);
    });

    bench(&format!("{label} delete"), || {
        tree.delete(key);
    });
}

fn main() {
    // Memory-only (cache_only) mode
    {
        let mut config = Config::default();
        config.cb_min_record_size(8);
        config.cache_only(true);
        let tree = BfTree::with_config(config, None).unwrap();
        run_benchmarks("Memory", &tree);
    }

    // Disk-backed mode
    {
        let path = "/tmp/bftree_bench_disk.bftree";
        let _ = std::fs::remove_file(path);
        let mut config = Config::default();
        config.cb_min_record_size(8);
        config.storage_backend(StorageBackend::Std);
        config.file_path(path);
        let tree = BfTree::with_config(config, None).unwrap();
        run_benchmarks("Disk", &tree);
        drop(tree);
        let _ = std::fs::remove_file(path);
    }
}
