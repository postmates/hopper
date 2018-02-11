#[macro_use]
extern crate criterion;
extern crate hopper;
extern crate tempdir;

use std::{mem, thread};
use criterion::{Bencher, Criterion};
use hopper::channel_with_explicit_capacity;
use std::sync::mpsc::channel;

fn mpsc_tst(input: MpscInput) -> () {
    let (tx, rx) = channel();

    let chunk_size = input.ith / input.total_senders;

    let mut snd_jh = Vec::new();
    for _ in 0..input.total_senders {
        let tx = tx.clone();
        snd_jh.push(thread::spawn(move || {
            for i in 0..chunk_size {
                tx.send(i).unwrap();
            }
        }))
    }

    let rcv_jh = thread::spawn(move || {
        let mut collected = 0;
        while collected < input.ith {
            let _ = rx.recv().unwrap();
            collected += 1;
        }
    });

    for jh in snd_jh {
        jh.join().expect("snd join failed");
    }
    rcv_jh.join().expect("rcv join failed");
}

fn hopper_tst(input: HopperInput) -> () {
    let sz = mem::size_of::<u64>();
    let in_memory_bytes = sz * input.in_memory_total;
    let max_disk_bytes = sz * input.max_disk_total;
    let dir = tempdir::TempDir::new("hopper").unwrap();
    let (snd, mut rcv) =
        channel_with_explicit_capacity("tst", dir.path(), in_memory_bytes, max_disk_bytes).unwrap();

    let chunk_size = input.ith / input.total_senders;

    let mut snd_jh = Vec::new();
    for _ in 0..input.total_senders {
        let mut thr_snd = snd.clone();
        snd_jh.push(thread::spawn(move || {
            for i in 0..chunk_size {
                thr_snd.send(i);
            }
        }))
    }

    let rcv_jh = thread::spawn(move || {
        let mut collected = 0;
        let mut rcv_iter = rcv.iter();
        while collected < input.ith {
            let _ = rcv_iter.next().unwrap();
            collected += 1;
        }
    });

    for jh in snd_jh {
        jh.join().expect("snd join failed");
    }
    rcv_jh.join().expect("rcv join failed");
}

#[derive(Debug, Clone, Copy)]
struct HopperInput {
    in_memory_total: usize,
    max_disk_total: usize,
    total_senders: usize,
    ith: usize,
}

#[derive(Debug, Clone, Copy)]
struct MpscInput {
    total_senders: usize,
    ith: usize,
}

fn hopper_benchmark(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "hopper_tst",
        |b: &mut Bencher, input: &HopperInput| b.iter(|| hopper_tst(*input)),
        vec![
            // all in-memory
            HopperInput {
                in_memory_total: 2 << 13,
                max_disk_total: 2 << 14,
                total_senders: 2 << 1,
                ith: 2 << 12,
            },
            // swap to disk
            HopperInput {
                in_memory_total: 2 << 11,
                max_disk_total: 2 << 14,
                total_senders: 2 << 1,
                ith: 2 << 12,
            },
        ],
    );
}

fn mpsc_benchmark(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "mpsc_tst",
        |b: &mut Bencher, input: &MpscInput| b.iter(|| mpsc_tst(*input)),
        vec![
            MpscInput {
                total_senders: 2 << 1,
                ith: 2 << 12,
            },
        ],
    );
}

criterion_group!(benches, hopper_benchmark, mpsc_benchmark);
criterion_main!(benches);
