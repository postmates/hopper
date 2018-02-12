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
        let builder = thread::Builder::new();
        if let Ok(handler) = builder.spawn(move || {
            for i in 0..chunk_size {
                tx.send(i).unwrap();
            }
        }) {
            snd_jh.push(handler);
        }
    }

    let total_senders = snd_jh.len();
    let builder = thread::Builder::new();
    match builder.spawn(move || {
        let mut collected = 0;
        while collected < (chunk_size * total_senders) {
            let _ = rx.recv().unwrap();
            collected += 1;
        }
    }) {
        Ok(rcv_jh) => {
            for jh in snd_jh {
                jh.join().expect("snd join failed");
            }
            rcv_jh.join().expect("rcv join failed");
        }
        _ => {
            return;
        }
    }
}

fn hopper_tst(input: HopperInput) -> () {
    let sz = mem::size_of::<u64>();
    let in_memory_bytes = sz * input.in_memory_total;
    let max_disk_bytes = sz * input.max_disk_total;
    if let Ok(dir) = tempdir::TempDir::new("hopper") {
        if let Ok((snd, mut rcv)) =
            channel_with_explicit_capacity("tst", dir.path(), in_memory_bytes, max_disk_bytes)
        {
            let chunk_size = input.ith / input.total_senders;

            let mut snd_jh = Vec::new();
            for _ in 0..input.total_senders {
                let mut thr_snd = snd.clone();
                let builder = thread::Builder::new();
                if let Ok(handler) = builder.spawn(move || {
                    for i in 0..chunk_size {
                        thr_snd.send(i);
                    }
                }) {
                    snd_jh.push(handler);
                }
            }

            let total_senders = snd_jh.len();
            let builder = thread::Builder::new();
            match builder.spawn(move || {
                let mut collected = 0;
                let mut rcv_iter = rcv.iter();
                while collected < (chunk_size * total_senders) {
                    if let Some(_) = rcv_iter.next() {
                        collected += 1;
                    }
                }
            }) {
                Ok(rcv_jh) => {
                    for jh in snd_jh {
                        jh.join().expect("snd join failed");
                    }
                    rcv_jh.join().expect("rcv join failed");
                }
                _ => {
                    return;
                }
            }
        }
    }
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

criterion_group!{
    name = benches;
    config = Criterion::default().without_plots();
    targets = hopper_benchmark, mpsc_benchmark
}
criterion_main!(benches);
