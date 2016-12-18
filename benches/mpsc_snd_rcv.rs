#![feature(test)]

extern crate test;
extern crate tempdir;
extern crate hopper;

use self::test::Bencher;

#[bench]
fn bench_snd(b: &mut Bencher) {
    let dir = tempdir::TempDir::new("hopper").unwrap();
    let (mut snd, _) = hopper::channel("bench_snd", dir.path()).unwrap();
    b.iter(|| {
        for _ in 0..10_000 {
            snd.send(412u64);
        }
    });
}

#[bench]
fn bench_snd_rcv(b: &mut Bencher) {
    let dir = tempdir::TempDir::new("hopper").unwrap();
    let (mut snd, mut rcv) = hopper::channel("bench_snd", dir.path()).unwrap();
    b.iter(|| {
        snd.send(12u64);
        rcv.next().unwrap();
    });
}

#[bench]
fn bench_all_snd_all_rcv(b: &mut Bencher) {
    let dir = tempdir::TempDir::new("hopper").unwrap();
    let (mut snd, mut rcv) = hopper::channel("bench_snd", dir.path()).unwrap();
    b.iter(|| {
        for _ in 0..10_000 {
            snd.send(89u64);
        }
        for _ in 0..10_000 {
            rcv.next().unwrap();
        }
    });
}
