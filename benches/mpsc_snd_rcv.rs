#![feature(test)]

extern crate hopper;
extern crate tempdir;
extern crate test;

use self::test::Bencher;

macro_rules! generate_snd {
    ($t:ty, $fn:ident, $s:expr) => {
        #[bench]
        fn $fn(b: &mut Bencher) {
            let dir = tempdir::TempDir::new("hopper").unwrap();
            let (mut snd, _) = hopper::channel("bench_snd", dir.path()).unwrap();
            b.iter(|| for i in 0..$s {
                snd.send(i as $t);
            });
        }
    }
}

macro_rules! generate_snd_rcv {
    ($t:ty, $fn:ident) => {
        #[bench]
        fn $fn(b: &mut Bencher) {
            let dir = tempdir::TempDir::new("hopper").unwrap();
            let (mut snd, mut rcv) = hopper::channel("bench_snd", dir.path()).unwrap();
            b.iter(|| {
                snd.send(12 as $t);
                rcv.iter().next().unwrap();
            });
        }
    }
}

macro_rules! generate_snd_all_rcv {
    ($t:ty, $fn:ident, $s:expr) => {
        #[bench]
        fn $fn(b: &mut Bencher) {
            let dir = tempdir::TempDir::new("hopper").unwrap();
            let (mut snd, mut rcv) = hopper::channel("bench_snd", dir.path()).unwrap();
            b.iter(|| {
                for i in 0..$s {
                    snd.send(i as $t);
                }
                for _ in 0..$s {
                    rcv.iter().next().unwrap();
                }
            });
        }
    }
}

mod u16 {
    use super::*;

    generate_snd!(u16, snd_100, 100);
    generate_snd!(u16, snd_1000, 1000);
    generate_snd!(u16, snd_10000, 10_000);
    generate_snd!(u16, snd_65535, 65_535);

    generate_snd_rcv!(u16, snd_rcv_100);
    generate_snd_rcv!(u16, snd_rcv_1000);
    generate_snd_rcv!(u16, snd_rcv_10000);
    generate_snd_rcv!(u16, snd_rcv_65535);

    generate_snd_all_rcv!(u16, snd_all_rcv_100, 100);
    generate_snd_all_rcv!(u16, snd_all_rcv_1000, 1000);
    generate_snd_all_rcv!(u16, snd_all_rcv_10000, 10_000);
    generate_snd_all_rcv!(u16, snd_all_rcv_65535, 65_535);
}

mod u32 {
    use super::*;

    generate_snd!(u32, snd_100, 100);
    generate_snd!(u32, snd_1000, 1000);
    generate_snd!(u32, snd_10000, 10_000);
    generate_snd!(u32, snd_65535, 65_535);

    generate_snd_rcv!(u32, snd_rcv_100);
    generate_snd_rcv!(u32, snd_rcv_1000);
    generate_snd_rcv!(u32, snd_rcv_10000);
    generate_snd_rcv!(u32, snd_rcv_65535);

    generate_snd_all_rcv!(u32, snd_all_rcv_100, 100);
    generate_snd_all_rcv!(u32, snd_all_rcv_1000, 1000);
    generate_snd_all_rcv!(u32, snd_all_rcv_10000, 10_000);
    generate_snd_all_rcv!(u32, snd_all_rcv_65535, 65_535);
}

mod u64 {
    use super::*;

    generate_snd!(u64, snd_100, 100);
    generate_snd!(u64, snd_1000, 1000);
    generate_snd!(u64, snd_10000, 10_000);
    generate_snd!(u64, snd_65535, 65_535);

    generate_snd_rcv!(u64, snd_rcv_100);
    generate_snd_rcv!(u64, snd_rcv_1000);
    generate_snd_rcv!(u64, snd_rcv_10000);
    generate_snd_rcv!(u64, snd_rcv_65535);

    generate_snd_all_rcv!(u64, snd_all_rcv_100, 100);
    generate_snd_all_rcv!(u64, snd_all_rcv_1000, 1000);
    generate_snd_all_rcv!(u64, snd_all_rcv_10000, 10_000);
    generate_snd_all_rcv!(u64, snd_all_rcv_65535, 65_535);
}
