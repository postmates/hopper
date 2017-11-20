#![deny(missing_docs, missing_debug_implementations, missing_copy_implementations,
        trivial_numeric_casts, unsafe_code, unstable_features, unused_import_braces,
        unused_qualifications)]
//! hopper - an unbounded mpsc with bounded memory
//!
//! This module provides a version of the rust standard
//! [mpsc](https://doc.rust-lang.org/std/sync/mpsc/) that is unbounded but
//! consumes a bounded amount of memory. This is done by paging elements to disk
//! at need. The ambition here is to support mpsc style communication without
//! allocating unbounded amounts of memory or dropping inputs on the floor.
//!
//! How does hopper work? Imagine that hopper's internal structure is laid out
//! like a contiguous array:
//!
//! ```c
//! [---------------|----------------|~~~~~~~~~~~. . .~~~~~~~~~~~~~~~~]
//! 0               1024             2048
//! ```
//!
//! Between the indicies of 0 and 1024 hopper stores items in-memory until they
//! are retrieved. Above index 1024 items are paged out to disk. Items stored
//! between index 1024 and 2048 are temporarily buffered in memory to allow a
//! single page to disk once this buffer is full. This scheme fixes the memory
//! burden of the system at the expense of disk IO.
//!
//! Hopper is intended to be used in situtations where your system cannot
//! load-shed inputs and _must_ eventually process them. Hopper does page to
//! disk but has the same durabilty guarantees as stdlib mpsc between restarts:
//! none.
//!
//! # Inside Baseball
//!
//! Hopper's channel looks very much like a named pipe in Unix. You supply a
//! name to either `channel_2` or `channel_with_max_bytes_3` and you push bytes
//! in and out. The disk paging adds a complication. In private, the name
//! supplied to the above two functions is used to create a directory under
//! `data_dir`. This directory gets filled up with monotonically increasing
//! files in situations where the disk paging is in use. We'll treat this
//! exclusively from here on.
//!
//! The on-disk structure look like so:
//!
//! ```text
//! data-dir/
//!    sink-name0/
//!       0
//!       1
//!    sink-name1/
//!       0
//! ```
//!
//! You'll notice exports of Sender and Receiver in this module's
//! namespace. These are the structures that back the send and receive side of
//! the named channel. The Senders--there may be multiples of them--are
//! responsible for _creating_ "queue files". In the above,
//! `data-dir/sink-name*/*` are queue files. These files are treated as
//! append-only logs by the Senders. The Receivers trawl through these logs to
//! read the data serialized there.
//!
//! ## Won't this fill up my disk?
//!
//! Maybe! Each Sender has a notion of the maximum bytes it may read--which you
//! can set explicitly when creating a channel with
//! `channel_with_max_bytes`--and once the Sender has gone over that limit it'll
//! attempt to mark the queue file as read-only and create a new file. The
//! Receiver is programmed to read its current queue file until it reaches EOF
//! and finds the file is read-only, at which point it deletes the file--it is
//! the only reader--and moves on to the next.
//!
//! If the Receiver is unable to keep up with the Senders then, oops, your disk
//! will gradually fill up.
//!
//! ## What kind of filesystem options will I need?
//!
//! Hopper is intended to work on any wacky old filesystem with any options,
//! even at high concurrency. As common filesystems do not support interleaving
//! [small atomic
//! writes](https://stackoverflow.com/questions/32851672/is-overwriting-a-small-file-atomic-on-ext4)
//! hopper limits itself to one exclusive Sender or one exclusive Receiver at a
//! time. This potentially limits the concurrency of mpsc but maintains data
//! integrity. We are open to improvements in this area.
extern crate serde;
extern crate zlo;

mod receiver;
mod sender;
mod private;

pub use self::receiver::Receiver;
pub use self::sender::Sender;

use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fs;
use std::path::Path;
use std::sync;
use std::mem;

/// Defines the errors that hopper will bubble up
///
/// Hopper's error story is pretty bare right now. Hopper should be given sole
/// ownership over a directory and assumes such. If you look in the codebase
/// you'll find that there are a number cases where we bail out--to the
/// detriment of your program--where we might be able to recover but assume that
/// if an unkonwn condition _is_ hit it's a result of something foreign tainting
/// hopper's directory.
#[derive(Debug, Clone, Copy)]
pub enum Error {
    /// The directory given for use does not exist
    NoSuchDirectory,
}

/// Create a (Sender, Reciever) pair in a like fashion to
/// [`std::sync::mpsc::channel`](https://doc.rust-lang.org/std/sync/mpsc/fn.channel.html)
///
/// This function creates a Sender and Receiver pair with name `name` whose
/// queue files are stored in `data_dir`. The Sender is clonable.
///
/// # Example
/// ```
/// extern crate tempdir;
/// extern crate hopper;
///
/// let dir = tempdir::TempDir::new("hopper").unwrap();
/// let (mut snd, mut rcv) = hopper::channel("example", dir.path()).unwrap();
///
/// snd.send(9);
/// assert_eq!(Some(9), rcv.iter().next());
/// ```
pub fn channel<T>(name: &str, data_dir: &Path) -> Result<(Sender<T>, Receiver<T>), Error>
where
    T: Serialize + DeserializeOwned,
{
    channel_with_max_bytes(name, data_dir, 1_048_576 * 100)
}

/// Create a (Sender, Reciever) pair in a like fashion to
/// [`std::sync::mpsc::channel`](https://doc.rust-lang.org/std/sync/mpsc/fn.channel.html)
///
/// This function creates a Sender and Receiver pair with name `name` whose
/// queue files are stored in `data_dir`. The Sender is clonable.
///
/// This function gives control to the user over the maximum size of hopper's
/// queue files as `max_bytes`, though not the total disk allocation that may be
/// made.
pub fn channel_with_max_bytes<T>(
    name: &str,
    data_dir: &Path,
    max_bytes: usize,
) -> Result<(Sender<T>, Receiver<T>), Error>
where
    T: Serialize + DeserializeOwned,
{
    use std::sync::Arc; 

    let root = data_dir.join(name);
    let snd_root = root.clone();
    let rcv_root = root.clone();
    if !root.is_dir() {
        fs::create_dir_all(root).expect("could not create directory");
    }
    let cap: usize = 1024;
    let sz = mem::size_of::<T>();
    let max_bytes = if max_bytes < sz { sz } else { max_bytes };
    let fs_lock = sync::Arc::new(sync::Mutex::new(private::FsSync::new(cap)));
    let sender = try!(Sender::new(name, &snd_root, max_bytes, Arc::clone(&fs_lock)));
    let receiver = try!(Receiver::new(&rcv_root, fs_lock));
    Ok((sender, receiver))
}

#[cfg(test)]
mod test {
    extern crate quickcheck;
    extern crate tempdir;

    use std::thread;
    use super::{channel, channel_with_max_bytes};
    use self::quickcheck::{QuickCheck, TestResult};

    #[test]
    fn one_item_round_trip() {
        let dir = tempdir::TempDir::new("hopper").unwrap();
        let (mut snd, mut rcv) = channel("one_item_round_trip", dir.path()).unwrap();

        snd.send(1);

        assert_eq!(Some(1), rcv.iter().next());
    }

    #[test]
    fn zero_item_round_trip() {
        let dir = tempdir::TempDir::new("hopper").unwrap();
        let (mut snd, mut rcv) = channel("zero_item_round_trip", dir.path()).unwrap();

        assert_eq!(None, rcv.iter().next());

        snd.send(1);
        assert_eq!(Some(1), rcv.iter().next());
    }

    #[test]
    fn all_mem_buffer_round_trip() {
        let dir = tempdir::TempDir::new("hopper").unwrap();
        let (mut snd, mut rcv) = channel("zero_item_round_trip", dir.path()).unwrap();

        assert_eq!(None, rcv.iter().next());

        let cap = 1022;
        for _ in 0..cap {
            snd.send(1);
        }
        for _ in 0..cap {
            assert_eq!(Some(1), rcv.iter().next());
        }
    }

    #[test]
    fn full_mem_buffer_into_disk_round_trip() {
        let dir = tempdir::TempDir::new("hopper").unwrap();
        let (mut snd, mut rcv) = channel("zero_item_round_trip", dir.path()).unwrap();

        assert_eq!(None, rcv.iter().next());

        let cap = 1024;
        for _ in 0..cap {
            snd.send(1);
        }
        for _ in 0..cap {
            assert_eq!(Some(1), rcv.iter().next());
        }
    }

    #[test]
    fn full_mem_buffer_full_disk_round_trip() {
        let dir = tempdir::TempDir::new("hopper").unwrap();
        let (mut snd, mut rcv) = channel("zero_item_round_trip", dir.path()).unwrap();

        assert_eq!(None, rcv.iter().next());

        let cap = 2048;
        for _ in 0..cap {
            snd.send(1);
        }
        for _ in 0..cap {
            assert_eq!(Some(1), rcv.iter().next());
        }
    }

    #[test]
    fn full_mem_buffer_full_disk_multi_round_trip() {
        let dir = tempdir::TempDir::new("hopper").unwrap();
        let (mut snd, mut rcv) = channel("zero_item_round_trip", dir.path()).unwrap();

        assert_eq!(None, rcv.iter().next());

        let cap = 4048;
        for _ in 0..cap {
            snd.send(1);
        }
        for _ in 0..cap {
            assert_eq!(Some(1), rcv.iter().next());
        }
    }

    #[test]
    fn round_trip() {
        fn rnd_trip(max_bytes: usize, evs: Vec<Vec<u32>>) -> TestResult {
            let dir = tempdir::TempDir::new("hopper").unwrap();
            let (mut snd, mut rcv) =
                channel_with_max_bytes("round_trip_order_preserved", dir.path(), max_bytes)
                    .unwrap();

            for ev in evs.clone() {
                snd.send(ev);
            }

            for ev in evs {
                assert_eq!(Some(ev), rcv.iter().next());
            }
            TestResult::passed()
        }
        QuickCheck::new()
            .tests(100)
            .max_tests(1000)
            .quickcheck(rnd_trip as fn(usize, Vec<Vec<u32>>) -> TestResult);
    }

    #[test]
    fn round_trip_small_max_bytes() {
        fn rnd_trip(evs: Vec<Vec<u32>>) -> TestResult {
            println!("START {:?}", evs);
            let max_bytes: usize = 128;
            let dir = tempdir::TempDir::new("hopper").unwrap();
            let (mut snd, mut rcv) =
                channel_with_max_bytes("small_max_bytes", dir.path(), max_bytes).unwrap();

            for ev in evs.clone() {
                snd.send(ev);
            }

            let mut total = evs.len();
            for ev in evs {
                println!("REMAINING: {}", total);
                assert_eq!(Some(ev), rcv.iter().next());
                total -= 1;
            }
            TestResult::passed()
        }
        QuickCheck::new()
            .tests(100)
            .max_tests(1000)
            .quickcheck(rnd_trip as fn(Vec<Vec<u32>>) -> TestResult);
    }

    #[test]
    fn concurrent_snd_and_rcv_round_trip() {
        let max_bytes: usize = 512;
        let dir = tempdir::TempDir::new("hopper").unwrap();
        println!("CONCURRENT SND_RECV TESTDIR: {:?}", dir);
        let (snd, mut rcv) = channel_with_max_bytes(
            "concurrent_snd_and_rcv_small_max_bytes",
            dir.path(),
            max_bytes,
        ).unwrap();
        let max_thrs = 32;
        let max_sz = 1000;

        // construct the payload to send repeatedly 'pyld' and the final payload
        // the sender should receive
        let mut tst_pylds = Vec::new();
        for i in 0..(max_sz * max_thrs) {
            tst_pylds.push(i);
        }

        let mut joins = Vec::new();

        // start our receiver thread
        joins.push(thread::spawn(move || {
            // assert that we receive every element in tst_pylds by pulling a new
            // value from the rcv, then marking it out of tst_pylds
            for _ in 0..(max_sz * max_thrs) {
                loop {
                    if let Some(nxt) = rcv.iter().next() {
                        let idx = tst_pylds.binary_search(&nxt).expect("DID NOT FIND ELEMENT");
                        tst_pylds.remove(idx);
                        break;
                    }
                }
            }
            assert!(tst_pylds.is_empty());
        }));

        // start all our sender threads and blast away
        for i in 0..max_thrs {
            let mut thr_snd = snd.clone();
            joins.push(thread::spawn(move || {
                let base = i * max_sz;
                for p in 0..max_sz {
                    thr_snd.send(base + p);
                }
            }));
        }

        // wait until the senders are for sure done
        for jh in joins {
            jh.join().expect("Uh oh, child thread paniced!");
        }
    }

    #[test]
    fn qc_concurrent_snd_and_rcv_round_trip() {
        fn snd_rcv(evs: Vec<Vec<u32>>) -> TestResult {
            let max_bytes: usize = 512;
            let dir = tempdir::TempDir::new("hopper").unwrap();
            println!("CONCURRENT SND_RECV TESTDIR: {:?}", dir);
            let (snd, mut rcv) = channel_with_max_bytes(
                "concurrent_snd_and_rcv_small_max_bytes",
                dir.path(),
                max_bytes,
            ).unwrap();

            let max_thrs = 32;

            let mut joins = Vec::new();

            // start our receiver thread
            let total_pylds = evs.len() * max_thrs;
            joins.push(thread::spawn(move || for _ in 0..total_pylds {
                loop {
                    if let Some(_) = rcv.iter().next() {
                        break;
                    }
                }
            }));

            // start all our sender threads and blast away
            for _ in 0..max_thrs {
                let mut thr_snd = snd.clone();
                let thr_evs = evs.clone();
                joins.push(thread::spawn(move || for e in thr_evs {
                    thr_snd.send(e);
                }));
            }

            // wait until the senders are for sure done
            for jh in joins {
                jh.join().expect("Uh oh, child thread paniced!");
            }
            TestResult::passed()
        }
        QuickCheck::new()
            .tests(100)
            .max_tests(1000)
            .quickcheck(snd_rcv as fn(Vec<Vec<u32>>) -> TestResult);
    }
}
