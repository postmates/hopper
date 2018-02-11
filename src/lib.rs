#![deny(missing_docs, missing_debug_implementations, missing_copy_implementations,
        trivial_numeric_casts, unstable_features, unused_import_braces, unused_qualifications)]
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
extern crate bincode;
extern crate byteorder;
extern crate flate2;
extern crate serde;

mod receiver;
mod sender;
mod private;
mod deque;

pub use self::receiver::Receiver;
pub use self::sender::Sender;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::{fmt, fs, io, mem};
use std::path::Path;

/// Defines the errors that hopper will bubble up
///
/// Hopper's error story is pretty bare right now. Hopper should be given sole
/// ownership over a directory and assumes such. If you look in the codebase
/// you'll find that there are a number cases where we bail out--to the
/// detriment of your program--where we might be able to recover but assume that
/// if an unkonwn condition _is_ hit it's a result of something foreign tainting
/// hopper's directory.
#[derive(Debug)]
pub enum Error {
    /// The directory given for use does not exist
    NoSuchDirectory,
    /// Stdlib IO Error
    IoError(io::Error),
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
    T: Serialize + DeserializeOwned + fmt::Debug,
{
    channel_with_explicit_capacity(name, data_dir, 1_048_576, 1_048_576 * 100)
}

/// Create a (Sender, Reciever) pair in a like fashion to
/// [`std::sync::mpsc::channel`](https://doc.rust-lang.org/std/sync/mpsc/fn.channel.html)
///
/// This function creates a Sender and Receiver pair with name `name` whose
/// queue files are stored in `data_dir`. The Sender is clonable.
///
/// This function gives control to the user over the maximum size of hopper's
/// queue files as `max_disk_bytes`, though not the total disk allocation that
/// may be made. Hopper will only be allowed to buffer 2*`max_memory_bytes`.
pub fn channel_with_explicit_capacity<T>(
    name: &str,
    data_dir: &Path,
    max_memory_bytes: usize,
    max_disk_bytes: usize,
) -> Result<(Sender<T>, Receiver<T>), Error>
where
    T: Serialize + DeserializeOwned + fmt::Debug,
{
    let root = data_dir.join(name);
    let snd_root = root.clone();
    let rcv_root = root.clone();
    if !root.is_dir() {
        match fs::create_dir_all(root) {
            Ok(()) => {}
            Err(e) => {
                return Err(Error::IoError(e));
            }
        }
    }
    let sz = mem::size_of::<T>();
    let max_disk_bytes = ::std::cmp::max(sz, max_disk_bytes);
    let in_memory_limit: usize = ::std::cmp::max(sz, max_memory_bytes / sz);
    let q: private::Queue<T> = deque::Queue::with_capacity(in_memory_limit);
    let sender = Sender::new(name, &snd_root, max_disk_bytes, q.clone())?;
    let receiver = Receiver::new(&rcv_root, q)?;
    Ok((sender, receiver))
}

#[cfg(test)]
mod test {
    extern crate quickcheck;
    extern crate tempdir;

    use self::quickcheck::{QuickCheck, TestResult};
    use super::{channel, channel_with_explicit_capacity};
    use std::{mem, thread};

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

        snd.send(1);
        assert_eq!(Some(1), rcv.iter().next());
    }

    #[test]
    fn all_mem_buffer_round_trip() {
        let dir = tempdir::TempDir::new("hopper").unwrap();
        let (mut snd, mut rcv) = channel("zero_item_round_trip", dir.path()).unwrap();

        let cap = 1022;
        for i in 0..cap {
            snd.send(i);
        }
        for i in 0..cap {
            assert_eq!(Some(i), rcv.iter().next());
        }
    }

    #[test]
    fn full_mem_buffer_full_disk_multi_round_trip() {
        let dir = tempdir::TempDir::new("hopper").unwrap();
        let sz = ::std::mem::size_of::<u64>();
        let (mut snd, mut rcv) =
            channel_with_explicit_capacity("tst", dir.path(), 4 * sz, 8 * sz).unwrap();

        let cap = 16;
        for i in 0..cap {
            snd.send(i);
        }
        for i in 0..cap {
            assert_eq!(Some(i), rcv.iter().next());
        }
    }

    #[test]
    fn round_trip() {
        fn rnd_trip(in_memory_limit: usize, max_bytes: usize, evs: Vec<Vec<u32>>) -> TestResult {
            let sz = mem::size_of_val(&evs);
            if (in_memory_limit / sz) == 0 || (max_bytes / sz) == 0 {
                return TestResult::discard();
            }
            let dir = tempdir::TempDir::new("hopper").unwrap();
            let (mut snd, mut rcv) = channel_with_explicit_capacity(
                "round_trip_order_preserved",
                dir.path(),
                in_memory_limit,
                max_bytes,
            ).unwrap();

            for ev in evs.clone() {
                snd.send(ev);
            }

            for ev in evs {
                assert_eq!(Some(ev), rcv.iter().next());
            }
            TestResult::passed()
        }
        QuickCheck::new().quickcheck(rnd_trip as fn(usize, usize, Vec<Vec<u32>>) -> TestResult);
    }

    #[test]
    fn round_trip_small_max_bytes() {
        fn rnd_trip(evs: Vec<Vec<u32>>) -> TestResult {
            let in_memory_limit: usize = 1024 * ::std::mem::size_of::<u32>();
            let max_bytes: usize = 128;
            let dir = tempdir::TempDir::new("hopper").unwrap();
            let (mut snd, mut rcv) = channel_with_explicit_capacity(
                "small_max_bytes",
                dir.path(),
                in_memory_limit,
                max_bytes,
            ).unwrap();

            for ev in evs.clone() {
                snd.send(ev);
            }

            for ev in evs {
                assert_eq!(Some(ev), rcv.iter().next());
            }
            TestResult::passed()
        }
        QuickCheck::new().quickcheck(rnd_trip as fn(Vec<Vec<u32>>) -> TestResult);
    }

    // TODO
    // - Drop for deque
    // - AFL everywhere
    // - benchmarking
    // - no println's
    // - no TODO in the codebase

    #[test]
    fn multi_thread_concurrent_snd_and_rcv_round_trip() {
        fn inner(
            total_senders: usize,
            in_memory_bytes: usize,
            disk_bytes: usize,
            vals: Vec<u64>,
        ) -> TestResult {
            let sz = mem::size_of::<u64>();
            if total_senders == 0 || total_senders > 10 || vals.len() == 0
                || (vals.len() < total_senders) || (in_memory_bytes / sz) == 0
                || (disk_bytes / sz) == 0
            {
                return TestResult::discard();
            }
            let dir = tempdir::TempDir::new("hopper").unwrap();
            let (snd, mut rcv) =
                channel_with_explicit_capacity("tst", dir.path(), in_memory_bytes, disk_bytes)
                    .unwrap();

            let chunk_size = vals.len() / total_senders;

            let mut snd_jh = Vec::new();
            let snd_vals = vals.clone();
            for chunk in snd_vals.chunks(chunk_size) {
                let mut thr_snd = snd.clone();
                let chunk = chunk.to_vec();
                snd_jh.push(thread::spawn(move || {
                    let mut queued = Vec::new();
                    for ev in chunk {
                        thr_snd.send(ev);
                        queued.push(ev);
                    }
                    queued
                }))
            }

            let expected_total_vals = vals.len();
            let rcv_jh = thread::spawn(move || {
                let mut collected = Vec::new();
                let mut rcv_iter = rcv.iter();
                while collected.len() < expected_total_vals {
                    let v = rcv_iter.next().unwrap();
                    collected.push(v);
                }
                collected
            });

            let mut snd_vals: Vec<u64> = Vec::new();
            for jh in snd_jh {
                snd_vals.append(&mut jh.join().expect("snd join failed"));
            }
            let mut rcv_vals = rcv_jh.join().expect("rcv join failed");

            rcv_vals.sort();
            snd_vals.sort();

            assert_eq!(rcv_vals, snd_vals);
            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(usize, usize, usize, Vec<u64>) -> TestResult);
    }

    #[test]
    fn single_sender_single_rcv_round_trip() {
        // Similar to the multi sender test except now with a single sender we
        // can guarantee order.
        fn inner(in_memory_bytes: usize, disk_bytes: usize, mut vals: Vec<u64>) -> TestResult {
            let sz = mem::size_of::<u64>();
            if vals.len() == 0 || (in_memory_bytes / sz) == 0 || (disk_bytes / sz) == 0 {
                return TestResult::discard();
            }
            let dir = tempdir::TempDir::new("hopper").unwrap();
            let (mut snd, mut rcv) =
                channel_with_explicit_capacity("tst", dir.path(), in_memory_bytes, disk_bytes)
                    .unwrap();

            let snd_vals = vals.clone();
            let snd_jh = thread::spawn(move || {
                for ev in snd_vals {
                    snd.send(ev);
                }
            });

            let rcv_jh = thread::spawn(move || {
                let mut rcv_iter = rcv.iter();
                while !vals.is_empty() {
                    assert_eq!(vals.remove(0), rcv_iter.next().unwrap());
                }
            });

            snd_jh.join().expect("snd join failed");
            rcv_jh.join().expect("rcv join failed");
            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(usize, usize, Vec<u64>) -> TestResult);
    }

}
