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
    if !root.is_dir() {
        match fs::create_dir_all(root.clone()) {
            Ok(()) => {}
            Err(e) => {
                return Err(Error::IoError(e));
            }
        }
    }
    let sz = mem::size_of::<T>();
    let max_disk_bytes = ::std::cmp::max(1_048_576, max_disk_bytes); // TODO
                                                                     // note that we cap the max_disk_bytes to 1Mb to avoid FD exhaustion, make GH
                                                                     // issue to remove restriction
    let in_memory_limit: usize = ::std::cmp::max(sz, max_memory_bytes / sz);
    let q: private::Queue<T> = deque::Queue::with_capacity(in_memory_limit);
    if let Err(e) = private::clear_directory(&root) {
        return Err(Error::IoError(e));
    }
    let receiver = Receiver::new(&root, q.clone())?;
    let sender = Sender::new(name, &root, max_disk_bytes, q)?;
    Ok((sender, receiver))
}

#[cfg(test)]
mod test {
    extern crate quickcheck;
    extern crate tempdir;

    use self::quickcheck::{QuickCheck, TestResult};
    use super::channel_with_explicit_capacity;
    use std::{mem, thread};

    #[test]
    fn round_trip() {
        fn rnd_trip(in_memory_limit: usize, max_bytes: usize, evs: Vec<Vec<u32>>) -> TestResult {
            let sz = mem::size_of_val(&evs);
            if (in_memory_limit / sz) == 0 || (max_bytes / sz) == 0 {
                return TestResult::discard();
            }
            if let Ok(dir) = tempdir::TempDir::new("hopper") {
                if let Ok((mut snd, mut rcv)) = channel_with_explicit_capacity(
                    "round_trip_order_preserved",
                    dir.path(),
                    in_memory_limit,
                    max_bytes,
                ) {
                    for mut ev in evs.clone() {
                        loop {
                            match snd.send(ev) {
                                Ok(()) => {
                                    break;
                                }
                                Err(res) => {
                                    ev = res.0;
                                }
                            }
                        }
                    }
                    for ev in evs {
                        let mut attempts = 0;
                        loop {
                            match rcv.iter().next() {
                                None => {
                                    attempts += 1;
                                    assert!(attempts < 10_000);
                                }
                                Some(res) => {
                                    assert_eq!(res, ev);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            TestResult::passed()
        }
        QuickCheck::new().quickcheck(rnd_trip as fn(usize, usize, Vec<Vec<u32>>) -> TestResult);
    }

    // TODO
    // - Drop for deque
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
            if let Ok(dir) = tempdir::TempDir::new("hopper") {
                if let Ok((snd, mut rcv)) =
                    channel_with_explicit_capacity("tst", dir.path(), in_memory_bytes, disk_bytes)
                {
                    let chunk_size = vals.len() / total_senders;

                    let mut snd_jh = Vec::new();
                    let snd_vals = vals.clone();
                    for chunk in snd_vals.chunks(chunk_size) {
                        let mut thr_snd = snd.clone();
                        let chunk = chunk.to_vec();
                        snd_jh.push(thread::spawn(move || {
                            let mut queued = Vec::new();
                            for mut ev in chunk {
                                loop {
                                    match thr_snd.send(ev) {
                                        Err(res) => {
                                            ev = res.0;
                                        }
                                        Ok(()) => {
                                            break;
                                        }
                                    }
                                }
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
                            let mut attempts = 0;
                            loop {
                                match rcv_iter.next() {
                                    None => {
                                        attempts += 1;
                                        assert!(attempts < 10_000);
                                    }
                                    Some(res) => {
                                        collected.push(res);
                                        break;
                                    }
                                }
                            }
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
                }
            }
            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(usize, usize, usize, Vec<u64>) -> TestResult);
    }

    fn single_sender_single_rcv_round_trip_exp(
        in_memory_bytes: usize,
        disk_bytes: usize,
        total_vals: usize,
    ) -> bool {
        if let Ok(dir) = tempdir::TempDir::new("hopper") {
            if let Ok((mut snd, mut rcv)) =
                channel_with_explicit_capacity("tst", dir.path(), in_memory_bytes, disk_bytes)
            {
                let builder = thread::Builder::new();
                if let Ok(snd_jh) = builder.spawn(move || {
                    for i in 0..total_vals {
                        loop {
                            if snd.send(i).is_ok() {
                                break;
                            }
                        }
                    }
                }) {
                    let builder = thread::Builder::new();
                    if let Ok(rcv_jh) = builder.spawn(move || {
                        let mut rcv_iter = rcv.iter();
                        let mut cur = 0;
                        while cur != total_vals {
                            let mut attempts = 0;
                            loop {
                                if let Some(rcvd) = rcv_iter.next() {
                                    debug_assert_eq!(
                                        cur, rcvd,
                                        "FAILED TO GET ALL IN ORDER: {:?}",
                                        rcvd,
                                    );
                                    cur += 1;
                                    break;
                                } else {
                                    attempts += 1;
                                    assert!(attempts < 10_000);
                                }
                            }
                        }
                    }) {
                        snd_jh.join().expect("snd join failed");
                        rcv_jh.join().expect("rcv join failed");
                    }
                }
            }
        }
        true
    }

    #[test]
    fn single_sender_single_rcv_round_trip() {
        // Similar to the multi sender test except now with a single sender we
        // can guarantee order.
        fn inner(in_memory_bytes: usize, disk_bytes: usize, total_vals: usize) -> TestResult {
            let sz = mem::size_of::<u64>();
            if total_vals == 0 || (in_memory_bytes / sz) == 0 || (disk_bytes / sz) == 0 {
                return TestResult::discard();
            }
            TestResult::from_bool(single_sender_single_rcv_round_trip_exp(
                in_memory_bytes,
                disk_bytes,
                total_vals,
            ))
        }
        QuickCheck::new().quickcheck(inner as fn(usize, usize, usize) -> TestResult);
    }

}
