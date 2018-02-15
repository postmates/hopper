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
//! Hopper is intended to be used in situtations where your system cannot
//! load-shed inputs and _must_ eventually process them. Hopper does page to
//! disk but has the same durabilty guarantees as stdlib mpsc between restarts:
//! none.
//!
//! # Inside Baseball
//!
//! Hopper's channel looks very much like a named pipe in Unix. You supply a
//! name to either `channel_2` or `channel_with_max_bytes_3` and you push bytes
//! in and out. The disk paging adds a complication. The name supplied to the
//! above two functions is used to create a directory under user-supplied
//! `data_dir`. This directory gets filled up with monotonically increasing
//! files.
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
//! Maybe! Each Sender has a notion of the maximum bytes that a queue file may
//! consume--which you can set explicitly when creating a channel with
//! `channel_with_explicit_capacity`--and once the Sender has gone over that
//! limit it'll attempt to mark the queue file as read-only and create a new
//! file. The Receiver is programmed to read its current queue file until it
//! reaches EOF and, finding the file is read-only, removes the queue file and
//! moves on to the next.
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
//! time. This potentially limits the concurrency but maintains data
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
    /// Could not flush Sender
    NoFlush,
}

/// Create a (Sender, Reciever) pair in a like fashion to
/// [`std::sync::mpsc::channel`](https://doc.rust-lang.org/std/sync/mpsc/fn.channel.html)
///
/// This function creates a Sender and Receiver pair with name `name` whose
/// queue files are stored in `data_dir`. The maximum number of bytes that will
/// be stored in-memory is 1Mb and the maximum size of a queue file will be
/// 100Mb. The Sender is clonable.
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
/// queue files are stored in `data_dir`. The maximum number of bytes that will
/// be stored in-memory are `max(max_memory_bytes, size_of(T))` and the maximum
/// size of a queue file will be `max(max_disk_bytes, 1Mb)`. The Sender is
/// clonable.
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
    let max_disk_bytes = ::std::cmp::max(1_048_576, max_disk_bytes);
    let total_memory_limit: usize = ::std::cmp::max(1, max_memory_bytes / sz);
    let q: private::Queue<T> = deque::Queue::with_capacity(total_memory_limit);
    if let Err(e) = private::clear_directory(&root) {
        return Err(Error::IoError(e));
    }
    let sender = Sender::new(name, &root, max_disk_bytes, q.clone())?;
    let receiver = Receiver::new(&root, q)?;
    Ok((sender, receiver))
}

#[cfg(test)]
mod test {
    extern crate quickcheck;
    extern crate tempdir;

    use self::quickcheck::{QuickCheck, TestResult};
    use super::channel_with_explicit_capacity;
    use std::{mem, thread};

    fn round_trip_exp(in_memory_limit: usize, max_bytes: usize, total_elems: usize) -> bool {
        // println!(
        //     "IN_MEMORY_LIMIT: {}, MAX_BYTES: {}, TOTAL_ELEMS: {}",
        //     in_memory_limit, max_bytes, total_elems
        // );
        if let Ok(dir) = tempdir::TempDir::new("hopper") {
            if let Ok((mut snd, mut rcv)) = channel_with_explicit_capacity(
                "round_trip_order_preserved",
                dir.path(),
                in_memory_limit,
                max_bytes,
            ) {
                for i in 0..total_elems {
                    loop {
                        if snd.send(i).is_ok() {
                            break;
                        }
                    }
                }
                // clear space for one more element
                let mut attempts = 0;
                loop {
                    match rcv.iter().next() {
                        None => {
                            attempts += 1;
                            assert!(attempts < 10_000);
                        }
                        Some(res) => {
                            assert_eq!(res, 0);
                            break;
                        }
                    }
                }
                // flush any disk writes
                loop {
                    if snd.flush().is_ok() {
                        break;
                    }
                }
                // pull the rest of the elements
                for i in 1..total_elems {
                    // println!("RECV: {}", i);
                    // the +1 is for the unflushed item
                    let mut attempts = 0;
                    loop {
                        match rcv.iter().next() {
                            None => {
                                attempts += 1;
                                assert!(attempts < 10_000);
                            }
                            Some(res) => {
                                assert_eq!(res, i);
                                break;
                            }
                        }
                    }
                }
            }
        }
        true
    }

    // #[test]
    // fn explicit_round_trip() {
    //     let in_memory_limit = 73;
    //     let max_bytes = 97;
    //     let total_elems = 0;
    //     assert!(round_trip_exp(in_memory_limit, max_bytes, total_elems))
    // }

    #[test]
    fn round_trip() {
        fn inner(in_memory_limit: usize, max_bytes: usize, total_elems: usize) -> TestResult {
            // println!(
            //     "IN_MEMORY_LIMIT: {}, MAX_BYTES: {}, TOTAL_ELEMS: {}",
            //     in_memory_limit, max_bytes, total_elems
            // );
            let sz = mem::size_of::<u64>();
            if (in_memory_limit / sz) == 0 || (max_bytes / sz) == 0 || total_elems == 0 {
                return TestResult::discard();
            }
            TestResult::from_bool(round_trip_exp(in_memory_limit, max_bytes, total_elems))
        }
        QuickCheck::new().quickcheck(inner as fn(usize, usize, usize) -> TestResult);
    }

    fn multi_thread_concurrent_snd_and_rcv_round_trip_exp(
        total_senders: usize,
        in_memory_bytes: usize,
        disk_bytes: usize,
        vals: Vec<u64>,
    ) -> bool {
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
                        println!("CHUNK: {:?}", chunk);
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
                        let mut attempts = 0;
                        loop {
                            if thr_snd.flush().is_ok() {
                                break;
                            }
                            thread::sleep(::std::time::Duration::from_millis(100));
                            attempts += 1;
                            assert!(attempts < 10);
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
                                    println!("RECVD: {:?}", res);
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
        true
    }

    #[test]
    fn explicit_multi_thread_concurrent_snd_and_rcv_round_trip() {
        let total_senders = 10;
        let in_memory_bytes = 50;
        let disk_bytes = 10;
        let vals = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        let mut loops = 0;
        loop {
            println!("\n\n\nLOOP: {}", loops);
            assert!(multi_thread_concurrent_snd_and_rcv_round_trip_exp(
                total_senders,
                in_memory_bytes,
                disk_bytes,
                vals.clone(),
            ));
            loops += 1;
            if loops > 1_000_000 {
                break;
            }
            thread::sleep(::std::time::Duration::from_millis(1));
        }
    }

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
            println!(
                "TOTAL_SENDERS: {}, IN_MEMORY_BYTES: {}, DISK_BYTES: {}, VALS: {:?}",
                total_senders, in_memory_bytes, disk_bytes, vals
            );
            TestResult::from_bool(multi_thread_concurrent_snd_and_rcv_round_trip_exp(
                total_senders,
                in_memory_bytes,
                disk_bytes,
                vals,
            ))
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
                            // println!("SEND: {}", i);
                            if snd.send(i).is_ok() {
                                break;
                            }
                        }
                    }
                    let mut attempts = 0;
                    loop {
                        if snd.flush().is_ok() {
                            break;
                        }
                        thread::sleep(::std::time::Duration::from_millis(100));
                        attempts += 1;
                        assert!(attempts < 10);
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
                                    // println!("SINGLE_SENDER RECV: {}", rcvd);
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
                        // println!("RECEIVER_DONE");
                    }) {
                        snd_jh.join().expect("snd join failed");
                        rcv_jh.join().expect("rcv join failed");
                    }
                }
            }
        }
        true
    }

    // #[test]
    // fn explicit_single_sender_single_rcv_round_trip() {
    //     let mut loops = 0;
    //     loop {
    //         // println!("\n\n\nLOOP: {}", loops);
    //         // if loops % 1000 == 0 {
    //         //     println!("LOOP {}", loops);
    //         // }
    //         assert!(single_sender_single_rcv_round_trip_exp(8, 8, 5));
    //         loops += 1;
    //         if loops > 1_000_000 {
    //             break;
    //         }
    //         thread::sleep(::std::time::Duration::from_millis(1));
    //     }
    // }

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
