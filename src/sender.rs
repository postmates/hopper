use bincode::{serialize_into, Infinite};
use byteorder::{BigEndian, WriteBytesExt};
use private;
use serde::{Deserialize, Serialize};
use std::fs;
use std::sync::MutexGuard;
use deque::BackGuardInner;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use flate2::Compression;
use flate2::write::DeflateEncoder;
use deque;

#[derive(Debug)]
/// The 'send' side of hopper, similar to `std::sync::mpsc::Sender`.
pub struct Sender<T> {
    name: String,
    root: PathBuf, // directory we store our queues in
    max_disk_bytes: usize,
    mem_buffer: private::Queue<T>,
    resource_type: PhantomData<T>,
}

#[derive(Default, Debug)]
pub struct SenderSync {
    pub sender_fp: Option<BufWriter<fs::File>>,
    pub bytes_written: usize,
    pub sender_seq_num: usize,
    pub path: PathBuf, // active fp filename
}

impl<'de, T> Clone for Sender<T>
where
    T: Serialize + Deserialize<'de>,
{
    fn clone(&self) -> Sender<T> {
        Sender {
            name: self.name.clone(),
            root: self.root.clone(),
            max_disk_bytes: self.max_disk_bytes,
            mem_buffer: self.mem_buffer.clone(),
            resource_type: self.resource_type,
        }
    }
}

impl<T> Sender<T>
where
    T: Serialize,
{
    #[doc(hidden)]
    pub fn new<S>(
        name: S,
        data_dir: &Path,
        max_disk_bytes: usize,
        mem_buffer: private::Queue<T>,
    ) -> Result<Sender<T>, super::Error>
    where
        S: Into<String>,
    {
        let setup_mem_buffer = mem_buffer.clone(); // clone is cheeeeeap
        let mut guard = setup_mem_buffer.lock_back();
        if !data_dir.is_dir() {
            return Err(super::Error::NoSuchDirectory);
        }
        match private::read_seq_num(data_dir) {
            Ok(seq_num) => {
                let log = data_dir.join(format!("{}", seq_num));
                match fs::OpenOptions::new().append(true).create(true).open(&log) {
                    Ok(fp) => {
                        (*guard).inner.sender_fp = Some(BufWriter::new(fp));
                        (*guard).inner.sender_seq_num = seq_num;
                        (*guard).inner.path = log;
                        Ok(Sender {
                            name: name.into(),
                            root: data_dir.to_path_buf(),
                            max_disk_bytes: max_disk_bytes,
                            mem_buffer: mem_buffer,
                            resource_type: PhantomData,
                        })
                    }
                    Err(e) => Err(super::Error::IoError(e)),
                }
            }
            Err(e) => Err(super::Error::IoError(e)),
        }
    }

    fn write_to_disk(
        &self,
        event: T,
        guard: &mut MutexGuard<BackGuardInner<SenderSync>>,
    ) -> Result<(), (T, super::Error)> {
        let mut buf: Vec<u8> = Vec::with_capacity(64);
        buf.clear();
        let mut e = DeflateEncoder::new(buf, Compression::fast());
        serialize_into(&mut e, &event, Infinite).expect("could not serialize");
        buf = e.finish().unwrap();
        let payload_len = buf.len();
        // If the individual sender writes enough to go over the max we mark the
        // file read-only--which will help the receiver to decide it has hit the
        // end of its log file--and create a new log file.
        let bytes_written =
            (*guard).inner.bytes_written + payload_len + ::std::mem::size_of::<u64>();
        if (bytes_written > self.max_disk_bytes) || (*guard).inner.sender_fp.is_none() {
            // Once we've gone over the write limit for our current file or find
            // that we've gotten behind the current queue file we need to seek
            // forward to find our place in the space of queue files. We mark
            // our current file read-only and then bump sender_seq_num to get up
            // to date.
            let _ = fs::metadata(&(*guard).inner.path).map(|p| {
                let mut permissions = p.permissions();
                permissions.set_readonly(true);
                let _ = fs::set_permissions(&(*guard).inner.path, permissions);
            });
            (*guard).inner.sender_seq_num = (*guard).inner.sender_seq_num.wrapping_add(1);
            (*guard).inner.path = self.root.join(format!("{}", (*guard).inner.sender_seq_num));
            match fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(&(*guard).inner.path)
            {
                Ok(fp) => {
                    (*guard).inner.sender_fp = Some(BufWriter::new(fp));
                    (*guard).inner.bytes_written = 0;
                }
                Err(e) => {
                    return Err((event, super::Error::IoError(e)));
                }
            }
        }

        assert!((*guard).inner.sender_fp.is_some());
        let mut bytes_written = 0;
        if let Some(ref mut fp) = (*guard).inner.sender_fp {
            match fp.write_u64::<BigEndian>(payload_len as u64) {
                Ok(()) => bytes_written += ::std::mem::size_of::<u64>(),
                Err(e) => {
                    return Err((event, super::Error::IoError(e)));
                }
            };
            match fp.write(&buf[..]) {
                Ok(written) => {
                    assert_eq!(payload_len, written);
                    bytes_written += written;
                }
                Err(e) => {
                    return Err((event, super::Error::IoError(e)));
                }
            }
        }
        (*guard).inner.bytes_written += bytes_written;
        Ok(())
    }

    /// send writes data out in chunks, like so:
    ///
    ///  u32: payload_size
    ///  [u8] payload
    ///
    pub fn send(&mut self, event: T) -> Result<(), (T, super::Error)> {
        // Welcome. Let me tell you about the time I fell off the toilet, hit my
        // head and when I woke up I saw this! ~passes knapkin drawing of the
        // flux capacitor over to you~
        //
        // This function pushes `event` into an in-memory deque unless that
        // deque is full. Previous versions of hopper had a series of
        // complicated flags and a monolock to coordinate with the Receiver to
        // make sure that the `event` would eventually make it to disk or be
        // stuffed into memory and order would be preserved throughout. This was
        // goofy.
        //
        // What struck me, noodling about how to keep order, was that if I
        // dropped all the flags and just used the data itself to signal where
        // and in what order we needed to read we'd be in business. Every
        // `event` gets wrapped in a `private::Placement` that indicates to the
        // Receiver if it's in-memory -- in which case, it's right there -- or
        // how many things are on disk needing to be read.
        //
        // Dang!
        //
        // We start off this function assuming that we can place the event into
        // memory and, failing that, then pop the last pushed item off the
        // deque, combine that and this event into a disk placement, write to
        // disk, and then push the disk placement onto the deque. Order is
        // preserved, a couple of things go to disk and we're capped on memory.
        let mut back_guard = self.mem_buffer.lock_back();
        let placed_event = private::Placement::Memory(event);
        match self.mem_buffer.push_back(placed_event, &mut back_guard) {
            Ok(must_wake_receiver) => {
                if must_wake_receiver {
                    let front_guard = self.mem_buffer.lock_front();
                    self.mem_buffer.notify_not_empty(&front_guard);
                    drop(front_guard);
                }
                Ok(())
            }
            Err(deque::Error::Full(placed_event)) => {
                match self.mem_buffer.pop_back_no_block(&mut back_guard) {
                    None => {
                        // receiver cleared us out
                        //
                        // It's possible between the time we've failed to write
                        // and then try to dequeue the last entry that the
                        // receiver has come through and pop_front'ed
                        // everything. In that case, we push_back and move on.
                        match self.mem_buffer.push_back(placed_event, &mut back_guard) {
                            Ok(must_wake_receiver) => {
                                if must_wake_receiver {
                                    let mut front_guard = self.mem_buffer.lock_front();
                                    self.mem_buffer.notify_not_empty(&front_guard);
                                }
                                Ok(())
                            }
                            _ => unreachable!(),
                        }
                    }
                    Some(inner) => {
                        let mut wrote_to_disk = 0;
                        // We know we have to create a disk placement here. It's
                        // possible that the previous item will be a disk
                        // placement -- in which case, we just write and bump
                        // the counter -- or is a memory placement and we have
                        // to write both to disk and create a disk placement to
                        // reflect that.
                        match inner {
                            private::Placement::Memory(frnt) => {
                                self.write_to_disk(frnt, &mut back_guard)?;
                                self.write_to_disk(
                                    placed_event.extract().unwrap(),
                                    &mut back_guard,
                                )?;
                                wrote_to_disk += 2;
                            }
                            private::Placement::Disk(sz) => {
                                self.write_to_disk(
                                    placed_event.extract().unwrap(),
                                    &mut back_guard,
                                )?;
                                wrote_to_disk += sz;
                                wrote_to_disk += 1;
                            }
                        }
                        assert!((*back_guard).inner.sender_fp.is_some());
                        if let Some(ref mut fp) = (*back_guard).inner.sender_fp {
                            fp.flush().expect("unable to flush");
                        } else {
                            unreachable!()
                        }
                        match self.mem_buffer
                            .push_back(private::Placement::Disk(wrote_to_disk), &mut back_guard)
                        {
                            Ok(must_wake_receiver) => {
                                if must_wake_receiver {
                                    let front_guard = self.mem_buffer.lock_front();
                                    self.mem_buffer.notify_not_empty(&front_guard);
                                    drop(front_guard);
                                }
                                Ok(())
                            }
                            _ => unreachable!(),
                        }
                    }
                }
            }
        }
    }

    /// Return the sender's name
    pub fn name(&self) -> &str {
        &self.name
    }
}
