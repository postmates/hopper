use bincode::serialize_into;
use byteorder::{BigEndian, WriteBytesExt};
use deque;
use deque::BackGuardInner;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use parking_lot::MutexGuard;
use private;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const PAYLOAD_LEN_BYTES: usize = ::std::mem::size_of::<u32>();

#[derive(Debug)]
/// The 'send' side of hopper, similar to `std::sync::mpsc::Sender`.
pub struct Sender<T> {
    name: String,
    root: PathBuf, // directory we store our queues in
    max_disk_bytes: usize,
    mem_buffer: private::Queue<T>,
    resource_type: PhantomData<T>,
    disk_files_capacity: Arc<AtomicUsize>,
}

#[derive(Default, Debug)]
pub struct SenderSync {
    pub sender_fp: Option<BufWriter<fs::File>>,
    pub bytes_written: usize,
    pub sender_seq_num: usize,
    pub total_disk_writes: usize,
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
            disk_files_capacity: Arc::clone(&self.disk_files_capacity),
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
        max_disk_files: Arc<AtomicUsize>,
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
                            max_disk_bytes,
                            mem_buffer,
                            resource_type: PhantomData,
                            disk_files_capacity: max_disk_files,
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
        let mut e = DeflateEncoder::new(buf, Compression::fast());
        serialize_into(&mut e, &event).expect("could not serialize");
        buf = e.finish().unwrap();
        let payload_len = buf.len();
        // If the individual sender writes enough to go over the max we mark the
        // file read-only--which will help the receiver to decide it has hit the
        // end of its log file--and create a new log file.
        let bytes_written = (*guard).inner.bytes_written + payload_len + PAYLOAD_LEN_BYTES;
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
            let disk_files_capacity = self.disk_files_capacity.load(Ordering::Acquire);
            if disk_files_capacity == 0 {
                return Err((event, super::Error::Full));
            } else {
                match fs::OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&(*guard).inner.path)
                {
                    Ok(fp) => {
                        self.disk_files_capacity.fetch_sub(1, Ordering::Release);
                        (*guard).inner.sender_fp = Some(BufWriter::new(fp));
                        (*guard).inner.bytes_written = 0;
                    }
                    Err(e) => {
                        return Err((event, super::Error::IoError(e)));
                    }
                }
            }
        }

        assert!((*guard).inner.sender_fp.is_some());
        let mut bytes_written = 0;
        if let Some(ref mut fp) = (*guard).inner.sender_fp {
            match fp.write_u32::<BigEndian>(payload_len as u32) {
                Ok(()) => bytes_written += PAYLOAD_LEN_BYTES,
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

    /// Attempt to flush any outstanding disk writes to the deque
    ///
    /// This function will attempt to flush outstanding disk writes, which may
    /// fail if the in-memory buffer is full. This function is useful when
    /// traffic patterns are bursty, meaning a write may end up being stranded
    /// in limbo for a good spell.
    pub fn flush(&mut self) -> Result<(), super::Error> {
        let mut back_guard = self.mem_buffer.lock_back();
        if (*back_guard).inner.total_disk_writes != 0 {
            // disk mode
            assert!((*back_guard).inner.sender_fp.is_some());
            if let Some(ref mut fp) = (*back_guard).inner.sender_fp {
                fp.flush().expect("unable to flush");
            } else {
                unreachable!()
            }
            match self.mem_buffer.push_back(
                private::Placement::Disk((*back_guard).inner.total_disk_writes),
                &mut back_guard,
            ) {
                Ok(must_wake_receiver) => {
                    (*back_guard).inner.total_disk_writes = 0;
                    if must_wake_receiver {
                        let front_guard = self.mem_buffer.lock_front();
                        self.mem_buffer.notify_not_empty(&front_guard);
                        drop(front_guard);
                    }
                }
                Err(_) => {
                    return Err(super::Error::NoFlush);
                }
            }
        }
        Ok(())
    }

    /// Send a event into the queue
    ///
    /// This function will fail with IO errors if the underlying queue files are
    /// temporarily exhausted -- say, due to lack of file descriptors -- of with
    /// Full if there is no more space in the in-memory buffer _or_ on disk, as
    /// per the `max_disk_files` setting from
    /// `channel_with_explicit_capacity`. Ownership of the event will be
    /// returned back to the caller on failure.
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
        // memory and, failing that, write the value to disk and flip into 'disk
        // mode'. In disk mode every event is written to disk, then we attempt
        // to write a disk placement into the deque. If that succeeeds we move
        // back to memory-mode for the next write, in which we attempt to write
        // to the deque first. In this was order is preserved, a couple of
        // things go to disk and we're capped on memory. Or, more specifically:
        //
        // There are two sending modes: in-memory and to-disk. We detect that
        // we're in to-disk mode by the value of `total_disk_writes`. If it's
        // non-zero we default to writing to disk, then attempt a
        // `placement::Disk(total_disk_writes)` push_back. If that is a success
        // we're in in-memory mode. If that's a failure we're still in
        // to-disk. Similar story for flipping from in-memory to to-disk.
        let mut back_guard = self.mem_buffer.lock_back();
        if (*back_guard).inner.total_disk_writes == 0 {
            // in-memory mode
            let placed_event = private::Placement::Memory(event);
            match self.mem_buffer.push_back(placed_event, &mut back_guard) {
                Ok(must_wake_receiver) => {
                    if must_wake_receiver {
                        let front_guard = self.mem_buffer.lock_front();
                        self.mem_buffer.notify_not_empty(&front_guard);
                        drop(front_guard);
                    }
                }
                Err(deque::Error::Full(placed_event)) => {
                    self.write_to_disk(placed_event.extract().unwrap(), &mut back_guard)?;
                    (*back_guard).inner.total_disk_writes += 1;
                }
            }
        } else {
            // disk mode
            self.write_to_disk(event, &mut back_guard)?;
            (*back_guard).inner.total_disk_writes += 1;
            assert!((*back_guard).inner.sender_fp.is_some());
            if let Some(ref mut fp) = (*back_guard).inner.sender_fp {
                fp.flush().expect("unable to flush");
            } else {
                unreachable!()
            }
            if let Ok(must_wake_receiver) = self.mem_buffer.push_back(
                private::Placement::Disk((*back_guard).inner.total_disk_writes),
                &mut back_guard,
            ) {
                (*back_guard).inner.total_disk_writes = 0;
                if must_wake_receiver {
                    let front_guard = self.mem_buffer.lock_front();
                    self.mem_buffer.notify_not_empty(&front_guard);
                    drop(front_guard);
                }
            }
        }
        Ok(())
    }

    /// Return the sender's name
    pub fn name(&self) -> &str {
        &self.name
    }
}
