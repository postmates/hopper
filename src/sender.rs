use bincode::{serialize_into, Infinite};
use byteorder::{BigEndian, WriteBytesExt};
use private;
use serde::{Deserialize, Serialize};
use std::fmt;
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
    path: PathBuf, // active fp filename
    seq_num: usize,
    max_disk_bytes: usize,
    mem_buffer: private::Queue<T>,
    resource_type: PhantomData<T>,
}

#[derive(Default, Debug)]
pub struct SenderSync {
    pub sender_fp: Option<BufWriter<fs::File>>,
    pub bytes_written: usize,
    pub sender_seq_num: usize,
}

impl<'de, T> Clone for Sender<T>
where
    T: Serialize + Deserialize<'de>,
{
    fn clone(&self) -> Sender<T> {
        Sender::new(
            self.name.clone(),
            &self.root,
            self.max_disk_bytes,
            self.mem_buffer.clone(),
        ).expect("COULD NOT CLONE")
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
        S: Into<String> + fmt::Display,
    {
        let setup_mem_buffer = mem_buffer.clone(); // clone is cheeeeeap
        let mut guard = setup_mem_buffer.lock_back();
        if !data_dir.is_dir() {
            return Err(super::Error::NoSuchDirectory);
        }
        let seq_num = match fs::read_dir(data_dir)
            .unwrap()
            .map(|de| {
                de.unwrap()
                    .path()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .parse::<usize>()
                    .unwrap()
            })
            .max()
        {
            Some(sn) => sn,
            None => 0,
        };
        let log = data_dir.join(format!("{}", seq_num));
        match fs::OpenOptions::new().append(true).create(true).open(&log) {
            Ok(fp) => {
                (*guard).inner.sender_fp = Some(BufWriter::new(fp));
                (*guard).inner.sender_seq_num = seq_num;
                Ok(Sender {
                    name: name.into(),
                    root: data_dir.to_path_buf(),
                    path: log,
                    seq_num: seq_num,
                    max_disk_bytes: max_disk_bytes,
                    mem_buffer: mem_buffer,
                    resource_type: PhantomData,
                })
            }
            Err(e) => panic!("[Sender] failed to start {:?}", e),
        }
    }

    fn write_to_disk(&mut self, event: T, mut guard: &mut MutexGuard<BackGuardInner<SenderSync>>) {
        let mut buf: Vec<u8> = Vec::with_capacity(64);
        buf.clear();
        let mut e = DeflateEncoder::new(buf, Compression::fast());
        serialize_into(&mut e, &event, Infinite).expect("could not serialize");
        buf = e.finish().unwrap();
        let payload_len = buf.len();
        // If the individual sender writes enough to go over the max
        // we mark the file read-only--which will help the receiver
        // to decide it has hit the end of its log file--and create
        // a new log file.
        let bytes_written =
            (*guard).inner.bytes_written + payload_len + ::std::mem::size_of::<u64>();
        if (bytes_written > self.max_disk_bytes) || (self.seq_num != (*guard).inner.sender_seq_num)
            || (*guard).inner.sender_fp.is_none()
        {
            // Once we've gone over the write limit for our current
            // file or find that we've gotten behind the current
            // queue file we need to seek forward to find our place
            // in the space of queue files. We mark our current file
            // read-only--there's some possibility that this will be
            // done redundantly, but that's okay--and then read the
            // current sender_seq_num to get up to date.
            let _ = fs::metadata(&self.path).map(|p| {
                let mut permissions = p.permissions();
                permissions.set_readonly(true);
                let _ = fs::set_permissions(&self.path, permissions);
            });
            if (*guard).inner.sender_fp.is_some() {
                if self.seq_num != (*guard).inner.sender_seq_num {
                    // This thread is behind the leader. We've got to
                    // set our current notion of seq_num forward and
                    // then open the corresponding file.
                    self.seq_num = (*guard).inner.sender_seq_num;
                } else {
                    // This thread is the leader. We reset the
                    // sender_seq_num and bytes written and open the
                    // next queue file. All follower threads will hit
                    // the branch above this one.
                    (*guard).inner.sender_seq_num = self.seq_num.wrapping_add(1);
                    self.seq_num = (*guard).inner.sender_seq_num;
                    (*guard).inner.bytes_written = 0;
                }
            }
            self.path = self.root.join(format!("{}", self.seq_num));
            match fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(&self.path)
            {
                Ok(fp) => (*guard).inner.sender_fp = Some(BufWriter::new(fp)),
                Err(e) => panic!("FAILED TO OPEN {:?} WITH {:?}", &self.path, e),
            }
        }

        assert!((*guard).inner.sender_fp.is_some());
        let mut bytes_written = 0;
        if let Some(ref mut fp) = (*guard).inner.sender_fp {
            match fp.write_u64::<BigEndian>(payload_len as u64) {
                Ok(()) => bytes_written += ::std::mem::size_of::<u64>(),
                Err(e) => panic!("Write error: {}", e),
            };
            match fp.write(&buf[..]) {
                Ok(written) => {
                    assert_eq!(payload_len, written);
                    bytes_written += written;
                }
                Err(e) => panic!("Write error: {}", e),
            }
        }
        (*guard).inner.bytes_written += bytes_written;
    }

    /// send writes data out in chunks, like so:
    ///
    ///  u32: payload_size
    ///  [u8] payload
    ///
    pub fn send(&mut self, event: T) {
        let mut back_guard = self.mem_buffer.lock_back();
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
                // NOTES
                // I only have to fiddle with the back. I need to
                //
                //  * drop the size by one so that the receiver won't come through
                //  to read it
                //  * once that's done, pop_back
                //  * fiddle with the back and do the write routines
                //    - a memory placement at the back when full means two memory
                //      pops into disk, else no space to add
                //    - pop_front_no_block doesn't have to exist, nor push_front
                match self.mem_buffer.pop_back_no_block(&mut back_guard) {
                    None => {
                        // receiver cleared us out
                        match self.mem_buffer.push_back(placed_event, &mut back_guard) {
                            Ok(must_wake_receiver) => {
                                if must_wake_receiver {
                                    let mut front_guard = self.mem_buffer.lock_front();
                                    self.mem_buffer.notify_not_empty(&front_guard);
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                    Some(inner) => {
                        let mut wrote_to_disk = 0;
                        match inner {
                            private::Placement::Memory(frnt) => {
                                self.write_to_disk(frnt, &mut back_guard);
                                self.write_to_disk(
                                    placed_event.extract().unwrap(),
                                    &mut back_guard,
                                );
                                wrote_to_disk += 2;
                            }
                            private::Placement::Disk(sz) => {
                                self.write_to_disk(
                                    placed_event.extract().unwrap(),
                                    &mut back_guard,
                                );
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
                                    let mut front_guard = self.mem_buffer.lock_front();
                                    self.mem_buffer.notify_not_empty(&front_guard);
                                }
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
