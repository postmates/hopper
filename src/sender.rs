use std::io::{Cursor, Seek, SeekFrom};
use bincode::{serialize_into, serialized_size, Infinite};
use byteorder::{BigEndian, WriteBytesExt};
use private;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

#[derive(Debug)]
/// The 'send' side of hopper, similar to `std::sync::mpsc::Sender`.
pub struct Sender<T> {
    name: String,
    root: PathBuf, // directory we store our queues in
    path: PathBuf, // active fp filename
    seq_num: usize,
    in_memory_limit: usize,
    max_disk_bytes: usize,
    fs_lock: private::FSLock<T>,
    resource_type: PhantomData<T>,
}

impl<'de, T> Clone for Sender<T>
where
    T: Serialize + Deserialize<'de>,
{
    fn clone(&self) -> Sender<T> {
        use std::sync::Arc;
        Sender::new(
            self.name.clone(),
            &self.root,
            self.in_memory_limit,
            self.max_disk_bytes,
            Arc::clone(&self.fs_lock),
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
        in_memory_limit: usize,
        max_disk_bytes: usize,
        fs_lock: private::FSLock<T>,
    ) -> Result<Sender<T>, super::Error>
    where
        S: Into<String> + fmt::Display,
    {
        use std::sync::Arc;
        let init_fs_lock = Arc::clone(&fs_lock);
        let mut syn = init_fs_lock.lock().unwrap();
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
                syn.sender_fp = Some(BufWriter::new(fp));
                (*syn).sender_seq_num = seq_num;
                Ok(Sender {
                    name: name.into(),
                    root: data_dir.to_path_buf(),
                    path: log,
                    seq_num: seq_num,
                    in_memory_limit: in_memory_limit,
                    max_disk_bytes: max_disk_bytes,
                    fs_lock: fs_lock,
                    resource_type: PhantomData,
                })
            }
            Err(e) => panic!("[Sender] failed to start {:?}", e),
        }
    }

    /// send writes data out in chunks, like so:
    ///
    ///  u32: payload_size
    ///  [u8] payload
    ///
    pub fn send(&mut self, event: T) {
        let mut syn = self.fs_lock.lock().unwrap();
        let fslock = &mut (*syn);

        fslock
            .mem_buffer
            .push_back(private::Placement::Memory(event));
        if fslock.mem_buffer.len() >= self.in_memory_limit {
            let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::with_capacity(64));
            let mut wrote_to_disk = 0;
            while fslock.mem_buffer.len().saturating_sub(self.in_memory_limit) > 0 {
                if let Some(placement) = fslock.mem_buffer.pop_front() {
                    match placement {
                        private::Placement::Memory(ev) => {
                            buf.seek(SeekFrom::Start(0)).unwrap();
                            let payload_len: u64 = serialized_size(&ev);
                            buf.write_u64::<BigEndian>(payload_len)
                                .expect("could not write size prefix");
                            serialize_into(&mut buf, &ev, Infinite).expect("could not serialize");
                            // If the individual sender writes enough to go over the max
                            // we mark the file read-only--which will help the receiver
                            // to decide it has hit the end of its log file--and create
                            // a new log file.
                            let bytes_written = fslock.bytes_written + buf.get_ref().len();
                            if (bytes_written > self.max_disk_bytes)
                                || (self.seq_num != fslock.sender_seq_num)
                                || fslock.sender_fp.is_none()
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
                                if fslock.sender_fp.is_some() {
                                    if self.seq_num != fslock.sender_seq_num {
                                        // This thread is behind the leader. We've got to
                                        // set our current notion of seq_num forward and
                                        // then open the corresponding file.
                                        self.seq_num = fslock.sender_seq_num;
                                    } else {
                                        // This thread is the leader. We reset the
                                        // sender_seq_num and bytes written and open the
                                        // next queue file. All follower threads will hit
                                        // the branch above this one.
                                        fslock.sender_seq_num = self.seq_num.wrapping_add(1);
                                        self.seq_num = fslock.sender_seq_num;
                                        fslock.bytes_written = 0;
                                    }
                                }
                                self.path = self.root.join(format!("{}", self.seq_num));
                                match fs::OpenOptions::new()
                                    .append(true)
                                    .create(true)
                                    .open(&self.path)
                                {
                                    Ok(fp) => fslock.sender_fp = Some(BufWriter::new(fp)),
                                    Err(e) => {
                                        panic!("FAILED TO OPEN {:?} WITH {:?}", &self.path, e)
                                    }
                                }
                            }

                            assert!(fslock.sender_fp.is_some());
                            if let Some(ref mut fp) = fslock.sender_fp {
                                let max: usize =
                                    payload_len as usize + ::std::mem::size_of::<u64>();
                                match fp.write(&buf.get_ref()[..max]) {
                                    Ok(written) => fslock.bytes_written += written,
                                    Err(e) => panic!("Write error: {}", e),
                                }
                                wrote_to_disk += 1;
                            }
                        }
                        private::Placement::Disk(sz) => wrote_to_disk += sz,
                    }
                } else {
                    unreachable!()
                }
            }
            assert!(fslock.sender_fp.is_some());
            if let Some(ref mut fp) = fslock.sender_fp {
                fp.flush().expect("unable to flush");
                fslock
                    .mem_buffer
                    .push_front(private::Placement::Disk(wrote_to_disk));
            }
        }
    }

    /// Return the sender's name
    pub fn name(&self) -> &str {
        &self.name
    }
}
