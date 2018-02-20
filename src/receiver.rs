use bincode::{deserialize_from, Infinite};
use private;
use byteorder::{BigEndian, ReadBytesExt};
use serde::de::DeserializeOwned;
use std::{fs, sync};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::io::{BufReader, ErrorKind, Read, Seek, SeekFrom};
use std::iter::IntoIterator;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use flate2::read::DeflateDecoder;

#[derive(Debug)]
/// The 'receive' side of hopper, similar to
/// [`std::sync::mpsc::Receiver`](https://doc.rust-lang.org/std/sync/mpsc/struct.Receiver.html).
pub struct Receiver<T> {
    root: PathBuf,           // directory we store our queues in
    fp: BufReader<fs::File>, // active fp
    resource_type: PhantomData<T>,
    mem_buffer: private::Queue<T>,
    disk_writes_to_read: usize,
    max_disk_files: sync::Arc<AtomicUsize>,
}

impl<T> Receiver<T>
where
    T: DeserializeOwned,
{
    #[doc(hidden)]
    pub fn new(
        data_dir: &Path,
        mem_buffer: private::Queue<T>,
        max_disk_files: sync::Arc<AtomicUsize>,
    ) -> Result<Receiver<T>, super::Error> {
        let setup_mem_buffer = mem_buffer.clone(); // clone is cheeeeeap
        let guard = setup_mem_buffer.lock_front();
        if !data_dir.is_dir() {
            return Err(super::Error::NoSuchDirectory);
        }
        match private::read_seq_num(data_dir) {
            Ok(seq_num) => {
                let log = data_dir.join(format!("{}", seq_num));
                match fs::OpenOptions::new().read(true).open(log) {
                    Ok(mut fp) => {
                        fp.seek(SeekFrom::End(0))
                            .expect("could not get to end of file");
                        drop(guard);
                        Ok(Receiver {
                            root: data_dir.to_path_buf(),
                            fp: BufReader::new(fp),
                            resource_type: PhantomData,
                            mem_buffer: mem_buffer,
                            disk_writes_to_read: 0,
                            max_disk_files: max_disk_files,
                        })
                    }
                    Err(e) => Err(super::Error::IoError(e)),
                }
            }
            Err(e) => Err(super::Error::IoError(e)),
        }
    }

    // This function is _only_ called when there's disk writes to be read. If a
    // disk read happens and no `T` is returned this is an unrecoverable error.
    fn read_disk_value(&mut self) -> Result<T, super::Error> {
        loop {
            match self.fp.read_u32::<BigEndian>() {
                Ok(payload_size_in_bytes) => {
                    let mut payload_buf = vec![0; payload_size_in_bytes as usize];
                    match self.fp.read_exact(&mut payload_buf[..]) {
                        Ok(()) => {
                            let mut dec = DeflateDecoder::new(&payload_buf[..]);
                            match deserialize_from(&mut dec, Infinite) {
                                Ok(event) => {
                                    self.disk_writes_to_read -= 1;
                                    return Ok(event);
                                }
                                Err(e) => panic!("Failed decoding. Skipping {:?}", e),
                            }
                        }
                        Err(e) => {
                            panic!(
                                "Error, on-disk payload of advertised size not available! \
                                 Recv failed with error {:?}",
                                e
                            );
                        }
                    }
                }
                Err(e) => {
                    match e.kind() {
                        ErrorKind::UnexpectedEof => {
                            // Okay, we're pretty sure that no one snuck data in
                            // on us. We check the metadata condition of the
                            // file and, if we find it read-only, switch on over
                            // to a new log file.
                            let metadata = self.fp
                                .get_ref()
                                .metadata()
                                .expect("could not get metadata at UnexpectedEof");
                            if metadata.permissions().readonly() {
                                match private::read_seq_num_min(&self.root) {
                                    Ok(seq_num) => {
                                        let old_log = self.root.join(format!("{}", seq_num));
                                        fs::remove_file(old_log).expect("could not remove log");
                                        self.max_disk_files.fetch_add(1, Ordering::Relaxed);
                                        let lg =
                                            self.root.join(format!("{}", seq_num.wrapping_add(1)));
                                        match fs::OpenOptions::new().read(true).open(&lg) {
                                            Ok(fp) => {
                                                self.fp = BufReader::new(fp);
                                                continue;
                                            }
                                            Err(e) => return Err(super::Error::IoError(e)),
                                        }
                                    }
                                    Err(e) => {
                                        return Err(super::Error::IoError(e));
                                    }
                                }
                            }
                        }
                        _ => return Err(super::Error::IoError(e)),
                    }
                }
            }
        }
    }

    fn next_value(&mut self) -> Option<T> {
        // The receive loop
        //
        // The receiver is two interlocked state machines. The in-memory state
        // machine is done by calling `pop_front` on the in-memory deque, which
        // blocks until there's an item available. If the item that comes back
        // is a 'memory' placement we stay in the in-memory state machine. If
        // disk, we switch machines. The disk state machine has a counter of how
        // many items need to be read from disk. It's possible that disk reads
        // will suffer transient failures -- think file-descriptor exhaustion --
        // and so we only move out of disk back to memory state machine when the
        // counter is fully exhausted.
        loop {
            if self.disk_writes_to_read == 0 {
                match self.mem_buffer.pop_front() {
                    private::Placement::Memory(ev) => {
                        return Some(ev);
                    }
                    private::Placement::Disk(sz) => {
                        self.disk_writes_to_read = sz;
                        continue;
                    }
                }
            } else {
                match self.read_disk_value() {
                    Ok(ev) => return Some(ev),
                    Err(_) => return None,
                }
            }
        }
    }

    /// An iterator over messages on a receiver, this iterator will block
    /// whenever `next` is called, waiting for a new message, and `None` will be
    /// returned when the corresponding channel has hung up.
    pub fn iter(&mut self) -> Iter<T> {
        Iter { rx: self }
    }
}

#[derive(Debug)]
pub struct Iter<'a, T>
where
    T: 'a + DeserializeOwned,
{
    rx: &'a mut Receiver<T>,
}

#[derive(Debug)]
pub struct IntoIter<T>
where
    T: DeserializeOwned,
{
    rx: Receiver<T>,
}

impl<T> IntoIterator for Receiver<T>
where
    T: DeserializeOwned,
{
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> IntoIter<T> {
        IntoIter { rx: self }
    }
}

impl<'a, T> Iterator for Iter<'a, T>
where
    T: DeserializeOwned,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.rx.next_value()
    }
}

impl<T> Iterator for IntoIter<T>
where
    T: DeserializeOwned,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.rx.next_value()
    }
}
