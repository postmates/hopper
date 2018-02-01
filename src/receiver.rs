use bincode::deserialize;
use private;
use byteorder::{BigEndian, ReadBytesExt};
use serde::de::DeserializeOwned;
use std::fs;
use std::io::{BufReader, ErrorKind, Read, Seek, SeekFrom};
use std::iter::IntoIterator;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

#[derive(Debug)]
/// The 'receive' side of hopper, similar to
/// [`std::sync::mpsc::Receiver`](https://doc.rust-lang.org/std/sync/mpsc/struct.Receiver.html).
pub struct Receiver<T> {
    root: PathBuf,           // directory we store our queues in
    fp: BufReader<fs::File>, // active fp
    fs_lock: private::FSLock<T>,
    resource_type: PhantomData<T>,
    in_memory_limit: usize,
    disk_writes_to_read: usize,
}

impl<T> Receiver<T>
where
    T: DeserializeOwned,
{
    #[doc(hidden)]
    pub fn new(
        data_dir: &Path,
        in_memory_limit: usize,
        fs_lock: private::FSLock<T>,
    ) -> Result<Receiver<T>, super::Error> {
        let _ = fs_lock.lock();
        if !data_dir.is_dir() {
            return Err(super::Error::NoSuchDirectory);
        }
        let seq_num = fs::read_dir(data_dir)
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
            .unwrap();
        // Remove all index files we've fast-forwarded over
        //
        // As the senders will restart with writes_to_read at 0, we're going to
        // have to make sure that receiver is on the same page with regard to
        // place on disk.
        for fname in fs::read_dir(data_dir).unwrap() {
            let path = fname.unwrap().path();
            let id = path.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .parse::<usize>()
                .unwrap();
            let full_path = data_dir.join(path.file_name().unwrap().to_str().unwrap());
            if id != seq_num {
                fs::remove_file(full_path).expect("could not remove index file");
            }
        }
        let log = data_dir.join(format!("{}", seq_num));
        let mut fp = fs::OpenOptions::new()
            .read(true)
            .open(log)
            .expect("RECEIVER could not open file");
        fp.seek(SeekFrom::End(0))
            .expect("could not get to end of file");

        Ok(Receiver {
            root: data_dir.to_path_buf(),
            fp: BufReader::new(fp),
            resource_type: PhantomData,
            in_memory_limit: in_memory_limit,
            fs_lock: fs_lock,
            disk_writes_to_read: 0,
        })
    }

    // This function is _only_ called when there's disk writes to be read. If a
    // disk read happens and no `T` is returned this is an unrecoverable error.
    fn read_disk_value(&mut self) -> Result<T, ()> {
        loop {
            match self.fp.read_u64::<BigEndian>() {
                Ok(payload_size_in_bytes) => {
                    let mut payload_buf = vec![0; payload_size_in_bytes as usize];
                    match self.fp.read_exact(&mut payload_buf[..]) {
                        Ok(()) => match deserialize(&payload_buf) {
                            Ok(event) => {
                                self.disk_writes_to_read -= 1;
                                return Ok(event);
                            }
                            Err(e) => panic!("Failed decoding. Skipping {:?}", e),
                        },
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
                                // TODO all these unwraps are a silent death
                                let seq_num = fs::read_dir(&self.root)
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
                                    .min()
                                    .unwrap();
                                let old_log = self.root.join(format!("{}", seq_num));
                                fs::remove_file(old_log).expect("could not remove log");
                                let lg = self.root.join(format!("{}", seq_num.wrapping_add(1)));
                                match fs::OpenOptions::new().read(true).open(&lg) {
                                    Ok(fp) => {
                                        self.fp = BufReader::new(fp);
                                        continue;
                                    }
                                    Err(_n) => return Err(()),
                                }
                            }
                        }
                        _ => return Err(()),
                    }
                }
            }
        }
    }

    fn next_value(&mut self) -> Option<T> {
        // The receive loop
        //
        // The receiver works by regularly attempting to read a payload from its
        // current log file. In the event we hit EOF without detecting that the
        // file is read-only, we swing around and try again. If a Sender thread
        // has a bug and is unable to mark a file its finished with as read-only
        // this _will_ cause a livelock situation. If the file _is_ read-only
        // this is a signal from the senders that the file is no longer being
        // written to. It's safe for the Receiver to declare the log done by
        // deleting it and moving on to the next file.
        loop {
            if self.disk_writes_to_read == 0 {
                let mut syn = self.fs_lock.lock().unwrap();
                let fslock = &mut (*syn);
                if let Some(placement) = fslock.mem_buffer.pop_front() {
                    match placement {
                        private::Placement::Memory(ev) => {
                            return Some(ev);
                        }
                        private::Placement::Disk(sz) => {
                            self.disk_writes_to_read = sz;
                            continue;
                        }
                    }
                } else {
                    return None;
                }
            } else {
                return Some(self.read_disk_value().unwrap());
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
pub struct Iter<'a, T: 'a + DeserializeOwned> {
    rx: &'a mut Receiver<T>,
}

#[derive(Debug)]
pub struct IntoIter<T: DeserializeOwned> {
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
