use bincode::serde::deserialize;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{BufReader, ErrorKind, Read, SeekFrom, Seek};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use private;

#[inline]
fn u8tou32abe(v: &[u8]) -> u32 {
    (v[3] as u32) + ((v[2] as u32) << 8) + ((v[1] as u32) << 24) + ((v[0] as u32) << 16)
}

#[derive(Debug)]
/// The 'receive' side of hopper, similar to
/// [`std::sync::mpsc::Receiver`](https://doc.rust-lang.org/std/sync/mpsc/struct.Receiver.html).
pub struct Receiver<T> {
    root: PathBuf, // directory we store our queues in
    fp: BufReader<fs::File>, // active fp
    fs_lock: private::FSLock<T>,
    resource_type: PhantomData<T>,
}

impl<T> Receiver<T>
    where T: Deserialize
{
    #[doc(hidden)]
    pub fn new(data_dir: &Path, fs_lock: private::FSLock<T>) -> Result<Receiver<T>, super::Error> {
        let _ = fs_lock.lock().expect("Sender fs_lock poisoned");
        if !data_dir.is_dir() {
            return Err(super::Error::NoSuchDirectory);
        }
        let seq_num = fs::read_dir(data_dir)
            .unwrap()
            .map(|de| {
                de.unwrap().path().file_name().unwrap().to_str().unwrap().parse::<usize>().unwrap()
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
            let id = path.file_name().unwrap().to_str().unwrap().parse::<usize>().unwrap();
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
        fp.seek(SeekFrom::End(0)).expect("could not get to end of file");

        Ok(Receiver {
            root: data_dir.to_path_buf(),
            fp: BufReader::new(fp),
            resource_type: PhantomData,
            fs_lock: fs_lock,
        })
    }
}


impl<T> Iterator for Receiver<T>
    where T: Serialize + Deserialize
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        let mut sz_buf = [0; 4];
        let mut syn = self.fs_lock.lock().expect("Receiver fs_lock was poisoned!");
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
        let fslock = &mut (*syn);

        while fslock.writes_to_read > 0 {
            fslock.receiver_read_id = fslock.receiver_read_id.wrapping_add(1);

            if fslock.receiver_idx.is_none() {
                let bnds = fslock.write_bounds.pop_back().expect("NO BOUND");
                fslock.receiver_idx = Some(bnds.0);
                fslock.receiver_max_idx = Some(bnds.1);
            }
            if fslock.receiver_idx.unwrap() < fslock.in_memory_idx {
                let event = fslock.mem_buffer
                    .pop_front()
                    .expect("there was not an event in the in-memory");
                fslock.writes_to_read -= 1;
                fslock.receiver_idx = fslock.receiver_idx.map(|x| x + 1);
                return Some(event);
            } else if (fslock.disk_writes_to_read == 0) &&
                      (fslock.receiver_idx.unwrap() >= fslock.in_memory_idx) {
                let event = fslock.disk_buffer
                    .pop_front()
                    .expect("there was not an event in the disk buffer!");
                fslock.writes_to_read -= 1;
                fslock.receiver_idx = fslock.receiver_idx.map(|x| x + 1);
                return Some(event);
            } else {
                match self.fp.read_exact(&mut sz_buf) {
                    Ok(()) => {
                    let payload_size_in_bytes = u8tou32abe(&sz_buf);
                    let mut payload_buf = vec![0; (payload_size_in_bytes as usize)];
                    match self.fp.read_exact(&mut payload_buf) {
                        Ok(()) => {
                            match deserialize(&payload_buf) {
                                Ok(event) => {
                                    fslock.receiver_idx = fslock.receiver_idx.map(|x| x + 1);
                                    fslock.writes_to_read -= 1;
                                    fslock.disk_writes_to_read -= 1;
                                    return Some(event);
                                }
                                Err(e) => panic!("Failed decoding. Skipping {:?}", e),
                            }
                        }
                        Err(e) => {
                            panic!("Error, on-disk payload of advertised size not available! \
                                    Recv failed with error {:?}",
                                   e);
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
                                        Err(e) => panic!("[Receiver] could not open {:?}", e),
                                    }
                                }
                            }
                            _ => {
                                panic!("unable to cope");
                            }
                        }
                    }
                }
            }
        }
        None
    }
}
