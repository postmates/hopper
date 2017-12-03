use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::path::PathBuf;
use common;

#[derive(Debug)]
/// The 'send' side of hopper, similar to
/// [`std::sync::mpsc::Sender`](https://doc.rust-lang.org/std/sync/mpsc/struct.
/// Sender.html).
pub struct Sender<T> {
    config: common::Config,
    path: PathBuf, // active fp filename
    seq_num: u32,
    resource_type: PhantomData<T>,
}

impl<'de, T> Clone for Sender<T>
where
    T: Serialize + Deserialize<'de>,
{
    fn clone(&self) -> Sender<T> {
        Sender::new(self.config.clone()).expect("COULD NOT CLONE")
    }
}

impl<T> Sender<T>
where
    T: Serialize,
{
    #[doc(hidden)]
    pub fn new(config: common::Config) -> Result<Sender<T>, super::Error>
    {
        if !config.root_dir.is_dir() {
            return Err(super::Error::NoSuchDirectory);
        }
        let hindex = common::HIndex::new(&config.root_dir).unwrap();
        let seq_num = hindex.sender_idx();
        let log = config.root_dir.join(format!("{}", seq_num));
        Ok(Sender {
            config: config,
            path: log,
            seq_num: seq_num,
            // fs_lock: fs_lock,
            resource_type: PhantomData,
        })
    }

    /// send writes data out in chunks, like so:
    ///
    ///  u32: payload_size
    ///  [u8] payload
    ///
    pub fn send(&mut self, _event: T) {
        // use bincode::{serialize_into, Infinite};
        // use std::io::{BufWriter, Write};
        unimplemented!()

        // let mut syn = self.fs_lock.lock().expect("Sender fs_lock poisoned");
        // let fslock = &mut (*syn);

        // if fslock.sender_idx < fslock.in_memory_idx {
        //     fslock.mem_buffer.push_back(event);
        // } else {
        //     fslock.disk_buffer.push_back(event);
        //     if fslock.disk_buffer.len() >= fslock.in_memory_idx {
        //         while let Some(ev) = fslock.disk_buffer.pop_front() {
        //             let mut pyld = Vec::with_capacity(64);
        //             serialize_into(&mut pyld, &ev, Infinite).expect("could not serialize");
        //             // NOTE The conversion of t.len to u32 and usize is _only_
        //             // safe when u32 <= usize. That's very likely to hold true
        //             // for machines--for now?--that hopper will run on. However!
        //             let pyld_sz_bytes: [u8; 4] = common::u32tou8abe(pyld.len() as u32);
        //             let mut t = vec![0, 0, 0, 0];
        //             t[0] = pyld_sz_bytes[3];
        //             t[1] = pyld_sz_bytes[2];
        //             t[2] = pyld_sz_bytes[1];
        //             t[3] = pyld_sz_bytes[0];
        //             t.append(&mut pyld);
        //             // If the individual sender writes enough to go over the max
        //             // we mark the file read-only--which will help the receiver
        //             // to decide it has hit the end of its log file--and create
        //             // a new log file.
        //             let bytes_written = fslock.bytes_written + t.len();
        //             if (bytes_written > self.max_bytes) || (self.seq_num != fslock.sender_seq_num)
        //                 || fslock.sender_fp.is_none()
        //             {
        //                 // Once we've gone over the write limit for our current
        //                 // file or find that we've gotten behind the current
        //                 // queue file we need to seek forward to find our place
        //                 // in the space of queue files. We mark our current file
        //                 // read-only--there's some possibility that this will be
        //                 // done redundantly, but that's okay--and then read the
        //                 // current sender_seq_num to get up to date.
        //                 let _ = fs::metadata(&self.path).map(|p| {
        //                     let mut permissions = p.permissions();
        //                     permissions.set_readonly(true);
        //                     let _ = fs::set_permissions(&self.path, permissions);
        //                 });
        //                 if fslock.sender_fp.is_some() {
        //                     if self.seq_num != fslock.sender_seq_num {
        //                         // This thread is behind the leader. We've got to
        //                         // set our current notion of seq_num forward and
        //                         // then open the corresponding file.
        //                         self.seq_num = fslock.sender_seq_num;
        //                     } else {
        //                         // This thread is the leader. We reset the
        //                         // sender_seq_num and bytes written and open the
        //                         // next queue file. All follower threads will hit
        //                         // the branch above this one.
        //                         fslock.sender_seq_num = self.seq_num.wrapping_add(1);
        //                         self.seq_num = fslock.sender_seq_num;
        //                         fslock.bytes_written = 0;
        //                     }
        //                 }
        //                 self.path = self.root.join(format!("{}", self.seq_num));
        //                 match fs::OpenOptions::new()
        //                     .append(true)
        //                     .create(true)
        //                     .open(&self.path)
        //                 {
        //                     Ok(fp) => fslock.sender_fp = Some(BufWriter::new(fp)),
        //                     Err(e) => panic!("FAILED TO OPEN {:?} WITH {:?}", &self.path, e),
        //                 }
        //             }

        //             assert!(fslock.sender_fp.is_some());
        //             if let Some(ref mut fp) = fslock.sender_fp {
        //                 match fp.write(&t[..]) {
        //                     Ok(written) => fslock.bytes_written += written,
        //                     Err(e) => panic!("Write error: {}", e),
        //                 }
        //                 fslock.disk_writes_to_read += 1;
        //             }
        //         }
        //         assert!(fslock.sender_fp.is_some());
        //         if let Some(ref mut fp) = fslock.sender_fp {
        //             fp.flush().expect("unable to flush");
        //         }
        //     }
        // }
        // fslock.writes_to_read += 1;
        // if (fslock.sender_captured_recv_id != fslock.receiver_read_id)
        //     || fslock.write_bound.is_none()
        // {
        //     fslock.sender_captured_recv_id = fslock.receiver_read_id;
        //     fslock.write_bound = Some(fslock.sender_idx);
        // }
        // fslock.sender_idx += 1;
    }

    // /// Return the sender's name
    // pub fn name(&self) -> &str {
    //     &self.name
    // }
}
