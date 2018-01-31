use std::collections::VecDeque;
use std::sync;
use std::io::BufWriter;
use std::fs;

#[derive(Default, Debug)]
pub struct FsSync<T> {
    pub disk_writes_to_read: usize, // shared
    pub mem_buffer: VecDeque<T>,    // shared

    pub sender_fp: Option<BufWriter<fs::File>>, // sender only
    pub bytes_written: usize,                   // sender only
    pub sender_seq_num: usize,                  // sender only
}

impl<T> FsSync<T> {
    pub fn new(in_memory_limit: usize) -> FsSync<T> {
        FsSync {
            sender_fp: None,

            bytes_written: 0,
            disk_writes_to_read: 0,
            sender_seq_num: 0,
            mem_buffer: VecDeque::with_capacity(in_memory_limit),
        }
    }
}

pub type FSLock<T> = sync::Arc<sync::Mutex<FsSync<T>>>;
