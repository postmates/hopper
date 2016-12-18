use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Default, Debug)]
pub struct FsSync<T> {
    pub max_buffer: usize,
    pub bytes_written: usize,
    pub writes_to_read: usize,
    pub disk_writes_to_read: usize,
    pub sender_seq_num: usize,
    pub mem_buffer: VecDeque<T>,
    pub disk_buffer: VecDeque<T>,
}

impl<T> FsSync<T> {
    pub fn new(cap: usize) -> FsSync<T> {
        FsSync {
            max_buffer: cap,
            bytes_written: 0,
            writes_to_read: 0,
            disk_writes_to_read: 0,
            sender_seq_num: 0,
            mem_buffer: VecDeque::with_capacity(cap),
            disk_buffer: VecDeque::with_capacity(cap),
        }
    }
}

pub type FSLock<T> = Arc<Mutex<FsSync<T>>>;
