use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Default, Debug)]
pub struct FsSync<T> {
    pub small_buffer: VecDeque<T>,
    pub max_small_buffer: usize,
    pub bytes_written: usize,
    pub writes_to_read: usize,
    pub sender_seq_num: usize,
}

impl<T> FsSync<T> {
    pub fn new(cap: usize) -> FsSync<T> {
        FsSync {
            small_buffer: VecDeque::with_capacity(cap),
            max_small_buffer: cap,
            bytes_written: 0,
            writes_to_read: 0,
            sender_seq_num: 0,
        }
    }
}

pub type FSLock<T> = Arc<Mutex<FsSync<T>>>;
