use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Default, Debug)]
pub struct FsSync<T> {
    pub receiver_read_id: u64,
    pub receiver_idx: Option<usize>,
    pub receiver_max_idx: Option<usize>,

    pub write_bounds: VecDeque<(usize, usize)>, // yikes, unbounded
    pub writes_to_read: usize,

    pub sender_idx: usize,
    pub sender_captured_recv_id: u64,

    pub in_memory_idx: usize,
    pub bytes_written: usize,
    pub disk_writes_to_read: usize,
    pub sender_seq_num: usize,
    pub mem_buffer: VecDeque<T>,
    pub disk_buffer: VecDeque<T>,
}

impl<T> FsSync<T> {
    pub fn new(cap: usize) -> FsSync<T> {
        FsSync {
            receiver_read_id: 0,
            receiver_idx: None,
            receiver_max_idx: None,

            write_bounds: VecDeque::with_capacity(128),
            writes_to_read: 0,

            sender_idx: 0,
            sender_captured_recv_id: 0,

            in_memory_idx: cap,
            bytes_written: 0,
            disk_writes_to_read: 0,
            sender_seq_num: 0,
            mem_buffer: VecDeque::with_capacity(cap),
            disk_buffer: VecDeque::with_capacity(cap),
        }
    }
}

pub type FSLock<T> = Arc<Mutex<FsSync<T>>>;
