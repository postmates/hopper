use std::collections::VecDeque;
use std::sync;
use std::io::BufWriter;
use std::fs;

#[derive(Debug)]
pub enum Placement<T> {
    Memory(T),
    Disk(usize),
}

#[derive(Default, Debug)]
pub struct FsSync<T> {
    pub mem_buffer: VecDeque<Placement<T>>, // shared

    pub sender_fp: Option<BufWriter<fs::File>>, // sender only
    pub bytes_written: usize,                   // sender only
    pub sender_seq_num: usize,                  // sender only
}

impl<T> FsSync<T> {
    pub fn new(in_memory_limit: usize) -> FsSync<T> {
        FsSync {
            sender_fp: None,

            bytes_written: 0,
            sender_seq_num: 0,
            mem_buffer: VecDeque::with_capacity(in_memory_limit),
        }
    }
}

pub type FSLock<T> = sync::Arc<sync::Mutex<FsSync<T>>>;
