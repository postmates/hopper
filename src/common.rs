use std::{fs, mem, path};
use memmap; 

#[inline]
pub fn u8tou32abe(v: &[u8]) -> u32 {
    u32::from(v[3]) + (u32::from(v[2]) << 8) + (u32::from(v[1]) << 24) + (u32::from(v[0]) << 16)
}

#[inline]
pub fn u32tou8abe(v: u32) -> [u8; 4] {
    [v as u8, (v >> 8) as u8, (v >> 24) as u8, (v >> 16) as u8]
}

#[derive(Debug, Clone)]
pub struct Config {
    pub maximum_queue_in_bytes: u32,
    pub root_dir: path::PathBuf, 
}

#[derive(Debug)]
pub struct HIndex {
    root: path::PathBuf,
    path: path::PathBuf, 
    block: memmap::MmapMut,
}

impl HIndex {
    pub fn new(data_dir: &path::Path) -> Result<HIndex, super::Error>
    {
        if !data_dir.is_dir() {
            return Err(super::Error::NoSuchDirectory);
        }
        let idx = data_dir.join("index");

        let file = fs::OpenOptions::new()
                       .read(true)
                       .write(true)
                       .create(true)
                       .open(&idx).unwrap(); // TODO no unwrap 
        file.set_len((mem::size_of::<u32>() * 2) as u64).unwrap(); // TODO no unwrap
        let mmap = unsafe { memmap::MmapMut::map_mut(&file).unwrap() /* TODO no unwrap */ };
        Ok(HIndex {
            root: data_dir.to_path_buf(),
            path: idx,
            block: mmap
        })
    }

    pub fn sender_idx(&self) -> u32 {
        u8tou32abe(&self.block[0..3])
    }

    // TODO not safe because of multiple senders writing to one location
    pub fn set_sender_idx(&mut self, val: u32) -> () {
        let abe = u32tou8abe(val);
        for i in 0..4 {
            self.block[i] = abe[i];
        }
    }

    pub fn receiver_idx(&self) -> u32 {
        u8tou32abe(&self.block[4..7])
    }

    pub fn set_receiver_idx(&mut self, val: u32) -> () {
        let abe = u32tou8abe(val);
        for i in 5..8 {
            self.block[i] = abe[i];
        }
    }
}
