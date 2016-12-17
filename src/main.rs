#![feature(plugin)]
#![plugin(afl_plugin)]

extern crate afl;
extern crate hopper;
extern crate tempdir;

use std::io;
use std::io::BufRead;
use std::str::FromStr;

fn main() {
    let dir = tempdir::TempDir::new("hopper").unwrap();
    let (mut snd, mut rcv) = hopper::channel("afl", dir.path());

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let fields: Vec<u64> = line.unwrap()
            .split_whitespace()
            .map(|f| u64::from_str(f))
            .filter(|f| f.is_ok())
            .map(|f| f.unwrap())
            .collect();

        if !fields.is_empty() {
            let tot = fields[0];

            for idx in 0..tot {
                snd.send(idx);
            }
            loop {
                if !rcv.next().is_some() {
                    break;
                }
            }

        }
    }
}
