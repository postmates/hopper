#![feature(plugin)]
#![plugin(afl_plugin)]

extern crate afl;
extern crate tempdir;
extern crate hopper;

use self::hopper::channel_with_max_bytes;
use std::io;
use std::io::BufRead;
use std::str::FromStr;
use std::time;
use std::thread;

fn main() {
    let stdin = io::stdin();
    for s in stdin.lock().lines() {
        let pyld: Vec<u64> = s.unwrap()
            .split_whitespace()
            .filter_map(|f| u64::from_str(f).ok())
            .collect();

        if pyld.len() < 3 {
            return;
        }
        let cap = pyld[0] as usize;
        let max_thrs = pyld[1] as usize;
        if max_thrs > 1000 {
            return;
        }
        let max_bytes = pyld[2] as usize;
        if max_bytes < 8 {
            return;
        }

        let dir = tempdir::TempDir::new("hopper").unwrap();
        let (snd, mut rcv) = channel_with_max_bytes("concurrent_snd_and_rcv_small_max_bytes",
                                                    dir.path(),
                                                    max_bytes)
            .unwrap();

        let mut joins = Vec::new();

        // start our receiver thread
        let mut nxt = 0;
        let expected = max_thrs * cap;
        let recv_jh = thread::spawn(move || {
            let mut count = 0;
            let dur = time::Duration::from_millis(1);
            for _ in 0..200 {
                thread::sleep(dur);
                loop {
                    if let Some(i) = rcv.next() {
                        count += 1;
                        if max_thrs == 1 {
                            assert_eq!(i, nxt);
                            nxt += 1;
                        }
                    } else {
                        break;
                    }
                }
            }
            count
        });

        // start all our sender threads and blast away
        for _ in 0..max_thrs {
            let mut thr_snd = snd.clone();
            joins.push(thread::spawn(move || {
                for i in 0..cap {
                    thr_snd.send(i);
                }
            }));
        }

        // wait until the senders are for sure done
        for jh in joins {
            jh.join().expect("Uh oh, child thread paniced!");
        }
        let count = recv_jh.join().expect("no count! :<");
        assert_eq!(count, expected);
    }
}
