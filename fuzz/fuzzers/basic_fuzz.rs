#![no_main]
#[macro_use]
extern crate libfuzzer_sys;
extern crate hopper;
extern crate tempdir;

use std::str;
use std::str::FromStr;

fuzz_target!(|data: &[u8]| {
    let dir = tempdir::TempDir::new("hopper").unwrap();
    let (mut snd, mut rcv) = hopper::channel("fizzy_lifting", dir.path()).unwrap();

    match str::from_utf8(data) {
        Ok(st) => {
            let lines = st.lines();
            for line in lines {
                let fields: Vec<u64> = line.split_whitespace()
                    .map(|f| u64::from_str(f))
                    .filter(|f| f.is_ok())
                    .map(|f| f.expect("could not convert to u64"))
                    .collect();

                if !fields.is_empty() {
                    let tot = fields[0];

                    for idx in 0..tot {
                        snd.send(idx);
                    }
                    loop {
                        if !rcv.iter().next().is_some() {
                            break;
                        }
                    }

                }
            }
        }
        Err(_) => {}
    }
});
