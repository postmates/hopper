mod integration {
    extern crate hopper;
    extern crate tempdir;

    use self::hopper::channel_with_explicit_capacity;
    use std::thread;
    use std::time;
    use std::fs::File;
    use std::io::Read;
    use std::path::PathBuf;
    use std::str::FromStr;

    // TODO cope with 0 bytes issue

    // #[test] // TODO re-enable, think issue is explicit capacity check...
    fn test_run_afl_crashes() {
        let mut resource = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        resource.push("resources/afl_crashes.txt");

        let mut f = File::open(resource).expect("could not open resource file");
        let mut buffer = String::new();
        f.read_to_string(&mut buffer)
            .expect("could not read resource file");

        for s in buffer.lines() {
            println!("{}", s);
            let pyld: Vec<u64> = s.split_whitespace()
                .filter_map(|f| u64::from_str(f).ok())
                .collect();

            if pyld.len() < 3 {
                return;
            }
            let cap = pyld[0] as usize;
            let max_thrs = pyld[1] as usize;
            let max_bytes = pyld[2] as usize;

            let dir = tempdir::TempDir::new("hopper").unwrap();
            let (snd, mut rcv) = channel_with_explicit_capacity(
                "concurrent_snd_and_rcv_small_max_bytes",
                dir.path(),
                cap * ::std::mem::size_of::<usize>(),
                max_bytes,
            ).unwrap();

            let mut joins = Vec::new();

            // start our receiver thread
            let mut nxt = 0;
            let expected = max_thrs * cap;
            let recv_jh = thread::spawn(move || {
                let mut count = 0;
                let dur = time::Duration::from_millis(1);
                for _ in 0..100 {
                    thread::sleep(dur);
                    for i in rcv.iter() {
                        count += 1;
                        if max_thrs == 1 {
                            assert_eq!(i, nxt);
                            nxt += 1;
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

}
