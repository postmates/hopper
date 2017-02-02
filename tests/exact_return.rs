mod integration {
    extern crate tempdir;
    extern crate hopper;
    extern crate quickcheck;

    use self::hopper::channel_with_max_bytes;
    use std::thread;
    use self::quickcheck::{QuickCheck, TestResult};
    use std::time;

    #[test]
    fn large_sequence_comes_back_exactly() {
        let dir = tempdir::TempDir::new("hopper").unwrap();
        let (mut snd, mut rcv) =
            channel_with_max_bytes("zero_item_round_trip", dir.path(), 1_048_576).unwrap();

        assert_eq!(None, rcv.iter().next());

        let max = 10;

        for i in 0..max {
            snd.send(i);
        }

        let mut nxt = 0;
        let mut count = 0;
        loop {
            match rcv.iter().next() {
                Some(i) => {
                    count += 1;
                    assert_eq!(i, nxt);
                    nxt += 1;
                }
                None => break,
            }
        }
        assert_eq!(count, max);
    }


    #[test]
    fn qc_concurrent_snd_and_rcv_round_trip() {
        fn snd_rcv(cap: usize, max_thrs: usize, max_bytes: usize) -> TestResult {
            if max_thrs < 1 || max_bytes < 1 || cap < 1 {
                return TestResult::discard();
            }
            let dir = tempdir::TempDir::new("hopper").unwrap();
            println!("CONCURRENT SND_RECV TESTDIR: {:?}", dir);
            let (snd, mut rcv) = channel_with_max_bytes("concurrent_snd_and_rcv_small_max_bytes",
                                                        dir.path(),
                                                        max_bytes)
                .unwrap();

            let mut joins = Vec::new();

            // start our receiver thread
            let expected = max_thrs * cap;
            let recv_jh = thread::spawn(move || {
                let mut count = 0;
                let dur = time::Duration::from_millis(1);
                for _ in 0..250 {
                    thread::sleep(dur);
                    loop {
                        if let Some(_) = rcv.iter().next() {
                            count += 1;
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
            TestResult::passed()
        }
        QuickCheck::new()
            .tests(100)
            .max_tests(1000)
            .quickcheck(snd_rcv as fn(usize, usize, usize) -> TestResult);
    }

    #[test]
    fn concurrent_snd_and_rcv_round_trip() {
        let cap = 58;
        let max_thrs = 38;
        let max_bytes = 14;
        let dir = tempdir::TempDir::new("hopper").unwrap();
        println!("CONCURRENT SND_RECV TESTDIR: {:?}", dir);
        let (snd, mut rcv) = channel_with_max_bytes("concurrent_snd_and_rcv_small_max_bytes",
                                                    dir.path(),
                                                    max_bytes)
            .unwrap();

        let mut joins = Vec::new();

        // start our receiver thread
        let expected = max_thrs * cap;
        let recv_jh = thread::spawn(move || {
            let mut count = 0;
            let dur = time::Duration::from_millis(10);
            for _ in 0..250 {
                thread::sleep(dur);
                loop {
                    if let Some(_) = rcv.iter().next() {
                        count += 1;
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
