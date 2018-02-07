// Indebted to "The Art of Multiprocessor Programming"

use std::sync::{Arc, Mutex};
use std::marker::PhantomData;
use std::ptr;

unsafe impl<T: ::std::fmt::Debug> Send for Queue<T> {}
unsafe impl<T: ::std::fmt::Debug> Sync for Queue<T> {}

#[derive(Debug, Clone)]
struct Node<T>
where
    T: ::std::fmt::Debug,
{
    elem: T,
    next: *mut Node<T>,
}

#[derive(Clone)]
pub struct Queue<T>
where
    T: ::std::fmt::Debug,
{
    head: *mut Node<T>,
    tail: *mut Node<T>,
    enq_lock: Arc<Mutex<()>>,
    deq_lock: Arc<Mutex<()>>,
    _marker: PhantomData<T>,
}

#[allow(dead_code)]
impl<T> Queue<T>
where
    T: ::std::fmt::Debug,
{
    pub fn new() -> Queue<T> {
        Queue {
            head: ptr::null_mut(),
            tail: ptr::null_mut(),
            enq_lock: Arc::new(Mutex::new(())),
            deq_lock: Arc::new(Mutex::new(())),
            _marker: PhantomData,
        }
    }

    pub fn enq(&mut self, elem: T) {
        println!("\nENQ: {:?}", elem);
        let raw_tail = Box::into_raw(Box::new(Node {
            elem: elem,
            next: ptr::null_mut(),
        }));
        println!("    HEAD: {:?}\n    TAIL: {:?}", self.head, self.tail);
        println!("    NEW TAIL: {:?}", raw_tail);
        if self.tail.is_null() {
            self.head = raw_tail;
        } else {
            unsafe {
                (*self.tail).next = raw_tail;
            }
        }
        self.tail = raw_tail;
    }

    pub fn deq(&mut self) -> Option<T> {
        println!("\nDEQ");
        println!("    HEAD: {:?}\n    TAIL: {:?}", self.head, self.tail);
        let _ = self.deq_lock.lock().unwrap();
        if self.head.is_null() {
            None
        } else {
            unsafe {
                let head = self.head;
                self.head = (*head).next;
                if self.head.is_null() {
                    self.tail = ptr::null_mut();
                }
                let node: Box<Node<T>> = Box::from_raw(head);
                Some(node.elem)
            }
        }
    }
}

#[cfg(test)]
mod test {
    extern crate quickcheck;

    use self::quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
    use std::thread;
    use super::*;

    #[test]
    fn simple() {
        let total_senders = 52;
        let mut vals = vec![12, 9, 41, 30, 48, 50, 86, 89];

        let mut sut: Queue<u64> = Queue::new();

        let mut snd_jh = Vec::new();
        let snd_vals = vals.clone();
        for chunk in snd_vals.chunks(total_senders) {
            println!("CHUNK: {:?}", chunk);
            let mut snd_q = sut.clone();
            let chunk: Vec<u64> = chunk.to_vec();
            snd_jh.push(thread::spawn(move || {
                for ev in chunk {
                    snd_q.enq(ev)
                }
            }))
        }

        let expected_total_vals = vals.len();
        let mut rcv_vals: Vec<u64> = thread::spawn(move || {
            let mut collected: Vec<u64> = Vec::new();
            while collected.len() != expected_total_vals {
                match sut.deq() {
                    None => {
                        // println!("NOTHING TO DEQUEUE: {}", collected.len());
                        continue;
                    }
                    Some(v) => {
                        println!("SOME V: {:?}", v);
                        collected.push(v);
                    }
                }
            }
            collected
        }).join()
            .unwrap();

        for jh in snd_jh {
            jh.join().unwrap();
        }

        rcv_vals.sort();
        vals.sort();

        assert_eq!(rcv_vals, vals);
    }

    #[derive(Clone, Debug)]
    enum Action {
        Enq(u64),
        Deq,
    }

    impl Arbitrary for Action {
        fn arbitrary<G>(g: &mut G) -> Action
        where
            G: Gen,
        {
            let i: usize = g.gen_range(0, 100);
            match i {
                0...50 => Action::Enq(g.gen::<u64>()),
                _ => Action::Deq,
            }
        }
    }

    #[test]
    fn sequential_model_check() {
        fn inner(actions: Vec<Action>) -> TestResult {
            use std::collections::VecDeque;

            let mut model: VecDeque<u64> = VecDeque::new();
            let mut sut: Queue<u64> = Queue::new();

            for action in actions {
                match action {
                    Action::Enq(v) => {
                        model.push_back(v);
                        sut.enq(v);
                    }
                    Action::Deq => {
                        assert_eq!(model.pop_front(), sut.deq());
                    }
                }
            }
            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(Vec<Action>) -> TestResult);
    }

    #[test]
    fn model_check() {
        fn inner(total_senders: usize, mut vals: Vec<u64>) -> TestResult {
            println!("TOTAL_SENDERS: {} | VALS: {:?}", total_senders, vals);
            if total_senders == 0 {
                return TestResult::discard();
            }

            let mut sut: Queue<u64> = Queue::new();

            let mut snd_jh = Vec::new();
            let snd_vals = vals.clone();
            for chunk in snd_vals.chunks(total_senders) {
                let mut snd_q = sut.clone();
                let chunk: Vec<u64> = chunk.to_vec();
                snd_jh.push(thread::spawn(move || {
                    for ev in chunk {
                        snd_q.enq(ev)
                    }
                }))
            }

            let expected_total_vals = vals.len();
            let mut rcv_vals: Vec<u64> = thread::spawn(move || {
                let mut collected: Vec<u64> = Vec::new();
                while collected.len() != expected_total_vals {
                    match sut.deq() {
                        None => {
                            // println!("NOTHING TO DEQUEUE: {}", collected.len());
                            continue;
                        }
                        Some(v) => {
                            collected.push(v);
                        }
                    }
                }
                collected
            }).join()
                .unwrap();

            for jh in snd_jh {
                jh.join().unwrap();
            }

            rcv_vals.sort();
            vals.sort();

            assert_eq!(rcv_vals, vals);
            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(usize, Vec<u64>) -> TestResult);
    }
}
