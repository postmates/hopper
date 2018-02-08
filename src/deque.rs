// Indebted to "The Art of Multiprocessor Programming"

use std::sync::{Condvar, Mutex, MutexGuard};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::ptr;

unsafe impl<T: ::std::fmt::Debug> Send for Queue<T> {}
unsafe impl<T: ::std::fmt::Debug> Sync for Queue<T> {}

#[derive(Debug)]
struct Node<T>
where
    T: ::std::fmt::Debug,
{
    elem: Option<T>,
    next: *mut Node<T>,
}

struct InnerQueue<T>
where
    T: ::std::fmt::Debug,
{
    head: *mut Node<T>,
    tail: *mut Node<T>,
    capacity: usize,
    size: AtomicUsize,
    enq_lock: Mutex<()>,
    deq_lock: Mutex<()>,
    not_empty: Condvar,
    not_full: Condvar,
}

impl<T> InnerQueue<T>
where
    T: ::std::fmt::Debug,
{
    pub fn new() -> InnerQueue<T> {
        let head = Box::into_raw(Box::new(Node {
            elem: None,
            next: ptr::null_mut(),
        }));
        InnerQueue {
            head: head,
            tail: head,
            capacity: 1024,
            size: AtomicUsize::new(1),
            enq_lock: Mutex::new(()),
            deq_lock: Mutex::new(()),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
        }
    }

    pub fn enq(&mut self, elem: T) {
        // println!("ENQ: {:?}", elem);
        let mut must_wake_dequeuers = false;
        let mut guard: MutexGuard<()> = self.enq_lock.lock().unwrap();
        while self.size.load(Ordering::SeqCst) == self.capacity {
            guard = self.not_empty.wait(guard).unwrap();
        }
        let new_tail = Box::into_raw(Box::new(Node {
            elem: Some(elem),
            next: ptr::null_mut(),
        }));
        unsafe {
            (*self.tail).next = new_tail;
        }
        self.tail = new_tail;
        if self.size.fetch_add(1, Ordering::SeqCst) == 0 {
            must_wake_dequeuers = true;
        }
        drop(guard);
        println!("LET GO OF LOCK");
        if must_wake_dequeuers {
            let _ = self.deq_lock.lock().unwrap();
            self.not_empty.notify_all();
        }
    }

    pub fn deq(&mut self) -> Option<T> {
        let mut must_wake_enqueuers = false;
        let mut guard: MutexGuard<()> = self.deq_lock.lock().unwrap();
        while self.size.load(Ordering::SeqCst) == 0 {
            println!("OOPS SIZE IS ZERO");
            guard = self.not_empty.wait(guard).unwrap();
        }
        let head = self.head;
        unsafe {
            if (*head).next.is_null() {
                self.head = Box::into_raw(Box::new(Node {
                    elem: None,
                    next: ptr::null_mut(),
                }));
            } else {
                self.head = (*head).next;
            }
        }
        unsafe {
            println!("OLD HEAD: {:?}\nHEAD:    {:?}", *head, *self.head);
        }
        let node: Box<Node<T>> = unsafe { Box::from_raw(head) };
        let result = node.elem;
        if self.size.fetch_sub(1, Ordering::SeqCst) == self.capacity {
            must_wake_enqueuers = true;
        }
        drop(guard);
        if must_wake_enqueuers {
            let _ = self.enq_lock.lock().unwrap();
            self.not_full.notify_all();
        }
        println!("DEQ: {:?}", result);
        return result;
    }
}

struct Queue<T>
where
    T: ::std::fmt::Debug,
{
    // we need to use unsafe cell so that innerqueue has a fixed place in memory
    inner: *mut InnerQueue<T>,
}

impl<T> Clone for Queue<T>
where
    T: ::std::fmt::Debug,
{
    fn clone(&self) -> Queue<T> {
        Queue { inner: self.inner }
    }
}

#[allow(dead_code)]
impl<T> Queue<T>
where
    T: ::std::fmt::Debug,
{
    pub fn new() -> Queue<T> {
        let inner = Box::into_raw(Box::new(InnerQueue::new()));
        Queue { inner: inner }
    }

    pub fn enq(&mut self, elem: T) {
        unsafe { (*self.inner).enq(elem) }
    }

    pub fn deq(&mut self) -> Option<T> {
        unsafe { (*self.inner).deq() }
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
        let total_senders = 2;
        let mut vals = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];

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
                    Some(v) => {
                        collected.push(v);
                    }
                    None => continue,
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
            println!("TOTAL_SENDERS: {}\nVALS: {:?}", total_senders, vals);
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
                        Some(v) => {
                            collected.push(v);
                        }
                        None => continue,
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
