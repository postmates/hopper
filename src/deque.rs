// Indebted to "The Art of Multiprocessor Programming"
use std::sync::{Condvar, Mutex, MutexGuard};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{mem, ptr};

unsafe impl<T, S> Send for Queue<T, S> {}
unsafe impl<T, S> Sync for Queue<T, S> {}

struct InnerQueue<T, S> {
    capacity: usize,
    data: *mut (*const T),
    size: AtomicUsize,
    back_lock: Mutex<BackGuardInner<S>>,
    front_lock: Mutex<FrontGuardInner>,
    not_empty: Condvar,
}

#[derive(Debug, Clone, Copy)]
pub enum Error<T> {
    Full(T),
}

#[derive(Debug, Clone, Copy)]
pub struct FrontGuardInner {
    offset: isize,
}

#[derive(Debug)]
pub struct BackGuardInner<S> {
    offset: isize,
    pub inner: S,
}

impl<T, S> InnerQueue<T, S>
where
    S: ::std::default::Default,
{
    pub fn with_capacity(capacity: usize) -> InnerQueue<T, S> {
        let mut data: Vec<*const T> = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            data.push(ptr::null());
        }
        let raw_data = (&mut data).as_mut_ptr();
        mem::forget(data);
        InnerQueue {
            capacity: capacity,
            data: raw_data,
            size: AtomicUsize::new(0),
            back_lock: Mutex::new(BackGuardInner {
                offset: 0,
                inner: S::default(),
            }),
            front_lock: Mutex::new(FrontGuardInner { offset: 0 }),
            not_empty: Condvar::new(),
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn lock_back(&mut self) -> MutexGuard<BackGuardInner<S>> {
        self.back_lock.lock().expect("enq lock poisoned")
    }

    pub fn lock_front(&mut self) -> MutexGuard<FrontGuardInner> {
        self.front_lock.lock().expect("deq lock poisoned")
    }

    pub unsafe fn push_back(
        &self,
        elem: T,
        guard: &mut MutexGuard<BackGuardInner<S>>,
    ) -> Result<bool, Error<T>> {
        let mut must_wake_dequeuers = false;
        if !(*self.data.offset((*guard).offset)).is_null() {
            return Err(Error::Full(elem));
        } else {
            *self.data.offset((*guard).offset) = Box::into_raw(Box::new(elem));
            (*guard).offset += 1;
            (*guard).offset %= self.capacity as isize;
            if self.size.fetch_add(1, Ordering::Release) == 0 {
                must_wake_dequeuers = true;
            };
        }
        return Ok(must_wake_dequeuers);
    }

    pub unsafe fn pop_front_no_block(
        &mut self,
        guard: &mut MutexGuard<FrontGuardInner>,
    ) -> Option<T> {
        if self.size.load(Ordering::Acquire) == 0 {
            return None;
        } else {
            let elem: T = ptr::read(*self.data.offset((*guard).offset));
            *self.data.offset((*guard).offset) = ptr::null_mut();
            (*guard).offset += 1;
            (*guard).offset %= self.capacity as isize;
            self.size.fetch_sub(1, Ordering::Release);
            return Some(elem);
        }
    }

    /// WARNING do not call this if deq_lock has been locked by the same thread
    /// you WILL deadlock and have a bad time
    pub unsafe fn pop_front(&mut self) -> T {
        let mut guard = self.front_lock.lock().expect("deq lock poisoned");
        while self.size.load(Ordering::Acquire) == 0 {
            guard = self.not_empty.wait(guard).expect("oops could not wait deq");
        }
        let elem: T = ptr::read(*self.data.offset((*guard).offset));
        *self.data.offset((*guard).offset) = ptr::null_mut();
        (*guard).offset += 1;
        (*guard).offset %= self.capacity as isize;
        self.size.fetch_sub(1, Ordering::Release);
        return elem;
    }

    pub unsafe fn push_front(
        &mut self,
        _elem: T,
        _guard: &mut MutexGuard<FrontGuardInner>,
    ) -> Result<bool, Error<T>> {
        unimplemented!();
    }
}

#[derive(Debug)]
pub struct Queue<T, S> {
    inner: *mut InnerQueue<T, S>,
}

impl<T, S> Clone for Queue<T, S> {
    fn clone(&self) -> Queue<T, S> {
        Queue { inner: self.inner }
    }
}

#[allow(dead_code)]
impl<T, S: ::std::default::Default> Queue<T, S> {
    pub fn with_capacity(capacity: usize) -> Queue<T, S> {
        let inner = Box::into_raw(Box::new(InnerQueue::with_capacity(capacity)));
        Queue { inner: inner }
    }

    pub fn capacity(&self) -> usize {
        unsafe { (*self.inner).capacity() }
    }

    pub fn size(&self) -> usize {
        unsafe { (*self.inner).size() }
    }

    pub fn lock_back(&mut self) -> MutexGuard<BackGuardInner<S>> {
        unsafe { (*self.inner).lock_back() }
    }

    pub fn lock_front(&mut self) -> MutexGuard<FrontGuardInner> {
        unsafe { (*self.inner).lock_front() }
    }

    /// Enqueue a value
    ///
    /// If an error is returned there was not enough space in the memory
    /// buffer. Caller is responsible for coping.
    ///
    /// If return is an okay _and_ the value is true the caller is responsible
    /// for calling notify_not_empty OR A DEADLOCK WILL HAPPEN. Thank you and
    /// god bless; you're welcome.
    pub fn push_back(
        &mut self,
        elem: T,
        mut guard: &mut MutexGuard<BackGuardInner<S>>,
    ) -> Result<bool, Error<T>> {
        unsafe { (*self.inner).push_back(elem, &mut guard) }
    }

    pub fn push_front(
        &mut self,
        elem: T,
        mut guard: &mut MutexGuard<FrontGuardInner>,
    ) -> Result<bool, Error<T>> {
        unsafe { (*self.inner).push_front(elem, &mut guard) }
    }

    pub fn notify_not_empty(&mut self, _guard: &MutexGuard<FrontGuardInner>) {
        // guard is not used here but is required to verifiy that 1. a deadlock
        // situation has not happened and 2. we're not doing a notify without
        // holding the lock.
        unsafe { (*self.inner).not_empty.notify_all() }
    }

    pub fn pop_front_no_block(&mut self, guard: &mut MutexGuard<FrontGuardInner>) -> Option<T> {
        unsafe { (*self.inner).pop_front_no_block(guard) }
    }

    pub fn pop_front(&mut self) -> T {
        unsafe { (*self.inner).pop_front() }
    }
}

// #[cfg(test)]
// mod test {
//     extern crate quickcheck;

//     use self::quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
//     use std::thread;
//     use super::*;

//     #[derive(Clone, Debug)]
//     enum Action {
//         Enq(u64),
//         Deq,
//     }

//     impl Arbitrary for Action {
//         fn arbitrary<G>(g: &mut G) -> Action
//         where
//             G: Gen,
//         {
//             let i: usize = g.gen_range(0, 100);
//             match i {
//                 0...50 => Action::Enq(g.gen::<u64>()),
//                 _ => Action::Deq,
//             }
//         }
//     }

//     #[test]
//     fn sequential_model_check() {
//         fn inner(actions: Vec<Action>) -> TestResult {
//             use std::collections::VecDeque;

//             let mut model: VecDeque<u64> = VecDeque::new();
//             let mut sut: Queue<u64> = Queue::new();

//             for action in actions {
//                 match action {
//                     Action::Enq(v) => {
//                         model.push_back(v);
//                         assert!(sut.enq(v).is_ok());
//                     }
//                     Action::Deq => match model.pop_front() {
//                         Some(v) => {
//                             assert_eq!(v, sut.deq());
//                         }
//                         None => continue,
//                     },
//                 }
//             }
//             TestResult::passed()
//         }
//         QuickCheck::new().quickcheck(inner as fn(Vec<Action>) -> TestResult);
//     }

//     #[test]
//     fn model_check() {
//         fn inner(total_senders: usize, capacity: usize, vals: Vec<u64>) -> TestResult {
//             if total_senders == 0 || total_senders > 10 || capacity == 0 || vals.len() == 0
//                 || (vals.len() < total_senders)
//             {
//                 return TestResult::discard();
//             }

//             let mut sut: Queue<u64> = Queue::with_capacity(capacity);

//             let chunk_size = vals.len() / total_senders;

//             let mut snd_jh = Vec::new();
//             let snd_vals = vals.clone();
//             for chunk in snd_vals.chunks(chunk_size) {
//                 let mut snd_q = sut.clone();
//                 let chunk: Vec<u64> = chunk.to_vec();
//                 snd_jh.push(thread::spawn(move || {
//                     let mut queued: Vec<u64> = Vec::new();
//                     for ev in chunk {
//                         loop {
//                             if snd_q.enq(ev).is_ok() {
//                                 queued.push(ev);
//                                 break;
//                             }
//                         }
//                     }
//                     queued
//                 }))
//             }

//             let expected_total_vals = vals.len();
//             let rcv_jh = thread::spawn(move || {
//                 let mut collected: Vec<u64> = Vec::new();
//                 while collected.len() < expected_total_vals {
//                     let v = sut.deq();
//                     collected.push(v);
//                 }
//                 collected
//             });

//             let mut snd_vals: Vec<u64> = Vec::new();
//             for jh in snd_jh {
//                 snd_vals.append(&mut jh.join().expect("snd join failed"));
//             }
//             let mut rcv_vals: Vec<u64> = rcv_jh.join().expect("rcv join failed");

//             rcv_vals.sort();
//             snd_vals.sort();

//             assert_eq!(rcv_vals, snd_vals);
//             TestResult::passed()
//         }
//         QuickCheck::new().quickcheck(inner as fn(usize, usize, Vec<u64>) -> TestResult);
//     }
// }
