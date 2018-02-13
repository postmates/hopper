// Indebted to "The Art of Multiprocessor Programming"
use std::sync::{Condvar, Mutex, MutexGuard};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fmt, mem, ptr};

unsafe impl<T: fmt::Debug, S> Send for Queue<T, S> {}
unsafe impl<T: fmt::Debug, S> Sync for Queue<T, S> {}

struct InnerQueue<T, S>
where
    T: fmt::Debug,
{
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
    T: fmt::Debug,
    S: ::std::default::Default,
{
    pub fn with_capacity(capacity: usize) -> InnerQueue<T, S> {
        assert!(capacity > 0);
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

    pub fn lock_back(&self) -> MutexGuard<BackGuardInner<S>> {
        self.back_lock.lock().expect("enq lock poisoned")
    }

    pub fn lock_front(&self) -> MutexGuard<FrontGuardInner> {
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
        Ok(must_wake_dequeuers)
    }

    pub unsafe fn pop_back_no_block(&self, guard: &mut MutexGuard<BackGuardInner<S>>) -> Option<T> {
        if self.size.load(Ordering::Acquire) == 0 {
            None
        } else {
            if ((*guard).offset - 1) < 0 {
                (*guard).offset = (self.capacity - 1) as isize;
            } else {
                (*guard).offset -= 1;
            };
            let elem: Box<T> = Box::from_raw(*self.data.offset((*guard).offset) as *mut T);
            *self.data.offset((*guard).offset) = ptr::null_mut();
            self.size.fetch_sub(1, Ordering::Release);
            Some(*elem)
        }
    }

    /// WARNING do not call this if deq_lock has been locked by the same thread
    /// you WILL deadlock and have a bad time
    pub unsafe fn pop_front(&self) -> T {
        let mut guard = self.front_lock.lock().expect("deq lock poisoned");
        while self.size.load(Ordering::Acquire) == 0 {
            guard = self.not_empty.wait(guard).expect("oops could not wait deq");
        }
        let elem: Box<T> = Box::from_raw(*self.data.offset((*guard).offset) as *mut T);
        *self.data.offset((*guard).offset) = ptr::null_mut();
        (*guard).offset += 1;
        (*guard).offset %= self.capacity as isize;
        self.size.fetch_sub(1, Ordering::Release);
        *elem
    }
}

#[derive(Debug)]
pub struct Queue<T, S>
where
    T: fmt::Debug,
{
    inner: *mut InnerQueue<T, S>,
}

impl<T, S> Clone for Queue<T, S>
where
    T: fmt::Debug,
{
    fn clone(&self) -> Queue<T, S> {
        Queue { inner: self.inner }
    }
}

impl<T, S: ::std::default::Default> Queue<T, S>
where
    T: fmt::Debug,
{
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

    pub fn lock_back(&self) -> MutexGuard<BackGuardInner<S>> {
        unsafe { (*self.inner).lock_back() }
    }

    pub fn lock_front(&self) -> MutexGuard<FrontGuardInner> {
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
        &self,
        elem: T,
        mut guard: &mut MutexGuard<BackGuardInner<S>>,
    ) -> Result<bool, Error<T>> {
        unsafe { (*self.inner).push_back(elem, &mut guard) }
    }

    /// Pops the back but DOES NOT shift the offset
    ///
    /// pop_back is probably a bad name. We remove the item under the current
    /// offset but assume that the spot will be used again, eg in an overflow
    /// situation.
    pub fn pop_back_no_block(&self, mut guard: &mut MutexGuard<BackGuardInner<S>>) -> Option<T> {
        unsafe { (*self.inner).pop_back_no_block(&mut guard) }
    }

    pub fn notify_not_empty(&self, _guard: &MutexGuard<FrontGuardInner>) {
        // guard is not used here but is required to verifiy that 1. a deadlock
        // situation has not happened and 2. we're not doing a notify without
        // holding the lock.
        unsafe { (*self.inner).not_empty.notify_all() }
    }

    pub fn pop_front(&mut self) -> T {
        unsafe { (*self.inner).pop_front() }
    }
}
