// Indebted to "The Art of Multiprocessor Programming"
//
// Welcome friends. This module implements a doubly ended queue that allows
// concurrent access to the back and front of the queue. This is used to give
// Sender and Receiver more or less uncoordinated enqueue/dequeue
// operations. The underlying structure is a contiguous allocation operated like
// a ring buffer. When the buffer fills up enqueue fails. The only coordination
// that does happen is through a condvar, waking up a pop_front operation that
// blocks when there's no data to pop.
//
// The exact API is a little weird, which we'll get into below. Just keep in
// mind: it's a contiguous block of memory with some fancy bits tacked on.
use std::sync::{Condvar, Mutex, MutexGuard};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fmt, mem, sync};

unsafe impl<T, S> Send for Queue<T, S> {}
unsafe impl<T, S> Sync for Queue<T, S> {}

// This is InnerQueue. You can see in our self-derived Send / Sync that there's
// an actual Queue somewhere below. What gives?
//
// InnerQueue is the real deal. This is where the data lives, this is where all
// the locks live. When the user creates a Queue this InnerQueue is allocated on
// the heap and then that's it, each subsequent clone of Queue stores a pointer
// to InnerQueue.
struct InnerQueue<T, S> {
    capacity: usize,
    data: *mut Option<T>,
    size: AtomicUsize,
    back_lock: Mutex<BackGuardInner<S>>,
    front_lock: Mutex<FrontGuardInner>,
    not_empty: Condvar,
}

// There are two distinct things in InnerQueue that are pointers and we've got
// to be careful about deallocation. Namely, the contiguous array is an array of
// pointers. This is... well, less than ideal for memory locality but that's a
// thing for another time. Anyhow.
impl<T, S> Drop for InnerQueue<T, S> {
    fn drop(&mut self) {
        unsafe {
            // Turn self.data back into a droppable thing...
            let data =
                Vec::from_raw_parts(self.data, self.size.load(Ordering::Acquire), self.capacity);
            // drop the deflated self.data.
            drop(data);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Error<T> {
    Full(T),
}

// FrontGuardInner and BackGuardInner are the insides of the front and back
// locks. What's curious about BackGuardInner is that you can smuggle data
// inside of it. This is driven _entirely_ by the needs of Sender, which has to
// coordinate the sender threads. There's only ever one Receiver and thus no
// need for coordination.
#[derive(Debug, Clone, Copy)]
pub struct FrontGuardInner {
    offset: isize,
}

#[derive(Debug)]
pub struct BackGuardInner<S> {
    offset: isize,
    pub inner: S,
}

// You'll notice that InnerQueue functions tend to take a MutexGuard as an
// argument, rather than managing the mutex by themselves. This is done because
// upstream in Sender we need to be sure that _multiple_ operations to Queue
// happen isolated from other Senders, the Receiver on occasion. It's a little
// tedious but since Rust mutex is tied to scope what else are you gonna do?
impl<T, S> InnerQueue<T, S>
where
    S: ::std::default::Default,
    T: fmt::Debug,
{
    pub fn with_capacity(capacity: usize) -> InnerQueue<T, S> {
        assert!(capacity > 0);
        println!("{:<2}CAPACITY: {}", "", capacity);
        let mut data: Vec<Option<T>> = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            data.push(None);
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
        self.back_lock.lock().expect("back lock poisoned")
    }

    pub fn lock_front(&self) -> MutexGuard<FrontGuardInner> {
        self.front_lock.lock().expect("front lock poisoned")
    }

    pub unsafe fn push_back(
        &self,
        elem: T,
        guard: &mut MutexGuard<BackGuardInner<S>>,
    ) -> Result<bool, Error<T>> {
        println!("{:<2}PUSH_BACK[{}] <- {:?}", "", (*guard).offset, elem);
        let mut must_wake_dequeuers = false;
        let cur_size = self.size.load(Ordering::Acquire);
        println!("{:<3}PUSH_BACK CURRENT_SIZE {}", "", cur_size);
        if cur_size == self.capacity {
            println!("{:<4}FULL", "");
            return Err(Error::Full(elem));
        } else {
            assert!((*self.data.offset((*guard).offset)).is_none());
            *self.data.offset((*guard).offset) = Some(elem);
            (*guard).offset += 1;
            (*guard).offset %= self.capacity as isize;
            if self.size.fetch_add(1, Ordering::Release) == 0 {
                must_wake_dequeuers = true;
            }
        }
        Ok(must_wake_dequeuers)
    }

    pub unsafe fn pop_front(&self) -> T {
        let mut guard = self.front_lock.lock().expect("front lock poisoned");
        while self.size.load(Ordering::Acquire) == 0 {
            println!("{:<4}BLOCK POP_FRONT", "");
            guard = self.not_empty
                .wait(guard)
                .expect("oops could not wait pop_front");
        }
        let elem: Option<T> = mem::replace(&mut *self.data.offset((*guard).offset), None);
        println!("{:<2}POP_FRONT[{}] -> {:?}", "", (*guard).offset, elem);
        assert!(elem.is_some());
        *self.data.offset((*guard).offset) = None;
        (*guard).offset += 1;
        (*guard).offset %= self.capacity as isize;
        let prev_size = self.size.fetch_sub(1, Ordering::Release);
        println!("{:<3}POP_FRONT PREVIOUS SIZE: {}", "", prev_size);
        return elem.unwrap();
    }
}

pub struct Queue<T, S> {
    inner: sync::Arc<InnerQueue<T, S>>,
}

impl<T, S> ::std::fmt::Debug for Queue<T, S> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "sry")
    }
}

impl<T, S> Clone for Queue<T, S> {
    fn clone(&self) -> Queue<T, S> {
        Queue {
            inner: sync::Arc::clone(&self.inner),
        }
    }
}

impl<T, S> Queue<T, S>
where
    S: ::std::default::Default,
    T: fmt::Debug,
{
    pub fn with_capacity(capacity: usize) -> Queue<T, S> {
        let inner = sync::Arc::new(InnerQueue::with_capacity(capacity));
        Queue { inner: inner }
    }

    pub fn capacity(&self) -> usize {
        (*self.inner).capacity()
    }

    pub fn size(&self) -> usize {
        (*self.inner).size()
    }

    pub fn lock_back(&self) -> MutexGuard<BackGuardInner<S>> {
        (*self.inner).lock_back()
    }

    pub fn lock_front(&self) -> MutexGuard<FrontGuardInner> {
        (*self.inner).lock_front()
    }

    /// Push an element onto the back of the queue.
    ///
    /// This function will return an error if the InnerQueue holds `capacity`
    /// elements. The passed `T` will be smuggled out through the error,
    /// returning ownership to the caller.
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

    pub fn notify_not_empty(&self, _guard: &MutexGuard<FrontGuardInner>) {
        // guard is not used here but is required to verifiy that 1. a deadlock
        // situation has not happened and 2. we're not doing a notify without
        // holding the lock.
        (*self.inner).not_empty.notify_all()
    }

    /// Pop an element from the front of the queue
    ///
    /// This function WILL block if there are no elements to be popped from the
    /// front. This block will take no CPU time and the caller thread will only
    /// wake once an element has been pushed onto the queue.
    pub fn pop_front(&mut self) -> T {
        unsafe { (*self.inner).pop_front() }
    }
}
