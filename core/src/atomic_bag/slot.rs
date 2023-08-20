use std::{
    marker::PhantomData,
    mem::ManuallyDrop,
    ptr,
    ptr::NonNull,
    sync::{
        atomic::{
            fence, AtomicPtr,
            Ordering::{Acquire, Release},
        },
        Arc,
    },
    time::{Duration, Instant},
};

use super::Allocated;

#[must_use]
pub fn mpsc<T: Allocated>() -> (Sender<T>, SoloReceiver<T>) {
    let inner = Arc::new(Inner {
        ptr: AtomicPtr::new(ptr::null_mut()),
        _marker: PhantomData,
    });

    (
        Sender {
            inner: inner.clone(),
        },
        SoloReceiver { inner },
    )
}

pub struct Sender<T: Allocated> {
    inner: Arc<Inner<T>>,
}

pub struct SoloReceiver<T: Allocated> {
    inner: Arc<Inner<T>>,
}

struct Inner<T: Allocated> {
    ptr: AtomicPtr<T>,
    _marker: PhantomData<T>,
}

impl<T: Allocated> Inner<T> {
    fn into_inner(self) -> Option<T> {
        let me = ManuallyDrop::new(self);
        match Kind::from(me.ptr.load(Acquire)) {
            Kind::Null | Kind::Sleeping => None,
            Kind::Value(value) => Some(value),
        }
    }
}

impl<T: Allocated> Drop for Inner<T> {
    fn drop(&mut self) {
        let Self { ptr, _marker: _ } = self;
        drop(Kind::from(ptr.load(Acquire)));
    }
}

enum Kind<T> {
    Null,
    Sleeping,
    Value(T),
}

impl<T> Kind<T> {
    // This could be made generic by putting it in the Allocated trait and asking
    // people to give us invalid types, but I don't think there's a lot of use in
    // passing around single usize values.
    const SLEEP_SENTINEL: *mut T = ptr::invalid_mut(usize::MAX);
}

impl<T: Allocated> From<*mut T> for Kind<T> {
    fn from(value: *mut T) -> Self {
        Self::from((value, || ()))
    }
}

impl<T: Allocated, F: FnOnce()> From<(*mut T, F)> for Kind<T> {
    fn from((value, f): (*mut T, F)) -> Self {
        if value.is_null() {
            Self::Null
        } else if value == Self::SLEEP_SENTINEL {
            Self::Sleeping
        } else {
            f();
            Self::Value(unsafe { T::from_ptr(NonNull::new_unchecked(value.cast())) })
        }
    }
}

impl<T: Allocated> Sender<T> {
    pub fn try_send(&self, value: T) -> Option<T> {
        match Kind::from((
            self.inner
                .ptr
                .swap(value.into_ptr().cast().as_ptr(), Release),
            || fence(Acquire),
        )) {
            Kind::Null => None,
            Kind::Sleeping => todo!("wake"),
            Kind::Value(value) => Some(value),
        }
    }
}

impl<T: Allocated> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: Allocated> SoloReceiver<T> {
    #[must_use]
    pub fn try_recv(&self) -> Option<T> {
        self.recv_(None)
    }

    #[must_use]
    pub fn recv(&self) -> Option<T> {
        self.recv_timeout(Duration::MAX)
    }

    #[must_use]
    pub fn recv_timeout(&self, timeout: Duration) -> Option<T> {
        self.recv_(Instant::now().checked_add(timeout))
    }

    #[must_use]
    pub fn recv_deadline(&self, deadline: Instant) -> Option<T> {
        self.recv_(Some(deadline))
    }

    fn recv_(&self, deadline: Option<Instant>) -> Option<T> {
        match Kind::from(self.inner.ptr.swap(ptr::null_mut(), Acquire)) {
            Kind::Null => {
                if let Some(deadline) = deadline {
                    loop {
                        match Kind::from(self.inner.ptr.swap(Kind::SLEEP_SENTINEL, Acquire)) {
                            Kind::Null | Kind::Sleeping => todo!("sleep"),
                            Kind::Value(value) => break Some(value),
                        }
                    }
                } else {
                    None
                }
            }
            Kind::Sleeping => unsafe { std::hint::unreachable_unchecked() },
            Kind::Value(value) => Some(value),
        }
    }

    pub fn shutdown(self) -> Option<T> {
        let Self { inner: data } = self;
        Arc::into_inner(data).and_then(Inner::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;
    use crate::atomic_bag::test::Boxed;

    #[test]
    fn write() {
        let (sender, _) = mpsc();

        assert!(sender.try_send(Boxed::new(0)).is_none());
        for i in 1..10 {
            assert_eq!(Some(Boxed::new(i - 1)), sender.try_send(Boxed::new(i)));
        }
    }

    #[test]
    fn read() {
        let (_, receiver) = mpsc::<Boxed>();

        assert!(receiver.try_recv().is_none());
    }

    #[test]
    fn push_pop() {
        let (sender, receiver) = mpsc();

        for i in 0..10 {
            assert!(sender.try_send(Boxed::new(i)).is_none());
            assert_eq!(Some(Boxed::new(i)), receiver.try_recv());
        }
    }

    #[test]
    fn push_pop_multi_multi_threaded() {
        const ITERS: usize = 100;

        let (sender, receiver) = mpsc();

        let generate = move || {
            let mut holding_cell = None;
            let mut i = 0;
            loop {
                holding_cell = if let Some(v) = holding_cell {
                    sender.try_send(v)
                } else if i < ITERS {
                    i += 1;
                    sender.try_send(Boxed::new(i - 1))
                } else {
                    break;
                };
            }
        };
        let producer1 = thread::spawn(generate.clone());
        let producer2 = thread::spawn(generate);
        thread::spawn(move || {
            let mut total = 0usize;
            for i in 0..ITERS {
                total += i;
            }
            total *= 2;

            let mut received = 0;
            while received < ITERS * 2 {
                if let Some(v) = receiver.try_recv() {
                    total = total.checked_sub(*v).unwrap();
                    received += 1;
                }
            }

            assert_eq!(0, total);
        })
        .join()
        .unwrap();
        producer1.join().unwrap();
        producer2.join().unwrap();
    }
}
