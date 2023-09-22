use std::{
    marker::PhantomData,
    mem::ManuallyDrop,
    ptr,
    ptr::NonNull,
    sync::{
        atomic::{
            fence, AtomicPtr, AtomicU32,
            Ordering::{Acquire, Relaxed, Release},
        },
        Arc,
    },
    time::{Duration, Instant},
};

use crate::{atomic_wait, atomic_wake, Allocated};

#[must_use]
pub fn mpsc<T: Allocated>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        ptr: AtomicPtr::new(ptr::null_mut()),
        sender_refs: AtomicU32::new(1),
        _marker: PhantomData,
    });

    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}

pub struct Sender<T: Allocated> {
    inner: Arc<Inner<T>>,
}

pub struct Receiver<T: Allocated> {
    inner: Arc<Inner<T>>,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Outcome<T> {
    Ok(T),
    Empty,
    Dead(Option<T>),
}

/// Using the lower 32-bits of a pointer to sleep is totally broken if said
/// pointer has all-zeros in those lower bits, but in practice that (hopefully)
/// won't happen.
#[allow(clippy::missing_const_for_fn)] // Not true in big endian
const fn evil_sleep_cast<T>(ptr: &AtomicPtr<T>) -> &AtomicU32 {
    #[cfg(target_endian = "big")]
    {
        let og = std::mem::size_of::<AtomicPtr<T>>();
        let offset = og.checked_sub(usize::try_from(u32::BITS).unwrap()).unwrap();
        let bottom_32 = unsafe { ptr.as_ptr().byte_add(offset) };
        unsafe { AtomicU32::from_ptr(bottom_32.cast()) }
    }

    #[cfg(target_endian = "little")]
    unsafe {
        AtomicU32::from_ptr(ptr.as_ptr().cast())
    }
}

struct Inner<T: Allocated> {
    ptr: AtomicPtr<T>,
    sender_refs: AtomicU32,
    _marker: PhantomData<T>,
}

impl<T: Allocated> Inner<T> {
    fn shutdown_sender(&self) -> Option<T> {
        let Self {
            ptr,
            sender_refs,
            _marker: _,
        } = self;

        if sender_refs.fetch_sub(1, Relaxed) > 1 {
            return None;
        }

        match Kind::from((ptr.swap(Kind::DEAD_SENTINEL, Relaxed), || fence(Acquire))) {
            Kind::Null | Kind::Dead => None,
            Kind::Sleeping => {
                atomic_wake(evil_sleep_cast(ptr));
                None
            }
            Kind::Value(value) => Some(value),
        }
    }

    fn shutdown_receiver(&self) -> Option<T> {
        let Self {
            ptr,
            sender_refs: _,
            _marker: _,
        } = self;

        match Kind::from((ptr.swap(Kind::DEAD_SENTINEL, Relaxed), || fence(Acquire))) {
            Kind::Null | Kind::Dead => None,
            Kind::Sleeping => unsafe { std::hint::unreachable_unchecked() },
            Kind::Value(value) => Some(value),
        }
    }
}

impl<T: Allocated> Drop for Inner<T> {
    fn drop(&mut self) {
        let Self {
            ptr,
            sender_refs: _, // Handled in Sender::drop
            _marker: _,
        } = self;
        drop(Kind::from(ptr.load(Acquire)));
    }
}

enum Kind<T> {
    Null,
    Sleeping,
    Dead,
    Value(T),
}

impl<T> Kind<T> {
    // This could be made generic by putting it in the Allocated trait and asking
    // people to give us invalid types, but I don't think there's a lot of use in
    // passing around single usize values.
    const SLEEP_SENTINEL: *mut T = ptr::invalid_mut(usize::MAX);
    const DEAD_SENTINEL: *mut T = ptr::invalid_mut(usize::MAX - 1);
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
        } else if value == Self::DEAD_SENTINEL {
            Self::Dead
        } else {
            f();
            Self::Value(unsafe { T::from_ptr(NonNull::new_unchecked(value.cast())) })
        }
    }
}

impl<T: Allocated> Sender<T> {
    pub fn try_send(&self, value: T) -> Outcome<T> {
        match Kind::from((
            self.inner
                .ptr
                .swap(value.into_ptr().cast().as_ptr(), Release),
            || fence(Acquire),
        )) {
            Kind::Null => Outcome::Empty,
            Kind::Dead => {
                // The undo sucks quite a bit on the sender side: you can get tearing where a
                // sender will not realize that the receiver is dead.
                match Kind::from((self.inner.ptr.swap(Kind::DEAD_SENTINEL, Relaxed), || {
                    fence(Acquire);
                })) {
                    Kind::Null | Kind::Sleeping | Kind::Dead => unsafe {
                        std::hint::unreachable_unchecked()
                    },
                    Kind::Value(value) => Outcome::Dead(Some(value)),
                }
            }
            Kind::Sleeping => {
                atomic_wake(evil_sleep_cast(&self.inner.ptr));
                Outcome::Empty
            }
            Kind::Value(value) => Outcome::Ok(value),
        }
    }

    #[must_use]
    pub fn shutdown(self) -> Option<T> {
        let mut me = ManuallyDrop::new(self);
        let inner = unsafe { ptr::from_mut(&mut me.inner).read() };

        inner.shutdown_sender()
    }
}

impl<T: Allocated> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.inner.sender_refs.fetch_add(1, Relaxed);
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: Allocated> Drop for Sender<T> {
    fn drop(&mut self) {
        drop(self.inner.shutdown_sender());
    }
}

#[derive(Copy, Clone, Debug)]
enum Wait {
    Never,
    Forever,
    Deadline(Instant),
}

impl<T: Allocated> Receiver<T> {
    #[must_use]
    pub fn try_recv(&self) -> Outcome<T> {
        self.recv_(Wait::Never)
    }

    #[must_use]
    pub fn recv(&self) -> Outcome<T> {
        self.recv_(Wait::Forever)
    }

    #[must_use]
    pub fn recv_timeout(&self, timeout: Duration) -> Outcome<T> {
        self.recv_(
            Instant::now()
                .checked_add(timeout)
                .map_or(Wait::Forever, Wait::Deadline),
        )
    }

    #[must_use]
    pub fn recv_deadline(&self, deadline: Instant) -> Outcome<T> {
        self.recv_(Wait::Deadline(deadline))
    }

    fn recv_(&self, wait: Wait) -> Outcome<T> {
        match Kind::from(self.inner.ptr.swap(ptr::null_mut(), Acquire)) {
            Kind::Null => {
                if matches!(wait, Wait::Never) {
                    Outcome::Empty
                } else {
                    match Kind::from(self.inner.ptr.swap(Kind::SLEEP_SENTINEL, Acquire)) {
                        Kind::Null => {
                            #[allow(clippy::cast_possible_truncation)]
                            atomic_wait(
                                evil_sleep_cast(&self.inner.ptr),
                                Kind::<T>::SLEEP_SENTINEL.addr() as u32,
                                match wait {
                                    Wait::Never => unsafe { std::hint::unreachable_unchecked() },
                                    Wait::Forever => None,
                                    Wait::Deadline(deadline) => Some(deadline),
                                },
                            );
                            Outcome::Empty
                        }
                        Kind::Dead => {
                            self.inner.ptr.store(Kind::DEAD_SENTINEL, Relaxed);
                            Outcome::Dead(None)
                        }
                        Kind::Sleeping => unsafe { std::hint::unreachable_unchecked() },
                        Kind::Value(value) => Outcome::Ok(value),
                    }
                }
            }
            Kind::Dead => {
                self.inner.ptr.store(Kind::DEAD_SENTINEL, Relaxed);
                Outcome::Dead(None)
            }
            Kind::Sleeping => unsafe { std::hint::unreachable_unchecked() },
            Kind::Value(value) => Outcome::Ok(value),
        }
    }

    #[must_use]
    pub fn shutdown(self) -> Option<T> {
        let mut me = ManuallyDrop::new(self);
        let inner = unsafe { ptr::from_mut(&mut me.inner).read() };

        inner.shutdown_receiver()
    }
}

impl<T: Allocated> Drop for Receiver<T> {
    fn drop(&mut self) {
        drop(self.inner.shutdown_receiver());
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;
    use crate::test::Boxed;

    #[test]
    fn write() {
        let (sender, _receiver) = mpsc();

        assert_eq!(Outcome::Empty, sender.try_send(Boxed::new(0)));
        for i in 1..10 {
            assert_eq!(
                Outcome::Ok(Boxed::new(i - 1)),
                sender.try_send(Boxed::new(i))
            );
        }
    }

    #[test]
    fn write_dead() {
        let (sender, _) = mpsc();

        for i in 0..10 {
            assert_eq!(
                Outcome::Dead(Some(Boxed::new(i))),
                sender.try_send(Boxed::new(i))
            );
        }
    }

    #[test]
    fn read() {
        let (_sender, receiver) = mpsc::<Boxed>();

        assert_eq!(Outcome::Empty, receiver.try_recv());
    }

    #[test]
    fn read_dead() {
        let (_, receiver) = mpsc::<Boxed>();

        assert_eq!(Outcome::Dead(None), receiver.try_recv());
    }

    #[test]
    fn push_pop() {
        let (sender, receiver) = mpsc();

        for i in 0..10 {
            assert_eq!(Outcome::Empty, sender.try_send(Boxed::new(i)));
            assert_eq!(Outcome::Ok(Boxed::new(i)), receiver.try_recv());
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
                    match sender.try_send(v) {
                        Outcome::Ok(value) => Some(value),
                        Outcome::Empty => None,
                        Outcome::Dead(_) => {
                            unreachable!()
                        }
                    }
                } else if i < ITERS {
                    i += 1;
                    match sender.try_send(Boxed::new(i - 1)) {
                        Outcome::Ok(value) => Some(value),
                        Outcome::Empty => None,
                        Outcome::Dead(_) => {
                            unreachable!()
                        }
                    }
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

            let mut i = 0;
            while i < ITERS * 2 {
                match receiver.try_recv() {
                    Outcome::Ok(v) => {
                        total = total.checked_sub(*v).unwrap();
                        i += 1;
                    }
                    Outcome::Dead(_) => {
                        if i == ITERS * 2 - 1 {
                            // Close enough, the producer handled the last value
                            break;
                        }
                    }
                    Outcome::Empty => {}
                }
            }

            assert!(total < ITERS);
        })
        .join()
        .unwrap();
        producer1.join().unwrap();
        producer2.join().unwrap();
    }

    #[test]
    #[allow(clippy::similar_names)]
    #[cfg(not(miri))] // Fails presumably because we sleep on half a word
    fn sleep_test() {
        let (sender, receiver) = mpsc();
        let _keep_alive = sender.clone();

        let received = thread::spawn(move || {
            assert_eq!(Outcome::Empty, receiver.recv());
            receiver.recv()
        });
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            assert_eq!(Outcome::Empty, sender.try_send(Boxed::new(42)));
        })
        .join()
        .unwrap();

        assert_eq!(Outcome::Ok(Boxed::new(42)), received.join().unwrap());
    }
}
