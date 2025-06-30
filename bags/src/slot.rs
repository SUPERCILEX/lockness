use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    process::abort,
    ptr,
    sync::{
        Arc,
        atomic::{
            AtomicU32, AtomicU64,
            Ordering::{Acquire, Relaxed, Release},
            fence,
        },
        mpsc::{RecvError, SendError, TryRecvError, TrySendError},
    },
};

use bitflags::bitflags;

use crate::{atomic_sleep, atomic_wake, cache_padded::CachePadded, u64_to_futex};

#[must_use]
pub fn mpsc<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        send: CachePadded(AtomicU64::new(SendStatus::default().bits())),
        recv: AtomicU32::new(RecvStatus::default().bits()),
        num_senders: AtomicU32::new(1),
        slot: UnsafeCell::new(MaybeUninit::uninit()),
    });
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}

#[derive(Debug)]
struct Inner<T> {
    send: CachePadded<AtomicU64>,
    recv: AtomicU32,
    num_senders: AtomicU32,
    slot: UnsafeCell<MaybeUninit<T>>,
}

#[derive(Debug)]
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

#[derive(Debug)]
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
}

unsafe impl<T: Send> Send for Sender<T> {}
unsafe impl<T> Sync for Sender<T> {}
unsafe impl<T: Send> Send for Receiver<T> {}

bitflags! {
    #[derive(Copy, Clone, Default, Debug)]
    pub struct SendStatus: u64 {
        const RECEIVER_DEAD = 1 << 63;
        const SLEEPING = 1 << 62;
        // RESERVE is implicitly encoded as any other bit
    }
}

impl SendStatus {
    const ABORT: u64 = (SendStatus::all().bits() + 1) >> 1;

    const fn reservations(self) -> u64 {
        self.difference(Self::all()).bits()
    }
}

bitflags! {
    #[derive(Copy, Clone, Default, Debug)]
    pub struct RecvStatus: u32 {
        const COMMIT = 1 << 0;
        const SLEEPING = 1 << 1;
        const SENDER_DEAD = 1 << 2;
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        let Self {
            send: _,
            recv,
            num_senders: _,
            slot,
        } = self;

        let has_value =
            RecvStatus::from_bits_retain(recv.load(Acquire)).contains(RecvStatus::COMMIT);
        if has_value {
            let slot = slot.get_mut();
            unsafe {
                slot.assume_init_drop();
            }
        }
    }
}

impl<T> Sender<T> {
    pub fn send(&self, mut data: T) -> Result<(), SendError<T>> {
        loop {
            match self.try_send_(data) {
                Ok(()) => return Ok(()),
                Err((TrySendError::Full(restored), expected)) => {
                    data = restored;

                    let send = &self.inner.send;
                    send.fetch_or(SendStatus::SLEEPING.bits(), Relaxed);
                    let expected = (expected | SendStatus::SLEEPING).bits();
                    atomic_sleep(
                        u64_to_futex(send),
                        (expected >> u32::BITS).try_into().unwrap(),
                    );

                    // Pass the torch. If we've been woken up, it means we previously went to sleep
                    // and thus anybody attempting to write a value alongside us might have also
                    // gone to sleep. As long as each woken sleeper notes that somebody else might
                    // still be sleeping, we'll eventually wake everybody up at the cost of one
                    // spurious WAKE syscall at the end of the chain.
                    send.fetch_or(SendStatus::SLEEPING.bits(), Relaxed);
                }
                Err((TrySendError::Disconnected(data), _)) => return Err(SendError(data)),
            }
        }
    }

    pub fn try_send(&self, data: T) -> Result<(), TrySendError<T>> {
        self.try_send_(data).map_err(|(v, _)| v)
    }

    #[inline]
    fn try_send_(&self, data: T) -> Result<(), (TrySendError<T>, SendStatus)> {
        let Inner {
            send,
            recv,
            num_senders: _,
            slot,
        } = &*self.inner;

        let send = SendStatus::from_bits_retain(send.fetch_add(1, Relaxed));
        if send.contains(SendStatus::RECEIVER_DEAD) {
            return Err((TrySendError::Disconnected(data), send));
        }
        {
            let reservations = send.reservations();
            if reservations >= SendStatus::ABORT {
                // We would like to use fetch_or, but x86 is a fat pile of shit. Unlike good
                // instructions sets (RISC-V), atomic bit-wise operations in x86 can only use
                // the bit operated upon (by virtue of https://en.wikipedia.org/wiki/Bit_Test
                // instructions) or else face a cmpxchg loop. However, xadd and xsub can
                // atomically return the full previous value.
                //
                // We therefore implement a hack wherein the RESERVE bit is represented as any
                // unknown bits being set. Unfortunately, this will eventually cause us to wrap
                // around leading to the following race:
                // 1. Thread A reserves the slot, but hasn't committed it yet.
                // 2. Thread B gets a wrapped reservation and smashes the slot.
                // If the value is multiple words and you interleave the threads evilly, you can
                // get a torn value. Of course with a u64 this will never happen but eh.
                ran_out_of_address_space();
            }
            if reservations > 0 {
                return Err((TrySendError::Full(data), send));
            }
        }

        {
            let slot = slot.get();
            let data = MaybeUninit::new(data);
            // Synchronize on the "send" reservation to write new data after the receiver
            // has read the previous value.
            // Also ensures we load "receive" after it was published.
            fence(Acquire);
            unsafe {
                ptr::write(slot, data);
            }
        }
        let receiver_sleeping =
            RecvStatus::from_bits_retain(recv.swap(RecvStatus::COMMIT.bits(), Release))
                .contains(RecvStatus::SLEEPING);

        if receiver_sleeping {
            atomic_wake(recv, 1);
        }
        Ok(())
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let Self { inner } = self;
        if inner.num_senders.fetch_add(1, Relaxed) > u32::MAX / 2 {
            too_many_senders();
        }
        Self {
            inner: inner.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let Inner {
            send: _,
            recv,
            num_senders,
            slot: _,
        } = &*self.inner;

        if num_senders.fetch_sub(1, Relaxed) > 1 {
            return;
        }

        let receiver_sleeping = RecvStatus::from_bits_retain(
            // As noted in my other comment x86 sucks so this will generate a cmpxchg loop, but
            // I guess that's ok since we're in a drop impl. We can't use the swap trick from
            // Receiver::drop because the COMMIT bit can't be changed.
            recv.fetch_or(RecvStatus::SENDER_DEAD.bits(), Relaxed),
        )
        .contains(RecvStatus::SLEEPING);
        if receiver_sleeping {
            atomic_wake(recv, u32::MAX);
        }
    }
}

impl<T> Receiver<T> {
    pub fn recv(&self) -> Result<T, RecvError> {
        loop {
            match self.try_recv_() {
                Ok(v) => return Ok(v),
                Err((TryRecvError::Empty, expected)) => {
                    let recv = &self.inner.recv;
                    recv.fetch_or(RecvStatus::SLEEPING.bits(), Relaxed);
                    atomic_sleep(recv, (expected | RecvStatus::SLEEPING).bits());
                }
                Err((TryRecvError::Disconnected, _)) => return Err(RecvError),
            }
        }
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.try_recv_().map_err(|(v, _)| v)
    }

    #[inline]
    fn try_recv_(&self) -> Result<T, (TryRecvError, RecvStatus)> {
        let Inner {
            send,
            recv,
            num_senders: _,
            slot,
        } = &*self.inner;

        let recv_ = RecvStatus::from_bits_retain(recv.load(Acquire));
        if !recv_.contains(RecvStatus::COMMIT) {
            if recv_.contains(RecvStatus::SENDER_DEAD) {
                return Err((TryRecvError::Disconnected, recv_));
            }
            return Err((TryRecvError::Empty, recv_));
        }
        recv.fetch_and(RecvStatus::SENDER_DEAD.bits(), Relaxed);

        let value = {
            let slot = slot.get();
            let data = unsafe { ptr::read(slot) };
            // Synchronize on the "send" reservations to read data before the sender
            // overwrites it. Also ensures that "receive" was written to before
            // publishing "send". Alternatively, we could use amoswap (and
            // restore the DEAD state), but splitting the swap allows the store to be
            // buffered.
            //
            // We don't use acquire/release semantics on the "send" atomic itself because we
            // expect the contended Sender case to result in many loads to "send" which
            // don't need synchronization.
            fence(Release);
            unsafe { data.assume_init() }
        };
        let sender_sleeping =
            SendStatus::from_bits_retain(send.load(Relaxed)).contains(SendStatus::SLEEPING);
        send.store(SendStatus::default().bits(), Release);

        if sender_sleeping {
            atomic_wake(u64_to_futex(send), 1);
        }

        Ok(value)
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let Inner {
            send,
            recv: _,
            num_senders: _,
            slot: _,
        } = &*self.inner;

        let sender_sleeping =
            SendStatus::from_bits_retain(send.swap(SendStatus::RECEIVER_DEAD.bits(), Relaxed))
                .contains(SendStatus::SLEEPING);

        if sender_sleeping {
            atomic_wake(u64_to_futex(send), u32::MAX);
        }
    }
}

#[cold]
fn ran_out_of_address_space() {
    eprintln!("Slot attempted to send a value unsuccessfully too many times, aborting.");
    abort()
}

#[cold]
fn too_many_senders() {
    eprintln!("Too many senders, aborting.");
    abort()
}

#[cfg(test)]
mod tests {
    use std::{
        sync::mpsc::{RecvError, SendError, TryRecvError, TrySendError},
        thread,
    };

    use rstest::rstest;

    use crate::{
        mpsc_slot,
        slot::{Receiver, Sender},
    };

    type SendFn<T> = fn(&Sender<T>, T) -> Result<(), SendError<T>>;
    type RecvFn<T> = fn(&Receiver<T>) -> Result<T, RecvError>;

    fn send_polyfill<T>(sender: &Sender<T>, mut value: T) -> Result<(), SendError<T>> {
        loop {
            match sender.try_send(value) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(t)) => return Err(SendError(t)),
                Err(TrySendError::Full(t)) => value = t,
            }
        }
    }

    fn recv_polyfill<T>(receiver: &Receiver<T>) -> Result<T, RecvError> {
        loop {
            match receiver.try_recv() {
                Ok(value) => return Ok(value),
                Err(TryRecvError::Disconnected) => return Err(RecvError),
                Err(TryRecvError::Empty) => (),
            }
        }
    }

    #[rstest]
    #[case(send_polyfill::<Box<i32>>, recv_polyfill::<Box<i32>>)]
    #[case(Sender::<Box<i32>>::send, Receiver::<Box<i32>>::recv)]
    fn drops(#[case] send: SendFn<Box<i32>>, #[case] recv: RecvFn<Box<i32>>) {
        {
            let (_sender, _receiver) = mpsc_slot::<Box<i32>>();
        }

        {
            let (sender, _receiver) = mpsc_slot::<Box<i32>>();
            send(&sender, Box::new(42)).unwrap();
        }

        {
            let (sender, receiver) = mpsc_slot::<Box<i32>>();
            drop(receiver);
            send(&sender, Box::new(42)).unwrap_err();
        }

        {
            let (sender, receiver) = mpsc_slot::<Box<i32>>();
            send(&sender, Box::new(42)).unwrap();
            drop(receiver);
            send(&sender, Box::new(666)).unwrap_err();
        }

        {
            let (sender, receiver) = mpsc_slot::<Box<i32>>();
            drop(sender);
            recv(&receiver).unwrap_err();
        }

        {
            let (sender, receiver) = mpsc_slot::<Box<i32>>();
            send(&sender, Box::new(666)).unwrap();
            drop(sender);

            let value = *recv(&receiver).unwrap();
            assert_eq!(666, value);
            recv(&receiver).unwrap_err();
        }
    }

    #[rstest]
    #[case(send_polyfill::<Box<usize>>, recv_polyfill::<Box<usize>>)]
    #[case(Sender::<Box<usize>>::send, Receiver::<Box<usize>>::recv)]
    fn basic(#[case] send: SendFn<Box<usize>>, #[case] recv: RecvFn<Box<usize>>) {
        let (sender, receiver) = mpsc_slot();
        let senders = (0..3)
            .map(|_| {
                thread::spawn({
                    let sender = sender.clone();
                    move || {
                        for i in 0..10 {
                            send(&sender, Box::new(i)).unwrap()
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        drop(sender);
        thread::spawn(move || {
            let mut counts = [0; 10];
            while let Ok(i) = recv(&receiver) {
                counts[*i] += 1;
            }
            for (i, &count) in counts.iter().enumerate() {
                assert_eq!(3, count, "At idx {i}");
            }
        })
        .join()
        .unwrap();
        for sender in senders {
            sender.join().unwrap();
        }
    }

    #[rstest]
    #[case(send_polyfill::<Vec<i32>>, recv_polyfill::<Vec<i32>>)]
    #[case(Sender::<Vec<i32>>::send, Receiver::<Vec<i32>>::recv)]
    fn ping_pong(#[case] send: SendFn<Vec<i32>>, #[case] recv: RecvFn<Vec<i32>>) {
        let (ping_sender, pong_receiver) = mpsc_slot();
        let (pong_sender, ping_receiver) = mpsc_slot();

        let ping = thread::spawn(move || {
            let mut data = Vec::new();
            for i in 0..100 {
                data.push(i);
                send(&ping_sender, data).unwrap();
                data = recv(&ping_receiver).unwrap();
            }
            data
        });
        let pong = thread::spawn(move || {
            while let Ok(mut data) = recv(&pong_receiver) {
                data.push(*data.last().unwrap());
                send(&pong_sender, data).unwrap();
            }
        });

        let actual = ping.join().unwrap();
        let () = pong.join().unwrap();

        let expected: Vec<_> = (0..100).flat_map(|i| [i, i]).collect();

        assert_eq!(expected, actual);
    }
}
