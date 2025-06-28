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

use crate::{atomic_wake, cache_padded::CachePadded, u64_to_futex};

#[must_use]
pub fn mpsc<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        send: CachePadded(AtomicU64::new(SendStatus::default().bits())),
        receive: AtomicU32::new(ReceiveStatus::default().bits()),
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
    receive: AtomicU32,
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
    const ABORT: u64 = 1 << 63;

    const fn reservations(self) -> u64 {
        self.difference(Self::all()).bits()
    }
}

bitflags! {
    #[derive(Copy, Clone, Default, Debug)]
    pub struct ReceiveStatus: u32 {
        const COMMIT = 1 << 0;
        const SLEEPING = 1 << 1;
        const SENDER_DEAD = 1 << 2;
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        let Self {
            send: _,
            receive,
            num_senders: _,
            slot,
        } = self;

        let has_value =
            ReceiveStatus::from_bits_retain(receive.load(Acquire)).contains(ReceiveStatus::COMMIT);
        if has_value {
            let slot = slot.get_mut();
            unsafe {
                slot.assume_init_drop();
            }
        }
    }
}

impl<T> Sender<T> {
    pub fn send(&self, data: T) -> Result<(), SendError<T>> {
        todo!()
    }

    pub fn try_send(&self, data: T) -> Result<(), TrySendError<T>> {
        let Inner {
            send,
            receive,
            num_senders: _,
            slot,
        } = &*self.inner;

        let send = SendStatus::from_bits_retain(send.fetch_add(1, Relaxed));
        if send.contains(SendStatus::RECEIVER_DEAD) {
            return Err(TrySendError::Disconnected(data));
        }
        {
            let reservations = send.reservations();
            if reservations >= SendStatus::ABORT {
                // We would like to use fetch_or, but x86 is a fat pile of shit.
                // Unlike good instructions sets (RISC-V), atomic bit-wise
                // operations in x86 can only use the bit operated upon
                // (by virtue of https://en.wikipedia.org/wiki/Bit_Test instructions)
                // or else face a cmpxchg loop. However, xadd and xsub can atomically return the
                // full previous value.
                //
                // We therefore implement a hack wherein the RESERVE bit is represented as any
                // unknown bits being set. Unfortunately, this will eventually cause us to wrap
                // around leading to the following race:
                // 1. Thread A reserves the slot, but hasn't committed it yet.
                // 2. Thread B gets a wrapped reservation and smashes the slot.
                // If the value is multiple words and you interleave the threads evilly, you
                // can get a torn value. Of course with a u64 this will never happen but eh.
                ran_out_of_address_space();
            }
            if reservations > 0 {
                return Err(TrySendError::Full(data));
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
        let receiver_sleeping = ReceiveStatus::from_bits_retain(receive.load(Relaxed))
            .contains(ReceiveStatus::SLEEPING);
        receive.store(ReceiveStatus::COMMIT.bits(), Release);

        if receiver_sleeping {
            atomic_wake(receive, 1);
        }
        Ok(())
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let Self { inner } = self;
        inner.num_senders.fetch_add(1, Relaxed);
        Self {
            inner: inner.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let Inner {
            send: _,
            receive,
            num_senders,
            slot: _,
        } = &*self.inner;

        if num_senders.fetch_sub(1, Relaxed) > 1 {
            return;
        }

        let receiver_sleeping = ReceiveStatus::from_bits_retain(
            // As noted in my other comment x86 sucks so this will generate a cmpxchg loop, but
            // I guess that's ok since we're in a drop impl.
            receive.fetch_or(ReceiveStatus::SENDER_DEAD.bits(), Relaxed),
        )
        .contains(ReceiveStatus::SLEEPING);
        if receiver_sleeping {
            atomic_wake(receive, u32::MAX);
        }
    }
}

impl<T> Receiver<T> {
    pub fn recv(&self) -> Result<T, RecvError> {
        todo!()
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let Inner {
            send,
            receive,
            num_senders: _,
            slot,
        } = &*self.inner;

        let receive_ = ReceiveStatus::from_bits_retain(receive.load(Acquire));
        if !receive_.contains(ReceiveStatus::COMMIT) {
            if receive_.contains(ReceiveStatus::SENDER_DEAD) {
                return Err(TryRecvError::Disconnected);
            }
            return Err(TryRecvError::Empty);
        }
        receive.fetch_and(ReceiveStatus::SENDER_DEAD.bits(), Relaxed);

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
            receive: _,
            num_senders: _,
            slot: _,
        } = &*self.inner;

        let sender_sleeping =
            SendStatus::from_bits_retain(send.load(Relaxed)).contains(SendStatus::SLEEPING);
        send.fetch_or(SendStatus::RECEIVER_DEAD.bits(), Relaxed);

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
