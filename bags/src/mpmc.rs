use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    num::NonZeroUsize,
    ptr,
    sync::{
        Arc,
        atomic::{
            AtomicU32,
            Ordering::{Acquire, Relaxed, Release},
            fence,
        },
        mpsc::{RecvError, SendError, TryRecvError, TrySendError},
    },
};

use arrayvec::ArrayVec;
use bitflags::bitflags;

use crate::{abort_with_message, atomic_sleep, atomic_wake, cache_padded::CachePadded};

#[must_use]
pub fn mpmc<T>() -> (Sender<30, T>, Receiver<30, T>) {
    let inner = Arc::new(Inner {
        send: CachePadded(AtomicU32::new(Status::default().bits())),
        recv: CachePadded(AtomicU32::new(Status::all().bits())),
        num_senders: AtomicU32::new(1),
        num_receivers: AtomicU32::new(1),
        bag: [const { CachePadded(UnsafeCell::new(MaybeUninit::uninit())) }; _],
    });
    (
        Sender {
            inner: inner.clone(),
            bias: 0,
        },
        Receiver { inner },
    )
}

struct Inner<const N: usize, T> {
    send: CachePadded<AtomicU32>,
    recv: CachePadded<AtomicU32>,
    num_senders: AtomicU32,
    num_receivers: AtomicU32,
    bag: [CachePadded<UnsafeCell<MaybeUninit<T>>>; N],
}

pub struct Sender<const N: usize, T> {
    inner: Arc<Inner<N, T>>,
    bias: u32,
}

pub struct Receiver<const N: usize, T> {
    inner: Arc<Inner<N, T>>,
}

unsafe impl<const N: usize, T: Send> Send for Sender<N, T> {}
unsafe impl<const N: usize, T: Send> Sync for Sender<N, T> {}
unsafe impl<const N: usize, T: Send> Send for Receiver<N, T> {}
unsafe impl<const N: usize, T: Send> Sync for Receiver<N, T> {}

bitflags! {
    #[derive(Copy, Clone, Default, Debug)]
    struct Status: u32 {
        const COUNTERPARTY_DEAD = 1 << 0;
        const SLEEPING = 1 << 1;
    }
}

impl<const N: usize, T> Drop for Inner<N, T> {
    fn drop(&mut self) {
        let Self {
            send: _,
            recv,
            num_senders: _,
            num_receivers: _,
            bag,
        } = self;

        let values = recv.load(Acquire) & !Status::all().bits();
        drain_mask(values, bag, |slot| {
            let slot = slot.get_mut();
            unsafe {
                slot.assume_init_drop();
            }
        });
    }
}

pub trait SendBuffer<T> {
    type Container: SendBufferContainer<T, Remit = Self>;

    fn available_items(self) -> (usize, Self::Container);
}

pub trait SendBufferContainer<T> {
    type Remit;

    fn take(&mut self) -> T;

    fn remit(self) -> Self::Remit;
}

impl<T> SendBuffer<T> for T {
    type Container = Option<T>;

    fn available_items(self) -> (usize, Self::Container) {
        (1, Some(self))
    }
}

impl<T> SendBufferContainer<T> for Option<T> {
    type Remit = T;

    fn take(&mut self) -> T {
        let item = self.take();
        unsafe { item.unwrap_unchecked() }
    }

    fn remit(self) -> Self::Remit {
        unsafe { self.unwrap_unchecked() }
    }
}

impl<T> SendBuffer<T> for &mut Vec<T> {
    type Container = Self;

    fn available_items(self) -> (usize, Self::Container) {
        (self.len(), self)
    }
}

impl<T> SendBufferContainer<T> for &mut Vec<T> {
    type Remit = Self;

    fn take(&mut self) -> T {
        let item = self.pop();
        unsafe { item.unwrap_unchecked() }
    }

    fn remit(self) -> Self::Remit {
        self
    }
}

impl<const N: usize, T> SendBuffer<T> for &mut ArrayVec<T, N> {
    type Container = Self;

    fn available_items(self) -> (usize, Self::Container) {
        (self.len(), self)
    }
}

impl<const N: usize, T> SendBufferContainer<T> for &mut ArrayVec<T, N> {
    type Remit = Self;

    fn take(&mut self) -> T {
        let item = self.pop();
        unsafe { item.unwrap_unchecked() }
    }

    fn remit(self) -> Self::Remit {
        self
    }
}

impl<const N: usize, T> Sender<N, T> {
    const _VALIDATE: () = {
        assert!(
            N < (u32::BITS - Status::all().bits().count_ones()) as usize,
            "Internal limit reached, too many buffers. Consider using a BufferedSender."
        );
        assert!(N > 0, "Use a ::mpsc_slot instead.");
    };

    pub fn send<B: SendBuffer<T>>(
        &self,
        mut data: B,
    ) -> Result<(), SendError<<B::Container as SendBufferContainer<T>>::Remit>> {
        'outer: loop {
            match self.try_send(data) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(data)) => return Err(SendError(data)),
                Err(TrySendError::Full(restored)) => {
                    data = restored;

                    let send = &self.inner.send;
                    let expected = {
                        let mut spin = 32;
                        loop {
                            std::hint::spin_loop();

                            let send = if spin == 0 {
                                send.fetch_or(Status::SLEEPING.bits(), Relaxed)
                                    | Status::SLEEPING.bits()
                            } else {
                                send.load(Relaxed)
                            };
                            if send | Status::all().bits() < u32::MAX {
                                continue 'outer;
                            }

                            let status = Status::from_bits_retain(send);
                            if status.contains(Status::COUNTERPARTY_DEAD) {
                                return Err(SendError(data));
                            }
                            if status.contains(Status::SLEEPING) {
                                break send;
                            }

                            spin -= 1;
                        }
                    };

                    atomic_sleep(send, expected);
                    send.fetch_or(Status::SLEEPING.bits(), Relaxed);
                }
            }
        }
    }

    #[inline]
    pub fn try_send<B: SendBuffer<T>>(
        &self,
        data: B,
    ) -> Result<(), TrySendError<<B::Container as SendBufferContainer<T>>::Remit>> {
        let Inner {
            send,
            recv,
            num_senders: _,
            num_receivers: _,
            bag,
        } = &*self.inner;

        // The process for claiming reservations slots is somewhat complicated because
        // hardware doesn't provide the necessary instructions to implement our goals.
        // See https://alexsaveau.dev/blog/atomic-bit-fill/ for what would be needed.
        //
        // Our goals are to:
        // - Try to preserve flows. Senders try to develop affinities with the smallest
        //   subset of bits possible to opportunistically pair with receivers and avoid
        //   collisions with other senders.
        // - Avoid overcommit without performing too many atomic writes. We could
        //   fetch_or the entire atomic word with 1s which would guarantee finding a
        //   free slot if it's available, but that would also exclude every other sender
        //   until we release the extra bits we've claimed.
        //
        // To try and accomplish these goals, we assign a fixed bias to each sender
        // (an adaptive bias would be better, but that seems like a pain to paper over
        // the missing hardware capabilities). We start by trying to claim bits starting
        // at the bias. If this fails, then we look at the returned free bits and
        // try to claim those, repeating some number of times. We never request more
        // bits than can be sent to avoid the do->undo problem described above—extra
        // bits are trimmed from the left or right depending on the parity of the bias.
        //
        // Additional complexity:
        // - Our reservation bits share the same word as the status bits which means we
        //   have to be careful to shift the request mask over the status bits.
        // - The sender proposes items for us to send, but we consider any positive
        //   number of items sent to be a success.
        //
        // Currently, receivers don't actually support claiming fewer than the entire
        // word's worth of bits making the affinitization somewhat moot, but you still
        // get sender contention avoidance plus future proofing.
        let (items, mut data) = data.available_items();
        let reserved = {
            let Some(items) = NonZeroUsize::new(items) else {
                return Ok(());
            };

            let mut aligned_request_mask = compute_request_mask::<N>(items, self.bias);
            debug_assert_eq!(0, aligned_request_mask & Status::all().bits());

            loop {
                let prev = send.fetch_or(aligned_request_mask, Relaxed);

                if Status::from_bits_retain(prev).contains(Status::COUNTERPARTY_DEAD) {
                    return Err(TrySendError::Disconnected(data.remit()));
                }

                let slots = prev | Status::all().bits();
                if slots == u32::MAX {
                    return Err(TrySendError::Full(data.remit()));
                }

                let reserved = !slots & aligned_request_mask;
                if reserved > 0 {
                    break reserved;
                }

                aligned_request_mask = clear_extra_bits(!slots, items, self.bias.is_multiple_of(2));
                std::hint::spin_loop();
            }
        };
        debug_assert!(reserved > 0);
        debug_assert!(usize::try_from(reserved.count_ones()).unwrap() <= items);

        fence(Acquire);
        drain_mask(reserved, bag, |slot| {
            let slot = slot.get();
            let value = MaybeUninit::new(data.take());
            unsafe {
                ptr::write(slot, value);
            }
        });

        let receiver_sleeping = Status::from_bits_retain({
            // Since we use fetch_or, the Status bits are actually inverted. 1 means "off"
            // and conversely 0 means "on".
            let commit = reserved | Status::all().bits();
            !if cfg!(target_arch = "x86_64") && commit == u32::MAX {
                // x86 optimization to avoid a cmpxchg loop in fetch_or
                recv.swap(commit, Release)
            } else {
                recv.fetch_or(commit, Release)
            }
        })
        .contains(Status::SLEEPING);

        if receiver_sleeping {
            atomic_wake(recv, 1);
        }
        Ok(())
    }
}

impl<const N: usize, T> Clone for Sender<N, T> {
    fn clone(&self) -> Self {
        let Self { inner, bias: _ } = self;

        let prev_senders = inner.num_senders.fetch_add(1, Relaxed);
        if prev_senders > u32::MAX / 2 {
            abort_with_message("Too many senders, aborting.");
        }
        // 7 was chosen as the smallest prime that doesn't gcd with 30 so that we get a
        // nice cyclic bias. You can break this by making a bunch of senders and then
        // drop->clone-ing them in sequence so that prev_senders is the same each time,
        // but whatever.
        const { assert!(N == 30) }
        let bias = (prev_senders * 7) % (u32::try_from(N).unwrap());

        Self {
            inner: inner.clone(),
            bias,
        }
    }
}

impl<const N: usize, T> Drop for Sender<N, T> {
    fn drop(&mut self) {
        let Inner {
            send: _,
            recv,
            num_senders,
            num_receivers: _,
            bag: _,
        } = &*self.inner;

        if num_senders.fetch_sub(1, Relaxed) > 1 {
            return;
        }

        let receiver_sleeping =
            Status::from_bits_retain(!recv.fetch_and(!Status::COUNTERPARTY_DEAD.bits(), Relaxed))
                .contains(Status::SLEEPING);
        if receiver_sleeping {
            atomic_wake(recv, u32::MAX);
        }
    }
}

impl<const N: usize, T> Receiver<N, T> {
    pub fn recv(&self) -> Result<ArrayVec<T, N>, RecvError> {
        'outer: loop {
            match self.try_recv() {
                Ok(data) => return Ok(data),
                Err(TryRecvError::Disconnected) => return Err(RecvError),
                Err(TryRecvError::Empty) => {
                    let recv = &self.inner.recv;
                    let expected = {
                        let mut spin = 32;
                        loop {
                            std::hint::spin_loop();

                            let recv = if spin == 0 {
                                recv.fetch_and(!Status::SLEEPING.bits(), Relaxed)
                                    & !Status::SLEEPING.bits()
                            } else {
                                recv.load(Relaxed)
                            };
                            if recv & !Status::all().bits() > 0 {
                                continue 'outer;
                            }

                            let status = Status::from_bits_retain(!recv);
                            if status.contains(Status::COUNTERPARTY_DEAD) {
                                return Err(RecvError);
                            }
                            if status.contains(Status::SLEEPING) {
                                break recv;
                            }

                            spin -= 1;
                        }
                    };

                    atomic_sleep(recv, expected);
                    recv.fetch_and(!Status::SLEEPING.bits(), Relaxed);
                }
            }
        }
    }

    #[inline]
    pub fn try_recv(&self) -> Result<ArrayVec<T, N>, TryRecvError> {
        let Inner {
            send,
            recv,
            num_senders: _,
            num_receivers: _,
            bag,
        } = &*self.inner;

        // Blindly take as many items as we can. The theory here is that the more items
        // there are, the less receivers are keeping up which means they should increase
        // their batching to improve performance by avoiding any unnecessary contention.
        // Conversely, the fewer items there are the better receivers are keeping up,
        // but they also take fewer items which means they share more of the load with
        // other receivers.
        //
        // TL;DR: simply taking all the items seems to have good control theory.
        //
        // The obvious counter-example is sending a small number of very large tasks all
        // at once. Now one receiver will take them all and leave nothing for other
        // receivers. In the Lockness Executor, this is handled by workers giving back
        // their tasks when they see suboptimal parallelism. The latency cost is the
        // processing time of one item before realizing that the rest should be given
        // back.
        let claimed = {
            let prev = recv.fetch_and(Status::all().bits(), Acquire);
            let claimed = prev & !Status::all().bits();
            if claimed == 0 {
                let status = Status::from_bits_retain(!prev);
                if status.contains(Status::COUNTERPARTY_DEAD) {
                    return Err(TryRecvError::Disconnected);
                }
                return Err(TryRecvError::Empty);
            }
            claimed
        };

        let mut values = ArrayVec::new_const();
        debug_assert!(claimed.count_ones() as usize <= N);
        debug_assert!(claimed > 0);
        drain_mask(claimed, bag, |slot| {
            let slot = slot.get();
            let value = unsafe { ptr::read(slot).assume_init() };
            if cfg!(debug_assertions) {
                values.push(value);
            } else {
                unsafe {
                    values.push_unchecked(value);
                }
            }
        });
        fence(Release);

        let sender_sleeping = Status::from_bits_retain({
            let commit = !claimed & !Status::all().bits();
            if cfg!(target_arch = "x86_64") && commit == 0 {
                // x86 optimization to avoid a cmpxchg loop in fetch_or
                send.swap(commit, Relaxed)
            } else {
                send.fetch_and(commit, Relaxed)
            }
        })
        .contains(Status::SLEEPING);

        if sender_sleeping {
            atomic_wake(send, 1);
        }

        Ok(values)
    }
}

impl<const N: usize, T> Clone for Receiver<N, T> {
    fn clone(&self) -> Self {
        let Self { inner } = self;
        if inner.num_receivers.fetch_add(1, Relaxed) > u32::MAX / 2 {
            abort_with_message("Too many receivers, aborting.");
        }
        Self {
            inner: inner.clone(),
        }
    }
}

impl<const N: usize, T> Drop for Receiver<N, T> {
    fn drop(&mut self) {
        let Inner {
            send,
            recv: _,
            num_senders: _,
            num_receivers,
            bag: _,
        } = &*self.inner;

        if num_receivers.fetch_sub(1, Relaxed) > 1 {
            return;
        }

        let sender_sleeping =
            Status::from_bits_retain(send.fetch_or(Status::COUNTERPARTY_DEAD.bits(), Relaxed))
                .contains(Status::SLEEPING);
        if sender_sleeping {
            atomic_wake(send, u32::MAX);
        }
    }
}

trait DrainMaskBuf<const N: usize, T, F> {
    fn do_(&mut self, f: &mut F, i: usize);
}

impl<const N: usize, T, F: FnMut(&T)> DrainMaskBuf<N, T, F> for &[CachePadded<T>; N] {
    fn do_(&mut self, f: &mut F, i: usize) {
        f(&self[i]);
    }
}

impl<const N: usize, T, F: FnMut(&mut T)> DrainMaskBuf<N, T, F> for &mut [CachePadded<T>; N] {
    fn do_(&mut self, f: &mut F, i: usize) {
        f(&mut self[i]);
    }
}

#[inline]
fn drain_mask<const N: usize, T, F, Buf: DrainMaskBuf<N, T, F>>(
    mut mask: u32,
    mut bag: Buf,
    mut f: F,
) {
    loop {
        let i = mask.leading_zeros();
        {
            let i = usize::try_from(i).unwrap();
            if i >= N {
                break;
            }

            bag.do_(&mut f, i);
        }
        mask &= !(1 << (u32::BITS - i - 1));
    }
}

#[allow(clippy::cast_sign_loss)]
fn compute_request_mask<const N: usize>(available_items: NonZeroUsize, bias: u32) -> u32 {
    debug_assert!((bias as usize) < N);

    let available = available_items.get().min(N);
    let high_bit: i32 = const { 1 << (u32::BITS - 1) };
    let mask = (high_bit >> (available - 1)) as u32;
    if bias == 0 {
        mask
    } else {
        let num_status_bits = const { Status::all().bits().count_ones() };
        let overstepped = mask.rotate_right(bias + num_status_bits);
        (overstepped & (high_bit >> (bias - 1)) as u32) | (overstepped << num_status_bits)
    }
}

fn clear_extra_bits(mut mask: u32, max: NonZeroUsize, direction: bool) -> u32 {
    let mut set_bits = usize::try_from(mask.count_ones()).unwrap();
    if direction {
        mask = mask.reverse_bits();
    }
    while set_bits > max.get() {
        mask &= mask - 1;
        set_bits -= 1;
    }
    if direction {
        mask = mask.reverse_bits();
    }
    mask
}

#[cfg(test)]
mod tests {
    use std::{
        fmt::Write,
        num::NonZeroUsize,
        sync::mpsc::{RecvError, SendError, TryRecvError, TrySendError},
        thread,
    };

    use arrayvec::ArrayVec;
    use expect_test::expect;
    use rstest::rstest;

    use crate::{MpmcReceiver, MpmcSender, mpmc, mpmc::compute_request_mask};

    #[test]
    fn sane_request_mask() {
        let mut actual = String::new();

        let mut r = |i, bias| {
            let result = compute_request_mask::<30>(NonZeroUsize::new(i).unwrap(), bias);
            writeln!(
                actual,
                "compute_request_mask(items={i:#2}, bias={bias:#2})={result:#034b}"
            )
            .unwrap();
            assert!(i >= result.count_ones() as usize);
        };

        r(1, 0);
        r(1, 1);
        r(1, 2);
        r(1, 5);
        r(1, 29);
        r(30, 0);
        r(30, 17);
        r(30, 29);
        r(7, 7);
        r(7, 27);

        let expected = expect![[r#"
            compute_request_mask(items= 1, bias= 0)=0b10000000000000000000000000000000
            compute_request_mask(items= 1, bias= 1)=0b01000000000000000000000000000000
            compute_request_mask(items= 1, bias= 2)=0b00100000000000000000000000000000
            compute_request_mask(items= 1, bias= 5)=0b00000100000000000000000000000000
            compute_request_mask(items= 1, bias=29)=0b00000000000000000000000000000100
            compute_request_mask(items=30, bias= 0)=0b11111111111111111111111111111100
            compute_request_mask(items=30, bias=17)=0b11111111111111111111111111111100
            compute_request_mask(items=30, bias=29)=0b11111111111111111111111111111100
            compute_request_mask(items= 7, bias= 7)=0b00000001111111000000000000000000
            compute_request_mask(items= 7, bias=27)=0b11110000000000000000000000011100
        "#]];
        expected.assert_eq(&actual);
    }

    type SendFn<T> = fn(&MpmcSender<T>, T) -> Result<(), SendError<T>>;
    type RecvFn<T> = fn(&MpmcReceiver<T>) -> Result<ArrayVec<T, 30>, RecvError>;

    fn send_polyfill<T>(sender: &MpmcSender<T>, mut value: T) -> Result<(), SendError<T>> {
        loop {
            match sender.try_send(value) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(t)) => return Err(SendError(t)),
                Err(TrySendError::Full(t)) => value = t,
            }
        }
    }

    fn recv_polyfill<T>(receiver: &MpmcReceiver<T>) -> Result<ArrayVec<T, 30>, RecvError> {
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
    #[case(MpmcSender::<Box<i32>>::send, MpmcReceiver::<Box<i32>>::recv)]
    fn drops(#[case] send: SendFn<Box<i32>>, #[case] recv: RecvFn<Box<i32>>) {
        {
            let (_sender, _receiver) = mpmc::<Box<i32>>();
        }

        {
            let (sender, _receiver) = mpmc::<Box<i32>>();
            send(&sender, Box::new(42)).unwrap();
        }

        {
            let (sender, receiver) = mpmc::<Box<i32>>();
            drop(receiver);
            send(&sender, Box::new(42)).unwrap_err();
        }

        {
            let (sender, receiver) = mpmc::<Box<i32>>();
            send(&sender, Box::new(42)).unwrap();
            drop(receiver);
            send(&sender, Box::new(666)).unwrap_err();
        }

        {
            let (sender, receiver) = mpmc::<Box<i32>>();
            drop(sender);
            recv(&receiver).unwrap_err();
        }

        {
            let (sender, receiver) = mpmc::<Box<i32>>();
            send(&sender, Box::new(666)).unwrap();
            drop(sender);

            let value = *recv(&receiver).unwrap().pop().unwrap();
            assert_eq!(666, value);
            recv(&receiver).unwrap_err();
        }
    }

    #[test]
    fn drops_multi() {
        let (sender, receiver) = mpmc::<Box<i32>>();
        for i in 0..30 {
            let sender = sender.clone();
            sender.send(Box::new(i)).unwrap();
        }
        sender.try_send(Box::new(666)).unwrap_err();

        {
            let mut nums = receiver
                .clone()
                .recv()
                .unwrap()
                .into_iter()
                .map(|n| *n)
                .collect::<Vec<_>>();
            nums.sort_unstable();
            let expected = (0..30).collect::<Vec<_>>();
            assert_eq!(&expected, &nums);
        }
        receiver.try_recv().unwrap_err();
        drop(sender);
        receiver.recv().unwrap_err();
    }

    #[rstest]
    #[case(send_polyfill::<Box<usize>>, recv_polyfill::<Box<usize>>)]
    #[case(MpmcSender::<Box<usize>>::send, MpmcReceiver::<Box<usize>>::recv)]
    fn basic(#[case] send: SendFn<Box<usize>>, #[case] recv: RecvFn<Box<usize>>) {
        let (sender, receiver) = mpmc();
        let senders = (0..3)
            .map(|_| {
                thread::spawn({
                    let sender = sender.clone();
                    move || {
                        for i in 0..1000 {
                            send(&sender, Box::new(i)).unwrap()
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        drop(sender);
        thread::spawn(move || {
            let mut counts = [0; 1000];
            while let Ok(mut items) = recv(&receiver) {
                for i in items.drain(..) {
                    counts[*i] += 1;
                }
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
    #[case(MpmcSender::<Vec<i32>>::send, MpmcReceiver::<Vec<i32>>::recv)]
    fn ping_pong(#[case] send: SendFn<Vec<i32>>, #[case] recv: RecvFn<Vec<i32>>) {
        let (ping_sender, pong_receiver) = mpmc();
        let (pong_sender, ping_receiver) = mpmc();

        let ping = thread::spawn(move || {
            let mut data = Vec::new();
            for i in 0..1000 {
                data.push(i);
                send(&ping_sender, data).unwrap();
                data = recv(&ping_receiver).unwrap().pop().unwrap();
            }
            data
        });
        let pong = thread::spawn(move || {
            while let Ok(mut data) = recv(&pong_receiver) {
                let mut data = data.pop().unwrap();
                data.push(*data.last().unwrap());
                send(&pong_sender, data).unwrap();
            }
        });

        let actual = ping.join().unwrap();
        let () = pong.join().unwrap();

        let expected: Vec<_> = (0..1000).flat_map(|i| [i, i]).collect();

        assert_eq!(expected, actual);
    }

    #[rstest]
    #[case(send_polyfill::<Box<i32>>, recv_polyfill::<Box<i32>>)]
    #[case(MpmcSender::<Box<i32>>::send, MpmcReceiver::<Box<i32>>::recv)]
    fn take_one_down_pass_it_around(
        #[case] send: SendFn<Box<i32>>,
        #[case] recv: RecvFn<Box<i32>>,
    ) {
        let (sender, receiver) = mpmc();

        let mut writers = Vec::with_capacity(3);
        let mut readers = Vec::with_capacity(3);

        for n in 0..3 {
            writers.push(thread::spawn({
                let sender = sender.clone();
                move || {
                    for i in (0..3000).filter(|i| i % 3 == n) {
                        send(&sender, Box::new(i)).unwrap();
                    }
                }
            }));
            readers.push(thread::spawn({
                let receiver = receiver.clone();
                move || {
                    let mut all = Vec::new();
                    loop {
                        match recv(&receiver) {
                            Ok(nums) => {
                                for num in nums {
                                    all.push(num);
                                }
                            }
                            Err(RecvError) => break all,
                        }
                    }
                }
            }));
        }
        drop(sender);
        drop(receiver);

        for writer in writers {
            let () = writer.join().unwrap();
        }

        let mut nums = Vec::with_capacity(3000);
        for reader in readers {
            nums.append(&mut reader.join().unwrap());
        }
        nums.sort_unstable();
        let nums = nums.into_iter().map(|n| *n).collect::<Vec<_>>();

        let expected: Vec<_> = (0..3000).collect();

        assert_eq!(expected, nums);
    }
}
