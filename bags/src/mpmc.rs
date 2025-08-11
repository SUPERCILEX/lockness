use std::{
    cell::UnsafeCell,
    marker::PhantomData,
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

use crate::{abort_with_message, atomic_wake, cache_padded::CachePadded};

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
        drain_mask(values, (bag, PhantomData), |slot| {
            let slot = slot.get_mut();
            unsafe {
                slot.assume_init_drop();
            }
        });
    }
}

trait SendBuffer<T> {
    type Remit;

    fn available_items(&self) -> NonZeroUsize;
    fn take(&mut self) -> T;

    fn remit(self) -> Self::Remit;
}

impl<const N: usize, T> Sender<N, T> {
    const _VALIDATE: () = {
        assert!(
            N < (u32::BITS - Status::all().bits().count_ones()) as usize,
            "Internal limit reached, too many buffers. Consider using a BufferedSender."
        );
        assert!(N > 0, "Use a ::mpsc_slot instead.");
    };

    pub fn send<I: ExactSizeIterator>(
        &self,
        data: impl IntoIterator<Item = T, IntoIter = I>,
    ) -> Result<(), SendError<impl IntoIterator<Item = T>>> {
        todo!();
        Err(SendError([]))
    }

    #[inline]
    pub fn try_send<B: SendBuffer<T>>(&self, mut data: B) -> Result<(), TrySendError<B::Remit>> {
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
        let reserved = {
            let items = data.available_items();
            let mut aligned_request_mask = compute_request_mask::<N>(items, self.bias);
            debug_assert_eq!(0, aligned_request_mask & Status::all().bits());

            let mut remaining_attempts = 32;
            loop {
                let prev = send.fetch_or(aligned_request_mask, Relaxed);

                if Status::from_bits_retain(prev).contains(Status::COUNTERPARTY_DEAD) {
                    return Err(TrySendError::Disconnected(data.remit()));
                }

                let slots = prev | Status::all().bits();
                if slots.count_zeros() == 0 {
                    return Err(TrySendError::Full(data.remit()));
                }

                let reserved = !slots & aligned_request_mask;
                if reserved.count_ones() > 0 {
                    break reserved;
                }
                if remaining_attempts == 0 {
                    return Err(TrySendError::Full(data.remit()));
                }
                aligned_request_mask = clear_extra_bits(!slots, items, self.bias.is_multiple_of(2));
                remaining_attempts -= 1;
                std::hint::spin_loop();
            }
        };
        debug_assert!(reserved > 0);
        debug_assert!(
            usize::try_from(reserved.count_ones()).unwrap() <= data.available_items().get()
        );

        fence(Acquire);
        drain_mask(reserved, (bag, PhantomData), |slot| {
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
            send,
            recv,
            num_senders,
            num_receivers: _,
            bag,
        } = &*self.inner;

        if num_senders.fetch_sub(1, Relaxed) > 1 {
            return;
        }

        todo!()
    }
}

impl<const N: usize, T> Receiver<N, T> {
    pub fn recv(&self) -> Result<ArrayVec<T, N>, RecvError> {
        todo!()
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
            if claimed.count_ones() == 0 {
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
        drain_mask(claimed, (bag, PhantomData), |slot| {
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
            recv,
            num_senders: _,
            num_receivers,
            bag,
        } = &*self.inner;

        if num_receivers.fetch_sub(1, Relaxed) > 1 {
            return;
        }

        todo!()
    }
}

trait DrainMaskBuf<const N: usize, T> {
    type F;

    fn do_(&mut self, f: &mut Self::F, i: usize);
}

impl<const N: usize, T, F: FnMut(&T)> DrainMaskBuf<N, T>
    for (&[CachePadded<T>; N], PhantomData<F>)
{
    type F = F;

    fn do_(&mut self, f: &mut F, i: usize) {
        let (bag, _) = self;
        f(&bag[i]);
    }
}

impl<const N: usize, T, F: FnMut(&mut T)> DrainMaskBuf<N, T>
    for (&mut [CachePadded<T>; N], PhantomData<F>)
{
    type F = F;

    fn do_(&mut self, f: &mut F, i: usize) {
        let (bag, _) = self;
        f(&mut bag[i]);
    }
}

#[inline]
fn drain_mask<const N: usize, T, Buf: DrainMaskBuf<N, T>>(
    mut mask: u32,
    mut bag: Buf,
    mut f: Buf::F,
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
    use std::{fmt::Write, num::NonZeroUsize};

    use expect_test::expect;

    use crate::mpmc::compute_request_mask;

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
}
