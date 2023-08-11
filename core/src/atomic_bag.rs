use std::{
    ptr,
    sync::{
        atomic::{
            AtomicPtr, AtomicUsize,
            Ordering::{Relaxed, Release},
        },
        Arc,
    },
    time::Duration,
};

use receiver_impls::{MultipleReceiverImpl, SingleReceiverImpl};
use status::{Block, StatusView, SLEEP_MASK};

pub trait Allocated {
    fn into_ptr(self) -> *mut ();

    unsafe fn from_ptr(ptr: *mut ()) -> Self;
}

pub type SingleReceiver<const N: usize, T> = Receiver<N, T, SingleReceiverImpl<N, T>>;

pub fn mpsc<const N: usize, T: Allocated + Send + 'static>()
-> (Sender<N, T, false>, SingleReceiver<N, T>) {
    todo!()
}

pub type MultipleReceiver<const N: usize, T> = Receiver<N, T, MultipleReceiverImpl>;

pub fn mpmc<const N: usize, T: Allocated + Send + 'static>()
-> (Sender<N, T, true>, MultipleReceiver<N, T>) {
    todo!()
}

pub struct Sender<const N: usize, T, const IS_MULTIPLE_RECEIVER: bool> {
    inner: Arc<Inner<N, T>>,
}

pub struct Receiver<const N: usize, T, Internal: ReceiverImpl<T>> {
    inner: Arc<Inner<N, T>>,
    internal: Internal,
}

struct Inner<const N: usize, T> {
    status: AtomicUsize,
    bag: [AtomicPtr<T>; N],
}

impl<const N: usize, T: Allocated, const IS_MULTIPLE_RECEIVER: bool>
    Sender<N, T, IS_MULTIPLE_RECEIVER>
{
    const _VALIDATE: () = assert!(N < usize::BITS as usize);

    pub fn add(&self, value: T) -> Option<T> {
        let Inner { status, bag } = &*self.inner;
        let status_view = StatusView::new(status.load(Relaxed));

        let Some(Block { index, mask }) = status_view.first_available_block() else {
            return Some(value);
        };
        if (status.fetch_or(mask, Relaxed) & mask) == mask {
            // Somebody else already allocated this block
            // TODO randomly pick one other block to try
            return Some(value);
        }

        if IS_MULTIPLE_RECEIVER {
            if bag[index].swap(value.into_ptr().cast(), Release) != ptr::null_mut() {
                // Protocol:
                // - The receiver always swaps read ptrs with 0.
                // - A set bit in the status implies eventual consistency in the atomic ptr
                //   (i.e. a value will be present).
                // - If the receiver swap/load returns zero, we are between the bit set
                //   instruction and the sender store/swap.
                // - Thus, the receiver swaps a non-zero value into the ptr.
                // - If the sender already performed its store, then the swap will return a
                //   non-zero value and the receiver will undo its swap.
                // - Otherwise, the receiver will still get a zero and will go to sleep.
                // - Thus, if we see a non-zero value we must wake the receiver up.

                // TODO it's also possible to accidentally steal a value (because the receiver
                //  must zero their bit before taking the ptr) so check for specific value
                todo!("wake")
            }
        } else {
            bag[index].store(value.into_ptr().cast(), Release);
        }

        if status_view.is_sleeping() {
            status.fetch_and(!SLEEP_MASK, Relaxed);

            todo!("wake");
        }

        None
    }
}

impl<const N: usize, T: Allocated, I: ReceiverImpl<T>> Receiver<N, T, I> {
    pub fn items(&'_ self, timeout: Duration) -> impl Iterator<Item = T> + '_ {
        let Self { inner, internal } = self;
        let Inner { status, bag } = &**inner;
        let status_view = StatusView::new(status.load(Relaxed));

        if !timeout.is_zero() && status_view.is_empty() {
            todo!("wait");
        }

        status_view.into_iter().map(|Block { index, mask }| {
            let ptr = internal.retrieve_ptr(mask, status, &bag[index]);
            unsafe { T::from_ptr(ptr.cast()) }
        })
    }
}

pub trait ReceiverImpl<T> {
    fn retrieve_ptr(&self, mask: usize, status: &AtomicUsize, ptr: &AtomicPtr<T>) -> *mut T;
}

mod receiver_impls {
    use std::sync::atomic::{AtomicPtr, AtomicUsize};

    use super::ReceiverImpl;

    pub struct SingleReceiverImpl<const N: usize, T> {
        prev_ptrs: [*mut T; N],
    }

    impl<const N: usize, T> ReceiverImpl<T> for SingleReceiverImpl<N, T> {
        fn retrieve_ptr(&self, mask: usize, status: &AtomicUsize, ptr: &AtomicPtr<T>) -> *mut T {
            todo!()
        }
    }

    pub struct MultipleReceiverImpl;

    impl<T> ReceiverImpl<T> for MultipleReceiverImpl {
        fn retrieve_ptr(&self, mask: usize, status: &AtomicUsize, ptr: &AtomicPtr<T>) -> *mut T {
            todo!()
        }
    }
}

mod status {
    #[derive(Copy, Clone, Eq, PartialEq, Debug)]
    pub struct Block {
        pub index: usize,
        pub mask: usize,
    }

    /// \[ occupied ]\[ sleep ] \
    /// --- 63 bits --- 1 bit
    #[derive(Copy, Clone, Eq, PartialEq, Debug)]
    pub struct StatusView(usize);

    pub const SLEEP_MASK: usize = 1;

    impl StatusView {
        pub fn new(status: usize) -> Self {
            Self(status)
        }

        pub fn is_sleeping(self) -> bool {
            (self.0 & SLEEP_MASK) == SLEEP_MASK
        }

        pub fn is_empty(self) -> bool {
            (self.0 & !SLEEP_MASK).count_ones() == 0
        }

        pub fn first_available_block(self) -> Option<Block> {
            let occupied = (self.0 | SLEEP_MASK).leading_ones();
            if occupied == usize::BITS {
                None
            } else {
                Some(Block {
                    index: usize::try_from(occupied).unwrap(),
                    mask: 1 << (usize::BITS - 1 - occupied),
                })
            }
        }
    }

    impl IntoIterator for StatusView {
        type Item = Block;
        type IntoIter = BlockIter;

        fn into_iter(self) -> Self::IntoIter {
            BlockIter(self.0 & !SLEEP_MASK)
        }
    }

    pub struct BlockIter(usize);

    impl Iterator for BlockIter {
        type Item = Block;

        fn next(&mut self) -> Option<Self::Item> {
            let index = self.0.leading_zeros();
            if index == usize::BITS {
                return None;
            }

            let mask = 1 << (usize::BITS - 1 - index);
            self.0 ^= mask;
            Some(Block {
                index: usize::try_from(index).unwrap(),
                mask,
            })
        }
    }

    #[cfg(test)]
    mod test {
        use super::*;

        #[test]
        fn sanity() {
            assert_eq!(StatusView(usize::MAX).first_available_block(), None);
            assert_eq!(StatusView(usize::MAX << 1).first_available_block(), None);
            assert_eq!(
                StatusView(usize::MAX << 2)
                    .first_available_block()
                    .map(|b| b.index),
                Some(62)
            );

            assert_eq!(
                StatusView(0).first_available_block().map(|b| b.index),
                Some(0)
            );
            assert_eq!(
                StatusView(1).first_available_block().map(|b| b.index),
                Some(0)
            );
            assert_eq!(
                StatusView(1 << (usize::BITS - 1))
                    .first_available_block()
                    .map(|b| b.index),
                Some(1)
            );
        }
    }
}
