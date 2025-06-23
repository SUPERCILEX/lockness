use std::{
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{
            AtomicPtr, AtomicUsize,
            Ordering::{Relaxed, Release},
        },
        mpsc::{RecvError, SendError},
    },
    time::Duration,
};

use arrayvec::ArrayVec;
use status::Block;

use crate::cache_padded::CachePadded;

#[must_use]
pub fn mpsc<const NUM_BUFFERS: usize, T>() -> (Sender<NUM_BUFFERS, T>, Receiver<NUM_BUFFERS, T>) {
    todo!()
}

struct Inner<const N: usize, T> {
    pending: CachePadded<AtomicUsize>,
    committed: CachePadded<AtomicUsize>,
    bag: [CachePadded<T>; N],
}

pub struct Sender<const N: usize, T> {
    inner: Arc<Inner<N, T>>,
}

pub struct Receiver<const N: usize, T> {
    inner: Arc<Inner<N, T>>,
}

impl<const N: usize, T> Clone for Sender<N, T> {
    fn clone(&self) -> Self {
        let Self { inner } = self;
        Self {
            inner: inner.clone(),
        }
    }
}

impl<const N: usize, T> Sender<N, T> {
    const _VALIDATE: () = {
        assert!(
            N < usize::BITS as usize,
            "Internal limit reached, too many buffers. Consider using a BufferedSender."
        );
        assert!(N > 0, "Use a ::mpsc_tunnel instead.");
        assert!(
            size_of::<T>() > 0,
            "Passing a ZST between cores doesn't make much sense. Please open a GitHub issue with \
             your use case."
        )
    };

    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        todo!()
    }
}

impl<const N: usize, T> Receiver<N, T> {
    pub fn recv(&self) -> Result<ArrayVec<T, N>, RecvError> {
        todo!()
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
    pub struct View(usize);

    const SLEEP_MASK: usize = 1;

    impl View {
        pub const fn new(status: usize) -> Self {
            Self(status)
        }

        pub const fn is_sleeping(self) -> bool {
            (self.0 & SLEEP_MASK) == SLEEP_MASK
        }

        pub const fn is_empty(self) -> bool {
            (self.0 & !SLEEP_MASK).count_ones() == 0
        }

        // TODO use max size
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

    impl IntoIterator for View {
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
            assert_eq!(View(usize::MAX).first_available_block(), None);
            assert_eq!(View(usize::MAX << 1).first_available_block(), None);
            assert_eq!(
                View(usize::MAX << 2)
                    .first_available_block()
                    .map(|b| b.index),
                Some(62)
            );

            assert_eq!(View(0).first_available_block().map(|b| b.index), Some(0));
            assert_eq!(View(1).first_available_block().map(|b| b.index), Some(0));
            assert_eq!(
                View(1 << (usize::BITS - 1))
                    .first_available_block()
                    .map(|b| b.index),
                Some(1)
            );
        }
    }
}
