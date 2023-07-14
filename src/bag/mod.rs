use std::{
    cmp::max,
    marker::PhantomData,
    mem::size_of,
    slice,
    sync::{
        atomic::{AtomicPtr, AtomicU32},
        Arc,
    },
};

use crate::{
    bag::{block::TaskBlock, status::StatusView},
    cache_line_size::CACHE_LINE_SIZE,
};

mod block;

pub struct ThreadTaskRoot<T: Send> {
    data: Arc<[u8]>,
    _items: PhantomData<T>,
}

struct ThreadTaskRootView<'a, T: Send> {
    task_blocks: &'a [AtomicPtr<TaskBlock<T>>],
    status: &'a AtomicU32,
}

impl<'a, T: Send> From<&'a ThreadTaskRoot<T>> for ThreadTaskRootView<'a, T> {
    fn from(ThreadTaskRoot { data, _items: _ }: &'a ThreadTaskRoot<T>) -> Self {
        let cap = ThreadTaskRoot::<T>::capacity();

        // TODO remove bounds checking if present
        let task_blocks_ptr = data[..cap].as_ptr().cast::<AtomicPtr<TaskBlock<T>>>();
        let status_ptr = data[cap..].as_ptr().cast::<AtomicU32>();

        Self {
            task_blocks: unsafe { slice::from_raw_parts(task_blocks_ptr, cap) },
            status: unsafe { &*status_ptr },
        }
    }
}

macro_rules! num_blocks {
    () => {
        15
    };
}

impl<T: Send> ThreadTaskRoot<T> {
    fn new() -> Self {
        let data = {
            let data = Arc::<[u8]>::new_zeroed_slice(max(
                Self::capacity()
                    // Bit vector
                    + size_of::<AtomicU32>(),
                CACHE_LINE_SIZE,
            ));

            let mut data = unsafe { data.assume_init() };
            unsafe {
                *(Arc::get_mut_unchecked(&mut data)[Self::capacity()..]
                    .as_mut_ptr()
                    .cast::<u32>()) = StatusView::init();
            }
            data
        };
        Self {
            data,
            _items: PhantomData,
        }
    }

    const fn capacity() -> usize {
        num_blocks!() * size_of::<AtomicPtr<TaskBlock<T>>>()
    }

    // fn append(&self, value: T) {
    //     let view = ThreadTaskRootView::from(self);
    //
    //     let status = StatusView(view.status.load(Ordering::Relaxed));
    //     if let Some(block_index) = status.best_available_block() {}
    // }

    // fn next(&self) -> Option<Task> {
    //     let ThreadTaskRootView {
    //         head,
    //         task_blocks,
    //         data_len,
    //     } = self.into();
    //
    //     let current = head.load(Ordering::Relaxed);
    //     let block = &task_blocks[current];
    //     let block_ptr = block.swap(null_mut(), Ordering::Acquire);
    //     if block_ptr.is_null() {
    //         return None;
    //     }
    //
    //     let (task, is_block_empty) = {
    //         let block_view = unsafe { &mut *block_ptr };
    //         (block_view.pop(), block_view.is_empty())
    //     };
    //
    //     if is_block_empty {
    //         drop(TaskBlock::from_raw(block_ptr));
    //
    //         // If somebody else already moved the head pointer along, that's fine
    //         let _ = head.compare_exchange(
    //             current,
    //             (current + 1) % data_len,
    //             Ordering::Relaxed,
    //             Ordering::Relaxed,
    //         );
    //     } else {
    //         let old_block_ptr = block.swap(block_ptr, Ordering::Release);
    //         if !old_block_ptr.is_null() {
    //             // This means someone filled in a new block while we were
    //             // looking at the current one
    //             // overflow_tasks::extend(TaskBlock::from_raw(old_block_ptr));
    //         }
    //     }
    //
    //     task
    // }
}

mod status {
    /// \[  empty   ]\[   full  ]\[ sleep ]\[ unused ] \
    /// --- 15 bits --- 15 bits --- 1 bit --- 1 bit
    #[derive(Copy, Clone)]
    pub struct StatusView(u32);

    const BLOCKS_MASK: u32 = (1 << num_blocks!()) - 1;

    impl StatusView {
        pub fn init() -> u32 {
            BLOCKS_MASK << (u32::BITS - num_blocks!())
        }

        fn empty_blocks(self) -> u32 {
            self.0 & (BLOCKS_MASK << (u32::BITS - num_blocks!()))
        }

        fn full_blocks(self) -> u32 {
            (self.0 & (BLOCKS_MASK << (u32::BITS - 2 * num_blocks!()))) << num_blocks!()
        }

        fn next_block(self, f: impl FnOnce(Self) -> u32) -> Option<u32> {
            let index = f(self).leading_zeros();
            if index < num_blocks!() {
                Some(index)
            } else {
                None
            }
        }

        fn next_empty_block(self) -> Option<u32> {
            self.next_block(Self::empty_blocks)
        }

        fn next_full_block(self) -> Option<u32> {
            self.next_block(Self::full_blocks)
        }

        fn next_non_full_block(self) -> Option<u32> {
            self.next_block(|status| !status.full_blocks())
        }

        fn best_available_block_for_testing(self) -> Option<u32> {
            self.next_empty_block()
                .or_else(|| self.next_non_full_block())
        }

        pub fn best_available_block(self) -> Option<u32> {
            let zeros = (self.0 ^ (BLOCKS_MASK << (u32::BITS - 2 * num_blocks!()))).leading_zeros();
            if zeros < num_blocks!() {
                Some(zeros)
            } else if zeros < 2 * num_blocks!() {
                Some(zeros - num_blocks!())
            } else {
                None
            }
        }
    }

    #[cfg(test)]
    mod test {
        use super::*;

        #[test]
        fn sanity() {
            assert_eq!(StatusView(0xFF_FE_00_00).empty_blocks(), 0xFF_FE_00_00);
            assert_eq!(
                StatusView(0xFF_FE_00_00 >> num_blocks!()).full_blocks(),
                0xFF_FE_00_00
            );
            assert_eq!(StatusView(u32::MAX).empty_blocks(), 0xFF_FE_00_00);
            assert_eq!(StatusView(u32::MAX).full_blocks(), 0xFF_FE_00_00);

            assert_eq!(StatusView((1 << 31) >> 4).next_empty_block(), Some(4));
            assert_eq!(
                StatusView((1 << 31) >> (4 + num_blocks!())).next_full_block(),
                Some(4)
            );

            assert_eq!(StatusView((1 << 31) >> 4).next_full_block(), None);
            assert_eq!(
                StatusView((1 << 31) >> (4 + num_blocks!())).next_empty_block(),
                None
            );

            assert_eq!(
                StatusView(0xFF_FE_00_00 >> num_blocks!()).best_available_block(),
                None
            );
            assert_eq!(StatusView(0x01_00_00_00).best_available_block(), Some(7));
            assert_eq!(StatusView(0x00_01_00_00).best_available_block(), Some(1));
        }

        #[test]
        #[cfg(not(debug_assertions))]
        fn exhaustive() {
            for n in 0..=u32::MAX {
                assert_eq!(
                    StatusView(n).best_available_block_for_testing(),
                    StatusView(n).best_available_block()
                );
            }
        }
    }
}
