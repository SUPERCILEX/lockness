use std::mem::{size_of, MaybeUninit};

use crate::cache_line_size::CACHE_LINE_SIZE;

pub struct TaskBlock<T: Send> {
    // TODO(https://github.com/rust-lang/rust/issues/76560): should be size_of::<T>()
    tasks: [MaybeUninit<T>; (CACHE_LINE_SIZE - 1) / size_of::<*const ()>()],
    count: usize,
}

impl<T: Send> TaskBlock<T> {
    pub fn new() -> Box<Self> {
        Box::new(Self {
            tasks: [const { MaybeUninit::uninit() }; _],
            count: 0,
        })
    }

    pub fn from_raw(this: *mut Self) -> Box<Self> {
        unsafe { Box::from_raw(this) }
    }

    pub const fn capacity() -> usize {
        // TODO(https://github.com/rust-lang/rust/issues/76560): should be size_of::<T>()
        (CACHE_LINE_SIZE - 1) / size_of::<*const ()>()
    }

    pub fn push(&mut self, value: T) -> Option<T> {
        let Self { tasks, count } = self;

        if *count == Self::capacity() {
            return Some(value);
        }

        tasks[*count] = MaybeUninit::new(value);
        *count += 1;

        None
    }

    pub fn pop(&mut self) -> Option<T> {
        self.next()
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

impl<T: Send> Iterator for TaskBlock<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let Self { tasks, count } = self;
        if *count == 0 {
            return None;
        }

        *count -= 1;
        Some(unsafe { tasks[*count].assume_init_read() })
    }
}

impl<T: Send> Drop for TaskBlock<T> {
    fn drop(&mut self) {
        self.for_each(drop);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fill_drain() {
        let mut tasks = TaskBlock::new();

        let mut i = 0;
        while tasks.push(i).is_none() {
            i += 1;
        }

        while let Some(j) = tasks.pop() {
            i -= 1;
            assert_eq!(i, j);
        }

        assert!(tasks.is_empty());
    }

    #[test]
    fn iter() {
        let mut tasks = TaskBlock::new();

        let mut i = 0;
        while tasks.push(i).is_none() {
            i += 1;
        }

        assert_eq!(tasks.collect::<Vec<_>>(), (0..i).rev().collect::<Vec<_>>());
    }
}
