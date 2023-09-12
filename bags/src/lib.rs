#![feature(strict_provenance)]

use std::ptr::NonNull;

pub use slot::mpsc as mpsc_slot;

mod bags;
mod slot;

pub trait Allocated {
    fn into_ptr(self) -> NonNull<()>;

    unsafe fn from_ptr(ptr: NonNull<()>) -> Self;
}

#[cfg(test)]
mod test {
    use std::ops::Deref;

    use super::*;

    #[derive(Eq, PartialEq, Debug)]
    pub struct Boxed(Box<usize>);

    impl Boxed {
        pub fn new(n: usize) -> Self {
            Self(Box::new(n))
        }
    }

    impl Deref for Boxed {
        type Target = usize;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl Allocated for Boxed {
        fn into_ptr(self) -> NonNull<()> {
            let ptr = Box::into_raw(self.0);
            unsafe { NonNull::new_unchecked(ptr).cast() }
        }

        unsafe fn from_ptr(ptr: NonNull<()>) -> Self {
            let ptr = ptr.cast().as_ptr();
            Self(unsafe { Box::from_raw(ptr) })
        }
    }
}
