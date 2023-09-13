#![feature(strict_provenance)]
#![feature(ptr_from_ref)]
#![feature(pointer_byte_offsets)]
#![feature(atomic_from_ptr)]

use std::{mem, ptr, ptr::NonNull, sync::atomic::AtomicU32, time::Instant};

use rustix::thread::{futex, FutexFlags, FutexOperation, Timespec};
pub use slot::{mpsc as mpsc_slot, Receiver as SlotReceiver, Sender as SlotSender};

mod bags;
mod slot;

pub trait Allocated {
    fn into_ptr(self) -> NonNull<()>;

    unsafe fn from_ptr(ptr: NonNull<()>) -> Self;
}

fn atomic_wake(a: &AtomicU32) {
    unsafe {
        let _ = futex(
            a.as_ptr(),
            FutexOperation::Wake,
            FutexFlags::PRIVATE,
            1,
            ptr::null(),
            ptr::null_mut(),
            0,
        );
    }
}

fn atomic_wait(a: &AtomicU32, expected: u32, deadline: Instant) {
    let timeout = deadline.saturating_duration_since(unsafe { mem::zeroed() });
    let deadline = Timespec {
        #[allow(clippy::cast_possible_wrap)]
        tv_sec: timeout.as_secs() as _,
        tv_nsec: timeout.subsec_nanos().into(),
    };
    unsafe {
        let _ = futex(
            a.as_ptr(),
            FutexOperation::WaitBitset,
            FutexFlags::PRIVATE,
            expected,
            &deadline,
            ptr::null_mut(),
            u32::MAX,
        );
    }
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
