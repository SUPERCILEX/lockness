mod cache_padded;
mod mpmc;
mod slot;

use std::sync::atomic::{AtomicU32, AtomicU64};

pub use mpmc::{Receiver as MpmcReceiver, Sender as MpmcSender, mpmc};
use rustix::thread::{futex, futex::Flags};
pub use slot::{Receiver as SlotReceiver, Sender as SlotSender, mpsc as mpsc_slot};

#[cold]
#[inline(always)]
#[allow(clippy::inline_always)]
fn atomic_wake(word: &AtomicU32, waiters: u32) -> usize {
    futex::wake(word, Flags::PRIVATE, waiters).unwrap()
}

/// Uses the high bits of a u64 to make a u32 futex
const fn u64_to_futex(word: &AtomicU64) -> &AtomicU32 {
    #[cfg(target_endian = "big")]
    {
        let ptr = word.as_ptr().cast::<u32>();
        unsafe { AtomicU32::from_ptr(ptr) }
    }
    #[cfg(target_endian = "little")]
    {
        let ptr = word.as_ptr().cast::<u32>();
        unsafe { AtomicU32::from_ptr(ptr.add(1)) }
    }
}
