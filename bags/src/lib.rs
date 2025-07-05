mod cache_padded;
mod mpmc;
mod slot;

use std::{
    process::abort,
    sync::atomic::{AtomicU32, AtomicU64},
};

pub use mpmc::{Receiver as MpmcReceiver, Sender as MpmcSender, mpmc};
use rustix::{
    io::Errno,
    thread::{futex, futex::Flags},
};
pub use slot::{Receiver as SlotReceiver, Sender as SlotSender, mpsc as mpsc_slot};

#[cold]
#[inline(always)]
#[allow(clippy::inline_always)]
fn atomic_wake(word: &AtomicU32, waiters: u32) -> usize {
    futex::wake(word, Flags::PRIVATE, waiters).expect("Futex wake bug")
}

#[cold]
fn atomic_sleep(word: &AtomicU32, expected: u32) {
    match futex::wait(word, Flags::PRIVATE, expected, None) {
        Ok(()) | Err(Errno::AGAIN | Errno::INTR) => (),
        Err(e) => unreachable!("Futex wait bug: {e}"),
    }
}

#[cold]
fn abort_with_message(m: &str) {
    eprintln!("{m}");
    abort()
}

/// Uses the high bits of a u64 to make a u32 futex
#[cfg(all(target_arch = "x86_64", not(miri)))]
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
