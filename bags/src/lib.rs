mod cache_padded;
mod mpmc;
mod slot;

use std::{
    process::abort,
    sync::atomic::{AtomicU32, AtomicU64},
};

pub use mpmc::{SendBuffer, SendBufferContainer, mpmc};
use rustix::{
    io::Errno,
    thread::{futex, futex::Flags},
};
pub use slot::{Receiver as SlotReceiver, Sender as SlotSender, mpsc as mpsc_slot};

pub type MpmcSender<T> = mpmc::Sender<30, T>;
pub type MpmcReceiver<T> = mpmc::Receiver<30, T>;

#[cold]
#[inline(always)]
#[allow(clippy::inline_always)]
fn atomic_wake(word: &AtomicU32, waiters: u32) -> usize {
    let result = futex::wake(word, Flags::PRIVATE, waiters);
    if cfg!(debug_assertions) {
        result.expect("Futex wake bug")
    } else {
        result.unwrap_or(0)
    }
}

#[cold]
fn atomic_sleep(word: &AtomicU32, expected: u32) {
    match futex::wait(word, Flags::PRIVATE, expected, None) {
        Ok(()) | Err(Errno::AGAIN | Errno::INTR) => (),
        Err(e) => {
            if cfg!(debug_assertions) {
                unreachable!("Futex wait bug: {e}")
            }
        }
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
    if cfg!(target_endian = "big") {
        let ptr = word.as_ptr().cast::<u32>();
        unsafe { AtomicU32::from_ptr(ptr) }
    } else if cfg!(target_endian = "little") {
        let ptr = word.as_ptr().cast::<u32>();
        unsafe { AtomicU32::from_ptr(ptr.add(1)) }
    } else {
        unreachable!();
    }
}
