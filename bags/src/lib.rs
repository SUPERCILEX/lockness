use std::{mem, ptr, sync::atomic::AtomicU32, time::Instant};

use rustix::thread::{FutexFlags, FutexOperation, Timespec, futex};

mod bags;
mod slot;

pub use slot::{Receiver as SlotReceiver, Sender as SlotSender, mpsc as mpsc_slot};

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

fn atomic_wait(a: &AtomicU32, expected: u32, deadline: Option<Instant>) {
    #[allow(clippy::option_if_let_else)]
    match deadline {
        None => unsafe {
            let _ = futex(
                a.as_ptr(),
                FutexOperation::Wait,
                FutexFlags::PRIVATE,
                expected,
                ptr::null(),
                ptr::null_mut(),
                0,
            );
        },
        Some(deadline) => {
            let timeout = deadline.saturating_duration_since(unsafe { mem::zeroed() });
            #[allow(clippy::cast_possible_wrap)]
            let deadline = Timespec {
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
    }
}
