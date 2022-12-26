use std::sync::{Condvar, Mutex};

pub struct RawOneLatest<T> {
    data: Mutex<Option<T>>,
    condvar: Condvar,
}

impl<T> Default for RawOneLatest<T> {
    fn default() -> Self {
        Self {
            data: Mutex::new(None),
            condvar: Condvar::new(),
        }
    }
}

#[allow(clippy::missing_panics_doc)]
impl<T> RawOneLatest<T> {
    pub fn push(&self, t: T) -> Option<T> {
        // TODO this is dumb. We can make pushing wait-free, but I need to
        //  implement a cross-platform futex API. parking_lot_core could probably
        //  be used, but it brings in all the unnecessary thread queuing crap.
        let mut data = self.data.lock().unwrap();
        let old = std::mem::replace(&mut *data, Some(t));
        self.condvar.notify_one();
        old
    }

    pub fn pop(&self) -> T {
        let mut data = self.data.lock().unwrap();
        loop {
            if let Some(t) = std::mem::replace(&mut *data, None) {
                return t;
            }

            data = self.condvar.wait(data).unwrap();
        }
    }

    pub fn try_pop(&self) -> Option<T> {
        let mut data = self.data.lock().unwrap();
        std::mem::replace(&mut *data, None)
    }
}
