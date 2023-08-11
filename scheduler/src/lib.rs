use std::{
    marker::PhantomData,
    num::{NonZeroU8, NonZeroUsize},
    sync::Arc,
    thread,
};

use crate::runtime::RuntimeContext;

mod cache_line_size;
mod runtime;
mod tasks;

pub struct Builder {
    max_num_threads: NonZeroU8,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            max_num_threads: thread::available_parallelism()
                .unwrap_or(NonZeroUsize::MIN)
                .try_into()
                .unwrap_or(NonZeroU8::MAX),
        }
    }
}

impl Builder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn max_num_threads(&mut self, max: NonZeroU8) -> &mut Self {
        self.max_num_threads = max;
        self
    }

    #[must_use]
    pub fn build(self) -> Scheduler<()> {
        self.build_for_detached()
    }

    #[must_use]
    pub fn build_for_detached<E: Send + 'static>(self) -> Scheduler<E> {
        todo!()
    }
}

pub struct Scheduler<E> {
    context: Arc<RuntimeContext>,
    _error_marker: PhantomData<E>,
}

impl<E: Send + 'static> Scheduler<E> {
    pub fn spawn<F: (FnOnce() -> T) + Send + 'static, T: Send + 'static>(
        &self,
        _f: F,
    ) -> JoinHandle<T> {
        todo!()
    }

    pub fn spawn_detached<F: (FnOnce() -> Result<(), E>) + Send + 'static>(&self, _f: F) {
        todo!()
    }

    #[must_use]
    pub fn shutdown<Errors: Iterator<Item = E>>(self) -> Errors {
        todo!()
    }

    /// The `kill` function (which would kill running threads) does not exist as
    /// it would almost certainly cause memory corruption, memory leaks, and
    /// deadlocks due to killing running code in an indeterminate state.
    pub fn abort(self) {
        todo!()
    }
}

impl<E> Drop for Scheduler<E> {
    fn drop(&mut self) {
        todo!()
    }
}

pub struct JoinHandle<T> {
    result: T,
}

impl<T> JoinHandle<T> {
    pub fn result(self) -> T {
        todo!()
    }
}

pub fn spawn<F: (FnOnce() -> T) + Send + 'static, T: Send + 'static>(_f: F) -> JoinHandle<T> {
    todo!()
}

pub fn spawn_detached<F: (FnOnce() -> Result<(), E>) + Send + 'static, E: Send + 'static>(_f: F) {
    todo!()
}
