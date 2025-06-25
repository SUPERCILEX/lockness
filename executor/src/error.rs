use std::{any::Any, fmt, io};

use crate::error::sync_wrapper::SyncWrapper;

pub enum Error<E> {
    Panic(JoinError),
    Error(E),
}

pub struct JoinError(SyncWrapper<Box<dyn Any + Send + 'static>>);

impl JoinError {
    pub fn from_join_error(panic: Box<dyn Any + Send + 'static>) -> Self {
        Self(SyncWrapper::new(panic))
    }

    pub fn into_inner(self) -> Box<dyn Any + Send + 'static> {
        self.0.into_inner()
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        dump_panic_payload(&self.0, fmt)
    }
}

impl fmt::Debug for JoinError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "JoinError(")?;
        dump_panic_payload(&self.0, fmt)?;
        write!(fmt, ")")?;
        Ok(())
    }
}

impl std::error::Error for JoinError {}

impl From<JoinError> for io::Error {
    fn from(value: JoinError) -> io::Error {
        io::Error::other(value)
    }
}

fn dump_panic_payload(
    payload: &SyncWrapper<Box<dyn Any + Send>>,
    fmt: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    if let Some(s) = payload.downcast_ref::<String>() {
        return write!(fmt, "task panic: {s}");
    }
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return write!(fmt, "task panic: {s}");
    }
    write!(fmt, "task panic with unknown payload")
}

// Derived from https://docs.rs/tokio/1.45.1/src/tokio/util/sync_wrapper.rs.html
mod sync_wrapper {
    //! This module contains a type that can make `Send + !Sync` types `Sync` by
    //! disallowing all immutable access to the value.
    //!
    //! A similar primitive is provided in the `sync_wrapper` crate.

    use std::any::Any;

    pub struct SyncWrapper<T>(T);

    // Safety: An immutable reference to a SyncWrapper is useless, so moving such an
    // immutable reference across threads is safe.
    unsafe impl<T> Sync for SyncWrapper<T> {}

    impl<T> SyncWrapper<T> {
        pub fn new(value: T) -> Self {
            Self(value)
        }

        pub fn into_inner(self) -> T {
            self.0
        }
    }

    impl SyncWrapper<Box<dyn Any + Send>> {
        /// Attempt to downcast using `Any::downcast_ref()` to a type that is
        /// known to be `Sync`.
        pub fn downcast_ref<T: Sync + 'static>(&self) -> Option<&T> {
            // SAFETY: if the downcast fails, the inner value is not touched,
            // so no thread-safety violation can occur.
            self.0.downcast_ref()
        }
    }
}
