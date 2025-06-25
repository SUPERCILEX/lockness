use std::{
    mem::MaybeUninit,
    sync::{
        Arc,
        atomic::AtomicU32,
        mpsc::{RecvError, SendError, TryRecvError, TrySendError},
    },
};

use crate::cache_padded::CachePadded;

#[must_use]
pub fn mpsc<T>() -> (Sender<T>, Receiver<T>) {
    todo!()
}

#[derive(Debug)]
struct Inner<T> {
    status: CachePadded<AtomicU32>,
    data: MaybeUninit<T>,
}

#[derive(Debug)]
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

pub struct Receiver<T> {
    t: T,
}

impl<T> Sender<T> {
    pub fn send(&self, data: T) -> Result<(), SendError<T>> {
        todo!()
    }

    pub fn try_send(&self, data: T) -> Result<(), TrySendError<T>> {
        todo!()
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let Self { inner } = self;
        Self {
            inner: inner.clone(),
        }
    }
}

impl<T> Receiver<T> {
    pub fn recv(&self) -> Result<T, RecvError> {
        todo!()
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        todo!()
    }
}
