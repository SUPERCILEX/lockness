use std::sync::{
    Arc,
    atomic::AtomicPtr,
    mpsc::{RecvError, SendError, TryRecvError, TrySendError},
};

pub fn mpsc<T: Send + 'static>() -> (Sender<T>, Receiver<T>) {
    todo!()
}

#[derive(Debug)]
struct Inner<T> {
    slot: AtomicPtr<T>,
}

#[derive(Debug)]
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

pub struct Receiver<T> {
    t: T,
}

impl<T: Send + 'static> Sender<T> {
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

impl<T: Send + 'static> Receiver<T> {
    pub fn recv(&self) -> Result<T, RecvError> {
        todo!()
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        todo!()
    }
}
