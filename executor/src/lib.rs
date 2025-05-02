// Bag can hold items or lists of items, cutoff is half size of bag at which
// point you must add lists

use std::marker::PhantomData;
use std::num::{NonZeroU32, NonZeroUsize};
use std::thread;

pub struct LocknessExecutorBuilder<Message, ThreadInitializer> {
    max_threads: Option<NonZeroUsize>,
    message_type: PhantomData<Message>,
    thread_initializer: ThreadInitializer,
}

impl<M, T> LocknessExecutorBuilder<M, T> {
    pub fn max_threads(self, max_threads: impl Into<NonZeroUsize>) -> Self {
        let Self {
            max_threads: _,
            message_type,
            thread_initializer,
        } = self;
        LocknessExecutorBuilder {
            max_threads: Some(max_threads.into()),
            message_type,
            thread_initializer,
        }
    }

    pub fn thread_initializer<ThreadInitializer>(
        self,
        thread_initializer: ThreadInitializer,
    ) -> LocknessExecutorBuilder<M, ThreadInitializer> {
        let Self {
            max_threads,
            message_type: message,
            thread_initializer: _,
        } = self;
        LocknessExecutorBuilder {
            max_threads,
            message_type: message,
            thread_initializer,
        }
    }

    pub fn build(self) -> LocknessExecutor<M, T> {
        let LocknessExecutorBuilder {
            max_threads,
            message_type,
            thread_initializer,
        } = self;
        LocknessExecutor {
            max_threads: max_threads
                .or_else(|| thread::available_parallelism().ok())
                .map(|p| NonZeroU32::try_from(p).unwrap_or(NonZeroU32::MAX))
                .unwrap_or(const { NonZeroU32::new(1).unwrap() }),
            thread_initializer,
            messenger: LocknessMessenger { message_type },
        }
    }
}

pub struct LocknessMessenger<M> {
    message_type: PhantomData<M>,
}

impl<M: Send> LocknessMessenger<M> {
    pub fn send(&self, message: M) -> Result<(), M> {
        todo!()
    }
}

pub struct LocknessExecutor<Message, ThreadInitializer> {
    max_threads: NonZeroU32,
    thread_initializer: ThreadInitializer,
    messenger: LocknessMessenger<Message>,
}

pub type NoInitializer<Message> = fn(&LocknessMessenger<Message>) -> ();

pub type SimpleLocknessExecutor<Message> = LocknessExecutor<Message, NoInitializer<Message>>;

impl LocknessExecutor<(), ()> {
    pub fn builder<Message>() -> LocknessExecutorBuilder<Message, NoInitializer<Message>> {
        LocknessExecutorBuilder {
            max_threads: None,
            message_type: PhantomData,
            thread_initializer: |_| (),
        }
    }
}

pub struct MessageIterator<M> {
    _message_type: PhantomData<M>,
}

impl<M> Iterator for MessageIterator<M> {
    type Item = M;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl<
    Message: Send,
    State,
    ThreadInitializer: (FnMut(&LocknessMessenger<Message>) -> State) + Send + 'static,
> LocknessExecutor<Message, ThreadInitializer>
{
    pub fn spawn<F: FnOnce((&mut State, &Self)) + Send + 'static>(&self, f: F) {}

    pub fn finish(self) -> Result<MessageIterator<Message>, ()> {
        Ok(todo!())
    }
}

impl<Message: Send, ThreadInitializer> LocknessExecutor<Message, ThreadInitializer> {
    pub fn messenger(&self) -> &LocknessMessenger<Message> {
        todo!()
    }

    pub fn send(&self, message: Message) -> Result<(), Message> {
        self.messenger().send(message)
    }
}

impl<Message, ThreadInitializer> LocknessExecutor<Message, ThreadInitializer> {
    pub fn cancel(&self) {
        todo!()
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        todo!()
    }
}

// impl Drop for LocknessExecutor {
//     fn drop(&mut self) {
//         // Forget everything
//         todo!()
//     }
// }
//
// pub struct ExecutorAccess<'a> {
//     pub spawner: Spawner<'a>,
//     pub reaper: Reaper<'a>,
// }

// Result iteration comes in two variants:
// - One which simply gives whatever is available
// - And one which waits until the whole executor is shut down

// To get the blocking list of tasks, call an API that consumes the executor and
// prepares it to shut down. This API returns the blocking tasks iterator and a
// panics iterator. Ideally you can call them in any order and we'll block
// appropriately.
