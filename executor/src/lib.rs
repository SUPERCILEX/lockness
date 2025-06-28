mod error;

use std::num::NonZeroUsize;

use bon::Builder;
pub use error::{Error, JoinError};

use crate::config::{Config, True};

pub mod config {
    pub trait Config {
        const NUM_TASK_TYPES: usize;
        type AllowTasksToSpawnMoreTasks;
        type DequeBias;

        type Error: Send + 'static;
        type ThreadLocalState;

        fn thread_initializer(self) -> Result<Self::ThreadLocalState, Self::Error>;
    }

    pub struct True;
    pub struct False;

    pub struct Fifo;
    pub struct Lifo;
}

#[derive(Builder)]
#[builder(builder_type(vis = "pub", name = LocknessExecutorBuilder))]
#[builder(finish_fn(vis = "", name = build_internal))]
struct DynamicParams {
    max_threads: Option<NonZeroUsize>,
}

impl LocknessExecutorBuilder {
    pub fn new() -> Self {
        DynamicParams::builder()
    }
}

impl Default for LocknessExecutorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: lockness_executor_builder::IsComplete> LocknessExecutorBuilder<S> {
    pub fn build<C>(self, config: C) -> LocknessExecutor<C> {
        LocknessExecutor {
            inner: Inner {
                params: self.build_internal(),
                config,
            },
        }
    }
}

struct Inner<C> {
    params: DynamicParams,
    config: C,
}

pub struct LocknessExecutor<C> {
    inner: Inner<C>,
}

pub struct Spawner<C> {
    inner: Inner<C>,
}

pub struct SpawnBuffer<C> {
    inner: Inner<C>,
}

pub struct Finisher<E> {
    inner: Inner<E>,
}

impl<C: Config> LocknessExecutor<C> {
    pub fn spawner(&self) -> Spawner<C> {
        todo!()
    }

    pub fn finisher(self) -> Finisher<C::Error> {
        todo!()
    }
}

impl<C> Spawner<C> {
    pub fn buffer(&self) -> SpawnBuffer<C> {
        todo!()
    }

    pub fn yield_(&self) {
        todo!()
    }

    pub fn drain(&self) {
        todo!(
            "Blocks until all local tasks have been offloaded to other threads. Does nothing if \
             max_threads=1"
        )
    }
}

impl<C> SpawnBuffer<C> {
    pub fn flush(&self) {
        todo!()
    }
}

impl<C: Config + Clone + Send + 'static> Spawner<C> {
    pub fn spawn0<F: FnOnce(&mut C::ThreadLocalState) -> Result<(), C::Error> + Send + 'static>(
        &self,
        f: F,
    ) {
    }
}

impl<C: Config<AllowTasksToSpawnMoreTasks = True> + Clone + Send + 'static> Spawner<C> {
    pub fn spawn1<
        F: FnOnce(&Self, &mut C::ThreadLocalState) -> Result<(), C::Error> + Send + 'static,
    >(
        &self,
        f: F,
    ) {
    }
}

impl<C: Config + Clone + Send + 'static> SpawnBuffer<C> {
    pub fn spawn0<F: FnOnce(&mut C::ThreadLocalState) -> Result<(), C::Error> + Send + 'static>(
        &self,
        f: F,
    ) {
    }
}

impl<C: Config<AllowTasksToSpawnMoreTasks = True> + Clone + Send + 'static> SpawnBuffer<C> {
    pub fn spawn1<
        F: FnOnce(&Spawner<C>, &mut C::ThreadLocalState) -> Result<(), C::Error> + Send + 'static,
    >(
        &self,
        f: F,
    ) {
    }
}

impl<E> Iterator for Finisher<E> {
    type Item = Error<E>;

    fn next(&mut self) -> Option<Self::Item> {
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
