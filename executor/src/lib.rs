#![ doc = include_str!( "../README.md")]

mod error;

use std::{env, marker::PhantomData, num::NonZeroUsize, sync::OnceLock, thread};

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

fn default_max_threads() -> NonZeroUsize {
    static ENV_NUM_THREADS: OnceLock<Option<NonZeroUsize>> = OnceLock::new();

    let env_num_threads = *ENV_NUM_THREADS.get_or_init(|| {
        env::var_os("MAX_LOCKNESS_THREADS").and_then(|s| {
            let num_threads = s
                .to_str()
                .and_then(|s| s.parse::<isize>().ok())
                .unwrap_or(-1);
            if num_threads < 1 {
                None
            } else {
                Some(NonZeroUsize::new(usize::try_from(num_threads).unwrap()).unwrap())
            }
        })
    });

    env_num_threads
        .unwrap_or_else(|| thread::available_parallelism().unwrap_or(NonZeroUsize::new(1).unwrap()))
}

#[derive(Builder)]
#[builder(builder_type(vis = "pub", name = LocknessExecutorBuilder))]
#[builder(finish_fn(vis = "", name = build_internal))]
struct DynamicParams {
    #[builder(default = default_max_threads())]
    max_threads: NonZeroUsize,
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

pub struct SpawnBuffer<'spawner, C, F> {
    inner: Inner<C>,
    _task: PhantomData<F>,
    _tie: PhantomData<&'spawner ()>,
}

pub struct Finisher<E> {
    inner: Inner<E>,
}

impl<C: Config> LocknessExecutor<C> {
    pub fn spawner(&self) -> &Spawner<C> {
        todo!()
    }

    pub fn finisher(self) -> Finisher<C::Error> {
        todo!()
    }
}

impl<C: Config> Spawner<C> {
    /// Acquire a task buffer in which to prepare tasks for scheduling.
    pub fn buffered<F>(&self) -> SpawnBuffer<'_, C, F> {
        todo!()
    }

    /// Tries to schedule submitted tasks on other threads.
    ///
    /// WARNING: a [SpawnBuffer] must be dropped before a [Spawner] can schedule
    /// its tasks, so calling flush without first dropping [SpawnBuffer]s does
    /// nothing.
    ///
    /// This will be called automatically on drop.
    pub fn flush(&self) {
        todo!()
    }
}

/// A task buffer of type `F`.
///
/// Note that tasks submitted via the buffer are NOT scheduled to run on other
/// threads until a call to `flush`. Additionally, unlike [Spawner], dropping
/// this type does not trigger a scheduling action to allow for batched
/// scheduling across task types.
impl<'spawner, C, F> SpawnBuffer<'spawner, C, F> {
    /// Tries to schedule _this buffer's_ submitted tasks on other threads.
    ///
    /// Use [Spawner::flush] after dropping this type if you would like to
    /// schedule tasks across types.
    pub fn flush(&self) {
        todo!()
    }
}

impl<
    'a,
    C: Config + Clone + Send + 'static,
    F: FnOnce(&mut C::ThreadLocalState) -> Result<(), C::Error> + Send + 'static,
> SpawnBuffer<'a, C, F>
{
    pub fn spawn(&self, f: F) {}
}

impl<
    'a,
    C: Config<AllowTasksToSpawnMoreTasks = True> + Clone + Send + 'static,
    F: FnOnce(&Spawner<C>, &mut C::ThreadLocalState) -> Result<(), C::Error> + Send + 'static,
> SpawnBuffer<'a, C, F>
{
    pub fn spawn_recursive(&self, f: F) {}
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
