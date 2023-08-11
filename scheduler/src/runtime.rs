use std::{any::TypeId, num::NonZeroU8, sync::atomic::AtomicBool};

// TODO Make every struct an Arc wrapper as opposed to being wrapped by an Arc
pub struct RuntimeContext {
    max_num_threads: NonZeroU8,
    error_type: TypeId,

    abort: AtomicBool,
    // threads: Threads,
}

// struct Threads {
//     clock: AtomicU32,
//     pool: RwLock<Vec<ThreadTaskRoot>>,
// }
//
// impl Threads {
//     fn maybe_update(&self, ThreadsCache { clock, pool }: &mut ThreadsCache)
// -> bool {         let new_clock = self.clock.load(Ordering::Relaxed);
//         if new_clock == *clock {
//             return false;
//         }
//
//         *clock = new_clock;
//         pool.clone_from(&self.pool.read().unwrap());
//
//         true
//     }
// }
//
// struct ThreadsCache {
//     clock: u32,
//     pool: Vec<ThreadTaskRoot>,
// }
//
// struct ThreadContext {
//     root: ThreadTaskRoot,
//     runtime: Arc<RuntimeContext>,
// }
//
// impl ThreadTaskRoot {
//     fn spawn(&self, context: &ThreadContext) {
//         let handle = thread::spawn({
//             let context = ThreadContext {
//                 root: ThreadTaskRoot::new(context.runtime.max_num_threads),
//                 runtime: context.runtime.clone(),
//             };
//             || {
//                 context::init(context);
//                 worker(unsafe { context::get() })
//             }
//         });
//         todo!()
//     }
//
//     fn spawn_detached(&self) {
//         todo!()
//     }
// }
//
// fn worker(ThreadContext { root, runtime }: &ThreadContext) {
//     loop {
//         if runtime.abort.load(Ordering::Relaxed) {
//             break;
//         }
//         if let Some(task) = overflow_tasks::pop() {
//             todo!("run")
//         }
//         if let Some(task) = root.next() {}
//
//         // TODO
//         //  Fail count is num threads, plus one means go to sleep. Store
//         //  pointers to tasks plus len (at end of allocation) as Bago bites
// that         //  gets reconstructed as a vec
//     }
//     todo!("return errors")
// }
//
// mod context {
//     use std::cell::Cell;
//
//     use crate::runtime::ThreadContext;
//
//     thread_local! {
//         static CONTEXT: Cell<ThreadContext> = panic!("Not in the lockness
// runtime");     }
//
//     pub fn init(context: ThreadContext) {
//         CONTEXT.set(context);
//     }
//
//     pub unsafe fn get() -> &'static ThreadContext {
//         CONTEXT.with(|context| unsafe { &*context.as_ptr().cast_const() })
//     }
// }

// pub mod overflow_tasks {
//     use std::cell::Cell;
//
//     use crate::tasks::Task;
//
//     thread_local! {
//         static OVERFLOW_TASKS: Cell<Vec<Task>> = Cell::new(Vec::new());
//     }
//
//     pub fn pop() -> Option<Task> {
//         OVERFLOW_TASKS.with(|tasks| unsafe { &mut *tasks.as_ptr() }.pop())
//     }
//
//     pub fn extend(value: impl IntoIterator<Item = Task>) {
//         OVERFLOW_TASKS.with(|tasks| unsafe { &mut *tasks.as_ptr()
// }.extend(value));     }
// }
