pub type Task = Box<TaskInner>;

// const _SIZE_CHECK: () = assert!(size_of::<TaskBlock<Task>>() ==
// CACHE_LINE_SIZE);

pub struct TaskInner {}
