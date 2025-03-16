// Bag can hold items or lists of items, cutoff is half size of bag at which
// point you must add lists

use std::thread;

pub struct LocknessExecutor {}

impl LocknessExecutor {
    pub fn access_mut(&mut self) -> ExecutorAccess {
        todo!()
    }

    pub fn shutdown(self) -> thread::Result<()> {
        // Block
        todo!()
    }
}

impl Drop for LocknessExecutor {
    fn drop(&mut self) {
        // Forget everything
        todo!()
    }
}

pub struct ExecutorAccess<'a> {
    pub spawner: Spawner<'a>,
    pub reaper: Reaper<'a>,
}

// Result iteration comes in two variants:
// - One which simply gives whatever is available
// - And one which waits until the whole executor is shut down

// To get the blocking list of tasks, call an API that consumes the executor and
// prepares it to shut down. This API returns the blocking tasks iterator and a
// panics iterator. Ideally you can call them in any order and we'll block
// appropriately.
