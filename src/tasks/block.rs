use std::{
    mem,
    mem::{size_of, transmute, ManuallyDrop, MaybeUninit},
    ptr,
};

// TODO ZST
pub struct TaskBlock {
    data: Vec<MaybeUninit<u8>>,
}

#[derive(Copy, Clone)]
struct Header {
    task_fn: *mut (),
    task_size: usize,
    vec_len: usize,
    vec_capacity: usize,
}

union FirstEntry<T> {
    header: Header,
    t: ManuallyDrop<T>, // Used to ensure proper alignment
}

impl Header {
    fn update(from: &mut Vec<MaybeUninit<u8>>) {
        let len = from.len();
        let capacity = from.capacity();

        let Header {
            task_fn: _,
            task_size: _,
            vec_len,
            vec_capacity,
        } = <&mut Header>::from(from.as_mut_slice());
        *vec_len = len;
        *vec_capacity = capacity;
    }
}

impl From<&mut [MaybeUninit<u8>]> for &mut Header {
    fn from(value: &mut [MaybeUninit<u8>]) -> Self {
        unsafe { &mut *ptr::from_mut(value).cast::<Header>() }
    }
}

impl From<&[MaybeUninit<u8>]> for &Header {
    fn from(value: &[MaybeUninit<u8>]) -> Self {
        unsafe { &*ptr::from_ref(value).cast::<Header>() }
    }
}

impl TaskBlock {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn reset(&mut self) {
        while !self.rop(true) {}

        let Self { data } = self;
        unsafe {
            data.set_len(0);
        }
    }

    pub fn into_raw(self) -> *mut () {
        let mut this = ManuallyDrop::new(self);
        let data = unsafe { ptr::from_mut(&mut this.data).read() };
        data.into_raw_parts().0.cast()
    }

    pub fn from_raw(ptr: *mut ()) -> Self {
        let &Header {
            task_fn: _,
            task_size: _,
            vec_len,
            vec_capacity,
        } = unsafe { &*ptr.cast::<Header>() };
        Self {
            data: unsafe {
                Vec::from_raw_parts(ptr.cast::<MaybeUninit<u8>>(), vec_len, vec_capacity)
            },
        }
    }

    unsafe fn aligned_vec_op<T>(
        data: &mut Vec<MaybeUninit<u8>>,
        op: impl FnOnce(&mut Vec<FirstEntry<T>>),
    ) {
        let vec = mem::replace(data, Vec::new());

        let mut vec = {
            let (ptr, len, cap) = vec.into_raw_parts();
            let ptr = ptr.cast::<FirstEntry<T>>();
            let header_len = size_of::<FirstEntry<T>>();
            let len = len.div_ceil(header_len);
            let cap = cap / header_len;

            debug_assert!(ptr.is_aligned());
            debug_assert!(cap % header_len == 0);

            unsafe { Vec::from_raw_parts(ptr, len, cap) }
        };

        op(&mut vec);

        *data = Self::typed_vec_to_bytes(vec);
    }

    fn typed_vec_to_bytes<T>(vec: Vec<FirstEntry<T>>) -> Vec<MaybeUninit<u8>> {
        let (ptr, len, cap) = vec.into_raw_parts();
        let ptr = ptr.cast::<MaybeUninit<u8>>();
        let header_len = size_of::<FirstEntry<T>>();
        let len = len * header_len;
        let cap = cap * header_len;

        unsafe { Vec::from_raw_parts(ptr, len, cap) }
    }

    fn maybe_init<F: FnOnce()>(&mut self) {
        let Self { data } = self;

        if !data.is_empty() {
            return;
        }

        if !data.as_ptr().cast::<FirstEntry<F>>().is_aligned() {
            *data = Self::typed_vec_to_bytes::<F>(Vec::new());
        }
        unsafe {
            Self::aligned_vec_op::<F>(data, |v| v.reserve(2));
        }

        {
            unsafe fn call<F: FnOnce()>(f: *mut (), just_drop: bool) {
                let f = unsafe { f.cast::<F>().read() };
                if !just_drop {
                    f();
                }
            }

            let init = |v: &mut Vec<_>| {
                v.push(FirstEntry {
                    header: Header {
                        task_fn: call::<F> as *mut (),
                        task_size: size_of::<F>(),
                        vec_len: 0,
                        vec_capacity: 0,
                    },
                })
            };
            unsafe {
                Self::aligned_vec_op::<F>(data, init);
            }
        }

        Header::update(data);
    }

    // // TODO figure out bounds
    // pub unsafe fn push_for_result<F: FnOnce() -> R, R>(&mut self, f: F) ->
    // TodoJoinHandle<R> {     // TODO create join handle
    //     self.push(|| {
    //         let result = f();
    //         // TODO write
    //     });
    // }
    //
    // pub unsafe fn push_for_completion<F: FnOnce() -> Result<(), E>, E>(&mut self,
    // f: F) {     self.push(|| {
    //         if let Err(e) = f() {
    //             // TODO write
    //         }
    //     });
    // }

    fn push<F: FnOnce()>(&mut self, value: F) {
        self.maybe_init::<F>();

        let Self { data } = self;
        let additional = size_of::<F>();

        unsafe {
            Self::aligned_vec_op::<F>(data, |v| v.reserve(1));
        }

        {
            let data_ptr = ptr::from_mut(data.spare_capacity_mut()).cast::<F>();
            unsafe {
                *data_ptr = value;
            }
        }
        {
            let new_len = data.len() + additional;
            unsafe {
                data.set_len(new_len);
            }
        }

        Header::update(data);
    }

    /// Returns true if the task block is complete, false otherwise.
    pub fn pop_and_run(&mut self) -> bool {
        self.rop(false)
    }

    fn rop(&mut self, just_drop: bool) -> bool {
        let Self { data } = self;

        let &Header {
            task_fn,
            task_size,
            vec_len: _,
            vec_capacity: _,
        } = <&Header>::from(data.as_slice());

        // TODO(https://github.com/rust-lang/rust/issues/113757): use max
        if data.len() > size_of::<FirstEntry<()>>() && data.len() > task_size {
            let task = unsafe { transmute::<_, fn(*mut (), bool)>(task_fn) };

            let start = data.len() - task_size;
            task(ptr::from_mut(&mut data[start..]).cast(), just_drop);
            unsafe {
                data.set_len(start);
            }
            Header::update(data);

            false
        } else {
            true
        }
    }
}

impl Drop for TaskBlock {
    fn drop(&mut self) {
        self.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fill_drain() {
        let mut tasks = TaskBlock::new();

        let mut i = 0;
        while tasks.push(i).is_none() {
            i += 1;
        }

        while let Some(j) = tasks.pop() {
            i -= 1;
            assert_eq!(i, j);
        }

        assert!(tasks.is_empty());
    }

    #[test]
    fn iter() {
        let mut tasks = TaskBlock::new();

        let mut i = 0;
        while tasks.push(i).is_none() {
            i += 1;
        }

        assert_eq!(tasks.collect::<Vec<_>>(), (0..i).rev().collect::<Vec<_>>());
    }

    #[test]
    fn drop() {
        let mut tasks = TaskBlock::new();

        tasks.push(0);
    }
}
