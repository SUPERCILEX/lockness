use std::{
    alloc::{Allocator, Layout},
    cmp::max,
    marker::PhantomData,
    mem,
    mem::{align_of, size_of, transmute, ManuallyDrop, MaybeUninit},
    ptr,
    ptr::NonNull,
};

pub struct ClosureVec<F: FnOnce() + 'static> {
    data: Vec<MaybeUninit<u8>>,
    _type: PhantomData<F>,
}

#[derive(Copy, Clone)]
struct Header {
    task_fn: NonNull<()>,
    task_size: usize,
    vec_align: usize,
    vec_len: usize,
    vec_capacity: usize,
}

union FirstEntry<T> {
    header: Header,
    _t: ManuallyDrop<T>, // Used to ensure proper alignment
}

impl Header {
    fn update(from: &mut Vec<MaybeUninit<u8>>) {
        let len = from.len();
        let capacity = from.capacity();

        let Self {
            task_fn: _,
            task_size: _,
            vec_align: _,
            vec_len,
            vec_capacity,
        } = <&mut Self>::from(from.as_mut_slice());
        *vec_len = len;
        *vec_capacity = capacity;
    }
}

impl<'a> From<&'a mut [MaybeUninit<u8>]> for &'a mut Header {
    fn from(value: &mut [MaybeUninit<u8>]) -> Self {
        debug_assert!(value.len() >= size_of::<Header>());

        #[allow(clippy::cast_ptr_alignment)]
        let header = ptr::from_mut(value).cast::<Header>();
        unsafe { &mut *header }
    }
}

impl<'a> From<&'a [MaybeUninit<u8>]> for &'a Header {
    fn from(value: &[MaybeUninit<u8>]) -> Self {
        debug_assert!(value.len() >= size_of::<Header>());

        #[allow(clippy::cast_ptr_alignment)]
        let header = ptr::from_ref(value).cast::<Header>();
        unsafe { &*header }
    }
}

impl<F: FnOnce() + 'static> ClosureVec<F> {
    pub const fn new() -> Self {
        Self {
            data: Vec::new(),
            _type: PhantomData,
        }
    }

    pub fn clear(&mut self) {
        while !self._pop_and_run(true) {}
    }

    pub fn into_raw(self) -> *mut Self {
        let mut this = ManuallyDrop::new(self);
        let data = {
            let vec_ptr = ptr::from_mut(&mut this.data);
            unsafe { vec_ptr.read() }
        };
        data.into_raw_parts().0.cast()
    }

    pub unsafe fn from_raw(ptr: *mut Self) -> Self {
        let &Header {
            task_fn: _,
            task_size: _,
            vec_align: _,
            vec_len,
            vec_capacity,
        } = unsafe { &*ptr.cast::<Header>() };
        Self {
            data: unsafe {
                Vec::from_raw_parts(ptr.cast::<MaybeUninit<u8>>(), vec_len, vec_capacity)
            },
            _type: PhantomData,
        }
    }

    unsafe fn aligned_vec_op(
        data: &mut Vec<MaybeUninit<u8>>,
        op: impl FnOnce(&mut Vec<FirstEntry<F>>),
    ) {
        let mut vec = Self::bytes_vec_to_typed(mem::take(data));
        op(&mut vec);
        *data = Self::typed_vec_to_bytes(vec);
    }

    fn bytes_vec_to_typed(vec: Vec<MaybeUninit<u8>>) -> Vec<FirstEntry<F>> {
        let (ptr, len, cap) = vec.into_raw_parts();
        let ptr = ptr.cast::<FirstEntry<F>>();
        let header_len = size_of::<FirstEntry<F>>();
        debug_assert!(ptr.is_aligned());
        debug_assert!(cap % header_len == 0);
        debug_assert!(len <= cap);

        let len = len.div_ceil(header_len);
        let cap = cap / header_len;

        unsafe { Vec::from_raw_parts(ptr, len, cap) }
    }

    fn typed_vec_to_bytes(vec: Vec<FirstEntry<F>>) -> Vec<MaybeUninit<u8>> {
        let (ptr, len, cap) = vec.into_raw_parts();
        let ptr = ptr.cast::<MaybeUninit<u8>>();
        let header_len = size_of::<FirstEntry<F>>();
        let len = len * header_len;
        let cap = cap * header_len;

        unsafe { Vec::from_raw_parts(ptr, len, cap) }
    }

    fn maybe_init(&mut self) {
        if !self.is_empty() {
            return;
        }
        let Self { data, _type } = self;

        if !data.as_ptr().cast::<FirstEntry<F>>().is_aligned() {
            *data = Self::typed_vec_to_bytes(Vec::new());
        } else {
            debug_assert!(
                data.is_empty()
                    || <&Header>::from(data.as_slice()).vec_align == align_of::<FirstEntry<F>>()
            );
            data.clear();
        }

        {
            unsafe fn call<F: FnOnce()>(f: NonNull<()>, just_drop: bool) {
                let f = unsafe { f.cast::<F>().as_ptr().read() };
                if !just_drop {
                    f();
                }
            }

            let init = |v: &mut Vec<_>| {
                if size_of::<F>() > 0 {
                    v.reserve(2);
                }
                v.push(FirstEntry {
                    header: Header {
                        task_fn: NonNull::new(call::<F> as *mut ()).unwrap(),
                        task_size: size_of::<F>(),
                        vec_align: align_of::<FirstEntry<F>>(),
                        vec_len: 0,
                        vec_capacity: 0,
                    },
                });
            };
            unsafe {
                Self::aligned_vec_op(data, init);
            }
        }

        Header::update(data);
    }

    pub fn push(&mut self, value: F) {
        self.maybe_init();
        let Self { data, _type } = self;

        if size_of::<F>() == 0 {
            let Header {
                task_fn: _,
                task_size: _,
                vec_align: _,
                vec_len,
                vec_capacity: _,
            } = <&mut Header>::from(data.as_mut_slice());
            *vec_len += 1;
            return;
        }

        unsafe {
            let len = data.len();
            Self::aligned_vec_op(data, |v| v.reserve(1));
            data.set_len(len);
        }

        {
            let data_ptr = ptr::from_mut(data.spare_capacity_mut()).cast::<F>();
            unsafe {
                data_ptr.write(value);
            }
        }
        {
            let new_len = data.len() + size_of::<F>();
            unsafe {
                data.set_len(new_len);
            }
        }

        Header::update(data);
    }

    /// Returns true if the vec is empty, false otherwise.
    pub fn pop_and_run(&mut self) -> bool {
        self._pop_and_run(false)
    }

    fn _pop_and_run(&mut self, just_drop: bool) -> bool {
        if self.is_empty() {
            return true;
        }

        let Self { data, _type } = self;
        let &mut Header {
            task_fn,
            task_size,
            vec_align: _,
            ref mut vec_len,
            vec_capacity: _,
        } = <&mut Header>::from(data.as_mut_slice());
        let task = unsafe { transmute::<_, unsafe fn(NonNull<()>, bool)>(task_fn) };
        *vec_len -= max(1, task_size);

        if task_size > 0 {
            let start = *vec_len;
            let captures = if cfg!(debug_assertions) {
                NonNull::from(&mut data[start..]).cast()
            } else {
                unsafe { NonNull::new_unchecked(data.as_mut_ptr().add(start)) }.cast()
            };

            unsafe {
                data.set_len(start);
                task(captures, just_drop);
            }
        } else {
            unsafe {
                task(NonNull::dangling(), just_drop);
            }
        };

        false
    }

    pub fn is_empty(&self) -> bool {
        let &Header {
            task_fn: _,
            task_size,
            vec_align: _,
            vec_len,
            vec_capacity: _,
        } = {
            let Self { data, _type } = self;
            if data.is_empty() {
                return true;
            }

            <&Header>::from(data.as_slice())
        };

        vec_len <= max(size_of::<FirstEntry<()>>(), task_size)
    }
}

impl<F: FnOnce()> Drop for ClosureVec<F> {
    fn drop(&mut self) {
        if self.data.is_empty() {
            return;
        }
        let &Header {
            task_fn: _,
            task_size: _,
            vec_align,
            vec_len: _,
            vec_capacity: _,
        } = <&Header>::from(self.data.as_slice());

        struct PanicGuard<'a, F: FnOnce() + 'static>(&'a mut ClosureVec<F>, usize);

        impl<'a, F: FnOnce() + 'static> Drop for PanicGuard<'a, F> {
            fn drop(&mut self) {
                let v = mem::take(&mut self.0.data);
                let (ptr, _len, cap, alloc) = v.into_raw_parts_with_alloc();
                unsafe {
                    alloc.deallocate(
                        NonNull::new(ptr).unwrap_unchecked().cast(),
                        Layout::from_size_align_unchecked(cap, self.1),
                    );
                }
            }
        }

        PanicGuard(self, vec_align).0.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, rc::Rc};

    use super::*;

    fn validate_correctness<F: FnOnce() + 'static>(mut f: impl FnMut(usize) -> F) {
        let mut tasks = ClosureVec::new();

        let count = Rc::new(Cell::new(0));

        for i in 0..3 {
            let f = f(i);
            let count = count.clone();
            tasks.push(move || {
                f();
                count.update(|i| i + 1);
            });
        }
        while !tasks.pop_and_run() {}

        assert_eq!(3, count.get());
    }

    fn validate_empty<F: FnOnce() + 'static>(mut f: impl FnMut(usize) -> F) {
        let mut tasks = ClosureVec::new();
        assert!(tasks.is_empty());

        for i in 0..3 {
            tasks.push(f(i));
            assert!(!tasks.is_empty());
        }

        for _ in 0..3 {
            assert!(!tasks.is_empty());
            assert!(!tasks.pop_and_run());
        }
        assert!(tasks.is_empty());
    }

    //noinspection RsConstantConditionIf
    fn validate_do_nothing<F: FnOnce() + 'static>(mut f: impl FnMut(usize) -> F) {
        let mut tasks = ClosureVec::new();
        if false {
            tasks.push(f(42));
        }
    }

    fn validate_drop_without_running<F: FnOnce() + 'static>(mut f: impl FnMut(usize) -> F) {
        let mut tasks = ClosureVec::new();
        let f = f(69);
        tasks.push(move || {
            f();
            panic!("Should never run");
        });
    }

    fn validate_type_erased_ops<F: FnOnce() + 'static>(mut f: impl FnMut(usize) -> F) {
        let mut tasks = ClosureVec::new();
        let count = Rc::new(Cell::new(0));

        tasks.push({
            let f = f(88);
            let count = count.clone();
            move || {
                f();
                count.set(666);
            }
        });

        let mut tasks = unsafe { transmute::<_, ClosureVec<fn()>>(tasks) };
        assert!(!tasks.pop_and_run());
        assert_eq!(666, count.get());
    }

    fn validate_clear_add_cycle<F: FnOnce() + 'static>(mut f: impl FnMut(usize) -> F) {
        let mut tasks = ClosureVec::new();

        let count = Rc::new(Cell::new(0));

        for i in 0..3 {
            tasks.clear();
            let f = f(i);
            let count = count.clone();
            tasks.push(move || {
                f();
                count.update(|i| i + 1);
            });
        }
        while !tasks.pop_and_run() {}
        tasks.clear();

        assert_eq!(1, count.get());
    }

    fn validate_raw<F: FnOnce() + 'static>(mut f: impl FnMut(usize) -> F) {
        let mut tasks = ClosureVec::new();
        tasks.push(f(2048));

        let mut tasks = unsafe { ClosureVec::from_raw(tasks.into_raw()) };
        tasks.push(f(1234));

        let mut tasks = unsafe { ClosureVec::<fn()>::from_raw(tasks.into_raw().cast()) };
        while !(tasks.pop_and_run()) {}
    }

    fn validate_all<F: FnOnce() + 'static>(c: impl FnMut(usize) -> F + Clone + 'static) {
        validate_correctness(c.clone());
        validate_empty(c.clone());
        validate_clear_add_cycle(c.clone());
        validate_do_nothing(c.clone());
        validate_drop_without_running(c.clone());
        validate_type_erased_ops(c.clone());
        validate_raw(c);
    }

    #[test]
    fn zst() {
        validate_all(|_| || dbg!(()));
    }

    #[test]
    fn normal() {
        validate_all(|i| {
            move || {
                dbg!(i);
            }
        });
    }

    #[test]
    fn big_ass() {
        #[repr(align(512))]
        #[derive(Clone, Debug)]
        struct Big(Vec<usize>);

        let mut v = Big(Vec::new());
        validate_all(move |i| {
            v.0.push(i);
            {
                let v = v.clone();
                move || {
                    dbg!(&v);
                }
            }
        });
    }

    #[test]
    #[should_panic]
    fn panic_in_drop() {
        #[derive(Debug)]
        struct P;

        impl Drop for P {
            fn drop(&mut self) {
                panic!("Lol get fucked");
            }
        }

        let mut tasks = ClosureVec::new();
        let p = P;
        let v = vec![true, false];
        tasks.push(move || {
            dbg!(&v, &p);
        });
    }

    #[test]
    #[should_panic]
    fn panic_in_f() {
        let mut tasks = ClosureVec::new();
        let v = vec![true, false];
        tasks.push(move || {
            dbg!(&v);
            panic!("Life's tough");
        });
        tasks.pop_and_run();
    }

    #[test]
    fn validate_reuse_with_different_type() {
        let mut tasks = ClosureVec::new();

        fn use_<F: FnOnce() + Clone + 'static>(tasks: &mut ClosureVec<fn()>, f: F) {
            let tasks: &mut ClosureVec<F> = unsafe { transmute(tasks) };
            tasks.push(f.clone());
            assert!(!tasks.pop_and_run());

            tasks.push(f);
            tasks.clear();
        }

        use_(&mut tasks, || {
            dbg!(42);
        });
        let v = vec!["a", "b", "c"];
        use_(&mut tasks, move || {
            dbg!(v);
        })
    }
}
