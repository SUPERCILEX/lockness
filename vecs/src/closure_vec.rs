use std::{mem::ManuallyDrop, ptr, ptr::NonNull};

use crate::freestanding_vec::FreestandingVec;

struct Metadata {
    task_fn: unsafe fn(NonNull<()>, bool),
}

pub struct ClosureVec<F: FnOnce()> {
    data: FreestandingVec<Metadata, F>,
}

// TODO should there be a 'static bound here?
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl<T: FnOnce() + Send> Send for ClosureVec<T> {}

unsafe impl<T: FnOnce() + Sync> Sync for ClosureVec<T> {}

impl<F: FnOnce()> ClosureVec<F> {
    const INIT: Metadata = {
        unsafe fn call<F: FnOnce()>(f: NonNull<()>, just_drop: bool) {
            let f = unsafe { f.cast::<F>().as_ptr().read() };
            if !just_drop {
                f();
            }
        }

        Metadata { task_fn: call::<F> }
    };

    #[must_use]
    pub fn new() -> Self {
        Self {
            data: FreestandingVec::new(Self::INIT),
        }
    }

    pub fn clear(&mut self) {
        while !self._pop_and_run(true) {}
    }

    #[must_use]
    pub fn into_raw(self) -> *mut Self {
        let mut me = ManuallyDrop::new(self);
        let data = ptr::from_mut(&mut me.data);
        unsafe { data.read() }.into_raw().cast()
    }

    pub const unsafe fn from_raw(ptr: *mut Self) -> Self {
        unsafe {
            Self {
                data: FreestandingVec::from_raw(ptr.cast()),
            }
        }
    }

    pub fn update_type(&mut self) {
        self.data.init(Self::INIT);
    }

    pub fn push(&mut self, value: F) {
        self.data.push(value);
    }

    /// Returns true if the vec is empty, false otherwise.
    pub fn pop_and_run(&mut self) -> bool {
        self._pop_and_run(false)
    }

    fn _pop_and_run(&mut self, just_drop: bool) -> bool {
        self.data
            .pop(|&mut Metadata { task_fn }, captures| unsafe {
                task_fn(captures.cast(), just_drop);
            })
            .is_none()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl<F: FnOnce()> Default for ClosureVec<F> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F: FnOnce()> Drop for ClosureVec<F> {
    fn drop(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, ptr, rc::Rc};

    use super::*;

    fn validate_correctness<F: FnOnce()>(mut f: impl FnMut(usize) -> F) {
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

    fn validate_empty<F: FnOnce()>(mut f: impl FnMut(usize) -> F) {
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
    fn validate_do_nothing<F: FnOnce()>(mut f: impl FnMut(usize) -> F) {
        let mut tasks = ClosureVec::new();
        if false {
            tasks.push(f(42));
        }
    }

    fn validate_drop_without_running<F: FnOnce()>(mut f: impl FnMut(usize) -> F) {
        let mut tasks = ClosureVec::new();
        let f = f(69);
        tasks.push(move || {
            f();
            panic!("Should never run");
        });
    }

    fn validate_type_erased_ops<F: FnOnce()>(mut f: impl FnMut(usize) -> F) {
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

        assert!(!tasks.pop_and_run());
        assert_eq!(666, count.get());
    }

    fn validate_clear_add_cycle<F: FnOnce()>(mut f: impl FnMut(usize) -> F) {
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

    fn validate_raw<F: FnOnce()>(mut f: impl FnMut(usize) -> F) {
        let mut tasks = ClosureVec::new();
        tasks.push(f(2048));

        let mut tasks = unsafe { ClosureVec::from_raw(tasks.into_raw()) };
        tasks.push(f(1234));

        let mut tasks = unsafe { ClosureVec::<fn()>::from_raw(tasks.into_raw().cast()) };
        while !(tasks.pop_and_run()) {}
    }

    fn validate_all<F: FnOnce()>(c: impl FnMut(usize) -> F + Clone) {
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
    #[should_panic(expected = "Lol get fucked")]
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
    #[should_panic(expected = "Life's tough")]
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
        fn use_<F: FnOnce() + Clone>(tasks: &mut ClosureVec<fn()>, f: F) {
            let tasks = unsafe { &mut *ptr::from_mut(tasks).cast::<ClosureVec<F>>() };
            tasks.update_type();
            tasks.push(f.clone());
            assert!(!tasks.pop_and_run());

            tasks.push(f);
            tasks.clear();
        }

        let mut tasks = ClosureVec::new();
        use_(&mut tasks, || {
            dbg!(42);
        });
        let v = vec!["a", "b", "c"];
        use_(&mut tasks, move || {
            dbg!(v);
        });
    }
}
