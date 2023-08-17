use std::{
    alloc::{alloc, dealloc, handle_alloc_error, realloc, Layout},
    cmp,
    mem::{align_of, size_of, ManuallyDrop},
    ptr,
    ptr::NonNull,
};

struct Header<M> {
    metadata: M,
    value_size: usize,
    alignment: usize,
    len: usize,
    capacity: usize,
}

impl<M> Header<M> {
    unsafe fn from_ref<T>(value: &'_ FreestandingVec<M, T>) -> &'_ Self {
        debug_assert!(value.is_allocated());
        unsafe { &*value.data.cast::<Self>() }
    }

    unsafe fn from_mut<T>(value: &'_ mut FreestandingVec<M, T>) -> &'_ mut Self {
        debug_assert!(value.is_allocated());
        unsafe { &mut *value.data.cast::<Self>() }
    }
}

union FirstEntry<M, T> {
    _header: ManuallyDrop<Header<M>>,
    _t: ManuallyDrop<T>, // Used to ensure proper alignment
}

pub struct FreestandingVec<M, T> {
    data: *mut FirstEntry<M, T>,
}

impl<M, T> FreestandingVec<M, T> {
    const MIN_NON_ZERO_CAP: usize = if size_of::<T>() == 1 {
        8
    } else if size_of::<T>() <= 1024 {
        4
    } else {
        1
    } * size_of::<T>()
        + size_of::<FirstEntry<M, T>>();

    #[must_use]
    pub fn new(metadata: M) -> Self {
        let mut me = Self {
            data: ptr::null_mut(),
        };
        me.init(metadata);
        me
    }

    fn deallocate(&mut self) {
        debug_assert!(self.is_allocated());

        let &mut Header {
            ref mut metadata,
            value_size: _,
            alignment,
            len: _,
            capacity: _,
        } = unsafe { Header::from_mut(self) };

        let _metadata_for_drop = unsafe { ptr::from_mut(metadata).read() };
        unsafe {
            let layout = Layout::from_size_align_unchecked(self.capacity_bytes(), alignment);
            dealloc(self.data.cast(), layout);
        }
        self.data = ptr::null_mut();
    }

    fn is_allocated(&self) -> bool {
        !self.data.is_null()
    }

    #[must_use]
    pub fn into_raw(self) -> *mut () {
        let this = ManuallyDrop::new(self);
        this.data.cast()
    }

    pub const unsafe fn from_raw(ptr: *mut ()) -> Self {
        Self { data: ptr.cast() }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        debug_assert!(self.is_allocated());
        unsafe { Header::from_ref(self) }.len
    }

    fn capacity_bytes(&self) -> usize {
        debug_assert!(self.is_allocated());
        unsafe { Header::from_ref(self) }.capacity
    }

    fn allocate(&mut self) -> usize {
        debug_assert!(!self.is_allocated());

        let capacity = Self::MIN_NON_ZERO_CAP;
        let layout =
            unsafe { Layout::from_size_align_unchecked(capacity, align_of::<FirstEntry<M, T>>()) };
        let ptr = unsafe { alloc(layout) };

        if ptr.is_null() {
            handle_alloc_error(layout);
        } else {
            self.data = ptr.cast();
        }

        capacity
    }

    fn reserve(&mut self, additional_bytes: usize) {
        if additional_bytes == 0 {
            return;
        }

        let required_cap = (self.len() * size_of::<T>() + size_of::<FirstEntry<M, T>>())
            .checked_add(additional_bytes)
            .expect("Requested allocation overflows");

        let og_cap = self.capacity_bytes();
        if og_cap >= required_cap {
            return;
        }

        let new_capacity = cmp::max(og_cap.saturating_mul(2), required_cap);
        let layout =
            unsafe { Layout::from_size_align_unchecked(og_cap, align_of::<FirstEntry<M, T>>()) };
        let ptr = unsafe { realloc(self.data.cast(), layout, new_capacity) };

        if ptr.is_null() {
            handle_alloc_error(layout);
        } else {
            self.data = ptr.cast();
        }

        unsafe { Header::from_mut(self) }.capacity = new_capacity;
    }

    pub fn init(&mut self, metadata: M) {
        if !self.data.is_aligned() && self.is_allocated() {
            self.deallocate();
        }
        if self.is_allocated() && self.capacity_bytes() < Self::MIN_NON_ZERO_CAP {
            self.reserve(Self::MIN_NON_ZERO_CAP - self.capacity_bytes());
        }

        let was_allocated = self.is_allocated();
        let capacity = if self.is_allocated() {
            self.capacity_bytes()
        } else {
            self.allocate()
        };

        let ptr = self.data.cast::<Header<M>>();
        let header = Header {
            metadata,
            value_size: size_of::<T>(),
            alignment: align_of::<FirstEntry<M, T>>(),
            len: 0,
            capacity,
        };
        if was_allocated {
            unsafe {
                *ptr = header;
            }
        } else {
            unsafe {
                ptr.write(header);
            }
        }
    }

    pub fn push(&mut self, value: T) {
        self.reserve(size_of::<T>());
        unsafe {
            self.data.add(1).cast::<T>().add(self.len()).write(value);
            Header::from_mut(self).len += 1;
        }
    }

    pub fn pop<O, F: FnOnce(&mut M, NonNull<T>) -> O>(&mut self, f: F) -> Option<O> {
        let len = self.len();
        if len == 0 {
            return None;
        }
        let len = len - 1;

        let ptr = self.data;
        let &mut Header {
            ref mut metadata,
            value_size,
            alignment: _,
            len: ref mut header_len,
            capacity: _,
        } = unsafe { Header::from_mut(self) };
        let ptr = {
            let base = cmp::max(size_of::<FirstEntry<M, ()>>(), value_size);
            unsafe { NonNull::new_unchecked(ptr.cast::<u8>().add(base + len * value_size)) }
        };

        *header_len = len;
        Some(f(metadata, ptr.cast()))
    }
}

impl<M, T> Drop for FreestandingVec<M, T> {
    fn drop(&mut self) {
        self.deallocate();
    }
}

#[cfg(test)]
mod tests {
    use std::mem::transmute;

    use proptest::prelude::*;
    use proptest_derive::Arbitrary;

    use super::*;

    #[test]
    fn validate_empty() {
        let mut v = FreestandingVec::new(());
        assert!(v.is_empty());

        for i in 0..3 {
            v.push(i);
            assert!(!v.is_empty());
        }

        for _ in 0..3 {
            assert!(!v.is_empty());
            assert!(v.pop(|(), _| ()).is_some());
        }
        assert!(v.is_empty());
    }

    #[test]
    fn validate_do_nothing() {
        drop(FreestandingVec::<Vec<usize>, Vec<usize>>::new(vec![1]));
    }

    #[test]
    fn validate_type_erased_ops() {
        let mut v = FreestandingVec::new(|| ());

        v.push("42".to_string());

        let mut v = unsafe { transmute::<_, FreestandingVec<(), ()>>(v) };
        assert_eq!(
            Some("42".to_string()),
            v.pop(|(), ptr| unsafe { ptr.cast::<String>().as_ptr().read() })
        );
    }

    #[test]
    fn validate_raw() {
        let mut v = FreestandingVec::new(|| ());
        v.push(2048u32);

        let mut v = unsafe { FreestandingVec::<(), _>::from_raw(v.into_raw()) };
        v.push(1234u32);

        let mut v = unsafe { FreestandingVec::<(), ()>::from_raw(v.into_raw().cast()) };
        assert_eq!(
            Some(1234),
            v.pop(|(), ptr| unsafe { ptr.cast::<u32>().as_ptr().read() })
        );
        assert_eq!(
            Some(2048),
            v.pop(|(), ptr| unsafe { ptr.cast::<u32>().as_ptr().read() })
        );
    }

    #[test]
    fn zst() {
        let mut v = FreestandingVec::new(());

        for i in 0..1000 {
            v.push(i);
        }
        for i in (0..1000).rev() {
            assert_eq!(Some(i), v.pop(|(), ptr| unsafe { ptr.as_ptr().read() }));
        }
    }

    #[test]
    fn normal() {
        let mut v = FreestandingVec::new(vec![1, 2, 3]);

        {
            for i in 0..1000 {
                v.push(Box::new(i));
            }
        }

        for i in (0..1000).rev() {
            assert_eq!(
                Some(i),
                v.pop(|v, ptr| {
                    assert_eq!(&[1, 2, 3], v.as_slice());
                    *unsafe { ptr.as_ptr().read() }
                })
            );
        }
    }

    #[test]
    fn big() {
        #[repr(align(512))]
        #[derive(Clone, Debug)]
        struct Big(Vec<usize>);

        let mut v = FreestandingVec::new(vec![1, 2, 3]);

        {
            let mut b = Big(Vec::new());
            for i in 0..100 {
                b.0.push(i);
                v.push(b.clone());
            }
        }

        for i in (0..100).rev() {
            assert_eq!(
                Some((0..=i).collect::<Vec<_>>()),
                v.pop(|v, ptr| {
                    assert_eq!(&[1, 2, 3], v.as_slice());
                    unsafe { ptr.as_ptr().read() }.0
                })
            );
        }
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

        let mut v = FreestandingVec::new(P);
        v.push(());
    }

    #[test]
    fn validate_reuse_with_different_type() {
        fn use_<F: Clone>(tasks: &mut FreestandingVec<(), ()>, f: F) {
            let tasks = unsafe { &mut *ptr::from_mut(tasks).cast::<FreestandingVec<(), F>>() };
            tasks.init(());
            for _ in 0..4 {
                tasks.push(f.clone());
            }
            for _ in 0..4 {
                assert!(
                    tasks
                        .pop(|(), ptr| unsafe { ptr.as_ptr().read() })
                        .is_some()
                );
            }
        }

        let mut tasks = FreestandingVec::new(());

        use_(&mut tasks, 42usize);
        use_(&mut tasks, vec!["a", "b", "c"]);
    }

    #[derive(Copy, Clone, Eq, PartialEq, Debug, Arbitrary)]
    enum Path {
        Push,
        Pop,
        Clear,
        Replace,
    }

    proptest! {
        #[test]
        #[cfg(not(miri))]
        fn all(path: Vec<Path>) {
            #[repr(align(512))]
            #[derive(Clone, Debug)]
            struct Big(Vec<usize>);

            let mut v = FreestandingVec::<Big, Big>::new(Big((0..100).rev().collect()));
            for (i, choice) in path.iter().enumerate() {
                match choice {
                    Path::Push => {
                        let val = Big((0..i).collect());
                        let _init = val.clone();
                        v.push(val);
                    }
                    Path::Pop => {
                        if let Some(m) = v
                            .pop(|_, ptr| unsafe { ptr.as_ptr().read() })
                            .and_then(|b| b.0.into_iter().max())
                        {
                            assert!(m < i);
                        }
                    }
                    Path::Clear => {
                        while v
                            .pop(|_, ptr| unsafe { ptr.as_ptr().drop_in_place() })
                            .is_some()
                        {}
                    }
                    Path::Replace => {
                        while v
                            .pop(|_, ptr| unsafe { ptr.as_ptr().drop_in_place() })
                            .is_some()
                        {}
                        v = FreestandingVec::new(Big((0..i).rev().collect()));
                    }
                }
            }
        }
    }
}
