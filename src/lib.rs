use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::ptr;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize};

pub trait Reclaim {}

impl<T> Reclaim for T {}
pub trait Deleter {
    unsafe fn delete(&self, ptr: *mut dyn Reclaim);
}

impl Deleter for unsafe fn(*mut (dyn Reclaim + 'static)) {
    unsafe fn delete(&self, ptr: *mut dyn Reclaim) {
        unsafe { (*self)(ptr) }
    }
}

pub mod deleters {
    use crate::Reclaim;

    unsafe fn drop_in_place2(ptr: *mut dyn Reclaim) {
        unsafe { std::ptr::drop_in_place(ptr) };
    }
    #[allow(non_upper_case_globals)]
    pub static drop_in_place: unsafe fn(*mut dyn Reclaim) = drop_in_place2;

    unsafe fn drop_box2(ptr: *mut dyn Reclaim) {
        let _ = unsafe { Box::from(ptr) };
    }

    #[allow(non_upper_case_globals)]
    pub static drop_box: unsafe fn(*mut dyn Reclaim) = drop_box2;
}

#[derive(Default)]
pub struct HazPtrHolder(Option<&'static HazPtr>);

impl HazPtrHolder {
    fn hazptr(&mut self) -> &'static HazPtr {
        if let Some(hazptr) = self.0 {
            hazptr
        } else {
            let hazptr = SHARED_DOMAIN.acquire();
            self.0 = Some(hazptr);
            hazptr
        }
    }

    pub unsafe fn load<'l, T>(&mut self, ptr: &'_ AtomicPtr<T>) -> Option<&'l T> {
        let hazptr = self.hazptr();
        let mut ptr1 = ptr.load(Ordering::SeqCst);
        loop {
            hazptr.protect(ptr1 as *mut u8);
            let ptr2 = ptr.load(Ordering::SeqCst);
            if ptr1 == ptr2 {
                break std::ptr::NonNull::new(ptr1).map(|nn| unsafe { nn.as_ref() });
            } else {
                ptr1 = ptr2
            }
        }
    }

    pub fn reset(&mut self) {
        if let Some(hazptr) = self.0 {
            hazptr.ptr.store(std::ptr::null_mut(), Ordering::SeqCst);
        }
    }
}

impl Drop for HazPtrHolder {
    fn drop(&mut self) {
        self.reset();

        if let Some(hazptr) = self.0 {
            hazptr.active.store(false, Ordering::SeqCst);
        }
    }
}

pub struct HazPtr {
    ptr: AtomicPtr<u8>,
    next: AtomicPtr<HazPtr>,
    active: AtomicBool,
}

impl HazPtr {
    fn protect(&self, ptr: *mut u8) {
        self.ptr.store(ptr, Ordering::SeqCst);
    }
}

pub struct HazPtrs {
    head: AtomicPtr<HazPtr>,
}

pub struct Retired {
    ptr: *mut dyn Reclaim,
    deleter: &'static dyn Deleter,
    next: AtomicPtr<Retired>,
}

struct RetiredList {
    head: AtomicPtr<Retired>,
    count: AtomicUsize,
}
// #[allow(drop_bounds)]
pub trait HazPtrObject
where
    Self: Sized + Reclaim + 'static,
{
    fn domain(&self) -> &HazPtrDomain;
    unsafe fn retire(ptr: *mut Self, deleter: &'static dyn Deleter) {
        unsafe { &*ptr }
            .domain()
            .retire(ptr as *mut dyn Reclaim, deleter);
    }
}

pub struct HazPtrObjectWrapper<T> {
    inner: T,
}

impl<T> HazPtrObjectWrapper<T> {
    pub fn with_default_domain(t: T) -> Self {
        Self { inner: t }
    }
}

impl<T: 'static> HazPtrObject for HazPtrObjectWrapper<T> {
    fn domain(&self) -> &HazPtrDomain {
        &SHARED_DOMAIN
    }
    // unsafe fn retire(ptr: *mut Self, deleter: &'static dyn Deleter) {
    //     HazPtrObject::retire(ptr, deleter);
    // }
}

impl<T> Drop for HazPtrObjectWrapper<T> {
    fn drop(&mut self) {}
}

impl<T> Deref for HazPtrObjectWrapper<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for HazPtrObjectWrapper<T> {
    //  type Target = &mut T;
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
// #[derive(Default)]
pub struct HazPtrDomain {
    hazptrs: HazPtrs,
    retired: RetiredList,
}

static SHARED_DOMAIN: HazPtrDomain = HazPtrDomain {
    hazptrs: HazPtrs {
        head: AtomicPtr::new(std::ptr::null_mut()),
    },
    retired: RetiredList {
        head: AtomicPtr::new(std::ptr::null_mut()),
        count: AtomicUsize::new(0),
    },
};

impl HazPtrDomain {
    fn acquire(&self) -> &'static HazPtr {
        let head_ptr = &self.hazptrs.head;
        let mut node = head_ptr.load(Ordering::SeqCst);
        loop {
            while (!node.is_null() && unsafe { &*node }.active.load(Ordering::SeqCst)) {
                node = unsafe { &*node }.next.load(Ordering::SeqCst);
            }

            if node.is_null() {
                let hazptr = Box::into_raw(Box::new(HazPtr {
                    ptr: AtomicPtr::new(std::ptr::null_mut()),
                    next: AtomicPtr::new(std::ptr::null_mut()),
                    active: AtomicBool::new(true),
                }));
                let mut head = head_ptr.load(Ordering::SeqCst);
                break loop {
                    *unsafe { &mut *hazptr }.next.get_mut() = head;
                    match head_ptr.compare_exchange_weak(
                        head,
                        hazptr,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => {
                            break unsafe { &*hazptr };
                        }
                        Err(head_now) => head = head_now,
                    }
                };
            } else {
                let node = unsafe { &*node };
                if node
                    .active
                    .compare_exchange_weak(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    break node;
                } else {
                }
            }
        }
    }

    fn retire(&self, ptr: *mut dyn Reclaim, deleter: &'static dyn Deleter) {
        let retired = Box::into_raw(Box::new(Retired {
            ptr,
            deleter,
            next: AtomicPtr::new(std::ptr::null_mut()),
        }));

        self.retired.count.fetch_add(1, Ordering::SeqCst);
        let head_ptr = &self.retired.head;
        let mut head = head_ptr.load(Ordering::SeqCst);
        loop {
            *unsafe { &mut *retired }.next.get_mut() = head;
            match head_ptr.compare_exchange_weak(head, retired, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => {
                    break;
                }
                Err(head_now) => head = head_now,
            }
        }

        if self.retired.count.load(Ordering::SeqCst) != 0 {
            self.bulk_reclaim(0, false);
        }
    }

    fn eager_reclaim(&self, block: bool) -> usize {
        self.bulk_reclaim(0, block)
    }

    fn bulk_reclaim(&self, prev_reclaimed: usize, block: bool) -> usize {
        let steal = self
            .retired
            .head
            .swap(std::ptr::null_mut(), Ordering::SeqCst);
        if steal.is_null() {
            return 0;
        }

        let mut reclaimed: usize = 0;

        let mut guard_list = HashSet::new();
        let mut node = self.hazptrs.head.load(Ordering::SeqCst);
        while !node.is_null() {
            let n = unsafe { &*node };
            if n.active.load(Ordering::SeqCst) {
                guard_list.insert(n.ptr.load(Ordering::SeqCst));
            }
            node = n.next.load(Ordering::SeqCst);
        }
        let mut node = steal;
        let mut remaining = std::ptr::null_mut();
        let mut tail = None;
        while !node.is_null() {
            let current = node;
            let n = unsafe { &*current };
            node = n.next.load(Ordering::SeqCst);
            if guard_list.contains(&(n.ptr as *mut u8)) {
                n.next.store(remaining, Ordering::SeqCst);
                //  remaining = Box::into_raw(n);
                remaining = current;
                if tail.is_none() {
                    tail = Some(remaining);
                }
            } else {
                let mut n = unsafe { Box::from_raw(current) };
                unsafe { n.deleter.delete(n.ptr) };
                reclaimed += 1;
            }
        }
        self.retired.count.fetch_sub(reclaimed, Ordering::SeqCst);
        let total_reclaimed = prev_reclaimed + reclaimed;
        let tail = if let Some(tail) = tail {
            assert!(!remaining.is_null());
            tail
        } else {
            assert!(remaining.is_null());
            return total_reclaimed;
        };

        let head_ptr = &self.retired.head;
        let mut head = head_ptr.load(Ordering::SeqCst);
        loop {
            *unsafe { &mut *tail }.next.get_mut() = head;
            match head_ptr.compare_exchange_weak(
                head,
                remaining,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    break;
                }
                Err(heah_now) => head = heah_now,
            }
        }

        if !remaining.is_null() && block {
            std::thread::yield_now();

            return self.bulk_reclaim(total_reclaimed, true);
        }

        total_reclaimed
    }
}

impl Drop for HazPtrDomain {
    fn drop(&mut self) {
        todo!()
    }
}
// pub struct SharedHazPtrDomain;

// #[cfg(test)]

// mod tests {
//     use std::sync::atomic::AtomicPtr;

//     use super::*;

//     #[test]
//     fn feels_good() {
//         let x = AtomicPtr::new(Box::into_raw(Box::new(42)));

//         let h = HazPtrHolder::default();
//         let my_x: &&i32 = h.load(&x);
//         drop(h);
//         //invalid
//         let _ = *my_x;
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    struct CountDrops(Arc<AtomicUsize>);
    impl Drop for CountDrops {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn feels_good() {
        let drops_42 = Arc::new(AtomicUsize::new(0));

        let x = AtomicPtr::new(Box::into_raw(Box::new(
            HazPtrObjectWrapper::with_default_domain((42, CountDrops(Arc::clone(&drops_42)))),
        )));

        // As a reader:
        let mut h = HazPtrHolder::default();

        // Safety:
        //
        //  1. AtomicPtr points to a Box, so is always valid.
        //  2. Writers to AtomicPtr use HazPtrObject::retire.
        let my_x = unsafe { h.load(&x) }.expect("not null");
        // valid:
        assert_eq!(my_x.0, 42);
        h.reset();
        // invalid:
        // let _: i32 = my_x.0;

        let my_x = unsafe { h.load(&x) }.expect("not null");
        // valid:
        assert_eq!(my_x.0, 42);
        drop(h);
        // invalid:
        // let _: i32 = my_x.0;

        let mut h = HazPtrHolder::default();
        let my_x = unsafe { h.load(&x) }.expect("not null");

        let mut h_tmp = HazPtrHolder::default();
        let _ = unsafe { h_tmp.load(&x) }.expect("not null");
        drop(h_tmp);

        // As a writer:
        let drops_9001 = Arc::new(AtomicUsize::new(0));
        let old = x.swap(
            Box::into_raw(Box::new(HazPtrObjectWrapper::with_default_domain((
                9001,
                CountDrops(Arc::clone(&drops_9001)),
            )))),
            std::sync::atomic::Ordering::SeqCst,
        );

        let mut h2 = HazPtrHolder::default();
        let my_x2 = unsafe { h2.load(&x) }.expect("not null");

        assert_eq!(my_x.0, 42);
        assert_eq!(my_x2.0, 9001);

        // Safety:
        //
        //  1. The pointer came from Box, so is valid.
        //  2. The old value is no longer accessible.
        //  3. The deleter is valid for Box types.
        unsafe { HazPtrObject::retire(old, &deleters::drop_box) };

        assert_eq!(drops_42.load(Ordering::SeqCst), 0);
        assert_eq!(my_x.0, 42);

        let n = SHARED_DOMAIN.eager_reclaim(false);
        assert_eq!(n, 0);

        assert_eq!(drops_42.load(Ordering::SeqCst), 0);
        assert_eq!(my_x.0, 42);

        drop(h);
        assert_eq!(drops_42.load(Ordering::SeqCst), 0);
        // _not_ drop(h2);

        let n = SHARED_DOMAIN.eager_reclaim(false);
        assert_eq!(n, 1);

        assert_eq!(drops_42.load(Ordering::SeqCst), 0);
        assert_eq!(drops_9001.load(Ordering::SeqCst), 0);

        drop(h2);
        let n = SHARED_DOMAIN.eager_reclaim(false);
        assert_eq!(n, 0);
        assert_eq!(drops_9001.load(Ordering::SeqCst), 0);
    }
}
