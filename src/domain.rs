use crate::{Deleter, HazPtr, Reclaim, deleter};
use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicPtr, AtomicUsize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::u8;

const SYNC_TIME_PERIOD: u64 = std::time::Duration::from_nanos(2000000000).as_nanos() as u64;
const RCOUNT_THRESHOLD: isize = 1000;
const HCOUNT_MULTIPLIER: isize = 2;

pub struct HazPtrDomain<F> {
    hazptrs: HazPtrs,
    retired: RetiredList,
    family: PhantomData<F>,
    sync_time: AtomicU64,
    nbulk_reclaims: AtomicUsize,
}

#[non_exhaustive]
pub struct Global;
impl Global {
    const fn new() -> Self {
        Global
    }
}

static SHARED_DOMAIN: HazPtrDomain<Global> = HazPtrDomain::new(&Global::new());

impl HazPtrDomain<Global> {
    pub fn global() -> &'static Self {
        &SHARED_DOMAIN
    }
}

#[macro_export]
macro_rules! unique_domain {
    () => {
        HazPtrDomain::new(&|| {})
    };
}

impl<F> HazPtrDomain<F> {
    pub const fn new(_: &F) -> Self {
        Self {
            hazptrs: HazPtrs {
                head: AtomicPtr::new(std::ptr::null_mut()),
                count: AtomicIsize::new(0),
            },
            retired: RetiredList {
                head: AtomicPtr::new(std::ptr::null_mut()),
                count: AtomicIsize::new(0),
            },
            family: PhantomData,
            sync_time: AtomicU64::new(0),
            nbulk_reclaims: AtomicUsize::new(0),
        }
    }

    pub(crate) fn acquire(&self) -> &HazPtr {
        if let Some(hazptr) = self.try_existing_acquire() {
            hazptr
        } else {
            self.acquire_new()
        }
    }

    fn try_existing_acquire(&self) -> Option<&HazPtr> {
        let head_ptr = &self.hazptrs.head;
        let mut node = head_ptr.load(Ordering::Acquire);
        while (!node.is_null()) {
            let n = unsafe { &*node };
            if n.try_acquire() {
                return Some(n);
            }
            node = n.next.load(Ordering::Relaxed);
        }
        None
    }

    pub(crate) fn acquire_new(&self) -> &HazPtr {
        let hazptr = Box::into_raw(Box::new(HazPtr {
            ptr: AtomicPtr::new(std::ptr::null_mut()),
            next: AtomicPtr::new(std::ptr::null_mut()),
            active: AtomicBool::new(true),
        }));

        let head_ptr = &self.hazptrs.head;
        let mut head = head_ptr.load(Ordering::Acquire);
        loop {
            *unsafe { &mut *hazptr }.next.get_mut() = head;
            match self.hazptrs.head.compare_exchange(
                head,
                hazptr,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.hazptrs.count.fetch_add(1, Ordering::SeqCst);
                    break unsafe { &*hazptr };
                }
                Err(head_now) => head = head_now,
            }
        }
    }

    const fn reached_threshold(rc: isize, hc: isize) -> bool {
        rc >= RCOUNT_THRESHOLD && rc >= HCOUNT_MULTIPLIER * hc
    }

    pub(crate) unsafe fn retire<'domain>(
        &'domain self,
        ptr: *mut (dyn Reclaim + 'domain),
        deleter: &'static dyn Deleter,
    ) {
        let retired = Box::into_raw(Box::new(unsafe { Retired::new(self, ptr, deleter) }));
        crate::asymmetric_light_barrier();
        //    self.retired.count.fetch_add(1, Ordering::SeqCst);
        let head_ptr = &self.retired.head;
        let mut head = head_ptr.load(Ordering::Acquire);
        loop {
            *unsafe { &mut *retired }.next.get_mut() = head;
            match head_ptr.compare_exchange_weak(head, retired, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    break;
                }
                Err(head_now) => head = head_now,
            }
        }

        self.retired.count.fetch_add(1, Ordering::SeqCst);

        self.check_cleanup_and_reclaim();
    }

    fn check_cleanup_and_reclaim(&self) {
        if self.try_timed_cleanup() {
            return;
        }
        if Self::reached_threshold(
            self.retired.count.load(Ordering::Acquire),
            self.hazptrs.count.load(Ordering::Acquire),
        ) {
            self.try_bulk_reclaim();
        }
    }

    fn try_bulk_reclaim(&self) {
        let hc = self.hazptrs.count.load(Ordering::Acquire);
        let rc = self.retired.count.load(Ordering::Acquire);
        if !Self::reached_threshold(rc, hc) {
            return;
        }

        let rc = self.retired.count.swap(0, Ordering::Release);
        if !Self::reached_threshold(rc, hc) {
            return;
        }
        self.bulk_reclaim(false);
    }

    fn try_timed_cleanup(&self) -> bool {
        if !self.check_sync_time() {
            return false;
        }
        self.relaxed_cleanup();
        true
    }

    fn relaxed_cleanup(&self) {
        self.retired.count.store(0, Ordering::Release);
        self.bulk_reclaim(true);
    }

    fn check_sync_time(&self) -> bool {
        use std::convert::TryFrom;
        let time = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time is set to before the epoch")
                .as_nanos(),
        )
        .expect("system time is too far into the future");
        let sync_time = self.sync_time.load(Ordering::Relaxed);
        time > sync_time
            && self
                .sync_time
                .compare_exchange(
                    sync_time,
                    time + SYNC_TIME_PERIOD,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
    }

    pub fn eager_reclaim(&self) -> usize {
        self.bulk_reclaim(true)
    }

    fn bulk_reclaim(&self, transitive: bool) -> usize {
        self.nbulk_reclaims.fetch_add(1, Ordering::Acquire);
        let mut reclaimed = 0;
        loop {
            let steal = self
                .retired
                .head
                .swap(std::ptr::null_mut(), Ordering::SeqCst);
            crate::asymmetric_heavy_barrier(crate::HeavyBarrierKind::Expedited);
            if steal.is_null() {
                return reclaimed;
            }

            //  let mut reclaimed: usize = 0;

            let mut guard_list = HashSet::new();
            let mut node = self.hazptrs.head.load(Ordering::SeqCst);
            while !node.is_null() {
                let n = unsafe { &*node };
                if n.active.load(Ordering::SeqCst) {
                    guard_list.insert(n.ptr.load(Ordering::SeqCst));
                }
                node = n.next.load(Ordering::SeqCst);
            }
            let (reclaimed_now, done) = self.bulk_lookup_and_reclaim(steal, guard_list);
            reclaimed += reclaimed_now;
            if done || transitive {
                break;
            }
        }

        self.nbulk_reclaims.fetch_sub(1, Ordering::Release);
        reclaimed
    }

    fn bulk_lookup_and_reclaim(
        &self,
        stolen_retired_head: *mut Retired,
        guard_list: HashSet<*mut u8>,
    ) -> (usize, bool) {
        let mut node = stolen_retired_head;
        let mut remaining = std::ptr::null_mut();
        let mut tail = None;
        let mut reclaimed: usize = 0;
        let mut still_retired: isize = 0;
        while !node.is_null() {
            //  let current = node;
            let n = unsafe { &*node };
            let next = n.next.load(Ordering::Relaxed);
            debug_assert_ne!(node, next);
            node = n.next.load(Ordering::SeqCst);
            if guard_list.contains(&(n.ptr as *mut u8)) {
                //     n.next.store(remaining, Ordering::SeqCst);
                //     //  remaining = Box::into_raw(n);
                //     remaining = current;
                //     if tail.is_none() {
                //         tail = Some(remaining);
                //     }
                // } else {
                let mut n = unsafe { Box::from_raw(node) };
                unsafe { n.deleter.delete(n.ptr) };
                reclaimed += 1;
            } else {
                n.next.store(remaining, Ordering::Relaxed);
                remaining = node;
                if tail.is_none() {
                    tail = Some(remaining);
                }
                still_retired += 1;
            }
            node = next;
        }
        let done = self.retired.head.load(Ordering::Acquire).is_null();

        let tail = if let Some(tail) = tail {
            assert!(!remaining.is_null());
            tail
        } else {
            assert!(remaining.is_null());
            return (reclaimed, done);
        };

        crate::asymmetric_light_barrier();
        // lt done = self.retired.count.fetch_sub(reclaimed, Ordering::SeqCst);
        let head_ptr = &self.retired.head;
        let mut head = head_ptr.load(Ordering::Acquire);
        loop {
            *unsafe { &mut *tail }.next.get_mut() = head;
            match head_ptr.compare_exchange_weak(
                head,
                remaining,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    break;
                }
                Err(heah_now) => head = heah_now,
            }
        }

        self.retired
            .count
            .fetch_add(still_retired, Ordering::Release);
        (reclaimed, done)
    }
    pub(crate) fn release(&self, hazard: &HazPtr) {
        hazard.release();
    }
}

impl<F> Drop for HazPtrDomain<F> {
    fn drop(&mut self) {
        let nretired = *self.retired.count.get_mut();
        let nreclaimed = self.bulk_reclaim(false);
        //  assert_eq!(nretired, nreclaimed);
        assert!(self.retired.head.get_mut().is_null());

        let mut node = *self.hazptrs.head.get_mut();
        while !node.is_null() {
            let mut n: Box<HazPtr> = unsafe { Box::from_raw(node) };
            assert!(!*n.active.get_mut());
            node = *n.next.get_mut();
            drop(n);
        }
    }
}

pub struct HazPtrs {
    head: AtomicPtr<HazPtr>,
    count: AtomicIsize,
}

struct Retired {
    ptr: *mut dyn Reclaim,
    deleter: &'static dyn Deleter,
    next: AtomicPtr<Retired>,
}

impl Retired {
    unsafe fn new<'domain, F>(
        _: &'domain HazPtrDomain<F>,
        ptr: *mut (dyn Reclaim + 'domain),
        deleter: &'static dyn Deleter,
    ) -> Self {
        Retired {
            ptr: unsafe { { std::mem::transmute::<_, *mut (dyn Reclaim + 'static)>(ptr) } },
            deleter,
            next: AtomicPtr::new(std::ptr::null_mut()),
        }
    }
}

struct RetiredList {
    head: AtomicPtr<Retired>,
    count: AtomicIsize,
}

/// ```compile_fail
/// use std::sync::atomic::AtomicPtr;
/// use haphazard::*;
/// let dw = HazPtrDomain::global();
/// let dr = HazPtrDomain::new(&());
///
/// let x = AtomicPtr::new(Box::into_raw(Box::new(HazPtrObjectWrapper::with_domain(&dw, 42))));
///
/// // Reader uses a different domain thant the writer!
/// let mut h = HazPtrHolder::for_domain(&dr);
///
/// // This shouldn't compile because families differ.
/// let _ = unsafe { h.load(&x) }.expect("not null");
/// ```
#[cfg(doctest)]
struct CannotConfuseGlobalWriter;

/// ```compile_fail
/// use std::sync::atomic::AtomicPtr;
/// use haphazard::*;
/// let dw = HazPtrDomain::new(&());
/// let dr = HazPtrDomain::global();
///
/// let x = AtomicPtr::new(Box::into_raw(Box::new(HazPtrObjectWrapper::with_domain(&dw, 42))));
///
/// // Reader uses a different domain thant the writer!
/// let mut h = HazPtrHolder::for_domain(&dr);
///
/// // This shouldn't compile because families differ.
/// let _ = unsafe { h.load(&x) }.expect("not null");
/// ```
#[cfg(doctest)]
struct CannotConfuseGlobalReader;
