use crate::{Deleter, HazPtrRecord, HeavyBarrierKind, Reclaim};
use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicPtr, AtomicUsize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::u8;

const SYNC_TIME_PERIOD: u64 = std::time::Duration::from_nanos(2000000000).as_nanos() as u64;
const RCOUNT_THRESHOLD: isize = 1000;
const HCOUNT_MULTIPLIER: isize = 2;
const NUM_SHARDS: usize = 8;
const IGNORED_LOW_BITS: u8 = 8;
const SHARD_MASK: usize = NUM_SHARDS - 1;

pub struct Domain<F> {
    hazptrs: HazPtrRecords,
    untagged: [RetiredList; NUM_SHARDS],
    family: PhantomData<F>,
    due_time: AtomicU64,
    nbulk_reclaims: AtomicUsize,
    count: AtomicIsize,
    shutdown: bool,
}

#[non_exhaustive]
pub struct Global;
impl Global {
    const fn new() -> Self {
        Global
    }
}

static SHARED_DOMAIN: Domain<Global> = Domain::new(&Global::new());

impl Domain<Global> {
    pub fn global() -> &'static Self {
        &SHARED_DOMAIN
    }
}

#[macro_export]
macro_rules! unique_domain {
    () => {
        Domain::new(&|| {})
    };
}

impl<F> Domain<F> {
    pub const fn new(_: &F) -> Self {
        const RETIRED_LIST: RetiredList = RetiredList::new();
        Self {
            hazptrs: HazPtrRecords {
                head: AtomicPtr::new(std::ptr::null_mut()),
                count: AtomicIsize::new(0),
            },
            untagged: [RETIRED_LIST; NUM_SHARDS],
            family: PhantomData,
            due_time: AtomicU64::new(0),
            nbulk_reclaims: AtomicUsize::new(0),
            count: AtomicIsize::new(0),
            shutdown: false,
        }
    }

    pub(crate) fn acquire(&self) -> &HazPtrRecord {
        if let Some(hazptr) = self.try_acquire_existing() {
            hazptr
        } else {
            self.acquire_new()
        }
    }

    fn try_acquire_existing(&self) -> Option<&HazPtrRecord> {
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

    pub(crate) fn acquire_new(&self) -> &HazPtrRecord {
        let hazptr = Box::into_raw(Box::new(HazPtrRecord {
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
        let retired = Box::new(unsafe { Retired::new(self, ptr, deleter) });
        self.push_list(retired);
    }

    fn push_list(&self, mut retired: Box<Retired>) {
        assert!(
            retired.next.get_mut().is_null(),
            "only single item retiring is suported atm"
        );
        let retired = Box::into_raw(retired);
        unsafe {
            self.untagged[Self::calc_shard(retired)].push(retired, retired);
        }
        self.count.fetch_add(1, Ordering::Release);
        self.check_threshold_and_reclaim();
    }

    fn check_threshold_and_reclaim(&self) {
        let mut rcount = self.check_count_threshold();
        if rcount == 0 {
            rcount = self.check_due_time();
            if rcount == 0 {
                return;
            }
        }

        self.nbulk_reclaims.fetch_add(1, Ordering::Acquire);
        self.do_reclamation(rcount);
    }

    fn check_count_threshold(&self) -> isize {
        let rcount = self.count.load(Ordering::Acquire);
        while rcount > self.threshold() {
            if self
                .count
                .compare_exchange_weak(rcount, 0, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                self.due_time
                    .store(Self::now() + SYNC_TIME_PERIOD, Ordering::Release);
                return rcount;
            }
        }
        0
    }

    fn threshold(&self) -> isize {
        RCOUNT_THRESHOLD.max(HCOUNT_MULTIPLIER * self.hazptrs.count.load(Ordering::Acquire))
    }

    fn now() -> u64 {
        use std::convert::TryFrom;
        let time = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time is set to before the epoch")
                .as_nanos(),
        )
        .expect("system time is too far into the future");
        time
    }

    fn check_due_time(&self) -> isize {
        let time = Self::now();
        let due = self.due_time.load(Ordering::Relaxed);
        if time < due
            || self
                .due_time
                .compare_exchange(
                    due,
                    time + SYNC_TIME_PERIOD,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_err()
        {
            return 0;
        }
        self.count.swap(0, Ordering::AcqRel)
    }

    pub fn eager_reclaim(&self) -> usize {
        let rcount = self.count.swap(0, Ordering::AcqRel);
        self.nbulk_reclaims.fetch_add(1, Ordering::Acquire);
        self.do_reclamation(rcount)
        // self.bulk_reclaim(true)
    }

    fn do_reclamation(&self, mut rcount: isize) -> usize {
        let mut total_reclaimed = 0;
        loop {
            let mut done = true;
            let mut stolen_heads = [std::ptr::null_mut(); NUM_SHARDS];
            let mut empty = true;
            for i in 0..NUM_SHARDS {
                stolen_heads[i] = self.untagged[i].pop_all();
                if !stolen_heads[i].is_null() {
                    empty = false;
                }
            }
            if !empty {
                crate::asymmetric_heavy_barrier(crate::HeavyBarrierKind::Expedited);

                #[allow(clippy::mutable_key_type)]
                let mut guarded_ptr = HashSet::new();
                let mut node = self.hazptrs.head.load(Ordering::Acquire);
                while (!node.is_null()) {
                    let n = unsafe { &*node };
                    if n.active.load(Ordering::SeqCst) {
                        guarded_ptr.insert(n.ptr.load(Ordering::Acquire));
                    }
                    node = n.next.load(Ordering::Relaxed);
                }
                let (nreclaimed, is_done) = self.match_reclaim_untagged(stolen_heads, &guarded_ptr);
                done = is_done;

                // let mut node = stolen_head;
                // let mut reclaimable = std::ptr::null_mut();
                // let mut unreclaimed = std::ptr::null_mut();
                // let mut unreclaimed_tail = unreclaimed;
                // let mut nreclaimable: isize = 0;
                // while !node.is_null() {
                //     let n = unsafe { &*node };
                //     let next = n.next.load(Ordering::Relaxed);
                //     debug_assert_ne!(node, next);
                //     if !guarded_ptr.contains(&(n.ptr as *mut u8)) {
                //         n.next.store(reclaimable, Ordering::Relaxed);
                //         reclaimable = node;
                //         nreclaimable += 1;
                //     } else {
                //         n.next.store(unreclaimed, Ordering::Relaxed);
                //         unreclaimed = node;
                //         if unreclaimed_tail.is_null() {
                //             unreclaimed_tail = unreclaimed;
                //         }
                //     }
                //     node = next;
                // }

                // unsafe {
                //     self.reclaim_unprotected(reclaimable);
                // }
                // done = self.untagged.is_empty();
                // unsafe {
                //     self.untagged.push(unreclaimed, unreclaimed_tail);
                // }
                rcount -= nreclaimed as isize;
                total_reclaimed += nreclaimed;
            }
            if rcount != 0 {
                self.count.fetch_add(rcount, Ordering::Release);
            }
            rcount = self.check_count_threshold();
            if rcount == 0 && done {
                break;
            }
        }
        self.nbulk_reclaims.fetch_add(1, Ordering::Acquire);
        total_reclaimed
    }

    fn match_reclaim_untagged(
        &self,
        stolen_heads: [*mut Retired; NUM_SHARDS],
        guarded_ptrs: &HashSet<*mut u8>,
    ) -> (usize, bool) {
        let mut unreclaimed = std::ptr::null_mut();
        let mut unreclaimed_tail = unreclaimed;
        let mut nreclaimed = 0;

        for i in 0..NUM_SHARDS {
            let mut node = stolen_heads[i];
            let mut reclaimable = std::ptr::null_mut();
            while !node.is_null() {
                let n = unsafe { &*node };
                let next = n.next.load(Ordering::Relaxed);
                debug_assert_ne!(node, next);
                if !guarded_ptrs.contains(&(n.ptr as *mut u8)) {
                    n.next.store(reclaimable, Ordering::Relaxed);
                    reclaimable = node;
                    nreclaimed += 1;
                } else {
                    n.next.store(unreclaimed, Ordering::Relaxed);
                    unreclaimed = node;
                    if unreclaimed_tail.is_null() {
                        unreclaimed_tail = unreclaimed;
                    }
                }
                node = next;
            }
            unsafe{self.reclaim_unprotected(reclaimable)};
        }
            let done = self.untagged.iter().all(|u| u.is_empty());
            unsafe{self.untagged[0].push(unreclaimed, unreclaimed_tail)};
            (nreclaimed, done)
        
    }

    fn reclaim_all_objects(&self) {
        for i in 0..NUM_SHARDS {
        let head = self.untagged[i].pop_all();
        unsafe { self.reclaim_list_transitive(head) };
        }
    }

    unsafe fn reclaim_list_transitive(&self, head: *mut Retired) {
        unsafe { self.reclaim_unconditional(head) };
    }

    unsafe fn reclaim_unconditional(&self, head: *mut Retired) {
        unsafe { self.reclaim_unprotected(head) };
    }
    unsafe fn reclaim_unprotected(&self, mut retired: *mut Retired) {
        //  let mut node = unsafe{retired};
        while !retired.is_null() {
            let next = unsafe { &mut *retired }.next.load(Ordering::Relaxed);
            let n = unsafe { Box::from_raw(retired) };
            // let free = unsafe { Box::from_raw(retired)};
            unsafe { n.deleter.delete(n.ptr) };
            retired = next;
        }
    }
    fn free_hazptr_recs(&mut self) {
        let mut node: *mut HazPtrRecord = *self.hazptrs.head.get_mut();
        while !node.is_null() {
            let mut n: Box<HazPtrRecord> = unsafe { Box::from_raw(node) };
            debug_assert!(!*n.active.get_mut());
            node = *n.next.get_mut();
            drop(n);
        }
    }

    fn calc_shard(input: *mut Retired) -> usize {
        (input as usize >> IGNORED_LOW_BITS) & SHARD_MASK
    }
    pub(crate) fn release(&self, hazard: &HazPtrRecord) {
        hazard.release();
    }
}

impl<F> Drop for Domain<F> {
    fn drop(&mut self) {
        self.shutdown = true;
        self.reclaim_all_objects();
        self.free_hazptr_recs();
    }
}

pub struct HazPtrRecords {
    head: AtomicPtr<HazPtrRecord>,
    count: AtomicIsize,
}

struct Retired {
    ptr: *mut dyn Reclaim,
    deleter: &'static dyn Deleter,
    next: AtomicPtr<Retired>,
}

impl Retired {
    unsafe fn new<'domain, F>(
        _: &'domain Domain<F>,
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
}

impl RetiredList {
    const fn new() -> Self {
        Self {
            head: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    unsafe fn push(&self, sublist_head: *mut Retired, sublist_tail: *mut Retired) {
        if sublist_head.is_null() {
            return;
        }

        let head_ptr = &self.head;
        let mut head = head_ptr.load(Ordering::Acquire);
        loop {
            unsafe { &*sublist_tail }
                .next
                .store(head, Ordering::Release);
            match head_ptr.compare_exchange_weak(
                head,
                sublist_head,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(head_now) => head = head_now,
            }
        }
    }

    fn pop_all(&self) -> *mut Retired {
        self.head.swap(std::ptr::null_mut(), Ordering::Acquire)
    }

    fn is_empty(&self) -> bool {
        self.head.load(Ordering::Relaxed).is_null()
    }
}
// /// ```compile_fail
// /// use std::sync::atomic::AtomicPtr;
// /// use haphazard::*;
// /// let dw = HazPtrDomain::global();
// /// let dr = HazPtrDomain::new(&());
// ///
// /// let x = AtomicPtr::new(Box::into_raw(Box::new(HazPtrObjectWrapper::with_domain(&dw, 42))));
// ///
// /// // Reader uses a different domain thant the writer!
// /// let mut h = HazPtrHolder::for_domain(&dr);
// ///
// /// // This shouldn't compile because families differ.
// /// let _ = unsafe { h.load(&x) }.expect("not null");
// /// ```
// #[cfg(doctest)]
// struct CannotConfuseGlobalWriter;

// /// ```compile_fail
// /// use std::sync::atomic::AtomicPtr;
// /// use haphazard::*;
// /// let dw = HazPtrDomain::new(&());
// /// let dr = HazPtrDomain::global();
// ///
// /// let x = AtomicPtr::new(Box::into_raw(Box::new(HazPtrObjectWrapper::with_domain(&dw, 42))));
// ///
// /// // Reader uses a different domain thant the writer!
// /// let mut h = HazPtrHolder::for_domain(&dr);
// ///
// /// // This shouldn't compile because families differ.
// /// let _ = unsafe { h.load(&x) }.expect("not null");
// /// ```
// #[cfg(doctest)]
// struct CannotConfuseGlobalReader;
