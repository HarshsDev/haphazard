use crate::{HazPtrRecord, Domain, HazPtrObject};
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;

pub struct HazPtrHolder<'domain, F> {
    hazard: &'domain HazPtrRecord,
    domain: &'domain Domain<F>,
}


impl HazPtrHolder<'static, crate::Global> {
    pub fn global() -> Self {
        HazPtrHolder::for_domain(Domain::global())
    }
}

impl<'domain, F> HazPtrHolder<'domain, F> {
    pub fn for_domain(domain: &'domain Domain<F>) -> Self {
        Self {
            hazard: domain.acquire(),
            domain,
        }
    }
    // fn hazptr(&mut self) -> &'domain HazPtr {
    //     if let Some(hazptr) = self.hazard {
    //         hazptr
    //     } else {
    //         let hazptr = HazPtrDomain::global().acquire();
    //         self.hazard = Some(hazptr);
    //         hazptr
    //     }
    // }

    pub unsafe fn protect<'l, 'o: 'l, T>(&'l mut self, src: &'_ AtomicPtr<T>) -> Option<&'l T>
    where
        T: HazPtrObject<'o, F>,
        F: 'static,
    {
        //  let hazptr = self.hazptr();
        let mut ptr = src.load(Ordering::SeqCst);
        loop {
            //  let r =
            match unsafe {self.try_protect(ptr, src)} {
                Ok(None) => break None,
                Ok(Some(r)) => break Some(unsafe {&*(r as *const _)}),
                Err(ptr2) => {
                    ptr = ptr2;
                }
            }
        }
    }

    pub unsafe fn try_protect<'l, 'o, T>(
        &'l mut self,
        ptr: *mut T,
        src: &'_ AtomicPtr<T>,
    ) -> Result<Option<&'l T>, *mut T>
    where
        T: HazPtrObject<'o, F>,
        'o: 'l,
        F: 'static,
    {
        self.hazard.protect(ptr as *mut u8);
        crate::asymmetric_light_barrier();
        let ptr2 = src.load(Ordering::Acquire);
        if ptr != ptr2 {
            self.hazard.reset();
            Err(ptr2)
        } else {
            Ok(std::ptr::NonNull::new(ptr).map(|nn| {
                let r = unsafe { nn.as_ref() };
                debug_assert_eq!(
                    self.domain as *const Domain<F>,
                    r.domain() as *const Domain<F>,
                    "Object guarded by different domain than holder used to access it",
                );
                r
            }))
        }
    }

    pub fn reset_protection(&mut self) {
        self.hazard.reset();
    }
}

impl<F> Drop for HazPtrHolder<'_, F> {
    fn drop(&mut self) {
    self.hazard.reset();
    self.domain.release(self.hazard);
    }
}
