use crate::domain::Global;
use crate::{HazPtr, HazPtrDomain, HazPtrObject};
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;

pub struct HazPtrHolder<'domain, F> {
    hazard: Option<&'domain HazPtr>,
    domain: &'domain HazPtrDomain<F>,
}

impl HazPtrHolder<'static, crate::Global> {
    pub fn global() -> Self {
        HazPtrHolder::for_domain(HazPtrDomain::global())
    }
}

impl<'domain,F> HazPtrHolder<'domain,F> {
    pub fn for_domain(domain: &'domain HazPtrDomain<F>) -> Self {
        Self {
            hazard: None,
            domain,
        }
    }
    fn hazptr(&mut self) -> &'domain HazPtr {
        if let Some(hazptr) = self.hazard {
            hazptr
        } else {
            let hazptr = HazPtrDomain::global().acquire();
            self.hazard = Some(hazptr);
            hazptr
        }
    }

    pub unsafe fn load<'l, 'o: 'l, T>(&'l mut self, ptr: &'_ AtomicPtr<T>) -> Option<&'l T>
    where
        T: HazPtrObject<'o,F>,
        F:'static
    {
        let hazptr = self.hazptr();
        let mut ptr1 = ptr.load(Ordering::SeqCst);
        loop {
            //  let r =
            hazptr.protect(ptr1 as *mut u8);
            let ptr2 = ptr.load(Ordering::SeqCst);
            if ptr1 == ptr2 {
                break std::ptr::NonNull::new(ptr1).map(|nn| {
                    let r = unsafe { nn.as_ref() };
                    debug_assert_eq!(
                        self.domain as *const HazPtrDomain<F>,
                        r.domain() as *const HazPtrDomain<F>,
                        "object guareded by diff domain than used to access"
                    );
                    r
                });
            } else {
                ptr1 = ptr2
            }
        }
    }

    pub fn reset(&mut self) {
        if let Some(hazptr) = self.hazard {
            hazptr.ptr.store(std::ptr::null_mut(), Ordering::SeqCst);
        }
    }
}

impl<F> Drop for HazPtrHolder<'_,F> {
    fn drop(&mut self) {
        self.reset();

        if let Some(hazptr) = self.hazard {
            hazptr.active.store(false, Ordering::SeqCst);
        }
    }
}
