use crate::domain::Global;
use crate::{HazPtr, HazPtrDomain, HazPtrObject};
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;

pub struct HazPtrHolder<'domain, F> {
    hazard: &'domain HazPtr,
    domain: &'domain HazPtrDomain<F>,
}

macro_rules! try_protect_actual {
    ($self: ident, $ptr: ident, $src: ident) => {{
        $self.hazard.protect($ptr as *mut u8);
        crate::asymmetric_light_barrier();
        let ptr2 = $src.load(Ordering::Acquire);
        if $ptr != ptr2 {
            $self.hazard.reset();
            Err(ptr2)
        } else {
            Ok(std::ptr::NonNull::new($ptr).map(|nn| {
                let r = unsafe { nn.as_ref() };
                debug_assert_eq!(
                    $self.domain as *const HazPtrDomain<F>,
                    r.domain() as *const HazPtrDomain<F>,
                    "object guarded by diff domain than holder used to access it."
                );
                r
            }))
        }
    }};
}
impl HazPtrHolder<'static, crate::Global> {
    pub fn global() -> Self {
        HazPtrHolder::for_domain(HazPtrDomain::global())
    }
}

impl<'domain, F> HazPtrHolder<'domain, F> {
    pub fn for_domain(domain: &'domain HazPtrDomain<F>) -> Self {
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
            match try_protect_actual!(self, ptr, src) {
                Ok(r) => break r,
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
        try_protect_actual!(self,ptr,src)
    }

    pub fn reset(&mut self) {
        self.hazard.reset();
    }
}

impl<F> Drop for HazPtrHolder<'_, F> {
    fn drop(&mut self) {
    self.hazard.reset();
    self.domain.release(self.hazard);
    }
}
