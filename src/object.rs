use crate::Domain;
use crate::{Deleter, Reclaim};
use std::ops::{Deref, DerefMut};

pub trait HazPtrObject<'domain, F:'static>
where
    Self: Sized + 'domain,
{
    fn domain(&self) -> &'domain Domain<F>;
    unsafe fn retire(ptr: *mut Self, deleter: &'static dyn Deleter) {
        let reclaim_ptr = ptr as *mut (dyn Reclaim + 'domain);
        unsafe { (&*ptr).domain().retire(reclaim_ptr, deleter) };
    }
}

pub struct HazPtrObjectWrapper<'domain, T,F> {
    inner: T,
    domain: &'domain Domain<F>,
}

impl<'domain, T> HazPtrObjectWrapper<'domain, T,crate::Global> {
    pub fn with_global_domain(t: T) -> Self {
        HazPtrObjectWrapper::with_domain(Domain::global(), t)
    }
}

impl<'domain, T,F> HazPtrObjectWrapper<'domain, T,F> {
    pub fn with_domain(domain: &'domain Domain<F>, t: T) -> Self {
        Self {
            inner: t,
            domain: domain,
        }
    }
}

impl<'domain, T: 'domain, F:'static> HazPtrObject<'domain,F> for HazPtrObjectWrapper<'domain, T,F> {
    fn domain(&self) -> &'domain Domain<F> {
        self.domain
    }
    // unsafe fn retire(ptr: *mut Self, deleter: &'static dyn Deleter) {
    //     HazPtrObject::retire(ptr, deleter);
    // }
}

// impl<T> Drop for HazPtrObjectWrapper<T> {
//     fn drop(&mut self) {}
// }

impl<T,F> Deref for HazPtrObjectWrapper<'_, T,F> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T,F> DerefMut for HazPtrObjectWrapper<'_, T,F> {
    //  type Target = &mut T;
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
