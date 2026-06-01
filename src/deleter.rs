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
    use super::Reclaim;

    unsafe fn drop_in_place2(ptr: *mut dyn Reclaim) {
        unsafe { std::ptr::drop_in_place(ptr) };
    }
    #[allow(non_upper_case_globals)]
    pub const drop_in_place: unsafe fn(*mut dyn Reclaim) = drop_in_place2;

    unsafe fn drop_box2(ptr: *mut dyn Reclaim) {
        let _ = unsafe { Box::from(ptr) };
    }

    #[allow(non_upper_case_globals)]
    pub const drop_box: unsafe fn(*mut dyn Reclaim) = drop_box2;
}
