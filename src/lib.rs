#![deny(unsafe_op_in_unsafe_fn)]
#![allow(dead_code)]
mod deleter;
mod domain;
mod holder;
mod object;
mod ptr;

pub use deleter::{Deleter, Reclaim, deleters};
pub use domain::Global;
pub use domain::HazPtrDomain;
pub use holder::HazPtrHolder;
pub use object::{HazPtrObject, HazPtrObjectWrapper};
pub use ptr::HazPtr;

fn asymmetric_light_barrier() {
    std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
}

enum HeavyBarrierKind {
    Normal,
    Expedited,
}

fn asymmetric_heavy_barrier(_: HeavyBarrierKind) {
    std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
}