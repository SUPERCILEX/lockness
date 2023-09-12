#![feature(ptr_from_ref)]
#![feature(pointer_is_aligned)]
#![feature(cell_update)]

pub use closure_vec::ClosureVec;
pub use freestanding_vec::FreestandingVec;

mod closure_vec;
mod freestanding_vec;
