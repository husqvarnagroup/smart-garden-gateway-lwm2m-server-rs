// Only with panic=abort; with panic=unwind the real unwinder (libgcc_s) is
// linked and these stubs would shadow it.
#[cfg(all(target_arch = "mips", panic = "abort"))]
mod unwind_stubs;

pub mod config;
pub mod error;
pub mod housekeeping;
pub mod ipc;
pub mod logging;
pub mod lwm2m;
pub mod model;
pub mod persistence;
pub mod registry;
