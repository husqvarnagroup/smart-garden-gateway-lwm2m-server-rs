// No-op stubs for _Unwind_* symbols required by std's backtrace code on MIPS.
// With panic=abort these are never called. Defining them here (compiled by
// rustc as soft-float per the mipsel-unknown-linux-gnu target spec) lets the
// linker satisfy -lgcc_s without adding the system's hard-float libgcc_s.so.1
// to DT_NEEDED.

use core::ffi::c_void;

#[unsafe(no_mangle)]
unsafe extern "C" fn _Unwind_Backtrace(
    _trace: Option<unsafe extern "C" fn(*mut c_void, *mut c_void) -> i32>,
    _arg: *mut c_void,
) -> i32 {
    0
}

#[unsafe(no_mangle)]
unsafe extern "C" fn _Unwind_GetIP(_ctx: *mut c_void) -> usize {
    0
}

#[unsafe(no_mangle)]
unsafe extern "C" fn _Unwind_GetCFA(_ctx: *mut c_void) -> usize {
    0
}

#[unsafe(no_mangle)]
unsafe extern "C" fn _Unwind_FindEnclosingFunction(_pc: *mut c_void) -> *mut c_void {
    core::ptr::null_mut()
}
