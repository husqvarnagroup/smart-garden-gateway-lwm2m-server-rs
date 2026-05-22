fn main() {
    // On MIPS (MT7688 gateway), the system userspace is soft-float, but the only
    // libgcc_s.so.1 on the device is hard-float — a mismatch the dynamic linker rejects.
    // With panic=abort, _Unwind_* is never actually called, so we:
    //   1. Provide no-op stubs in src/unwind_stubs.rs (compiled by rustc for the correct ABI)
    //   2. Intercept the linker's -lgcc_s lookup with a dummy linker script that satisfies
    //      the search without adding libgcc_s.so.1 to DT_NEEDED.
    if std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() == Ok("mips") {
        let out = std::env::var("OUT_DIR").unwrap();

        // An empty linker script: the linker finds this file when resolving -lgcc_s
        // (OUT_DIR is searched first) and processes it as a no-op, so no shared library
        // reference is emitted.
        std::fs::write(
            format!("{out}/libgcc_s.so"),
            "/* stub — symbols provided by unwind_stubs.rs */\n",
        )
        .expect("write libgcc_s.so linker script");

        println!("cargo:rustc-link-search=native={out}");
    }
}
