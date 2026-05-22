#!/usr/bin/env bash
# Build and optionally deploy the MIPS binary.
#
# Usage:
#   ./build-mips.sh                     # build only
#   ./build-mips.sh root@192.168.1.x    # build + deploy via scp
#
# The Debian mipsel-linux-gnu cross toolchain only ships a hard-float (FPXX)
# sysroot — there is no soft-float multilib.  The gateway's glibc userspace is
# soft-float, so after the cross build we patch two bytes in the binary to
# declare the correct FP ABI:
#   - PT_MIPS_ABIFLAGS.fp_abi   (read by glibc's ld.so at runtime)
#   - .gnu.attributes tag 4     (informational; patched for consistency)
# Both change FPXX (5) -> Soft float (3).  The Rust code itself is compiled
# soft-float via target-feature=+soft-float; only Scrt1.o from the toolchain
# carries the wrong attribute, and it contains no actual FP instructions.

set -euo pipefail

TARGET=mipsel-unknown-linux-gnu
BIN=target/$TARGET/release/lwm2mserver-rs

cross +nightly build --release --target $TARGET

python3 - "$BIN" <<'PYEOF'
import struct, sys

PT_MIPS_ABIFLAGS = 0x70000003
FP_SOFT = 3
fp_map = {0:'any', 1:'dbl-HW', 2:'sgl-HW', 3:'soft', 4:'old64', 5:'FPXX', 6:'FP64', 7:'FP64A'}

path = sys.argv[1]
with open(path, 'rb') as f:
    data = bytearray(f.read())

def patch(offset, val, label):
    before = data[offset]
    if before != val:
        data[offset] = val
        print(f'  patched {label}: {before} ({fp_map.get(before,"?")}) -> {val} ({fp_map.get(val,"?")})')
    else:
        print(f'  {label} already {val} ({fp_map.get(val,"?")}), no change')

# Patch PT_MIPS_ABIFLAGS.fp_abi
e_phoff = struct.unpack_from('<I', data, 28)[0]
e_phentsize = struct.unpack_from('<H', data, 42)[0]
e_phnum = struct.unpack_from('<H', data, 44)[0]
for i in range(e_phnum):
    p_type, p_offset = struct.unpack_from('<II', data, e_phoff + i*e_phentsize)
    if p_type == PT_MIPS_ABIFLAGS:
        patch(p_offset + 7, FP_SOFT, 'PT_MIPS_ABIFLAGS.fp_abi')
        break

# Patch .gnu.attributes Tag_GNU_MIPS_ABI_FP (byte 15 of section data)
e_shoff = struct.unpack_from('<I', data, 32)[0]
e_shentsize = struct.unpack_from('<H', data, 46)[0]
e_shnum = struct.unpack_from('<H', data, 48)[0]
e_shstrndx = struct.unpack_from('<H', data, 50)[0]
shstr_sh = data[e_shoff + e_shstrndx*e_shentsize : e_shoff + e_shstrndx*e_shentsize + e_shentsize]
stroff, strsize = struct.unpack_from('<II', shstr_sh, 16)
strtab = data[stroff:stroff+strsize]
for i in range(e_shnum):
    sh = data[e_shoff + i*e_shentsize : e_shoff + i*e_shentsize + e_shentsize]
    nameoff = struct.unpack_from('<I', sh, 0)[0]
    name = strtab[nameoff:strtab.index(b'\x00', nameoff)].decode()
    if name == '.gnu.attributes':
        sec_offset = struct.unpack_from('<I', sh, 16)[0]
        patch(sec_offset + 15, FP_SOFT, '.gnu.attributes Tag_GNU_MIPS_ABI_FP')
        break

with open(path, 'wb') as f:
    f.write(data)
print(f'Done: {path}')
PYEOF

if [ -n "${1:-}" ]; then
    echo "Deploying to $1..."
    scp -O "$BIN" "$1:/usr/local/bin/lwm2mserver-rs"
    echo "Deployed."
fi
