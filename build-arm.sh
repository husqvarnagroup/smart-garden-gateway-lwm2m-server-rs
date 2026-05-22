#!/usr/bin/env bash
# Build and optionally deploy the ARM binary.
#
# Usage:
#   ./build-arm.sh                     # build only
#   ./build-arm.sh root@192.168.1.x    # build + deploy via scp

set -euo pipefail

TARGET=armv5te-unknown-linux-gnueabi
BIN=target/$TARGET/release/lwm2mserver-rs

cross build --release --target $TARGET

if [ -n "${1:-}" ]; then
    echo "Deploying to $1..."
    scp -O "$BIN" "$1:/usr/local/bin/lwm2mserver-rs"
    echo "Deployed."
fi
