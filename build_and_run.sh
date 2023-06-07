#!/bin/bash
set -euo pipefail

# if run with `down` as first argument, stop the stack
if [ "$1" = "down" ]; then
    docker compose --file ./stacks/dev.docker-compose.yml down
    exit 0
fi

# check if zig is installed if not error out and point to ziglang.org
if ! command -v zig &> /dev/null
then
    echo "zig could not be found"
    echo "see https://ziglang.org/learn/getting-started/#tagged-release-or-nightly-build for installation instructions"
    exit 1
fi

# check if cargo-binstall is installed
if ! command -v cargo-binstall &> /dev/null
then
    echo "cargo-binstall could not be found"
    echo "installing cargo-binstall"
    curl -L --proto '=https' --tlsv1.2 -sSf\
     https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash
fi

# add musl toolchain to rustup if not already installed
if ! rustup target list | grep -q x86_64-unknown-linux-musl
then
    rustup target add x86_64-unknown-linux-musl
fi

# install cargo deps
cargo binstall --no-confirm --no-symlinks cargo-deb cargo-zigbuild

# first build nexus
pushd nexus
cargo zigbuild --release --target=x86_64-unknown-linux-musl
popd

# if run with `up` as first argument, start the stack
if [ "$1" = "up" ]; then
    docker compose --file ./stacks/dev.docker-compose.yml up
fi
