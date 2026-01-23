FROM rust:1-trixie AS pmix

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install --no-install-recommends -y \
        build-essential flex libhwloc-dev

WORKDIR /workspaces/openpmix
RUN git clone --recurse-submodules --branch=v5.0.7 https://github.com/openpmix/openpmix.git .

RUN <<EOF
set -euo pipefail

./autogen.pl
./configure --prefix=/usr/local CFLAGS="-g -O0"
make -j install
EOF

FROM pmix

RUN --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    rustup component add rustfmt

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install --no-install-recommends -y \
        kubectl libclang-dev lldb
