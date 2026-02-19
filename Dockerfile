FROM fedora:43 AS base

COPY <<EOF /etc/dnf/dnf.conf
[main]
tsflags=nodocs
keepcache=True
install_weak_deps=False
EOF

RUN --mount=type=cache,target=/var/cache/libdnf5 \
    dnf install -y @development-tools hwloc-devel clang-devel

FROM base AS ompi

RUN --mount=type=cache,target=/var/cache/libdnf5 \
    dnf install -y flex perl-File-Find autoconf libtool libevent-devel

ENV PKG_CONFIG_PATH=/usr/local/lib/pkgconfig

WORKDIR /workspaces/pmix
RUN git clone --depth=1 --recurse-submodules --branch=v5.0.9 https://github.com/openpmix/openpmix.git .

RUN <<EOF
set -euo pipefail

./autogen.pl
./configure --prefix=/usr/local CFLAGS="-g -O0"
make -j install
EOF

WORKDIR /workspaces/prrte
RUN git clone --depth=1 --recurse-submodules --branch=v3.0.13 https://github.com/openpmix/prrte.git .

RUN <<EOF
set -euo pipefail

./autogen.pl
./configure --with-pmix=/usr/local/ --prefix=/usr/local CFLAGS="-g -O0"
make -j install
EOF

WORKDIR /workspaces/open-mpi
RUN <<EOF
set -euo pipefail

git clone --depth=1 --branch=v5.0.9 https://github.com/open-mpi/ompi.git .
git submodule update --init --recursive -- config/oac
EOF

RUN <<EOF
set -euo pipefail

./autogen.pl --no-3rdparty=pmix,prrte
./configure --with-pmix=external --with-prrte=external --prefix=/usr/local CFLAGS="-g -O0"
make -j install
EOF

FROM base AS dev

ENV PATH=/root/.cargo/bin:$PATH PKG_CONFIG_PATH=/usr/local/lib/pkgconfig

RUN --mount=type=cache,target=/var/cache/libdnf5 <<EOF
set -euo pipefail

dnf install -y rustup
rustup-init -y --no-modify-path --profile minimal --default-toolchain stable
rustup component add rustfmt clippy
EOF

RUN --mount=type=cache,target=/var/cache/libdnf5 \
    dnf install -y kubectl lldb

COPY --link --from=ompi /usr/local /usr/local
COPY --link --from=ompi /workspaces/pmix /workspaces/pmix
COPY --link --from=ompi /workspaces/prrte /workspaces/prrte
COPY --link --from=ompi /workspaces/open-mpi /workspaces/open-mpi

FROM dev AS build

WORKDIR /workspaces/pmi-k8s

COPY --link . .
RUN --mount=type=cache,target=/workspaces/pmi-k8s/target \
    --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --bin pmi-k8s --release \
    && cp target/release/pmi-k8s /usr/local/bin

FROM fedora:43

COPY --link --from=build /usr/local/bin/pmi-k8s /usr/local/bin/pmi-k8s
