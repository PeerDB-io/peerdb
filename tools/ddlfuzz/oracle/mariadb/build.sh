#!/usr/bin/env bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
BUILD=$(cd "$HERE/../../build" && pwd)
SRC_RO=$HOME/Code/mariadb-server
COMMIT=c3ec2dc368a8c7165cdbea58208eb828e76ebc57
REPO_ROOT=$(cd "$HERE/../../../.." && pwd)

CCACHE=()
if ccache --version >/dev/null 2>&1; then
  export CCACHE_BASEDIR="$REPO_ROOT" CCACHE_NOHASHDIR=1
  CCACHE=(-DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache)
else
  echo "note: ccache not runnable; building without compile cache" >&2
fi

if [ ! -x "$BUILD/hostdeps/bin/bison" ]; then
  mkdir -p "$BUILD/hostdeps/src"
  cd "$BUILD/hostdeps/src"
  if [ ! -f bison-3.8.2.tar.gz ]; then
    curl -fsSLO https://ftp.gnu.org/gnu/bison/bison-3.8.2.tar.gz
  fi
  if [ ! -d bison-3.8.2 ]; then
    tar xzf bison-3.8.2.tar.gz
  fi
  cd bison-3.8.2
  ./configure --prefix="$BUILD/hostdeps"
  make -j6
  make install
fi

if [ ! -d "$BUILD/mariadb-src/.git" ]; then
  git clone --shared --no-checkout "$SRC_RO" "$BUILD/mariadb-src"
  git -C "$BUILD/mariadb-src" checkout --detach "$COMMIT"
  git -C "$BUILD/mariadb-src" config cmake.update-submodules no
  git -C "$BUILD/mariadb-src" submodule update --init --depth 1 libmariadb
else
  git -C "$BUILD/mariadb-src" config cmake.update-submodules no
  if [ "$(git -C "$BUILD/mariadb-src" rev-parse HEAD)" != "$COMMIT" ]; then
    git -C "$BUILD/mariadb-src" checkout --detach "$COMMIT"
  fi
  if [ ! -f "$BUILD/mariadb-src/libmariadb/CMakeLists.txt" ]; then
    git -C "$BUILD/mariadb-src" submodule update --init --depth 1 libmariadb
  fi
fi

if [ ! -f "$BUILD/mariadb-build/build.ninja" ]; then
  cmake -G Ninja -S "$BUILD/mariadb-src" -B "$BUILD/mariadb-build" \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DBISON_EXECUTABLE="$BUILD/hostdeps/bin/bison" \
    -DWITH_EMBEDDED_SERVER=ON -DWITH_WSREP=OFF -DWITH_UNIT_TESTS=OFF \
    -DWITH_SSL=system -DOPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3 \
    -DPLUGIN_INNOBASE=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_MROONGA=NO \
    -DPLUGIN_SPIDER=NO -DPLUGIN_SPHINX=NO -DPLUGIN_CONNECT=NO \
    -DPLUGIN_COLUMNSTORE=NO -DPLUGIN_S3=NO -DPLUGIN_OQGRAPH=NO \
    -DPLUGIN_FEDERATED=NO -DPLUGIN_FEDERATEDX=NO \
    ${CCACHE[@]+"${CCACHE[@]}"} \
    -DCMAKE_C_FLAGS="-fsanitize-coverage=inline-8bit-counters" \
    -DCMAKE_CXX_FLAGS="-fsanitize-coverage=inline-8bit-counters"
fi

ninja -C "$BUILD/mariadb-build" -j6 mysqlserver GenError

/usr/bin/c++ -std=c++17 -O2 -g \
  -DEMBEDDED_LIBRARY -DMYSQL_SERVER -DHAVE_CONFIG_H -DDBUG_OFF -DNDEBUG \
  -DDDLFUZZ_BUILD_DIR="\"$BUILD\"" \
  -I"$BUILD/mariadb-build/include" -I"$BUILD/mariadb-src/include/providers" \
  -I"$BUILD/mariadb-src/include" -I"$BUILD/mariadb-src/libmysqld" \
  -I"$BUILD/mariadb-src/sql" -I"$BUILD/mariadb-build/sql" \
  -I/opt/homebrew/include \
  "$HERE/main.cc" "$HERE/digest.cc" -o "$BUILD/oracle-mariadb" \
  "$BUILD/mariadb-build/libmysqld/libmariadbd.a" \
  -lz \
  /opt/homebrew/opt/openssl@3/lib/libssl.dylib \
  /opt/homebrew/opt/openssl@3/lib/libcrypto.dylib \
  -L/opt/homebrew/lib -lpcre2-8

echo "built $BUILD/oracle-mariadb"
# go run keeps builder and verifier on one hash implementation, and
# DDLFUZZ_ROOT pins hashing to THIS checkout — ddlsuper's root discovery would
# otherwise resolve the staged worktree to the main repo.
DDLDIR="$(cd "$HERE/../.." && pwd)"
REPO_ROOT="$(cd "$DDLDIR/../.." && pwd)"
if command -v go >/dev/null 2>&1; then
  (cd "$DDLDIR" && DDLFUZZ_ROOT="$REPO_ROOT" go run ./supervisor oracle-manifest --engine mariadb --out "$BUILD/oracle-mariadb.manifest.json")
else
  echo "warning: go missing; skipping oracle manifest" >&2
fi
