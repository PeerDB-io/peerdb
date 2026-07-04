#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SRC=/Users/ilia/Code/mysql-server
BUILD="$ROOT/build/mysql-build"
OUT="$ROOT/build/oracle-mysql"
BISON="$(brew --prefix bison)/bin/bison"
SSL="$(brew --prefix openssl@3)"

mkdir -p "$BUILD"

cmake -G Ninja -S "$SRC" -B "$BUILD" \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DWITH_UNIT_TESTS=ON \
  -DWITH_SHARED_UNITTEST_LIBRARY=OFF \
  -DWITH_SSL="$SSL" \
  -DBISON_EXECUTABLE="$BISON" \
  -DCMAKE_C_FLAGS="-fsanitize-coverage=inline-8bit-counters" \
  -DCMAKE_CXX_FLAGS="-fsanitize-coverage=inline-8bit-counters -include type_traits"

ninja -C "$BUILD" -j6 create_field-t

CC_LINE="$(
  ninja -C "$BUILD" -t commands unittest/gunit/CMakeFiles/create_field-t.dir/create_field-t.cc.o |
    grep 'create_field-t\.cc\.o -c ' |
    tail -1
)"
LD_LINE="$(
  ninja -C "$BUILD" -t commands runtime_output_directory/create_field-t |
    grep ' -o runtime_output_directory/create_field-t ' |
    tail -1
)"

if [[ -z "$CC_LINE" || -z "$LD_LINE" ]]; then
  echo "failed to extract create_field-t compile/link commands" >&2
  exit 1
fi

DRV_O="$BUILD/ddlfuzz_driver.o"

CC_CMD="$(python3 - "$CC_LINE" "$ROOT/oracle/mysql/driver.cc" "$DRV_O" <<'PY'
import shlex
import sys

line, src, out = sys.argv[1:]
argv = shlex.split(line)
result = []
skip_next = False
for i, tok in enumerate(argv):
    if skip_next:
        skip_next = False
        continue
    if tok in {"-o", "-c", "-MT", "-MF"}:
        skip_next = True
        continue
    if tok == "-MD":
        continue
    result.append(tok)
result.extend(["-Wno-unused-parameter", "-c", src, "-o", out])
print(shlex.join(result))
PY
)"

eval "$CC_CMD"

export DRV_O OUT
REF_OBJ="unittest/gunit/CMakeFiles/create_field-t.dir/create_field-t.cc.o"
REF_OUT="-o runtime_output_directory/create_field-t"
GUNIT_LIB="archive_output_directory/libgunit_large.a"
TEST_UTILS_OBJ="unittest/gunit/CMakeFiles/gunit_large.dir/test_utils.cc.o"
if [[ "$(grep -F -o "$REF_OBJ" <<<"$LD_LINE" | wc -l | tr -d ' ')" != "1" ]]; then
  echo "unexpected create_field object count in link command" >&2
  exit 1
fi
if [[ "$(grep -F -o -- "$REF_OUT" <<<"$LD_LINE" | wc -l | tr -d ' ')" != "1" ]]; then
  echo "unexpected create_field output count in link command" >&2
  exit 1
fi
if [[ "$(grep -F -o "$GUNIT_LIB" <<<"$LD_LINE" | wc -l | tr -d ' ')" != "1" ]]; then
  echo "unexpected gunit_large archive count in link command" >&2
  exit 1
fi
LD_CMD="${LD_LINE/$REF_OBJ/$DRV_O}"
LD_CMD="${LD_CMD/$REF_OUT/-o $OUT}"
LD_CMD="${LD_CMD/$GUNIT_LIB/$TEST_UTILS_OBJ}"

(cd "$BUILD" && eval "$LD_CMD")
echo "built $OUT"
# go run keeps builder and verifier on one hash implementation, and
# DDLFUZZ_ROOT pins hashing to THIS checkout — ddlsuper's root discovery would
# otherwise resolve the staged worktree to the main repo.
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"
if command -v go >/dev/null 2>&1; then
  (cd "$ROOT" && DDLFUZZ_ROOT="$REPO_ROOT" go run ./supervisor oracle-manifest --engine mysql --out "$ROOT/build/oracle-mysql.manifest.json")
else
  echo "warning: go missing; skipping oracle manifest" >&2
fi
