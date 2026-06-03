#!/usr/bin/env bash

TARGET_BUILD_TYPE=Debug
BUILD_PREF=build
BUILD_SUF=dbg
GENERATOR='-GNinja'

NC=$'\e[0m'
RED=$'\e[0;31m'
GREEN=$'\e[0;32m'

# Accumulates extra -D flags for cmake: the ccache launcher below (auto-detected)
# plus any -D* the user passes on the command line.
CMAKE_EXTRA=()

# Use ccache as the compiler launcher when present — speeds up incremental builds.
CCACHE_BIN=$(command -v ccache)
if [ -x "$CCACHE_BIN" ]; then
  echo "Using launcher $CCACHE_BIN"
  CMAKE_EXTRA+=("-DCMAKE_CXX_COMPILER_LAUNCHER=$CCACHE_BIN")
fi

# We can't assume a specific compiler is installed, so check for both GCC and Clang
if command -v g++ >/dev/null 2>&1; then
    DEFAULT_CXX='g++'
    DEFAULT_C='gcc'
elif command -v clang++ >/dev/null 2>&1; then
    DEFAULT_CXX='clang++'
    DEFAULT_C='clang'
else
    echo -e "${RED}Error: No C++ compiler (g++ or clang++) found in PATH.${NC}"
    exit 1
fi

CXX_COMPILER=$(command -v $DEFAULT_CXX)
C_COMPILER=$(command -v $DEFAULT_C)

while [ $# -gt 0 ]; do
  case "$1" in
    -release)
        TARGET_BUILD_TYPE=Release
        BUILD_SUF=opt
        shift
        ;;
    -clang)
        CXX_COMPILER=$(command -v clang++)
        C_COMPILER=$(command -v clang)
        BUILD_PREF=clang
        shift
        ;;
    -ninja)
        GENERATOR='-GNinja'
        shift
        ;;
    -make)
        GENERATOR=-G'Unix Makefiles'
        shift
        ;;
    -build-dir)
        # Consume the path that should follow -build-dir. Reject it if missing
        # (user wrote bare `-build-dir`) or if it looks like another flag
        # (e.g. `-build-dir -release` — easy mistake to make).
        # ${1:-} expands to "" when $1 is unset; ${1:0:1} is the first character of $1.
        shift
        if [ -z "${1:-}" ] || [ "${1:0:1}" = "-" ]; then
          echo -e "${RED}-build-dir requires a path argument${NC}"
          exit 1
        fi
        BUILD_DIR=$1
        shift
        ;;
    -D*)
        CMAKE_EXTRA+=("$1")
        shift
        ;;
    *)
        echo -e "${RED}bad option $1${NC}"
        exit 1
        ;;
  esac
done

BUILD_DIR=${BUILD_DIR:-${BUILD_PREF}-${BUILD_SUF}}
result="$(cmake --version | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+')"
major="${result%%.*}"
remainder="${result#*.}"
minor="${remainder%.*}"

if [[ "$major" -lt 3 || ("$major" -eq 3 && "$minor" -lt 16) ]]; then
    echo -e "${RED}you are using an older version of cmake, need cmake >= 3.16${NC}"
    exit 1
fi

(
  set -x
  cmake -L -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=$TARGET_BUILD_TYPE \
      -DCMAKE_CXX_COMPILER="$CXX_COMPILER" \
      -DCMAKE_C_COMPILER="$C_COMPILER" \
      "$GENERATOR" "${CMAKE_EXTRA[@]}"
)
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}========================================================"
    echo " Success!"
    echo " Build configuration is under: ${BUILD_DIR}"
    echo " To build, run: ninja -C ${BUILD_DIR}"
    echo " To inspect build instructions, check: ${BUILD_DIR}/compile_commands.json"
    echo "========================================================${NC}"
else

    echo -e "${RED}CMake configuration failed!${NC}"
fi

exit $EXIT_CODE