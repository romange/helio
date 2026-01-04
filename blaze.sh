#!/usr/bin/env bash

TARGET_BUILD_TYPE=Debug
BUILD_PREF=build
BUILD_SUF=dbg
GENERATOR='-GNinja'
LAUNCHER=$(command -v ccache)
# coloring
NC=$'\e[0m'
RED=$'\e[0;31m'
GREEN=$'\e[0;32m'
if [ -x "$LAUNCHER" ]; then
  echo "Using launcher $LAUNCHER"
  LAUNCHER="-DCMAKE_CXX_COMPILER_LAUNCHER=$LAUNCHER"
else
  LAUNCHER=''
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

for ((i=1;i <= $#;));do
  ARG=${!i}
  case "$ARG" in
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
    -D*) # bypass flags
      i=$((i + 1))
      ;;
    *)
     echo -e "${RED}bad option $ARG${NC}"
     exit 1
     ;;
  esac
done

BUILD_DIR=${BUILD_PREF}-${BUILD_SUF}
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
      "$GENERATOR" "$LAUNCHER" "$@"
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