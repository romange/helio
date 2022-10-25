#!/bin/bash

TARGET_BUILD_TYPE=Debug
BUILD_PREF=build
BUILD_SUF=dbg
COMPILER='g++'
GENERATOR='-GNinja'
LAUNCHER=$(command -v ccache)
if [ -x $LAUNCHER ]; then
  echo "Using launcher $LAUNCHER"
  LAUNCHER="-DCMAKE_CXX_COMPILER_LAUNCHER=$LAUNCHER"
else
  LAUNCHER=''
fi

for ((i=1;i <= $#;));do
  ARG=${!i}
  case "$ARG" in
    -release)
        TARGET_BUILD_TYPE=Release
        BUILD_SUF=opt
        shift
        ;;
    -clang)
        COMPILER=`which clang++`
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
     echo bad option "$ARG"
     exit 1
     ;;
  esac
done

BUILD_DIR=${BUILD_PREF}-${BUILD_SUF}

set -x

result="$(cmake --version | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+')"
major="${result%%.*}"
remainder="${result#*.}"
minor="${remainder%.*}"

if [ "$major" -lt 3 ] ||
    ( [ "$major" -eq 3 ] &&
        ( [ "$minor" -lt 4 ] )) ; then
    echo "you are using an older version of cmake, need cmake >= 3.4"
    exit 1
fi

cmake -L -B $BUILD_DIR -DCMAKE_BUILD_TYPE=$TARGET_BUILD_TYPE -DCMAKE_CXX_COMPILER=$COMPILER \
    "$GENERATOR" $LAUNCHER $@
