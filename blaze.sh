#!/bin/bash

TARGET_BUILD_TYPE=Debug
BUILD_DIR=build-dbg
COMPILER=`which g++`
GENERATOR='-GNinja'
LAUNCHER=$(command -v ccache)
if [ -x $LAUNCHER ]; then
  echo "Using launcher $LAUNCHER"
  LAUNCHER="-DCMAKE_CXX_COMPILER_LAUNCHER=$LAUNCHER"
else
  LAUNCHER=''
fi

for ARG in $*
do
  case "$ARG" in
    -release)
        TARGET_BUILD_TYPE=Release
        BUILD_DIR=build-opt
        shift
        ;;
    -clang)
        COMPILER=`which clang++`
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

    *)
     echo bad option "$ARG"
     exit 1
     ;;
  esac
  shift
done

mkdir -p $BUILD_DIR && cd $BUILD_DIR
set -x

cmake -L -DCMAKE_BUILD_TYPE=$TARGET_BUILD_TYPE -DCMAKE_CXX_COMPILER=$COMPILER "$GENERATOR" $LAUNCHER ..


