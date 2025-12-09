#!/bin/bash

set -e

apt install -y cmake libunwind-dev zip bison ninja-build autoconf-archive libtool file curl
g++ --version

BVER=1.76.0
BOOST=boost_${BVER//./_}   # replace all . with _

# For sake of boost install we always use g++.
export CXX=g++

install_boost() {
    mkdir -p /tmp/boost && pushd /tmp/boost
    if ! [ -d $BOOST ]; then
      url="https://archives.boost.io/release/${BVER}/source/$BOOST.tar.bz2"
      echo "Downloading from $url"

      if ! [ -e $BOOST.tar.bz2 ]; then
        if ! wget -nv ${url} -O $BOOST.tar.bz2; then
          echo "Error: wget failed to download $BOOST.tar.bz2 from $url"
          rm -f $BOOST.tar.bz2
          exit 1
        fi
      fi

      # Check if file is valid bzip2 archive before extracting
      if ! file $BOOST.tar.bz2 | grep -q 'bzip2 compressed data'; then
        echo "Error: Downloaded file is not a valid bzip2 archive. Possible download error."
        rm -f $BOOST.tar.bz2
        exit 1
      fi
      tar -xjf $BOOST.tar.bz2 && chown ${SUDO_USER}:${SUDO_USER} -R $BOOST.tar.bz2 $BOOST
    fi

    booststap_arg="--prefix=/opt/${BOOST} --without-libraries=graph_parallel,graph,wave,test,mpi,python"
    cd $BOOST
    boostrap_cmd=`readlink -f bootstrap.sh`

    echo "CXX compiler ${CXX}"
    echo "Running ${boostrap_cmd} ${booststap_arg}"
    ${boostrap_cmd} ${booststap_arg} || { cat bootstrap.log; return 1; }
    b2_args=(define=BOOST_COROUTINES_NO_DEPRECATION_WARNING=1 link=static variant=release debug-symbols=on
             threading=multi --without-test --without-math --without-log --without-locale --without-wave
             --without-regex --without-python -j4)

    echo "Building targets with ${b2_args[*]}"
    ./b2 "${b2_args[@]}" cxxflags='-std=c++14 -Wno-deprecated-declarations'
    ./b2 install "${b2_args[@]}" -d0
    chown ${SUDO_USER}:${SUDO_USER} -R ./
    popd
}

if ! [ -d /opt/${BOOST}/lib ]; then
  install_boost
else
  echo "Skipping installing ${BOOST}"
fi

if ! [ -d /opt/boost ]; then
  ln -sf /opt/${BOOST} /opt/boost
fi
