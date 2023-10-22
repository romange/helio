# Helio - backend development framework in C++ using io_uring and epoll event-loop.

=====

[![ci-tests](https://github.com/romange/async/actions/workflows/ci.yml/badge.svg)](https://github.com/romange/async/actions/workflows/ci.yml)

[![codecov](https://codecov.io/gh/romange/helio/graph/badge.svg?token=2TIU52DK17)](https://codecov.io/gh/romange/helio)

Async is a set of c++ primitives that allows you efficient and rapid development
in c++17 on linux systems. The focus is mostly for backend development, data processing etc.


1. Dependency on [abseil-cpp](https://github.com/abseil/abseil-cpp/)
2. Dependency on [Boost 1.71](https://www.boost.org/doc/libs/1_71_0/doc/html/)
3. Uses ninja-build on top of cmake
4. Built artifacts are docker-friendly.
6. HTTP server implementation.
7. Fibers library and fiber-friendly synchronization primitives.


I will gradually add explanations for most crucial blocks in this library.


## Setting Up & Building
   ```bash
   > sudo ./install-dependencies.sh
   > ./blaze.sh -release
   > cd build-opt && ninja -j4 echo_server

   ```
   *third_party* folder is checked out under build directories.

   Then, from 2 tabs run:

   ```bash
     server> ./echo_server --logtostderr
     client> ./echo_server --connect=localhost --n 100000 --c=4
   ```


## HTTP

HTTP handler is implemented using [Boost.Beast](https://www.boost.org/doc/libs/1_71_0/libs/beast/doc/html/index.html) library. It's integrated with the io_uring based ProactorPool.
Please see [http_main.cc](https://github.com/romange/async/blob/master/util/http/http_main.cc), for example. HTTP also provides support for backend monitoring (Varz status page) and for extensible debugging interface. With monitoring C++ backend returns json object that is formatted inside status page in the browser. To check how it looks, please go to [localhost:8080](http://localhost:8080) while `echo_server` is running.


### Self-profiling
Every http-powered backend has integrated CPU profiling capabilities using [gperf-tools](https://github.com/gperftools/gperftools) and [pprof](https://github.com/google/pprof)
Profiling can be triggered in prod using magic-url commands. Enabled profiling usually has very minimal impact on cpu performance of the running backend.

### Logging
Logging is based on Google's [glog library](https://github.com/google/glog). The library is very reliable, performant and solid. It has many features that allow resilient backend development.
Unfortunately, Google's version has some bugs, which I fixed (waiting for review...), so I use my own fork. Glog library gives me the ability to control logging levels of a backend at run-time without restarting it.

## Tests
ASYNC uses googletest+gmock unit-test environment.

## Conventions
Third_party packages have `TRDP::` prefix in `CMakeLists.txt`. absl libraries have prefix
`absl::...`.
