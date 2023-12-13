# Helio - backend development framework in C++ using io_uring and epoll event-loop.

=====

[![ci-tests](https://github.com/romange/async/actions/workflows/ci.yml/badge.svg)](https://github.com/romange/async/actions/workflows/ci.yml)

[![codecov](https://codecov.io/gh/romange/helio/graph/badge.svg?token=2TIU52DK17)](https://codecov.io/gh/romange/helio)

Async is a set of C++ primitives that allows you efficient and rapid development
in c++17 on Linux systems. The focus is mostly on backend development, data processing, etc.


1. Dependency on [abseil-cpp](https://github.com/abseil/abseil-cpp/)
2. Dependency on [Boost 1.71](https://www.boost.org/doc/libs/1_71_0/doc/html/)
3. Uses ninja-build on top of cmake
4. Built artifacts are docker-friendly.
6. HTTP server implementation.
7. Fibers library and fiber-friendly synchronization primitives.


I will gradually add explanations for the most crucial blocks in this library.

## Acknowledgments
Helio has utilized [Boost.Fiber](https://github.com/boostorg/fiber) until May 2023.
After this period, the fibers functionality
was integrated directly into Helio code. This transition involved adapting and reworking
portions of the original Boost.Fiber code to align with Helio's coding conventions
and architecture.

The design and implementation of Helio fibers draw significant inspiration from Boost.Fiber.
Certain segments of the Boost.Fiber code were selectively adopted and modified for
seamless incorporation into Helio. We extend our deepest gratitude and recognition to @olk
and all the contributors of Boost.Fiber for their foundational work.

The decision to replicate and modify the original code was driven by the necessity
for greater flexibility. Specifically, it was easier to evolve Helio's polling mechanism
when we had full control over the fibers internal interfaces.

## Differrences between Boost.Fiber and helio fb2 fibers.
Helio fb2 library adopts similar non-intrusive, shared nothing design similarly to Boost.Fiber.
However, helio's fb2 library is more opinionated and also provides full networking support
for asynchronous multi-threaded execution. helio fibers allows injecting a custom dispatch
policy "util::fb2::DispatchPolicy" directly into a dispatching fiber,
while Boost.Fiber requires using a separate fiber to handle the I/O polling.
In addition, Boost.Fiber inter-thread notification mechanism (aka remote ready queue)
required locking while helio fibers use a lockless queue.

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

HTTP handler is implemented using [Boost.Beast](https://www.boost.org/doc/libs/1_71_0/libs/beast/doc/html/index.html) library. It's integrated with the io_uring-based ProactorPool.
Please see [http_main.cc](https://github.com/romange/async/blob/master/util/http/http_main.cc), for example. HTTP also provides support for backend monitoring (Varz status page) and for an extensible debugging interface. With monitoring C++ backend returns JSON object that is formatted inside status page in the browser. To check how it looks, please go to [localhost:8080](http://localhost:8080) while `echo_server` is running.


### Self-profiling
Every http-powered backend has integrated CPU profiling capabilities using [gperf-tools](https://github.com/gperftools/gperftools) and [pprof](https://github.com/google/pprof)
Profiling can be triggered in prod using magic-url commands. Enabled profiling usually has a very minimal impact on CPU performance of the running backend.

### Logging
Logging is based on Google's [glog library](https://github.com/google/glog). The library is very reliable, performant, and solid. It has many features that allow resilient backend development.
Unfortunately, Google's version has some bugs, which I fixed (waiting for review...), so I use my own fork. Glog library gives me the ability to control the logging levels of a backend at run-time without restarting it.

## Tests
ASYNC uses a googletest+gmock unit-test environment.

## Conventions
Third_party packages have `TRDP::` prefix in `CMakeLists.txt`. absl libraries have prefix
`absl::...`.
