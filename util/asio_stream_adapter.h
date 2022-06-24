// Copyright 2022, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <boost/asio/detail/buffer_sequence_adapter.hpp>
#include <boost/system/error_code.hpp>
#include "util/fiber_socket_base.h"

namespace util {

template <typename Socket = FiberSocketBase> class AsioStreamAdapter {
  Socket& s_;

 public:
  using error_code = ::boost::system::error_code;

  explicit AsioStreamAdapter(Socket& s) : s_(s) {
  }

  // Read/Write functions should be called from IoContext thread.
  // (fiber) SyncRead interface:
  // https://www.boost.org/doc/libs/1_69_0/doc/html/boost_asio/reference/SyncReadStream.html
  template <typename MBS> size_t read_some(const MBS& bufs, error_code& ec);

  // To calm SyncReadStream compile-checker we provide exception-enabled
  // interface without implementing it.
  template <typename MBS> size_t read_some(const MBS& bufs);

  // SyncWrite interface:
  // https://www.boost.org/doc/libs/1_69_0/doc/html/boost_asio/reference/SyncWriteStream.html
  template <typename BS> size_t write_some(const BS& bufs, error_code& ec);

  // To calm SyncWriteStream compile-checker we provide exception-enabled
  // interface without implementing it.
  template <typename BS> size_t write_some(const BS& bufs);
};

template <typename Socket>
template <typename MBS>
size_t AsioStreamAdapter<Socket>::read_some(const MBS& bufs, error_code& ec) {
  using badapter = ::boost::asio::detail::buffer_sequence_adapter<
      boost::asio::mutable_buffer, const MBS&>;
  badapter bsa(bufs);

  auto res = s_.Recv(bsa.buffers(), bsa.count());
  if (res)
    return res.value();
  ec = error_code(std::move(res.error()).value(), boost::system::system_category());

  return 0;
}

template <typename Socket>
template <typename BS>
size_t AsioStreamAdapter<Socket>::write_some(const BS& bufs, error_code& ec) {
  using badapter =
      ::boost::asio::detail::buffer_sequence_adapter<boost::asio::const_buffer,
                                                     const BS&>;
  badapter bsa(bufs);

  std::error_code lec;
  auto res = s_.WriteSome(bsa.buffers(), bsa.count());
  if (res) {
    return res.value();
  }
  ec = error_code(std::move(res.error()).value(), boost::system::system_category());

  return 0;
}

}  // namespace util
