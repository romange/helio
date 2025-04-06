// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "io/file_util.h"

#include <fcntl.h>
#include <glob.h>
#include <sys/stat.h>

#include "base/logging.h"
#include "io/file.h"

#if (defined(__APPLE__) && defined(__MACH__)) || defined(__FreeBSD__)
#define _MAC_OS_ 1
#endif

namespace io {
using namespace std;

static int glob_errfunc(const char* epath, int eerrno) {
  LOG(ERROR) << "Error in glob() path: <" << epath << ">. errno: " << eerrno;
  return 0;
}

Result<vector<StatShort>> StatFiles(std::string_view path) {
  glob_t glob_result;

  vector<StatShort> res;
#ifdef _MAC_OS_
  constexpr int kTilde = GLOB_TILDE;
#else
  constexpr int kTilde = GLOB_TILDE_CHECK;
#endif

  int rv = glob(path.data(), kTilde, glob_errfunc, &glob_result);
  if (rv) {
    switch (rv) {
      case GLOB_NOSPACE:
        return nonstd::make_unexpected(make_error_code(errc::not_enough_memory));
      case GLOB_ABORTED:
        return nonstd::make_unexpected(make_error_code(errc::io_error));
      case GLOB_NOMATCH:
        return res;
    }
  }

#ifdef _MAC_OS_
  struct stat sbuf;
#define FSTAT(path) fstatat(AT_FDCWD, (path), &sbuf, 0)
#else
  struct statx sbufx;
  constexpr unsigned kMask = STATX_MTIME | STATX_SIZE | STATX_TYPE | STATX_MODE;
#define FSTAT(path) statx(AT_FDCWD, (path), 0, kMask, &sbufx)
#endif

  for (size_t i = 0; i < glob_result.gl_pathc; i++) {
    char* path = glob_result.gl_pathv[i];
    if (FSTAT(path) == 0) {
#ifdef _MAC_OS_
      const auto& st_mt = sbuf.st_mtimespec;
      size_t size = sbuf.st_size;
      mode_t mode = sbuf.st_mode;
#else
      const auto& st_mt = sbufx.stx_mtime;
      size_t size = sbufx.stx_size;
      uint16_t mode = sbufx.stx_mode;
#endif

      time_t ns = st_mt.tv_sec * 1000000000ULL + st_mt.tv_nsec;

      StatShort sshort{path, ns, size, mode};
      res.emplace_back(std::move(sshort));
    } else {
      LOG(WARNING) << "Bad stat for " << glob_result.gl_pathv[i] << " " << strerror(errno);
    }
  }
  globfree(&glob_result);

  return res;
}

void WriteStringToFileOrDie(std::string_view contents, std::string_view name) {
  Result<WriteFile*> res = OpenWrite(name);
  CHECK(res) << res.error();

  unique_ptr<WriteFile> wf(res.value());
  auto ec = wf->Write(contents);
  CHECK(!ec) << ec.message();
  ec = wf->Close();
  CHECK(!ec) << ec.message();
}

Result<string> ReadFileToString(string_view path) {
  auto res = OpenRead(path, ReadonlyFile::Options{});
  if (!res)
    return nonstd::make_unexpected(res.error());

  unique_ptr<ReadonlyFile> fl(res.value());

  string value;
  value.resize(4096);

  MutableBytes mb(reinterpret_cast<uint8_t*>(value.data()), value.size());
  size_t offset = 0;
  while (true) {
    auto status = fl->Read(offset, mb);
    if (!status) {
      error_code ec = fl->Close();
      (void)ec;
      return nonstd::make_unexpected(status.error());
    }
    if (*status < mb.size()) {
      value.resize(offset + *status);
      break;
    }
    offset += mb.size();
    value.resize(value.size() * 2);
    mb = MutableBytes(reinterpret_cast<uint8_t*>(value.data() + offset), value.size() - offset);
  }

  error_code ec = fl->Close();
  (void)ec;

  return value;
}

}  // namespace io
