// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "io/file_util.h"

#include <fcntl.h>
#include <glob.h>
#include <sys/stat.h>

#include "base/logging.h"
#include "io/file.h"

namespace io {
using namespace std;

static int glob_errfunc(const char* epath, int eerrno) {
  LOG(ERROR) << "Error in glob() path: <" << epath << ">. errno: " << eerrno;
  return 0;
}

Result<vector<StatShort>> StatFiles(std::string_view path) {
  glob_t glob_result;
  vector<StatShort> res;
  int rv = glob(path.data(), GLOB_TILDE_CHECK, glob_errfunc, &glob_result);
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

  struct statx sbuf;
  constexpr unsigned kMask = STATX_MTIME | STATX_SIZE | STATX_TYPE | STATX_MODE;

  for (size_t i = 0; i < glob_result.gl_pathc; i++) {
    if (statx(AT_FDCWD, glob_result.gl_pathv[i], AT_STATX_SYNC_AS_STAT, kMask, &sbuf) == 0) {
      time_t ns = sbuf.stx_mtime.tv_sec * 1000000000ULL + sbuf.stx_mtime.tv_nsec;
      StatShort sshort{glob_result.gl_pathv[i], ns, sbuf.stx_size, sbuf.stx_mode};
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

}  // namespace io
