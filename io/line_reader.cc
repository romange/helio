// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <cstring>  // for rawmemchr

#include "io/line_reader.h"
#include "io/file.h"

#include "base/logging.h"

namespace io {

using namespace std;

void LineReader::Init(uint32_t buf_log) {
  CHECK(buf_log > 10 && buf_log < 28) << buf_log;
  page_size_ = 1 << buf_log;

  buf_.reset(new char[page_size_]);
  next_ = end_ = buf_.get();
  *next_ = '\n';
}

LineReader::~LineReader() {
  if (ownership_ == TAKE_OWNERSHIP) {
    delete source_;
  }
}

bool LineReader::Next(std::string_view* result, std::string* scratch) {
  bool use_scratch = false;

  const char* const eof_page = buf_.get() + page_size_ - 1;
  while (true) {
    // Common case: search of EOL.
#ifdef HAS_RAWMEMCHR
    char* ptr =  reinterpret_cast<char*>(rawmemchr(next_, '\n'));
#else
    char* ptr =  reinterpret_cast<char*>(memchr(next_, '\n', end_ - next_ + 1));
#endif
    if (ptr < end_) {  // Found EOL.
      ++line_num_;

      unsigned delta = 1;
      if (ptr > next_ && ptr[-1] == '\r') {
        --ptr;
        delta = 2;
      }
      *ptr = '\0';

      if (use_scratch) {
        scratch->append(next_, ptr);
        *result = *scratch;
      } else {
        *result = std::string_view(next_, ptr - next_);
      }
      next_ = ptr + delta;

      return true;
    }

    if (next_ != end_) {
      // Our internal buffer was not empty, but we did not find EOL yet.
      // Now we've reach end of buffer, so we must copy the data to accomodate the broken line.
      if (!use_scratch) {
        if (scratch == nullptr)
          scratch = &scratch_;

        scratch->assign(next_, end_);
        use_scratch = true;
      } else {
        scratch->append(next_, end_);
      }
      next_ = end_;
      if (end_ != eof_page) {
        // It's EOF since we've read least than page size.
        line_num_ |= kEofMask;
        break;
      }
    }

    MutableBytes range{reinterpret_cast<uint8_t*>(buf_.get()),
                                    /* -1 to allow sentinel */ page_size_ - 1};
    auto sres = source_->ReadSome(range);
    if (!sres) {
      LOG(ERROR) << "LineReader read error " << sres.error() << " at line " << line_num_;
      status_ = sres.error();

      return false;
    }

    if (*sres == 0) {
      line_num_ |= kEofMask;
      break;
    }

    LOG_IF(ERROR, line_num_ & kEofMask) << "LineReader: read data after EOF was reached";
    next_ = buf_.get();
    end_ = next_ + *sres;
    *end_ = '\n';  // sentinel.
  }

  if (use_scratch) {
    *result = *scratch;
    ++line_num_;

    return true;
  }

  DCHECK(line_num_ & kEofMask);
  return false;
}

}  // namespace io
