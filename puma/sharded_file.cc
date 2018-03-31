// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/sharded_file.h"


#include <fcntl.h>

#include "base/logging.h"
#include "strings/strcat.h"
#include "strings/stringprintf.h"

namespace puma {

using std::string;
using util::Status;
using util::StatusCode;

namespace {

string Shard(const string& prefix, unsigned shard, unsigned count, const string& suffix) {
  string res = StrCat(prefix, StringPrintf("-%04d-%04d", shard, count));

  if (!suffix.empty()) {
    StrAppend(&res, ".", suffix);
  }
  return res;
}

Status StatusFileError() {
  char buf[1024];
  char* result = strerror_r(errno, buf, sizeof(buf));

  return Status(StatusCode::IO_ERROR, result);
}


}  // namespace


string ShardSpec::Name(uint32 index) const {
  DCHECK_LT(index, shard_count_);

  string res = StrCat(prefix_, StringPrintf("-%04d-%04d", index, shard_count_));

  if (!suffix_.empty()) {
    StrAppend(&res, ".", suffix_);
  }
  return res;
}

std::string ShardSpec::Glob() const {
  string res = StrCat(prefix_, StringPrintf(R"(-????-%04d)", shard_count_));
  if (!suffix_.empty()) {
    StrAppend(&res, ".", suffix_);
  }
  return res;
}

Status ShardSpec::ForEach(std::function<Status(uint32, const std::string&)> cb) {
  for (uint32 i = 0; i < shard_count_; ++i) {
    string name = Shard(prefix_, i, shard_count_, suffix_);
    RETURN_IF_ERROR(cb(i, name));
  }
  return Status::OK;
}

ShardedFile::ShardedFile(const std::string& prefix, const std::string& suffix, uint32 shard_count)
    : spec_{prefix, suffix, shard_count} {
  CHECK_GT(shard_count, 0);
}

ShardedFile::~ShardedFile() {
  CHECK_STATUS(FlushAndClose());
}

Status ShardedFile::Open(bool append_if_exists) {
  CHECK(!fd_arr_);

  fd_arr_ = new int[spec_.shard_count()]();

  const int kFlags = O_CREAT | O_WRONLY | O_CLOEXEC | (append_if_exists ? O_APPEND : O_TRUNC);

  std::function<Status(uint32, const std::string&)> cb =
    [this, kFlags](uint32 i, const std::string& name) {

    fd_arr_[i] = open(name.c_str(), kFlags, 0644);

    if (fd_arr_[i] < 0) {
      return StatusFileError();
    }
    return Status::OK;
  };

  Status res = spec_.ForEach(cb);
  if (!res.ok()) {
    FlushAndClose();
  }
  return res;
}

Status ShardedFile::Write(uint32 fp, const void* buf, size_t len) {
  uint32 index = fp % spec_.shard_count();

  int fd = fd_arr_[index];

  while (len) {
    ssize_t written = write(fd, buf, len);
    if (written < 0) {
      return StatusFileError();
    }
    buf = reinterpret_cast<const char*>(buf) + written;
    len -= written;
  }

  return Status::OK;
}

Status ShardedFile::FlushAndClose() {
  if (!fd_arr_)
    return Status::OK;
  Status status;
  for (uint32 i = 0; i < spec_.shard_count(); ++i) {
    if (!fd_arr_[i])
      continue;
    int res = close(fd_arr_[i]);

    if (res < 0) {
      status.AddError(StatusFileError());
    }
  }
  delete[] fd_arr_;
  fd_arr_ = nullptr;

  return status;
}

}  // namespace puma
