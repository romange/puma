// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <functional>
#include <string>

#include "base/integral_types.h"

#include "util/status.h"

namespace puma {

class ShardSpec {
 public:
  ShardSpec(const std::string& prefix, const std::string& suffix, uint32 shard_count)
      : prefix_(prefix), suffix_(suffix), shard_count_(shard_count) {}

  const std::string& prefix() const { return prefix_; }
  const std::string& suffix() const { return suffix_; }
  uint32 shard_count() const { return shard_count_; }

  std::string Name(uint32 index) const;
  std::string Glob() const;

  // Runs cb on each shard name until all are iterated.
  // If cb returns status not ok, breaks the iteration and returns the status.
  util::Status ForEach(std::function<util::Status(uint32, const std::string&)> cb);
 private:
  std::string prefix_, suffix_;
  uint32 shard_count_;
};

class ShardedFile {
 public:
  ShardedFile(const std::string& prefix, const std::string& suffix, uint32 shard_count);

  ~ShardedFile();

  util::Status Open(bool append_if_exists = false);
  util::Status Write(uint32 fp, const void* buf, size_t len);
  util::Status FlushAndClose();

  ShardSpec spec() const { return spec_; }
 private:
  int* fd_arr_ = nullptr;
  ShardSpec spec_;
};

}  // namespace puma
