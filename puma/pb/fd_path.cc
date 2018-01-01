// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/pb/fd_path.h"

#include <google/protobuf/descriptor.h>

#include "base/logging.h"
#include "strings/strcat.h"

namespace puma {
namespace pb {

using std::string;

string FdPath::PathStr() const {
  if (path_.empty()) return string();

  string res;
  for (const FD* pr : path_) {
    StrAppend(&res, pr->name(), ".");
  }
  return res;
}

string FdPath::FullPath(const FD* node) const {
  string res = PathStr();
  res += node->name();
  return res;
}

auto FdPath::leaf() const -> const FD* {
  CHECK(!path_.empty());
  return path_.back();
}


}  // namespace pb
}  // namespace puma

