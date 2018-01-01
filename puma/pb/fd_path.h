// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <string>
#include <vector>

namespace google {
namespace protobuf {
class FieldDescriptor;
class Message;
}  // namespace protobuf
}  // namespace google


namespace puma {
namespace pb {

class FdPath {
 public:
  using FD = ::google::protobuf::FieldDescriptor;

  FdPath() {}
  FdPath(const FdPath&) = default;
  FdPath(FdPath&&) = default;
  FdPath& operator=(const FdPath&) = default;


  bool IsRepeated() const;

  const std::vector<const FD*>& path() const { return path_; }
  void push_back(const FD* fd) { path_.push_back(fd); }

  bool valid() const { return !path_.empty(); }

  const FD* leaf() const;

  std::string PathStr() const;
  std::string FullPath(const FD* node) const;
  size_t size() const { return path_.size(); }
 private:
  std::vector<const FD*> path_;
};

}  // namespace pb
}  // namespace puma
