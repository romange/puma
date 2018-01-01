// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <google/protobuf/descriptor.h>

namespace puma {
namespace pb {
namespace gpb = ::google::protobuf;

class AnnotatedFd {
  const gpb::FieldDescriptor* fd_;

 public:
  AnnotatedFd(const gpb::FieldDescriptor* fd) : fd_(fd) {}
  const gpb::FieldDescriptor* operator->() const { return fd_;}
  const gpb::FieldDescriptor* get() const { return fd_; }

  bool is_unordered_set = false;

};


}  // namespace pb
}  // namespace puma
