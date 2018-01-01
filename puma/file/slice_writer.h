// Copyright 2015, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <memory>

#include "base/integral_types.h"
#include "strings/stringpiece.h"
#include "puma/data_types.h"
#include "puma/storage.pb.h"

namespace puma {

class SliceWriterBase {
 public:
  explicit SliceWriterBase(DataType dt) : dt_(dt) {}

  virtual void AddChunk(const strings::Range<const uint8*>& r) = 0;
  virtual void AddChunk64(const strings::Range<const int64*>& r) = 0;
  virtual void AddChunk32(const strings::Range<const int32*>& r) = 0;

  virtual void Finalize() = 0;

  virtual ~SliceWriterBase() {};

  DataType dt() const { return dt_;}
 private:
  DataType dt_;
};

}  // namespace puma
