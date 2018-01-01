// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/nullable_column.h"

#include "base/logging.h"
#include "strings/strpmr.h"

namespace puma {

NullableColumn::NullableColumn(pmr::memory_resource* mr) : def_(mr), data_(mr) {
}

void NullableColumn::Init(DataType dt, size_t reserve_s) {
  data_.Init(dt);
  data_.Reserve(reserve_s);
  def_.reserve(reserve_s);
  capacity_ = reserve_s;
}

void NullableColumn::ResizeFill(size_t sz, bool def) {
  DCHECK_LE(sz, capacity_);

  def_.resize_fill(sz, def);

  if (data_.is_initialized())
    data_.Resize(sz);
}


void NullableColumn::AppendFrom(const NullableColumn& other, pmr::memory_resource* mr) {
  CHECK_EQ(other.def_.size(), other.data_.size());

  ColumnSlice src(other);

  size_t prev = def_.size();
  def_.resize(prev + src.size());

  for (size_t i = 0; i < src.size(); ++i)
    def_[prev + i] = src.def(i);

  if (mr && data_type() == DataType::STRING) {
    DCHECK_EQ(prev, data_.size());

    data_.Resize(prev + src.size());
    for (size_t i = 0; i < src.size(); ++i) {
      StringPiece val;
      if (src.def(i)) {
        val = src.get<StringPiece>(i);
      }
      val = strings::DeepCopy(val, mr);
      data_.set(prev + i, val);
    }
  } else {
    data_.AppendFrom(other.data_);
  }
}

size_t NullableColumn::Append(const void* src, bool is_def) {
  def_.push_back(is_def);

  return data_.AppendFrom(src);
}

void NullableColumn::set_window_offset(size_t off) {
  DCHECK_LE(off, size());

  offset_ = off;
}

bool NullableColumn::MoveToBeginningIfSmaller(size_t sz_threshold) {
  const size_t sz = window_size();
  if (sz >= sz_threshold) {
    return false;
  }

  if (window_offset() > 0) {
    CHECK_GT(sz, 0);

    if (data_.is_initialized()) {
      memmove(data_.column_ptr().u8ptr,
              data_.raw(window_offset()),
              sz * data_.data_type_size());
      data_.Resize(sz);
    }

    // Currently define vector is always present.
    memmove(&def_.front(), def_.data() + offset_, sz);
    offset_ = 0;
    def_.resize_assume_reserved(sz);
  }
  return true;
}

void NullableColumn::ReduceBoolean() {
  CHECK_EQ(DataType::BOOL, data_type());
  DCHECK(data_.is_initialized());

  for (size_t i = window_offset(); i < size(); ++i) {
    def_[i] = def_[i] && data_.at<bool>(i);
  }
}

std::ostream& operator<<(std::ostream& o, const puma::ColumnSlice& viewer) {
  if (!viewer.is_initialized()) {
    return (o << "null");
  }
  if (viewer.col().data().is_initialized()) {
    DCHECK_LE(viewer.size(), viewer.col().data().size());
  }
  o << "[" << viewer.size() << ": {";
  size_t psize = std::min<size_t>(viewer.size(), 20);

#define CASE(x) case DataType::x: \
    o << viewer.get<DTCpp_t<DataType::x>>(i); \
    break

  for (size_t i = 0; i < psize; ++i) {
    o << "(" << int(viewer.def(i)) << ",";

    switch(viewer.data_type()) {
      CASE(BOOL);
      CASE(UINT32);
      CASE(INT32);
      CASE(INT64);
      CASE(DOUBLE);
      CASE(STRING);
      case DataType::MESSAGE:
        o << "msg";
      break;
      case DataType::NONE:
        o << "null";
      break;
      default:
        LOG(FATAL) << " not supported " << viewer.data_type();
    }

    o << "), ";
  }
  o << " }]";
  return o;
}
#undef CASE


ColumnSlice::ColumnSlice(const NullableColumn& nc, size_t o, size_t len)
    : nc_(&nc), offset_(o + nc.window_offset()), len_(len) {
  DCHECK_LE(offset_, nc.size());

  if (offset_ >= nc.size()) {
    len_ = 0;
  } else if (nc.size() -offset_ < len_) {
    len_ = nc.size() - offset_;
  }
}


}  // namespace puma
