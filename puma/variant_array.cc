// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/variant_array.h"

#include "base/logging.h"
#include "puma/data_types.h"

namespace puma {

VariantArray::VariantArray(pmr::memory_resource* upstream, DataType dt)
     : upstream_(upstream), capacity_(0), size_(0), dt_(dt) {
  type_size_ = dt_ != DataType::NONE ? aux::DataTypeSize(dt_) : 0;
}

VariantArray::~VariantArray() {
  if (data_.u8ptr && upstream_) {
    upstream_->deallocate(data_.u8ptr, capacity_ * type_size_);
  }
}

void VariantArray::Init(DataType dt) {
  CHECK(dt_ == DataType::NONE && dt != DataType::NONE);
  dt_ = dt;
  type_size_ = aux::DataTypeSize(dt_);
}

void VariantArray::Reserve(size_t new_capacity) {
  CHECK(is_initialized());
  if (capacity_ >= new_capacity) return;

  VLOG(1) << "reserving " << new_capacity << " over " << capacity_ << " for " << dt_;

  uint8* new_ptr = (uint8*)upstream_->allocate(type_size_ * new_capacity);
  if (size_) {
    memcpy(new_ptr, data_.u8ptr, size_ * type_size_);
  }
  if (data_.u8ptr) {
    upstream_->deallocate(data_.u8ptr, type_size_ * capacity_);
  }
  data_.u8ptr = new_ptr;
  capacity_ = new_capacity;
}


void VariantArray::AppendFrom(const VariantArray& other, size_t offset) {
  DCHECK_EQ(dt_, other.data_type());
  DCHECK_LE(offset, other.size());

  size_t len = other.size() - offset;
  Reserve(len + size_);
  memcpy(data_.u8ptr + size_ * type_size_, other.data_.u8ptr + offset*type_size_,
         type_size_ * len);
  size_ += len;
}

void VariantArray::AppendItem(const VariantArray& other, size_t row) {
  DCHECK_EQ(dt_, other.data_type());

  memcpy(data_.u8ptr + size_ * type_size_, other.data_.u8ptr + row*type_size_, type_size_);
  ++size_;
}


size_t VariantArray::AppendFrom(const void* src) {
  DCHECK_LT(size(), capacity());

  size_t val_size = aux::DataTypeSize(dt_);

  uint8* dest = data_.u8ptr + val_size*size_;
  memcpy(dest, src, val_size);

  ++size_;

  return val_size;
}

}  // namespace puma
