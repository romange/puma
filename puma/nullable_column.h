// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <base/pod_array.h>
#include "puma/data_types.h"

namespace puma {

// 16-byte alignment to allow SSE2 access.
typedef base::PODArray<uint8, 16> DefineVector;

class ColumnSlice;


// This class represents a nullable column - an unit of data processing.
// Currently it may or not may contain data vector - according to the requirements.
// In any case it contains DefineVector which in some cases is set to constant true.
class NullableColumn {
  DefineVector def_;
  VariantArray data_;

  friend class ColumnSlice;
 public:
  // mr - supplies memory resource that is used by data and def fields.
  NullableColumn(pmr::memory_resource* mr);

  NullableColumn(NullableColumn&&) = delete;
  NullableColumn& operator=(NullableColumn&& o) = delete;

  void Init(DataType dt, size_t reserve_s);

  void Clear() {
    def_.clear();
    data_.Resize(0);
    offset_ = 0;
  }

  // Uses arena to allocate strings in case the column is DataType::STRING.
  void AppendFrom(const NullableColumn& other, pmr::memory_resource* arena);

  // Appends a single value. data column must be initialized and have enough capacity.
  // Does not grow and check-fails if not enough capacity to append.
  // Returns number of bytes added.
  size_t Append(const void* src, bool def);

  void Reserve(size_t t) {
    def_.reserve(t);
    data_.Reserve(t);
    capacity_ = data_.capacity();
  }

  // sz should not go beyond capacity().
  // def_ data is not changed.
  void Resize(size_t sz) {
    def_.resize_assume_reserved(sz);
    if (data_.is_initialized())
      data_.Resize(sz);
  }

  // If sz is greater than size then the appeneded def range is filled with def.
  void ResizeFill(size_t sz, bool def);


  size_t size() const { return def_.size(); }
  DataType data_type() const { return data_.data_type(); }

  size_t pos() const { return pos_; }
  void set_pos(size_t p) { pos_ = p; }

  // Where the data starts. Can be in [0, size()) range.
  size_t window_offset() const { return offset_; }
  void set_window_offset(size_t off);

  // Read window size.
  size_t window_size() const { return size() - offset_; }

  size_t capacity() const { return capacity_; }
  size_t unused_capacity() const { return capacity() - size(); }

  const DefineVector& def() const { return def_; }
  const VariantArray& data() const { return data_; }
  VariantArray* mutable_data() { return &data_; }
  DefineVector* mutable_def() { return &def_; }

  template<typename T> const T& get(size_t index) const { return data_.at<T>(index); }
  template<typename T> T& get(size_t index) { return data_.at<T>(index); }

  ColumnSlice viewer(size_t read_offset = 0) const;

  // If WindowSize is less than sz_threshold than it moves column data to beginning (if needed).
  // and returns true. offset is set accordingly.
  // Otherwise, returns false.
  bool MoveToBeginningIfSmaller(size_t sz_threshold);

  // for boolean column - performs logical 'and' between data and def vectors.
  // The result is stored in def() vector.
  void ReduceBoolean();

private:

  size_t offset_ = 0, pos_ = 0;
  size_t capacity_ = 0;
};

class ColumnSlice {
 public:
  ColumnSlice() : nc_(nullptr), offset_(0), len_(0) {}

  // Wraps window portion of the column.
  ColumnSlice(const NullableColumn& nc, size_t o = 0, size_t len = size_t(-1));

  size_t read_offset() const { return offset_;}
  size_t size() const { return len_; }

  const NullableColumn& col() const { return *nc_; }

  template<typename T> const T& get(size_t index) const {
    return nc_->data().at<T>(read_offset() + index);
  }

  const uint8& def(size_t index) const { return nc_->def_[index + read_offset()]; }

  DataType data_type() const { return nc_->data_type(); }
  const uint8* offset_raw() const { return nc_->data().raw(read_offset()); }

  void remove_prefix(size_t s) {
    offset_ += s;
    len_ -= s;
  }

  bool is_initialized() const { return nc_ != nullptr; }
 private:
  const NullableColumn* nc_; // Not null for a valid viewer.
  size_t offset_;
  size_t len_;
};

// Debug helper method.
std::ostream& operator<<(std::ostream& o, const ColumnSlice& col);

inline ColumnSlice NullableColumn::viewer(size_t read_offset) const {
  return ColumnSlice(*this, read_offset);
}

namespace aux {
  inline void resize(size_t s, VariantArray* v) { return v->Resize(s); }
  inline void resize(size_t s, DefineVector* v) { return v->resize(s); }
}

}  // namespace puma
