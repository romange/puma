// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "pmr/polymorphic_allocator.h"
#include "puma/puma_types.pb.h"
#include "base/integral_types.h"
#include "strings/stringpiece.h"

namespace puma {

union SlicePtr {
  int32_t* i32ptr;
  uint32_t* u32ptr;
  int64_t* i64ptr;
  uint64_t* u64ptr;

  bool* bptr;
  uint8_t* u8ptr;
  double* dptr;
  StringPiece* strptr;

  SlicePtr() : u32ptr(nullptr) {}
  SlicePtr(int32_t* p) : i32ptr(p) {}
  SlicePtr(uint32_t* p) : u32ptr(p) {}
  SlicePtr(int64_t* p) : i64ptr(p) {}
  SlicePtr(uint64_t* p) : u64ptr(p) {}
  SlicePtr(uint8_t* p) : u8ptr(p) {}
  SlicePtr(char* p) : u8ptr(reinterpret_cast<uint8_t*>(p)) {}
  SlicePtr(double* p) : dptr(p) {}
};


class VariantArray {
  pmr::memory_resource* upstream_;
  SlicePtr data_;

  uint32_t capacity_, size_;

  DataType dt_;
  uint16_t type_size_;

 public:
  VariantArray(pmr::memory_resource* upstream, DataType dt = DataType::NONE);

  VariantArray(VariantArray&& other) : upstream_(other.upstream_), capacity_(other.capacity_),
      size_(other.size_), dt_(other.dt_), type_size_(other.type_size_) {
    if (other.data_.u32ptr) {
      data_.u32ptr = other.data_.u32ptr;
      other.data_.u32ptr = nullptr;
    }
    other.upstream_ = nullptr;
    other.type_size_ = 0;

    static_assert(offsetof(VariantArray, dt_) == 24, "");
  }

  void Swap(VariantArray* o) {
    std::swap(o->capacity_, capacity_);
    std::swap(o->size_, size_);
    std::swap(o->data_.u32ptr, data_.u32ptr);
    std::swap(o->dt_, dt_);
    std::swap(o->upstream_, upstream_);
    std::swap(o->type_size_, type_size_);
  }

  // Assuming that all the fields are plain types.
  ~VariantArray();

  void Init(DataType dt);

  void Reserve(std::size_t sz);


  // Reserves if needed.
  void Resize(std::size_t sz) {
    if (sz)
      Reserve(sz);
    size_ = sz;
  }

  bool is_initialized() const { return dt_ != DataType::NONE; }

  std::size_t size() const { return size_;}

  DataType data_type() const { return dt_; }

  uint16_t data_type_size() const { return type_size_; }
  size_t capacity() const { return capacity_; }

  const SlicePtr& column_ptr() const { return data_; }

  uint8_t* raw(size_t ofs) { return data_.u8ptr + type_size_ * ofs; }
  const uint8_t* raw(size_t ofs) const { return data_.u8ptr + type_size_ * ofs; }

  template<typename U> U& at(size_t ofs) {
    return *reinterpret_cast<U*>(data_.u8ptr + type_size_ * ofs);
  }

  template<typename U> const U& at(size_t ofs) const {
    return *reinterpret_cast<const U*>(data_.u8ptr + type_size_ * ofs);
  }

  template<typename U> std::pair<U*, U*> get() {
    U* first = reinterpret_cast<U*>(data_.u32ptr);
    return make_pair(first, first + size_);
  }

  template<typename U> void set(size_t index, U val) {
    U* first = reinterpret_cast<U*>(data_.u32ptr);
    first[index] = val;
  }

  template<typename U> std::pair<U*, U*> get() const {
    const U* first = reinterpret_cast<const U*>(data_.u32ptr);
    return make_pair(first, first + size_);
  }

  // The object must be initialized first. Copies data and reserves if needed.
  void CopyFrom(const VariantArray& other, size_t offset = 0) {
    size_ = 0;
    AppendFrom(other, offset);
  }

  // Appends other starting from offset. Reserves enough size to accomodate it.
  void AppendFrom(const VariantArray& other, size_t offset = 0);

  // Appends a single item from other[row]. Does not reallocates array.
  void AppendItem(const VariantArray& other, size_t row);

  // Appends a single value and increases size by 1.
  // Capacity must be big enough to accomodate (does not grow cap automatically).
  // Returns number of bytes read from 'value' ptr.
  size_t AppendFrom(const void* value);

  // Does not reallocate...
  void IncSize() { size_++; }

  pmr::memory_resource* mr() { return upstream_; }

};

static_assert(sizeof(VariantArray) == 32, "");

union Variant {
  int32_t i32val;
  uint32_t u32val;
  int64_t i64val;
  uint64_t u64val;

  double dval;
  bool bval;
  StringPiece strval;

  Variant() {
    memset(this, 0, sizeof(Variant));
  }
};
static_assert(sizeof(Variant) == 16, "");

template <DataType dt> struct DTraits;

#define DECLARE_DT_TRAITS(ENUM, field) \
  template <> struct DTraits<DataType::ENUM> { \
      using f_type = decltype(*SlicePtr::field ## ptr); \
      using type = std::decay<f_type>::type; \
      static constexpr bool kIsSigned = std::is_signed<type>::value; \
      static type* & get_ptr(SlicePtr& u) { return u.field ## ptr; } \
      static const type* get_ptr(const SlicePtr& u) { return u.field ## ptr ;} \
      static type& get_val(Variant& v) { return v.field ## val;} \
      static const type& get_val(const Variant& v) { return v.field ## val;} \
\
      template<template<typename> class Func>  \
        static void Apply(const Variant& left, const Variant& right, Variant* dest) { \
          get_val(*dest) = Func<type>()(get_val(left), get_val(right)); \
        } \
      static Variant variant(type val) { \
        Variant res; \
        get_val(res) = val; \
        return res; \
      } \
  }

DECLARE_DT_TRAITS(INT32, i32);
DECLARE_DT_TRAITS(UINT32, u32);
DECLARE_DT_TRAITS(INT64, i64);
DECLARE_DT_TRAITS(UINT64, u64);
DECLARE_DT_TRAITS(BOOL, b);
DECLARE_DT_TRAITS(STRING, str);
DECLARE_DT_TRAITS(DOUBLE, d);


template<DataType dt> using DTCpp_t = typename DTraits<dt>::type;

template<DataType dt> DTCpp_t<dt>* & get_ptr(SlicePtr& u) {
  return DTraits<dt>::get_ptr(u);
}

template<DataType dt> const DTCpp_t<dt>* get_ptr(const SlicePtr& u) {
  return DTraits<dt>::get_ptr(u);
}

template<DataType dt> DTCpp_t<dt>& get_val(Variant& v) {
  return DTraits<dt>::get_val(v);
}

template<DataType dt> const DTCpp_t<dt>& get_val(const Variant& v) {
  return DTraits<dt>::get_val(v);
}

struct TypedVariant {
  Variant variant;
  DataType type = DataType::NONE;

  TypedVariant() {}
  TypedVariant(const Variant& v, DataType t) : variant(v), type(t) {}

  std::string ToString() const;
};

namespace aux {

template<DataType dt> DataType Minus(Variant* tv) {
  get_val<dt>(*tv) *= -1;
  return dt;
}

template<> inline DataType Minus<DataType::UINT32>(Variant* tv) {
  if (tv->u32val < kint32max) {
    tv->i32val = - int32_t(tv->u32val);
    return DataType::INT32;
  }
  tv->i64val = - int64_t(tv->u32val);
  return DataType::INT64;
}

template<> inline DataType Minus<DataType::UINT64>(Variant* tv) {
  if (tv->u64val < kint64max) {
    tv->i64val = - int64_t(tv->u64val);
    return DataType::INT64;
  }
  return DataType::UINT64;
}

}  // namespace aux

}  // namespace puma
