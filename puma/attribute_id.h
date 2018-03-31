// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <cstdint>
#include <ostream>

namespace puma {

typedef uint16_t TableId;

class AttributeId {
 public:
  typedef uint16_t LocalId;
  typedef ::puma::TableId TableId;

  enum { kNullLocalId = 0, kInvalidTableId = 0xFFFF, kInvalidLocalId = 0xFFFF};

  AttributeId() : val_(0) {}
  AttributeId(TableId t, LocalId l) : val_(uint32_t(t) << 16 | l) {}

  static constexpr AttributeId Invalid() { return AttributeId(-1); }
  static constexpr AttributeId None() { return AttributeId(0); }

  LocalId local_id() const { return val_ & 0xFFFF; }
  uint16_t table_id() const { return val_ >> 16; }

  bool operator==(AttributeId o) const { return o.val_ == val_;}
  bool operator!=(AttributeId o) const { return ! (*this == o);}

  bool local_defined() const { return local_id() != kNullLocalId; }
 private:
  explicit constexpr AttributeId(uint32_t val) : val_(val) {}

  uint32_t val_ = 0;
};

constexpr AttributeId kInvalidAttrId = AttributeId::Invalid();
constexpr AttributeId kNullAttrId = AttributeId::None();
constexpr TableId kNullTableId = 0;

std::ostream& operator<<(std::ostream& o, const AttributeId& e);


}  // namespace puma
