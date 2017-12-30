// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <utility>
#include "base/integral_types.h"

#include "strings/stringpiece.h"
#include "puma/puma_types.pb.h"
#include "puma/variant_array.h"

namespace puma {

std::ostream& operator<<(std::ostream& o, DataType op);


typedef int AstId;
constexpr AstId kNullAstId = -1;

// type aux - helper functions.
namespace aux {

uint8 DataTypeSize(DataType dt);

inline bool IsInteger(DataType dt)  {
  return unsigned(dt) <= unsigned(DataType::UINT64) &&
         unsigned(dt) >= unsigned(DataType::INT32);
}

bool EqualizeTypes(TypedVariant* l, TypedVariant* r);
bool PromoteToType(DataType dt, TypedVariant* tv);

bool PromoteToDouble(TypedVariant* tv);

}  // namespace aux


constexpr DataType kArrayLenDt = DataType::UINT32;
constexpr DataType kStringLenDt = DataType::UINT32;
typedef DTCpp_t<kArrayLenDt> arrlen_t;
constexpr arrlen_t kNullStrSz = 0;

inline std::ostream& operator<<(std::ostream& o, const TypedVariant& tv) {
  o << tv.ToString();
  return o;
}

DataType UnaryMinus(DataType src, Variant* variant);
Variant MakeZero(DataType src);

}  // namespace puma
