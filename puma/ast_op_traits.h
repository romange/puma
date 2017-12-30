// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "puma/ast_op.h"
#include "puma/variant_array.h"

namespace puma {

namespace aux {

template<AstOp> struct AstOpSelector {};

template<DataType dt> std::pair<DTCpp_t<dt>, bool>
   BinaryOp(AstOpSelector<AstOp::DIV_OP>, const DTCpp_t<dt>& a,
            const DTCpp_t<dt>& b) {
  if (b == 0)
    return {0, false};

  return {a / b, true};
}

template<DataType dt> std::pair<DTCpp_t<dt>, bool>
   BinaryOp(AstOpSelector<AstOp::MUL_OP>, const DTCpp_t<dt>& a,
            const DTCpp_t<dt>& b) {
  return {a * b, true};
}

template<DataType dt> std::pair<DTCpp_t<dt>, bool>
   BinaryOp(AstOpSelector<AstOp::ADD_OP>, const DTCpp_t<dt>& a,
            const DTCpp_t<dt>& b) {
  return {a + b, true};
}

template<DataType dt> std::pair<DTCpp_t<dt>, bool>
   BinaryOp(AstOpSelector<AstOp::SUB_OP>, const DTCpp_t<dt>& a,
            const DTCpp_t<dt>& b) {
  return {a - b, true};
}

template<DataType dt> std::pair<bool, bool>
   BinaryOp(AstOpSelector<AstOp::EQ_OP>, const DTCpp_t<dt>& a,
            const DTCpp_t<dt>& b) {
  return {a == b, true};
}

template<DataType dt> std::pair<bool, bool>
   BinaryOp(AstOpSelector<AstOp::LT_OP>, const DTCpp_t<dt>& a,
            const DTCpp_t<dt>& b) {
  return {a < b, true};
}

template<DataType dt, AstOp op> bool BinaryOpVariant(
    const Variant& left, const Variant& right, TypedVariant* dest) {
  auto res = BinaryOp<dt>(AstOpSelector<op>{},
                          get_val<dt>(left), get_val<dt>(right));
  if (!res.second) {
    return false;
  }
  constexpr DataType kResultDt = IsLogical(op) ? DataType::BOOL : dt;
  get_val<kResultDt>(dest->variant) = res.first;
  dest->type = kResultDt;

  return true;
}

}  // namespace aux

bool ApplyVariant(DataType dt, AstOp op,
    const Variant& left, const Variant& right, TypedVariant* dest);


}  // namespace puma
