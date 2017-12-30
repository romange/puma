// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <ostream>

namespace puma {

enum class AstOp : unsigned char {
  DIV_OP = 0,
  MUL_OP = 1,
  ADD_OP = 2,
  SUB_OP = 3,
  //  = 4,
  SHIFT_RIGHT_OP = 5,
  SHIFT_LEFT_OP = 6,
  LAST_ARITHMETIC_INDEX = 6,
  CONST_VAL = 7,

  // LOGICAL and RELATION_OP
  GEQ_OP = 11,
  LEQ_OP = 12,
  GT_OP = 13,
  LT_OP = 14,
  EQ_OP = 15,
  NEQ_OP = 16,

  NOT_OP = 17,
  AND_OP = 18,
  OR_OP = 19,
  XOR_OP = 20,
  LAST_LOGICAL_INDEX = 20,   // End of logical operators.

  LEN_FN = 24,
  DEF_FN = 25,
  LOAD_FN = 26,

  ALIGN_FN = 32,  // used for extending columns with respect to other def vectors.
  RESTRICT_FN = 33,
  REPL_FN = 34,
  REDUCE_FN = 35,


  SUM_FN = 40,
  MAX_FN = 41,
  CNT_FN = 42,

  LAST_AGGREGATED_INDEX = 42,
};

std::ostream& operator<<(std::ostream& o, AstOp op);

namespace aux {

inline constexpr bool IsLogical(AstOp op) { return int(op) >= int(AstOp::GEQ_OP) &&
                                                   int(op) <= int(AstOp::LAST_LOGICAL_INDEX); }

inline constexpr bool IsArithmetic(AstOp op) {
  return int(op) < int(AstOp::LAST_ARITHMETIC_INDEX);
}

// Reverses the operation: "<" -> ">", "<=" -> ">="; Symmetric operators stay the same.
AstOp SwapLogicalDirection(AstOp);

inline constexpr bool IsAggregator(AstOp op) {
  return int(op) >= int(AstOp::SUM_FN) && int(op) <= int(AstOp::LAST_AGGREGATED_INDEX);
}

const char* ToString(AstOp op);

}  // namespace aux

}  // namespace puma
