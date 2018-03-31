// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "puma/ast_op.h"
#include "puma/data_types.h"
#include "strings/stringpiece.h"

namespace puma {

union ScanValue {
  AstOp  op;
  int64_t ival;
  uint64_t uval;
  double  dval;
  StringPiece strval;
  AstId ast_id;

  ScanValue() : ival(0) {}
};

}  // namespace puma
