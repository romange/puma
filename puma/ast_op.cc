// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/ast_op.h"

namespace puma {


using std::string;

std::ostream& operator<<(std::ostream& o, AstOp op) {
  o << aux::ToString(op);
  return o;
}

namespace aux {

AstOp SwapLogicalDirection(AstOp p) {
  switch(p) {
    case AstOp::LEQ_OP: return AstOp::GEQ_OP;
    case AstOp::GEQ_OP: return AstOp::LEQ_OP;
    case AstOp::LT_OP: return AstOp::GT_OP;
    case AstOp::GT_OP: return AstOp::LT_OP;
    default: return p;
  }
}

}  // namespace aux
}  // namespace puma
