// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/parser/ast_expr.h"

#include "base/logging.h"

#include "puma/ast_op_traits.h"
#include "strings/strcat.h"

namespace puma {
using std::string;
using util::Status;
using util::StatusCode;

namespace {

inline Status ToStatus(string msg) {
  return Status(StatusCode::PARSE_ERROR, std::move(msg));
}

}  // namespace


AstExpr AstExpr::OpNode(AstOp t, DataType data_type, AstId left, AstId right, FlagsMask flags) {
  AstExpr res{t, data_type};

  res.u_.children.left = left;
  res.u_.children.right = right;
  res.flags_ = flags;

  return res;
}

string AstExpr::ToString() const {
  string res;
  StrAppend(&res, aux::ToString(op_), " ");

  switch (op_) {
    case AstOp::CONST_VAL:
      StrAppend(&res, TypedVariant(u_.v, data_type()).ToString());
      break;
    case AstOp::LOAD_FN:
      StrAppend(&res, DataType_Name(data_type_), "/", u_.v.strval);
    break;
    default:;
  }
  StrAppend(&res, " flags: ", flags_, ", attr_id: ", attr_id_.table_id(), "/", attr_id_.local_id());

  return res;
}

AstExpr AstExpr::TransformConst1(AstOp op, const AstExpr& operand) {
  DCHECK(operand.is_immediate());

  switch (op) {
    case AstOp::SUB_OP: {
      Variant v = operand.variant();
      DataType dt = UnaryMinus(operand.data_type(), &v);
      return AstExpr{dt, v};
    }
    case AstOp::NOT_OP: {
      DCHECK_EQ(operand.data_type(), DataType::BOOL);
      return ConstVal<DataType::BOOL>(operand.variant().bval ^ true);
    }
    case AstOp::DEF_FN:
      return ConstVal<DataType::BOOL>(operand.is_defined());
    default:
      LOG(FATAL) << "Unsupported operator [" << op << "]";
  }
  return AstExpr(AstOp::CONST_VAL, DataType::NONE);
}

AstExpr AstExpr::TransformConst2(AstOp op, const AstExpr& left, const AstExpr& right) {
  if (!left.is_defined() || !right.is_defined())
    return AstExpr(AstOp::CONST_VAL, DataType::NONE);

  TypedVariant lv(left.variant(), left.data_type()), rv(right.variant(), right.data_type());

  CHECK(aux::EqualizeTypes(&lv, &rv));

  TypedVariant res_var;
  ApplyVariant(lv.type, op, lv.variant, rv.variant, &res_var);

  AstExpr res(AstOp::CONST_VAL, res_var.type);
  *res.mutable_variant() = res_var.variant;

  return res;
}

Status AstExpr::ResolveUnaryOpType(AstOp op, DataType operand_type, DataType* result) {
  if (operand_type == DataType::NONE) {
    *result = DataType::NONE;
    return Status::OK;
  }

  switch (op) {
    case AstOp::NOT_OP:
      if (operand_type != DataType::BOOL)
        return ToStatus("Expected boolean expression");
      *result = DataType::BOOL;
    break;
    case AstOp::SUB_OP:
      if (!aux::IsInteger(operand_type))
        return ToStatus("Expected integer expression");
      *result = operand_type;
    break;
    case AstOp::DEF_FN:
      *result = DataType::BOOL;
    break;
    case AstOp::LEN_FN:
      *result = kArrayLenDt;
    break;

    case AstOp::SUM_FN:
      if (aux::IsInteger(operand_type) || operand_type == DataType::BOOL) {
        *result = DataType::INT64;
      } else if (operand_type == DataType::DOUBLE) {
        *result = DataType::DOUBLE;
      } else {
        return ToStatus("Can not sum non-integer expression");
      }
    break;
    case AstOp::MAX_FN:
      *result = operand_type;
    break;

    default:
      return ToStatus(StrCat("Unsupported operator ", aux::ToString(op)));
  }
  return Status::OK;
}


// We ignore the flags when comparing expressions since it seems that astexpr is equal when
// its operand and children are equal.
bool AstExpr::operator==(const AstExpr& other) const {
  return other.data_type_ == data_type_ && other.op_ == op_ &&
        std::memcmp(&other.u_, &u_, sizeof u_) == 0;
}

std::ostream& operator<<(std::ostream& o, const AstExpr& e) {
  o << e.ToString();
  return o;
}

}  // namespace puma
