// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <cstddef>
#include <pmr/vector.h>

#include "base/chunked_array.h"
#include "util/status.h"

#include "puma/attribute_id.h"
#include "puma/data_types.h"
#include "puma/parser/scan_value.h"

namespace puma {

// Describes an expression node in the parser DAG. Has all the required static information
// data type, operator, relations to other nodes etc.
// In addition has the context information like the aggregation property, what parts of data
// are used in the computations etc. Ideally should contain all the possible information that
// can be extracted during parsing for performing most optimizations.
class AstExpr {
 public:
  typedef uint16 FlagsMask;

  enum Flags : FlagsMask {
    DEFINED_VALUE = 0x1,  // Set if u_.v is well defined, i.e. is not-null.

    // Marks transitively expressions that are dependent on constant values.
    // All the expressions that are dependent only on constants or on other scalar expressions but
    // could not be evaluated during the parsing phase are marked with this flag.
    // Scalar expressions can be computed using constant propagation during the evaluation of
    // the appropriate fence.
    // Aggregated expressions are NOT marked as scalar since they require evaluation of the
    // aggregating function over columnar data.
    SCALAR_EXPR = 0x2,

    UNARY_ARG = 0x8, // only 'left' child is defined. 'right' could be used for other stuff.
    BINARY_ARG = 0x10, // both children are defined.

    // Marks fence output variables.
    FENCE_COL = 0x20,

    // Whether values of this (non-scalar) expression are "always present",
    // i.e. will always be non-null and well defined because of how they are formed.
    // For example, it could represent required column or result of def() or len() functions.
    ALWAYS_PRESENT = 0x40,
  };

  union AstData {
    Variant v;

    struct Children {
      AstId left, right;
    } children;

    AstData() : v() {}

  } __attribute__((aligned(4)));

  static_assert(sizeof(AstData) == 16, "");

  AstExpr() : data_type_(DataType::NONE), flags_(0), op_(AstOp::CONST_VAL)   {
  }

  AstExpr(AstOp c, DataType v, FlagsMask flags = 0) : data_type_(v), flags_(flags), op_(c)  {}

  explicit AstExpr(const TypedVariant& tv) : data_type_(tv.type), flags_(DEFINED_VALUE),
                                             op_(AstOp::CONST_VAL) {
    u_.v = tv.variant;
  }

  explicit AstExpr(DataType dt, const Variant& var) : data_type_(dt), flags_(DEFINED_VALUE),
                                                      op_(AstOp::CONST_VAL) {
    u_.v = var;
  }

  static AstExpr OpNode(AstOp t, DataType data_type, AstId left, AstId right, FlagsMask flags = 0);


  DataType data_type() const { return data_type_; }

  // Immediate constant value. Known during the compilation (and particularly can not be
  // fence output column).
  bool is_immediate() const { return op_ == AstOp::CONST_VAL && (flags_ & FENCE_COL) == 0; }

  bool is_scalar() const { return op_ == AstOp::CONST_VAL || (flags_ & SCALAR_EXPR); }

  // returns true if the expression is defined.
  // Undefined expressions are typed because otherwise the following expression will be valid:
  // "string s := 5/0 " only because rhs is null.
  // For undefined expressions def(expr) is false.
  bool is_defined() const { return flags_ & DEFINED_VALUE;}

  bool is_unary() const { return (flags_ & UNARY_ARG) != 0; }

  StringPiece str() const { return u_.v.strval; }

  Variant* mutable_variant() { return &u_.v; }
  const Variant& variant() const { return u_.v;}

  AstOp op() const { return op_; }

  template<DataType dt> static AstExpr ConstVal(DTCpp_t<dt> v) {
    AstExpr res(AstOp::CONST_VAL, dt, DEFINED_VALUE);
    get_val<dt>(res.u_.v) = v;
    return res;
  }

  std::string ToString() const;
  FlagsMask flags() const { return flags_; }

  // Enables (sets) 'mask' bits to the current value.
  void set_mask(FlagsMask mask) { flags_ |= mask; }

  AstId left() const { return u_.children.left; }
  AstId right() const { return u_.children.right; }

  void set_left(AstId id) { u_.children.left = id; }
  void set_right(AstId id) { u_.children.right = id; }

  // Applies const computation and transforms expression into const expr.
  // operand must be const.
  static AstExpr TransformConst1(AstOp op, const AstExpr& operand);
  static AstExpr TransformConst2(AstOp op, const AstExpr& left, const AstExpr& right);

  // Resolves the operator datatype based on the operand and puts it into 'result'.
  // In addition it returns the relation mask of the operand.
  // Can we make it more elegant?!!
  static util::Status ResolveUnaryOpType(AstOp op, DataType operand_type, DataType* result);

  void Undefine() { flags_ &= (~DEFINED_VALUE);}

  AttributeId attr_id() const { return attr_id_; }
  void set_attr_id(AttributeId rgid) { attr_id_ = rgid; }

  bool operator==(const AstExpr& other) const;
 private:
  AstData u_;
  DataType data_type_;

  FlagsMask flags_ = 0;
  AstOp op_;
  AttributeId attr_id_ = kInvalidAttrId;

};

static_assert(sizeof(AstExpr) >= 24, "");

std::ostream& operator<<(std::ostream& o, const AstExpr& e);

typedef base::ChunkedArray<AstExpr> ExprArray;  // ChunkedArray so that it won't be reallocated.

}  // namespace puma
