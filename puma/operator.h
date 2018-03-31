// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <vector>

#include "base/integral_types.h"
#include "strings/stringpiece.h"

#include "puma/data_types.h"
#include "puma/file/bare_slice_provider.h"
#include "puma/nullable_column.h"
#include "puma/parser/ast_expr.h"
#include "puma/ops_builder.h"

#include "util/status.h"

namespace puma {

// Public - to allow unit-testing.
class ColumnConsumer;
class Operator;

class OutputData {
  friend ColumnConsumer;
  friend Operator;

  const NullableColumn& col_;
  size_t consume_limit_;
 public:
  OutputData(const NullableColumn& c) : col_(c), consume_limit_(0) {}
  void InitConsumeLimit() { consume_limit_ = col_.pos() + col_.window_size(); }
  size_t consume_limit() const { return consume_limit_; }
};

class ColumnConsumer {
  OutputData* rd_;

  size_t read_pos_; // Until what position we read from this column.
 public:
  ColumnConsumer() : rd_(nullptr),  read_pos_(0) {}
  explicit ColumnConsumer(OutputData& s) : rd_(&s), read_pos_(s.col_.pos()) {}
  const NullableColumn& column() const { return rd_->col_; }

  // Returns absolute offset inside column based on the read position of this consumer not
  // including the internal offset of the column.
  // In other words, if this consumer read_pos is 10 but column position is 4 then GetOffset()
  // returns 10-4=6.
  // We should start reading from relative position 6. If, in addition, the internal column offset
  // is 7 then we should start reading from absolute position 13 inside the column. GetOffset will
  // still return 6 though.
  size_t ConsumeOffset() const;

  size_t AvailableData() const { return column().window_size() - ConsumeOffset(); }

  // Must be called with 0 even if no advancement was made.
  void ConsumeHighMark(size_t offs);

  ColumnSlice viewer(size_t max_len) const {
    return ColumnSlice(rd_->col_, ConsumeOffset(), max_len);
  }

  bool is_defined() const { return rd_ != nullptr; }
};

class Operator {
  Operator(const Operator&) = delete;
  void operator=(const Operator&) = delete;

 public:

  virtual ~Operator();

  // Returns Status::OK if everything worked. Can still produce empty output.
  util::Status Apply();

  const DefineVector& def() const { return result_column_.def(); }
  const VariantArray& data() const { return result_column_.data(); }
  size_t result_size() const { return result_column_.size(); }

  // **********************************************************************************
  // We need a proxy object that wraps result_column_ and provides the following:
  // 1. Private state for each consumer of result_column_ that stores which position
  //    a reader reached.
  // 2. Shared state that is computed as minimum of all positions.
  // 3. DiscardConsumed method that uses the shared state to discard data that was consumed
  //    by every dependent reader.
  // 4. The execution should be stopped if load evaluators processed everything
  //    and every dependent evaluator also processed all its data.
  ColumnConsumer result() { return ColumnConsumer{result_data_}; }

  // Must run after Apply. Changes result_column's define vector so that only cells marked by pass
  // will be defined in this evaluator.
  // void ApplyPassMask(const DefineVector* pass);

  // For expr evaluator that corresponds to pass mask, it makes sure that only its define vector
  // can be used for describing which rows should pass. In practice it computes a logical AND
  // between def vector and the BOOL VariantArray data. Returns number of defined rows in this
  // evaluator.

  // TODO: Should do this preprocessing for every boolean evaluator? We should if we can.
  // It will make the computation of dependent expressions easier.
  void RealizePassMask();

  const AstExpr& expr() const { return expr_; }

  // Returns number of bytes discarded.
  size_t DiscardConsumed();

 protected:
  static constexpr unsigned kPageSize = 1024;

  virtual util::Status ApplyInternal() = 0;

  Operator(const AstExpr& e, pmr::memory_resource* mr);

  // Returns true if result column has enough data,
  // Otherwise moves its data to front and prepares for appending operation and returns false.
  bool PrepareResultColumn();

  struct OffsetResult;

  // Returns true if result_column_ has enough data so this evaluator may skip processing
  // its inputs during the current iteration.
  // Otherwise it will move contents of result_column so that it starts from the beginning
  // (offset = 0), it will also resize the result to accomodate new data and will return the
  // offset where to start writing and reading the new results.
  // ColumnConsumer will be advanced accordingly.
  bool ComputeOffsets(ColumnConsumer* src, OffsetResult* res);

  struct OffsetResult {
    size_t prev_len;
    ColumnSlice src_viewer;
  };


  AstExpr expr_;

  NullableColumn result_column_;
  OutputData result_data_;

  // If this vector is not null it means only rows that evaluate to true in this array should be
  // taken into the account.
  const DefineVector* pass_mask_ = nullptr;
};

class DefFnEvaluator : public Operator {
  ColumnConsumer src_;
 public:
  DefFnEvaluator(const AstExpr& e, const ColumnConsumer& cc);

  util::Status ApplyInternal() override;
};

class LenFnEvaluator : public Operator {
 public:
  LenFnEvaluator(const AstExpr& e, BareSliceProvider* length);

  util::Status ApplyInternal() override;

 private:
  BareSliceProvider* lengths_provider_;
};

class ReplOperator : public Operator {
  ColumnConsumer length_, operand_;
 public:
  ReplOperator(const AstExpr& e, const ColumnConsumer& len_col,
               const ColumnConsumer& operand);

 private:
  util::Status ApplyInternal() override;

  void Replicate(size_t operand_offs, size_t count);

  // How many items were replicated from the last item
  size_t last_repl_used_ = 0;
};



/*class RestrictFnEvaluator : public Operator {
  const NullableColumn& lhs_;
  const NullableColumn& restrict_;
 public:
  RestrictFnEvaluator(const AstExpr& e, const NullableColumn& left, const NullableColumn& right);

 private:
  util::Status ApplyInternal(uint32 window_size) override;
};
*/

// Implements "const <op> x" case.
class BinOpConstVarEvaluator : public Operator {
 public:
  BinOpConstVarEvaluator(const AstExpr& e, const ColumnConsumer& ref, const AstExpr& const_val);
  ~BinOpConstVarEvaluator();

 private:
  ColumnConsumer src_;
  TypedVariant const_val_;

  util::Status ApplyInternal() override;

  ConstOperandFunc func_ = nullptr;
};

class BinOpConstVarEvaluatorString : public Operator {
 public:
  BinOpConstVarEvaluatorString(const AstExpr& e, const ColumnConsumer& ref,
                               const AstExpr& const_val);

 private:
  ColumnConsumer src_;
  StringPiece const_val_;

  util::Status ApplyInternal() override;
};

// Implements x <op> y case with 2 variables. Currently only addition of integers is supported.
class BinOpEvaluator : public Operator {
 public:
  BinOpEvaluator(const AstExpr& e, const ColumnConsumer& left, const ColumnConsumer& right);
 protected:

  util::Status ApplyInternal() override;

  ColumnConsumer left_;
  ColumnConsumer right_;
};

class ReduceOperator : public Operator {
 public:
  ReduceOperator(const AstExpr& e, AstOp agg_op, const ColumnConsumer& data,
                 const ColumnConsumer& length);
 protected:

  util::Status ApplyInternal() override;

 private:
  bool Reduce(size_t len, const ColumnSlice& src);

  ColumnConsumer data_;
  ColumnConsumer length_;

  size_t interm_len_ = 0;
  Variant interm_data_;
  bool interm_def_ = false;
};


class AlignOperator : public Operator {
 public:
  AlignOperator(const AstExpr& e, const ColumnConsumer& align,
                 const ColumnConsumer& data);
 protected:

  util::Status ApplyInternal() override;

 private:
  ColumnConsumer align_;
  ColumnConsumer data_;
};


}  // namespace puma
