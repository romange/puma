// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/operator.h"
#include "base/logging.h"
#include "base/stl_util.h"

using util::Status;

namespace puma {

size_t ColumnConsumer::ConsumeOffset() const {
  DCHECK_GE(read_pos_, rd_->col_.pos());
  return read_pos_ - rd_->col_.pos();
}

void ColumnConsumer::ConsumeHighMark(size_t offs) {
  read_pos_ += offs;
  if (read_pos_ < rd_->consume_limit_) {
    rd_->consume_limit_ = read_pos_;
  }
}

Operator::Operator(const AstExpr& e, pmr::memory_resource* mr)
  : expr_(e), result_column_(mr), result_data_(result_column_) {
  CHECK(!aux::IsAggregator(e.op()));
}

Operator::~Operator() {
}

Status Operator::Apply() {
  RETURN_IF_ERROR(ApplyInternal());

  VLOG(2) << "Result: " << result_column_;
  result_data_.InitConsumeLimit();

  return Status::OK;
}

void Operator::RealizePassMask() {
  result_column_.ReduceBoolean();
}


size_t Operator::DiscardConsumed() {
  CHECK_GE(result_data_.consume_limit_, result_column_.pos());

  size_t consume_cnt = result_data_.consume_limit_ - result_column_.pos();
  result_column_.set_pos(result_data_.consume_limit_);

  ColumnSlice v = result_column_.viewer();

  DCHECK_LE(consume_cnt, v.size());
  if (consume_cnt == v.size()) {
    result_column_.Clear();
    VLOG(1) << "Consume and clear " << expr_;
  } else {
    result_column_.set_window_offset(result_column_.window_offset() + consume_cnt);
    VLOG(1) << "Consume and offset by " << consume_cnt;
  }
  return result_column_.data().data_type_size() * consume_cnt;
}

bool Operator::PrepareResultColumn() {
  // if MoveToBeginningIfSmall moved data, it means it has less than 32 records and we can add more.
  return !result_column_.MoveToBeginningIfSmaller(32);
}

bool Operator::ComputeOffsets(ColumnConsumer* src, OffsetResult* res) {
  if (PrepareResultColumn()) {
    src->ConsumeHighMark(0);
    return true;
  }

  res->src_viewer = src->viewer(result_column_.unused_capacity());

  // Computes how much items we can process. This is minimum of how many items we can read and
  // how much unused space there is in the result column.
  // Resizes the result column accordingly.
  res->prev_len = result_column_.size();
  result_column_.Resize(res->prev_len + res->src_viewer.size());
  return false;
}


#if 0
RestrictFnEvaluator::RestrictFnEvaluator(const AstExpr& e,
    const ColumnConsumer& left, const ColumnConsumer& right)
    : Operator(e, pmr::get_default_resource()), lhs_(left), restrict_(right) {
  CHECK_EQ(DataType::BOOL, restrict_.data_type());

  result_column_.Init(lhs_.data_type());
}

Status RestrictFnEvaluator::ApplyInternal() {
  VLOG(1) << "RestrictFnEvaluator: " << lhs_.size();

  DefineVector& dest_def = *result_column_.mutable_def();

  dest_def.assign(lhs_.def().begin() + lhs_.window_offset(), lhs_.def().end());

  CHECK_EQ(dest_def.size(), lhs_.size() - lhs_.window_offset()) << "def: " << dest_def.size();
  CHECK_EQ(dest_def.size(), restrict_.size() - restrict_.window_offset());
  CHECK_EQ(restrict_.data().size(), restrict_.def().size());

  const uint8* bptr = restrict_.data().begin<uint8>();
  for (size_t i = restrict_.window_offset(); i < restrict_.size(); ++i) {
    size_t new_offs = i - restrict_.window_offset();
    dest_def[new_offs] = dest_def[new_offs] && restrict_.def()[i] && bptr[i];
  }
  result_column_.mutable_data()->CopyFrom(lhs_.data(), lhs_.window_offset());

  CHECK_EQ(result_column_.size(), result_column_.def().size());

  return true;
}
#endif

DefFnEvaluator::DefFnEvaluator(const AstExpr& e, const ColumnConsumer& src)
    : Operator(e, pmr::get_default_resource()), src_(src) {
  result_column_.Init(DataType::BOOL, kPageSize);

  result_column_.mutable_def()->resize_fill(kPageSize, true);
  result_column_.mutable_def()->clear();
}

Status DefFnEvaluator::ApplyInternal() {
  OffsetResult frame;
  if (ComputeOffsets(&src_, &frame)) {
    return Status::OK;
  }

  DCHECK_EQ(0, result_column_.window_offset());

  const ColumnSlice& viewer = frame.src_viewer;

  const uint8* ptr = &viewer.def(0);
  std::copy(ptr, ptr + viewer.size(),
            result_column_.mutable_data()->column_ptr().u8ptr + frame.prev_len);

  // Advance the read marker.
  src_.ConsumeHighMark(viewer.size());
  return Status::OK;
}


ReplOperator::ReplOperator(const AstExpr& src, const ColumnConsumer& length,
                           const ColumnConsumer& operand)
    : Operator(src, pmr::get_default_resource()), length_(length), operand_(operand) {
  result_column_.Init(operand_.column().data_type(), kPageSize);
  CHECK_EQ(DataType::UINT32, length_.column().data_type());
}

Status ReplOperator::ApplyInternal() {
  if (PrepareResultColumn()) {
    length_.ConsumeHighMark(0);
    operand_.ConsumeHighMark(0);

    return Status::OK;
  }

  const NullableColumn& len_col = length_.column();
  const NullableColumn& data_col = operand_.column();
  CHECK_EQ(data_col.pos(), len_col.pos());

  ColumnSlice len_view(len_col.viewer(length_.ConsumeOffset()));

  size_t read_op_ofs = operand_.ConsumeOffset() + data_col.window_offset();
  size_t len = std::min(len_view.size(), data_col.size() - read_op_ofs);
  size_t unused = result_column_.unused_capacity();
  VLOG(2) << "Replicating " << len << " items, unused " << unused;

  size_t index = 0;
  if (last_repl_used_) {
    size_t repl = len_view.get<arrlen_t>(0) - last_repl_used_;
    CHECK_GT(repl, 0);

    if (repl <= unused) {
      Replicate(read_op_ofs, repl);
      unused -= repl;
      last_repl_used_ = 0;
      ++index;
    } else {
      Replicate(read_op_ofs, unused);
      last_repl_used_ += unused;
      unused = 0;
    }
  }

  for (; unused && index < len; ++index) {
    size_t repl = len_view.get<arrlen_t>(index);

    if (repl > unused) {
      Replicate(read_op_ofs + index, unused);
      last_repl_used_ = unused;
      break;
    }
    Replicate(read_op_ofs + index, repl);
    unused -= repl;
  }
  length_.ConsumeHighMark(index);
  operand_.ConsumeHighMark(index);
  return Status::OK;
}

void ReplOperator::Replicate(size_t operand_offs, size_t count) {
  if (!count)
    return;

  bool def = operand_.column().def()[operand_offs];
  if (!def) {
    result_column_.ResizeFill(result_column_.size() + count, false);
  } else {
    const void* src = operand_.column().data().raw(operand_offs);
    for (size_t i = 0; i < count; ++i) {
      result_column_.Append(src, true);
    }
  }
}

BinOpConstVarEvaluator::BinOpConstVarEvaluator(const AstExpr& e, const ColumnConsumer& cc,
                                               const AstExpr& const_val)
     : Operator(e, pmr::get_default_resource()), src_(cc),
        const_val_(const_val.variant(), const_val.data_type()) {
  CHECK(const_val.is_immediate());

  result_column_.Init(e.data_type(), kPageSize);

  CHECK_EQ(const_val.data_type(), src_.column().data_type());
  CHECK(const_val.is_defined()) << "TBD";

  func_ = CHECK_NOTNULL(BuildConstOperandFunc(e.op(), const_val.data_type()));
}

BinOpConstVarEvaluator::~BinOpConstVarEvaluator() {
  FreeJitFunc((void*)func_);
}

Status BinOpConstVarEvaluator::ApplyInternal() {
  VLOG(1) << "BinOpConstVarEvaluator::ApplyInternal: " << expr_.op();

  OffsetResult frame;
  if (ComputeOffsets(&src_, &frame)) {
    return Status::OK;
  }
  DCHECK_EQ(0, result_column_.window_offset());

  const ColumnSlice& viewer = frame.src_viewer;

  auto it = &viewer.def(0);
  std::copy(it, it + viewer.size(), result_column_.mutable_def()->begin() + frame.prev_len);

  func_(viewer.offset_raw(),
        result_column_.def().data(),
        result_column_.def().size(), const_val_.variant,
        result_column_.mutable_data()->column_ptr().u8ptr + frame.prev_len);

  // Advance the read marker.
  src_.ConsumeHighMark(viewer.size());
  return Status::OK;
}


BinOpConstVarEvaluatorString::BinOpConstVarEvaluatorString(
      const AstExpr& e, const ColumnConsumer& cc, const AstExpr& const_val)
     : Operator(e, pmr::get_default_resource()), src_(cc),
        const_val_(const_val.variant().strval) {
  CHECK(const_val.is_immediate());

  CHECK_EQ(const_val.data_type(), src_.column().data_type());
  CHECK_EQ(AstOp::EQ_OP, expr_.op()) << "TBD: to expand string evals";

  result_column_.Init(DataType::BOOL, kPageSize);
}

Status BinOpConstVarEvaluatorString::ApplyInternal() {
  VLOG(1) << "BinOpConstVarEvaluatorString::ApplyInternal: " << expr_.op();

  OffsetResult frame;
  if (ComputeOffsets(&src_, &frame)) {
    return Status::OK;
  }
  DCHECK_EQ(0, result_column_.window_offset());
  VLOG(1) << "BinOpConstVarEvaluatorString: src len " << frame.src_viewer.size() << ", prev_len: "
          << frame.prev_len;

  const ColumnSlice& viewer = frame.src_viewer;

  auto it = &viewer.def(0);
  std::copy(it, it + viewer.size(), result_column_.mutable_def()->begin() + frame.prev_len);

  const StringPiece* src = &viewer.get<StringPiece>(0);
  for (size_t i = 0; i < viewer.size(); ++i) {
    if (viewer.def(i)) {
      bool res = (src[i] == const_val_);
      result_column_.mutable_data()->set(i + frame.prev_len, res);
    }
  }

  // ConsumeHighMark the read marker.
  src_.ConsumeHighMark(viewer.size());
  return Status::OK;
}

BinOpEvaluator::BinOpEvaluator(const AstExpr& e, const ColumnConsumer& left,
                               const ColumnConsumer& right)
    : Operator(e, pmr::get_default_resource()), left_(left), right_(right) {
  CHECK_EQ(AstOp::ADD_OP, expr_.op());
  CHECK_EQ(DataType::INT64, left_.column().data_type());
  CHECK_EQ(DataType::INT64, right_.column().data_type());

  result_column_.Init(DataType::INT64, kPageSize);
}

Status BinOpEvaluator::ApplyInternal()  {
  VLOG(2) << "BinOpEvaluator::ApplyInternal";

  OffsetResult lframe;
  if (ComputeOffsets(&left_, &lframe)) {
    right_.ConsumeHighMark(0);
    return Status::OK;
  }
  DCHECK_EQ(0, result_column_.window_offset());

  const ColumnSlice& lviewer = lframe.src_viewer;
  ColumnSlice rviewer(right_.column(), right_.ConsumeOffset());

  size_t len = std::min(lviewer.size(), rviewer.size());
  result_column_.Resize(lframe.prev_len + len);

  const int64* l = lviewer.col().data().column_ptr().i64ptr + lviewer.read_offset();
  const int64* r = rviewer.col().data().column_ptr().i64ptr + rviewer.read_offset();
  auto& def = *result_column_.mutable_def();

  for (size_t i = 0; i < len; ++i) {
    def[i] = lviewer.def(i) && rviewer.def(i);
    if (def[i]) {
      result_column_.mutable_data()->set(i + lframe.prev_len, l[i] + r[i]);
    }
  }
  left_.ConsumeHighMark(len);
  right_.ConsumeHighMark(len);
  return Status::OK;
}

ReduceOperator::ReduceOperator(const AstExpr& e, AstOp agg_op, const ColumnConsumer& data,
                               const ColumnConsumer& length)
    : Operator(e, pmr::get_default_resource()), data_(data), length_(length) {
  CHECK_EQ(AstOp::SUM_FN, agg_op);
  DCHECK_EQ(kArrayLenDt, length.column().data_type());

  result_column_.Init(data.column().data_type(), kPageSize);

  std::memset(&interm_data_, 0, sizeof(Variant));
}

Status ReduceOperator::ApplyInternal() {
  if (PrepareResultColumn()) {
    data_.ConsumeHighMark(0);
    length_.ConsumeHighMark(0);
    return Status::OK;
  }

  DCHECK_EQ(0, result_column_.window_offset());

  ColumnSlice len_slice = length_.viewer(result_column_.unused_capacity());
  ColumnSlice data_slice = data_.viewer(result_column_.unused_capacity());

  size_t len = len_slice.size();
  size_t len_offset = 0;
  size_t start_offset = data_slice.read_offset();

  for (; len_offset < len; ++len_offset) {
    size_t reduce_len = len_slice.get<arrlen_t>(len_offset) - interm_len_;
    if (reduce_len <= data_slice.size()) {
      bool def = Reduce(reduce_len, data_slice);
      result_column_.Append(&interm_data_, def || interm_def_);
      data_slice.remove_prefix(reduce_len);

      std::memset(&interm_data_, 0, sizeof(Variant));
      interm_def_ = false;
      interm_len_ = 0;
    } else {
      interm_def_ = Reduce(data_slice.size(), data_slice);
      interm_len_ += data_slice.size();
      data_slice.remove_prefix(data_slice.size());
      break;
    }
  }
  data_.ConsumeHighMark(data_slice.read_offset() - start_offset);
  length_.ConsumeHighMark(len_offset);
  return Status::OK;
}


// TODO: 1. To redesign accumulators so that they will have shared code.
//       2. To benchmark JIT code vs cases like the one below.
template<DataType dt> bool Accumulate(size_t len, const ColumnSlice& src, Variant* dest) {
  bool def = false;
  for (size_t i = 0; i < len; ++i) {
    if (src.def(i)) {
      def = true;
      get_val<dt>(*dest) += src.get<DTCpp_t<dt>>(i);
    }
  }
  return def;
}

bool ReduceOperator::Reduce(size_t len, const ColumnSlice& src) {
  // Only sum_fn for now.
  switch (result_column_.data_type()) {
    case DataType::INT64:
      return Accumulate<DataType::INT64>(len, src, &interm_data_);
    case DataType::DOUBLE:
      return Accumulate<DataType::DOUBLE>(len, src, &interm_data_);
    break;
    default:
      LOG(FATAL) << "Not implemented " << result_column_.data_type();
  }
  return false;
}

AlignOperator::AlignOperator(const AstExpr& e, const ColumnConsumer& align,
                             const ColumnConsumer& data)
    : Operator(e, pmr::get_default_resource()), align_(align), data_(data) {
  CHECK_EQ(AstOp::ALIGN_FN, e.op());

  result_column_.Init(data_.column().data_type(), kPageSize);
}

Status AlignOperator::ApplyInternal() {
  if (PrepareResultColumn()) {
    data_.ConsumeHighMark(0);
    align_.ConsumeHighMark(0);
    return Status::OK;
  }
  ColumnSlice align_slice = align_.viewer(result_column_.unused_capacity());
  ColumnSlice data_slice = data_.viewer(result_column_.unused_capacity());

  size_t offset = result_column_.size();
  result_column_.Resize(offset + align_slice.size());

  size_t def_index = 0, align_index = 0;
  auto& dest_def = *result_column_.mutable_def();
  auto* dest_arr = result_column_.mutable_data();
  const uint8* src_ptr = reinterpret_cast<const uint8*>(data_slice.offset_raw());

  for (; align_index < align_slice.size(); ++align_index, ++offset) {
    bool b = align_slice.def(align_index);
    dest_def[offset] = b;
    if (!b)
      continue;
    if (def_index >= data_slice.size())
      break;
    dest_def[offset] = data_slice.def(def_index);
    if (dest_def[offset]) {
      memcpy(dest_arr->raw(offset), src_ptr + dest_arr->data_type_size() * def_index ,
             dest_arr->data_type_size());
    }
    ++def_index;
  }
  result_column_.Resize(offset);
  data_.ConsumeHighMark(def_index);
  align_.ConsumeHighMark(align_index);

  return Status::OK;
}

}  // namespace puma
