// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/fence_runner.h"

#include "base/logging.h"

using std::string;
using util::Status;

namespace puma {

namespace {

constexpr FenceMask kRefMask = FenceFlags::REFERENCE_DEF | FenceFlags::REFERENCE_VAL;
}


// Public classes implementation
/********************************
 ********************************
 ********************************
 ********************************
*/

FenceOutputWriter::FenceOutputWriter(StringPiece name, DataType dt, pmr::memory_resource* mr)
  : name_(name.as_string()), data_(mr, dt) {
}

void FenceOutputWriter::Append(size_t window_size, const DefineVector* pass_mask,
                               const NullableColumn& col) {
  DCHECK_LE(window_size, col.window_size());

  data_.Reserve(window_size);
  data_.Resize(0);

  if (pass_mask) {
    CHECK_LE(window_size, pass_mask->size());

    def_.resize(window_size);
    size_t dest_sz = 0;

    for (size_t i = 0; i < window_size; ++i) {
      if (!(*pass_mask)[i])
        continue;
      size_t col_indx = col.window_offset() + i;

      // We write densely into data slice, skipping "null" records.
      if (col.def()[col_indx]) {
        def_[dest_sz] = true;
        data_.AppendItem(col.data(), col_indx);
      } else {
        def_[dest_sz] = false;
      }
      ++dest_sz;
    }

    AppendInternal(strings::ByteRange(def_.begin(), dest_sz), data_);
  } else {
    for (size_t i = 0; i < window_size; ++i) {
      size_t col_indx = col.window_offset() + i;
      if (col.def()[col_indx]) {
        data_.AppendItem(col.data(), col_indx);
      }
    }
    strings::ByteRange def_range(col.def().begin() + col.window_offset(), window_size);
    AppendInternal(def_range, data_);
  }
}

FenceUnwinder::MaskedId::MaskedId(AstId i) : ast_id(i), mask(kRefMask) {}

FenceUnwinder::FenceUnwinder() {
  traverse_map_.set_empty_key(kNullAstId);
}

void FenceUnwinder::Unroll(const ParsedFence& fence, ExprArray* expr_array) {
  Stack stack;
  if (fence.pass_expr != kNullAstId) {
    stack.emplace_back(fence.pass_expr);
    Dfs(&stack, expr_array);
    pass_size_ = id_arr_.size();
  }

  CHECK(stack.empty());

  switch (fence.type) {
    case ParsedFence::HASH_AGG:
      for (AstId agg_id : fence.args2) {
        const AstExpr& e = (*expr_array)[agg_id];

        CHECK(aux::IsAggregator(e.op()));
        if (e.op() != AstOp::CNT_FN) {
          CHECK(e.is_unary());
          stack.emplace_back(e.left());
        }
      }
    // No break on purpose.
    case ParsedFence::OUT:
    case ParsedFence::SINGLE_ROW:
      stack.insert(stack.end(), fence.args.begin(), fence.args.end());
    break;
  }

  Dfs(&stack, expr_array);

  traverse_map_.clear();
}

void FenceUnwinder::Dfs(Stack* stack, ExprArray* expr_array) {
  size_t prev_size = id_arr_.size();

  while (!stack->empty()) {
    MaskedId cntx = stack->back();
    stack->pop_back();

    DCHECK_LT(cntx.ast_id, expr_array->size());
    DCHECK_GE(cntx.ast_id, 0);

    AstExpr& e = (*expr_array)[cntx.ast_id];
    if (e.is_immediate())
      continue;

    if (aux::IsAggregator(e.op())) {
      // Skipping aggregator expression but not its operand.
      // Aggregators implemented within the context of where they are implemented:
      // fence, reduce etc.
      Unwind(e, stack);
    } else {
      // Translate original ast_id to local id.
      auto res = traverse_map_.emplace(cntx.ast_id, id_arr_.size());
      AstId pos = res.first->second;

      if (!res.second) {  // it has been traversed already
        id_arr_[pos].mask |= cntx.mask;
        continue;
      }

      if (e.op() == AstOp::LOAD_FN) {
        if (e.flags() & AstExpr::FENCE_COL)
          cntx.mask |= FenceFlags::FENCE_COLUMN;
      } else {
        Unwind(e, stack);
      }

      VLOG(2) << "Visit " << cntx.ast_id << " " << e;
      id_arr_.push_back(cntx);
    }
  }

  // Sort it in the order of appearance. This invalidates traverse_map_
  std::sort(id_arr_.begin() + prev_size, id_arr_.end());

  for (size_t i = prev_size; i != id_arr_.size(); ++i) {
    AstId id = id_arr_[i].ast_id;
    CHECK(!traverse_map_.emplace(id, i).second);  // Fix traverse map.
    AstExpr& e = (*expr_array)[id];

    TransformExpr(*expr_array, &e);
  }
}

static FenceMask GetLeftFlags(const AstExpr& e) {
  switch(e.op()) {
    case AstOp::DEF_FN:
    case AstOp::ALIGN_FN:
    case AstOp::CNT_FN:
      return FenceFlags::REFERENCE_DEF;
    case AstOp::LEN_FN:
      return 0;
    default:
      CHECK_NE(AstOp::LOAD_FN, e.op());
      return kRefMask;
  }
}

void FenceUnwinder::Unwind(const AstExpr& e, Stack* stack) {
  CHECK_NE(kNullAstId, e.left());

  FenceMask left_flags = GetLeftFlags(e);

  stack->emplace_back(e.left(), left_flags);

  if (!e.is_unary()) {
    CHECK_NE(kNullAstId, e.right());;
    stack->emplace_back(e.right(), kRefMask);
  }
}

void FenceUnwinder::TransformExpr(const ExprArray& expr_array, AstExpr* src) {
  if (!src->is_scalar() || src->is_immediate())
    return;
  const AstExpr& op1 = expr_array[src->left()];
  DCHECK(op1.is_immediate()) << op1;
  AstExpr res;
  if (src->is_unary()) {
    res = AstExpr::TransformConst1(src->op(), op1);
  } else {
    const AstExpr& op2 = expr_array[src->right()];
    DCHECK(op2.is_immediate()) << *src << " with op2: " << op2;
    res = AstExpr::TransformConst2(src->op(), op1, op2);
  }
  res.set_mask(src->flags());
  *src = res;
}

FenceContext::FenceContext(const ParsedFence& b, const ExprArray& expr_arr, const EvalArray& ea)
      : fence(b), expr_array(expr_arr), eval_array(ea), memory_resource(nullptr) {
}

FenceOutputWriter::~FenceOutputWriter() {
}


FenceRunnerBase::FenceRunnerBase(const FenceContext& fc)
    : mr_(fc.memory_resource), parsed_fence_(fc.fence), expr_array_(fc.expr_array),
      input_arr_(fc.fence.args.size() + fc.fence.args2.size()) {
  pass_mask_ = fc.pass_mask;
}

FenceRunnerBase::~FenceRunnerBase() {}

MultiRowFence::MultiRowFence(const FenceContext& fc)
    : FenceRunnerBase(fc), out_arr_(fc.fence.output.size()) {
  CHECK(fc.column_writer_factory);

  for (size_t i = 0; i < out_arr_.size(); ++i) {
    AstId out_var = fc.fence.output[i];
    const AstExpr& expr = fc.expr_array[out_var];

    out_arr_[i].reset(fc.column_writer_factory(expr.str(), expr.data_type()));
  }
}

void MultiRowFence::OnFinish() {
  for (size_t i = 0; i < out_arr_.size(); ++i) {
    out_arr_[i]->Finalize();
  }
}

}  // namespace puma
