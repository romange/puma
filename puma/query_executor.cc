// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/query_executor.h"

#include "base/logging.h"

#include "puma/load_operator.h"
#include "puma/parser/attribute_index.h"
#include "puma/parser/parser_handler.h"


#include "strings/strcat.h"

using util::Status;
using util::StatusCode;
using util::StatusObject;
using namespace std;

namespace puma {

using schema::Attribute;

namespace {

// Covers "x W c", "x W y" cases.
Operator* CreateArithmEval(
  const AstExpr& src,
  const EvalArray& evals, const ExprArray& ast_vec) {
  AstId left = src.left();
  AstId right = src.right();

  if ((src.flags() & AstExpr::UNARY_ARG) && (src.op() == AstOp::SUB_OP)) {

    // Convert to " 0 - x" case.
    CHECK(evals[left]);
    return new BinOpConstVarEvaluator(src, evals[left]->result(),
                                      AstExpr{src.data_type(), MakeZero(src.data_type())});
  }

  const AstExpr& aleft = ast_vec[left];
  if (aleft.is_immediate()) {
    // "c <op> x" case.
    CHECK(evals[right]) << ast_vec[right];
    if (aleft.data_type() == DataType::STRING) {
      return new BinOpConstVarEvaluatorString(src, evals[right]->result(), aleft);
    }
    return new BinOpConstVarEvaluator(src, evals[right]->result(), aleft);
  }

  const AstExpr& aright = ast_vec[right];
  if (aright.is_immediate() && aux::IsLogical(src.op())) {
    CHECK(evals[left]);
    AstExpr a(aux::SwapLogicalDirection  (src.op()), DataType::BOOL);

    if (aright.data_type() == DataType::STRING) {
      return new BinOpConstVarEvaluatorString(a, evals[left]->result(), aright);
    }
    return new BinOpConstVarEvaluator(a, evals[left]->result(), aright);
  }

  CHECK(evals[left]);
  CHECK(AstOp::ADD_OP == src.op() || AstOp::MUL_OP == src.op()) << aleft.ToString()
      << " " << src.ToString();

  if (!aright.is_immediate()) {
    return new BinOpEvaluator(src, evals[left]->result(),
                              evals[right]->result());
  }
  CHECK(aright.is_immediate()) << ast_vec[right].ToString() << " " << src.ToString();
  return new BinOpConstVarEvaluator(src, evals[left]->result(), ast_vec[right]);
}

inline BareSliceProvider* LoadDef(SliceManager* mgr, StringPiece col_name) {
  auto res = mgr->GetReader(FieldClass::OPTIONAL, col_name, DataType::BOOL);
  CHECK(res.ok()) << res.status;

  return res.obj;
}

BareSliceProvider* LoadData(SliceManager* mgr, StringPiece col_name, DataType dt) {
  auto res = mgr->GetReader(FieldClass::DATA, col_name, dt);
  CHECK(res.ok()) << res.status;

  return res.obj;
}

BareSliceProvider* LoadData(BareSliceProviderFactory f, StringPiece col_name, DataType dt) {
  BareSliceProvider* res = f(FieldClass::DATA, col_name, dt);
  CHECK(res) << col_name;

  return res;
}
}  // namespace



QueryExecutor::QueryExecutor(const QueryContext& context)
    : expr_arr_(pmr::get_default_resource()), context_(context) {
  // CHECK_NOTNULL(context_.table);
}

QueryExecutor::~QueryExecutor() {
  eval_array_.clear();
}

Status QueryExecutor::Run(const ParserHandler& handler) {
  const auto& barrier_arr = handler.barrier_arr();
  expr_arr_ = handler.expr_arr();

  CHECK(!barrier_arr.empty());

  attrib_index_ = &handler.attr_index();
  for (size_t i = 0; i < barrier_arr.size(); ++i) {
    RETURN_IF_ERROR(PropagateFence(handler, barrier_arr[i], i + 1 == barrier_arr.size()));
  }
  return Status::OK;
}

Status QueryExecutor::PropagateFence(const ParserHandler& handler, const ParsedFence& parsed_fence,
                                     bool is_last) {
  CHECK(!parsed_fence.args.empty());

  FenceUnwinder fence_unwinder;

  fence_unwinder.Unroll(parsed_fence, &expr_arr_);

  // Not all ast nodes are translated to evaluators (i.e. constants are not).
  // Some of evaluators will stay null. However we must have eval_array_ of the same size as
  // expr_array so that we could connect evaluators based on their expression counterparts.
  eval_array_.resize(expr_arr_.size());

  // Stage 0 is for "pass" expression.
  FenceStage stages[2];

  for (unsigned i = 0; i < 2; ++i) {
    stages[i] = BuildStage(i, parsed_fence.pass_expr, fence_unwinder);  // fills eval_array_ as well
  }
  unsigned eval_expr_count = stages[0].length() + stages[1].length();

  VLOG(1) << "Creating barrier " << parsed_fence.name << ", type: " << parsed_fence.type << " with "
          << eval_expr_count << " evals";

  FenceContext fence_context(parsed_fence, expr_arr_, eval_array_);
  fence_context.memory_resource = pmr::get_default_resource();
  fence_context.column_writer_factory = context_.column_writer_factory;

  if (parsed_fence.pass_expr != kNullAstId) {
    if (stages[0].pass_evaluator) {
      fence_context.pass_mask = &stages[0].pass_evaluator->def();
    } else {
      const AstExpr& pass_expr = expr_arr_[parsed_fence.pass_expr];
      CHECK(pass_expr.is_immediate() && pass_expr.data_type() == DataType::BOOL);
      if (!pass_expr.variant().bval) {
        eval_expr_count = 0; // if pass is False - skip runnning evaluators.
      }
    }
  }

  fence_runner_.reset(CreateFenceRunner(fence_context));

  if (eval_expr_count > 0) {
    auto res = RunEvaluators(handler, fence_context, stages);
    if (!res.ok())
      return res.status;
    num_iterations_ += res.obj;
  }
  context_.slice_manager->FreeSlices();
  fence_runner_->OnFinish();

  eval_array_.clear();

  if (!is_last) {
    SetupFenceOutputVars(parsed_fence);
  }
  return Status::OK;
}

void QueryExecutor::SetupFenceOutputVars(const ParsedFence& parsed_fence) {
  size_t sz = fence_runner_->OutputSize();
  CHECK_EQ(sz, parsed_fence.output.size());

  for (size_t i = 0; i < sz; ++i) {
    AstId id = parsed_fence.output[i];
    AstExpr e = fence_runner_->GetOutExpr(i);
    VLOG(2) << "Setting expr " << id << " to " << e;
    if (parsed_fence.type == ParsedFence::SINGLE_ROW) {
      expr_arr_[id] = e;
    } else {
      CHECK_EQ(AstOp::LOAD_FN, e.op());
    }
  }
}

Operator* QueryExecutor::CreateEvaluator(const AstExpr& src) {
  Operator* left = eval_array_[src.left()].get();

  CHECK_NE(AstOp::CONST_VAL, src.op());
  CHECK_NE(AstOp::LOAD_FN, src.op());
  CHECK(!aux::IsAggregator(src.op())) << src;

  switch (src.op()) {
    case AstOp::DEF_FN:
      return new DefFnEvaluator(src, left->result());
    case AstOp::ALIGN_FN:
      return new AlignOperator(src, left->result(), eval_array_[src.right()]->result());
    case AstOp::REPL_FN:
      return new ReplOperator(src, left->result(), eval_array_[src.right()]->result());
    case AstOp::REDUCE_FN: {
      AstOp agg_op = expr_arr_[src.left()].op();
      CHECK(aux::IsAggregator(agg_op));
      AstId id = expr_arr_[src.left()].left();

      return new ReduceOperator(src, agg_op, CHECK_NOTNULL(eval_array_[id].get())->result(),
                                eval_array_[src.right()]->result());
    }
    default:;
  }

  if (aux::IsArithmetic(src.op()) || aux::IsLogical(src.op())) {
    return CreateArithmEval(src, eval_array_, expr_arr_);
  }
  LOG(FATAL) << "Evaluator not implemented for " << src.ToString();
  return nullptr;
}

LoadOperator* QueryExecutor::CreateLoadEval(const AstExpr& src, FenceMask mask) {
  DCHECK_EQ(AstOp::LOAD_FN, src.op());

  CHECK(mask) << "Must be referenced: " << src.ToString() << " " << src.str();

  string filename;
  BareSliceProvider* data = nullptr;
  BareSliceProvider* def = nullptr;
  StringPiece col_name = src.str();

  VLOG(1) << "CreateLoadEval " << col_name << ", flags: " << int(mask);

  if ((mask & FenceFlags::FENCE_COLUMN) == 0) {
    // If it's a table column (not a fence column).
    DCHECK_NE(kInvalidAttrId, src.attr_id());
    const auto& info = attrib_index_->info(src.attr_id());

    const Attribute* attr = info.field;
    CHECK(attr) << col_name << " for " << src;
    Attribute::Ordinality ordinality = attr->ordinality();

    bool is_required = (ordinality == Attribute::REQUIRED);
    CHECK_EQ(attr->is_nullable(), (src.flags() & AstExpr::ALWAYS_PRESENT) == 0) << col_name;

    // Can be few states:
    // For nullable fields:
    //    1. We load both data, def.
    //    2. We load def only.
    //
    // For required or repeated fields:
    //  1. We load data and fill def as "true" vector to simplify reads from it.
    //  2. We load data only (def is not referenced).
    //  3. We load data even though only def is referenced.
    //     This is needed for knowing how many items there are in the column. Since the case is
    //     isoteric with do not bother with optimizing this obvious inefficiency.
    //  In other words, for required columns we always load data.
    // To summarize, it can be:
    //  a. data, def loaded.
    //  b. no data, def loaded.
    //  c. data, constant_true def loaded.
    //  d. def provider is null only when is_required is true.

    if (is_required || (mask & FenceFlags::REFERENCE_VAL)) {
      data = LoadData(context_.slice_manager, col_name, src.data_type());
    }

    if (!is_required) {
      CHECK(mask & FenceFlags::REFERENCE_DEF) << "Weird...";
    }

    if (src.data_type() == DataType::STRING) {
      auto res = context_.slice_manager->GetReader(FieldClass::STRING_LEN, col_name, kStringLenDt);
      CHECK_STATUS(res.status);
      def = res.obj;
    } else if (attr->is_nullable() && mask & FenceFlags::REFERENCE_DEF) {
      def = LoadDef(context_.slice_manager, col_name);
    }
  } else {  // FENCE_COLUMN
    CHECK(mask & FenceFlags::REFERENCE_DEF);
    if (src.data_type() == DataType::STRING) {
      def = context_.fence_column_factory(FieldClass::STRING_LEN, col_name, kStringLenDt);
    } else {
      def = context_.fence_column_factory(FieldClass::OPTIONAL, col_name, DataType::BOOL);
    }
    if (mask & FenceFlags::REFERENCE_VAL)
      data = LoadData(context_.fence_column_factory, col_name, src.data_type());
  }

  return new LoadOperator(src, data, def);
}

LoadOperator* QueryExecutor::CreateLenEval(const AstExpr& src) {
  CHECK_EQ(AstOp::LEN_FN, src.op());
  AstId load_id = src.left();

  DCHECK_LT(load_id, expr_arr_.size());
  const AstExpr& ld = expr_arr_[load_id];

  CHECK_EQ(AstOp::LOAD_FN, ld.op());

  StringPiece col_name = ld.str();
  auto res = context_.slice_manager->GetReader(
        FieldClass::ARRAY_SIZE, col_name, kArrayLenDt);
  CHECK(res.ok()) << res.status;

  return new LoadOperator(src, res.obj, nullptr);
}

auto QueryExecutor::BuildStage(unsigned index, AstId pass_expr,
                               const FenceUnwinder& unwinder) -> FenceStage {
  FenceStage result;

  const auto& data = unwinder.id_array();
  size_t start_pos = unwinder.start_pos(index);
  size_t end_pos = unwinder.end_pos(index);

  auto& op_vec = result.operators;

  context_.slice_manager->PlanStage(index);

  for (size_t pos = start_pos; pos < end_pos; ++pos) {
    const FenceUnwinder::MaskedId& val = data[pos];
    const AstExpr& src = expr_arr_[val.ast_id];

    if (src.is_immediate())
      continue;
    DCHECK(!src.is_scalar());

    Operator* eval = nullptr;

    VLOG(2) << "BuildStage create: " << src;
    if (src.op() == AstOp::LOAD_FN) {
      if (val.mask) {
        eval = CreateLoadEval(src, val.mask);
      }
    } else if (src.op() == AstOp::LEN_FN) {
      eval = CreateLenEval(src);
    } else {
      eval = CHECK_NOTNULL(CreateEvaluator(src));
    }

    if (eval) {
      op_vec.push_back(eval);
    }

    eval_array_[val.ast_id].reset(eval);
    if (val.ast_id == pass_expr) {
      result.pass_evaluator = eval;
    }
  }
  context_.slice_manager->FinishPlanning();

  VLOG(1) << "Created stage " << index << " with " << result.operators.size() << " operators";
  return result;
}

StatusObject<uint32> QueryExecutor::RunEvaluators(const ParserHandler& handler,
      const FenceContext& fc, FenceStage stages[2]) {
  uint32 iteration = 0;

  while (true) {
    VLOG(1) << "Iteration " << iteration;

    bool has_output = false;
    bool produced_output = false;

    // We separate our operators into 2 stages:
    // First - to run whatever we need to compute pass mask. Motivation - performance since
    // it might be much smaller than computing and loading all the data.
    // After this we apply pass mask back to all the operators since they
    // can be reused in the second stage.
    // Second - to run the rest of the operators needed to compute the fence results and also
    // apply the mask.
    for (unsigned index = 0; index < 2; ++index) {
      const FenceStage& stage = stages[index];

      context_.slice_manager->ActivateStage(index);

      for (Operator* eval : stage.operators) {
        size_t prev_size = eval->result_size();
        CHECK_STATUS(eval->Apply());
        if (eval->result_size() > 0) {
          has_output = true;

          if (eval->result_size() > prev_size) {
            produced_output = true;
          }
        }
      }

      if (stage.pass_evaluator) {
        // Only stage 0 has pass_evaluator defined.
        DCHECK_EQ(0, index);

        VLOG(1) << "Pass evaluator column: " << stage.pass_evaluator->result().column();

        stage.pass_evaluator->RealizePassMask();  // Compute the mask.
      }
    }

    fence_runner_->OnIteration();

    if (!produced_output) {
      if (has_output) {
        LOG(ERROR) << "Blank iteration but some output was discarded!";
        return Status(StatusCode::RUNTIME_ERROR, StrCat("inconsistent data in ", fc.fence.name));
      }
      break;
    }
    ++iteration;

    for (unsigned i = 0; i < 2; ++i) {
      const FenceStage& stage = stages[i];
      for (Operator* eval : stage.operators) {
        consumed_bytes_ += eval->DiscardConsumed();
      }
    }
  }
  return iteration;
}

}  // namespace puma
