// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <functional>
#include <memory>

#include <pmr/monotonic_buffer_resource.h>
#include <sparsehash/dense_hash_map>

#include "puma/fence_runner.h"

#include "strings/stringpiece.h"
#include "util/status.h"

namespace puma {

class ParserHandler;
class LoadOperator;

struct QueryContext {
  BareSliceProviderFactory fence_column_factory;
  FenceWriterFactory column_writer_factory;
  SliceManager* slice_manager = nullptr;

  QueryContext() {}

  QueryContext(BareSliceProviderFactory fc, FenceWriterFactory c1)
    : fence_column_factory(fc), column_writer_factory(c1) {}
};

class QueryExecutor {
 public:
  QueryExecutor(const QueryContext& context);
  ~QueryExecutor();

  util::Status Run(const ParserHandler& handler);

  const FenceRunnerBase* barrier() const { return fence_runner_.get(); }

  uint32 num_iterations() const { return num_iterations_; }
  size_t consumed_bytes() const { return consumed_bytes_; }
 private:

  util::Status PropagateFence(const ParserHandler& handler, const ParsedFence& fence, bool is_last);

  struct FenceStage {
    std::vector<Operator*> operators;
    Operator* pass_evaluator = nullptr;

    size_t length() const { return operators.size(); }
  };

  void SetupFenceOutputVars(const ParsedFence& fence);

  Operator* CreateEvaluator(const AstExpr& src);
  LoadOperator* CreateLoadEval(const AstExpr& src, FenceMask mask);
  LoadOperator* CreateLenEval(const AstExpr& src);

  // There are 2 stages: optional pass (0) and run (1).
  FenceStage BuildStage(unsigned stage, AstId pass_expr, const FenceUnwinder& unwinder);

  // If run was successful returns number of iterations needed to run this fence.
  util::StatusObject<uint32> RunEvaluators(const ParserHandler& handler,
        const FenceContext& fc, FenceStage stages[2]);

  // We need a copy of expr_arr_ to perform scalar propagation when computing the ast DAG.
  ExprArray expr_arr_;

  QueryContext context_;
  const QueryAttributeIndex* attrib_index_ = nullptr;
  EvalArray eval_array_;

  std::unique_ptr<FenceRunnerBase> fence_runner_;
  uint32 num_iterations_ = 0;
  size_t consumed_bytes_ = 0;

  pmr::monotonic_buffer_resource arena_;
};

}  // namespace puma
