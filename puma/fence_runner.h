// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//


#pragma once

#include <pmr/monotonic_buffer_resource.h>

#include <sparsehash/dense_hash_map>
#include "puma/operator.h"
#include "puma/parser/parser_handler.h"  // For ParsedFence.

namespace puma {


// Used for writing output data from the fences.
// Can contain 1 or more column writers.
class FenceOutputWriter  {
 public:
  FenceOutputWriter(StringPiece name,  DataType dt, pmr::memory_resource* mr);

  virtual ~FenceOutputWriter();

  // pass_mask may be null, in that case col contains the data that needs to be appended.
  // appends at most window_size items from col.
  void Append(size_t window_size, const DefineVector* pass_mask, const NullableColumn& col);

  DataType data_type() const { return data_.data_type(); }
  const std::string name() const { return name_; }

  // Is called when everything is written to this writer and before the contents
  // of this column are read.
  virtual void Finalize() = 0;

 protected:
  virtual void AppendInternal(const strings::ByteRange& def, const VariantArray& arr) = 0;

  std::string name_;
  VariantArray data_;
  DefineVector def_;
};

// StringPiece - the name of the nullable column. Valid only during the call to the factory and
// is not owned by the caller after the call.
typedef std::function<FenceOutputWriter*(StringPiece col_name, DataType dt)>
    FenceWriterFactory;

typedef uint8 FenceMask;

enum FenceFlags : FenceMask {
  REFERENCE_VAL = 0x1,
  REFERENCE_DEF = 0x2,
  FENCE_COLUMN = 0x8,
};

class FenceUnwinder {
 public:
  struct MaskedId {
    AstId ast_id;
    FenceMask mask;

    MaskedId(AstId i);
    MaskedId(AstId i, FenceMask f) : ast_id(i), mask(f) {}

    bool operator<(const MaskedId& o) const { return ast_id < o.ast_id; }
  };

  typedef std::vector<MaskedId> MaskedIdArray;

  FenceUnwinder();

  // Unrolls expressions that needs to be computed for this fence.
  // May transforms some scalar expressions into immediate values on its way.
  // Updates expr_arr in-place.
  void Unroll(const ParsedFence& b, ExprArray* expr_arr);

  // The size of pass related expressions in data().
  // Everything that at positions [0, pass_size) is needed to compute pass evaluator.
  // Everything starting from pass_size is also needed to finish fence evaluation.
  size_t pass_size() const { return pass_size_; }

  const MaskedIdArray& id_array() const { return id_arr_; }

  // Logically MaskedIdArray is partitioned into expressions needed to compute each of 2 stages.
  size_t start_pos(unsigned index) const { return index ? pass_size() : 0; }
  size_t end_pos(unsigned index) const { return index ? id_arr_.size() : pass_size(); }

 private:
  typedef ::google::dense_hash_map<AstId, AstId> TraverseMap;

  typedef MaskedIdArray Stack;

  void Unwind(const AstExpr& e, Stack* expr_array);
  void Dfs(Stack* stack, ExprArray* expr_array);

  // Propagates const values and translates referenced ast ids to the new ids.
  static void TransformExpr(const ExprArray& arr, AstExpr* src);

  TraverseMap traverse_map_;
  MaskedIdArray id_arr_;

  size_t pass_size_ = 0;
};


typedef std::vector<std::unique_ptr<Operator>> EvalArray;

class FenceContext {
 public:
  const ParsedFence& fence;
  const ExprArray& expr_array;
  const EvalArray& eval_array;

  FenceWriterFactory column_writer_factory;

  pmr::memory_resource* memory_resource;

  const DefineVector* pass_mask = nullptr;

 public:
  FenceContext(const ParsedFence& f, const ExprArray& expr_arr, const EvalArray& ea);
};


typedef std::vector<std::unique_ptr<FenceOutputWriter>> FenceOutputArray;

class FenceRunnerBase {
  FenceRunnerBase(const FenceRunnerBase&) = delete;
  void operator=(const FenceRunnerBase&) = delete;
 public:
  virtual ~FenceRunnerBase();
  virtual void OnIteration() = 0;
  virtual void OnFinish() = 0;

  size_t OutputSize() const { return parsed_fence_.output.size(); }

  virtual AstExpr GetOutExpr(size_t index) const {
    return expr_array_[parsed_fence_.output[index]];
  }

  ParsedFence::Type type() const { return parsed_fence_.type; }

protected:
  explicit FenceRunnerBase(const FenceContext& fc);

  // Heap interface. not arena.
  pmr::memory_resource* mr_;

  const ParsedFence& parsed_fence_;
  const ExprArray& expr_array_;

  std::vector<ColumnConsumer> input_arr_;
  const DefineVector* pass_mask_ = nullptr;
};

class MultiRowFence : public FenceRunnerBase {
 protected:
  MultiRowFence(const FenceContext& fc);
  void OnFinish() override;

  FenceOutputArray out_arr_;
};

FenceRunnerBase* CreateFenceRunner(const FenceContext& fc);

}  // namespace puma
