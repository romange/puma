// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/fence_runner.h"

#include "puma/hashby.h"
#include "base/logging.h"

#include "strings/strcat.h"
#include "strings/strpmr.h"

namespace puma {

using std::string;
using std::vector;
using util::Status;

namespace {

class Aggregator {
 public:
  Aggregator(const ColumnConsumer& src, AstExpr* dest_value)
      : src_(src), dest_value_(dest_value) {}
  virtual ~Aggregator() {}

  void Apply() { ApplyInternal(); }
 protected:

  virtual void ApplyInternal() = 0;
  ColumnConsumer src_;
  AstExpr* dest_value_;
};

class SumFn : public Aggregator {
 public:
  SumFn(const ColumnConsumer& src, AstExpr* dest_value) : Aggregator(src, dest_value) {}

 private:
  void ApplyInternal() override;
};

class MaxFn : public Aggregator {
 public:
  MaxFn(const ColumnConsumer& src, AstExpr* dest_value) : Aggregator(src, dest_value) {}

 private:
  void ApplyInternal() override;
};

class CntFn : public Aggregator {
 public:
  CntFn(const ColumnConsumer& src, AstExpr* dest_value) : Aggregator(src, dest_value) {}

 private:
  void ApplyInternal() override;
};



template<typename T, typename Res> std::pair<Res, bool> SumVariant(const NullableColumn& col) {
  std::pair<Res, bool> res;
  const auto& src = col.data();

  for (size_t i = col.window_offset(); i < col.size(); ++i) {
    bool d = col.def()[i];
    res.first += (d ? src.at<T>(i) : 0);
    res.second = res.second || d;
  }
  return res;
}

template <DataType src_dt, DataType dest_t> void AggregateTo(const NullableColumn& src,
                                                             AstExpr* dest) {
  DCHECK_EQ(dest_t, dest->data_type());
  DCHECK_EQ(src_dt, src.data_type());

  using src_type = DTCpp_t<src_dt>;
  using dest_type = DTCpp_t<dest_t>;

  auto sum = SumVariant<src_type, dest_type>(src);

  if (sum.second) {
    VLOG(2) << "Summing " << *dest << " and " << sum.first;
    if (dest->is_defined()) {
      get_val<dest_t>(*dest->mutable_variant()) += sum.first;
    } else {
      dest->set_mask(AstExpr::DEFINED_VALUE);
      get_val<dest_t>(*dest->mutable_variant()) = sum.first;
    }
  }
}

void SumFn::ApplyInternal() {
  const NullableColumn& nc = src_.column();
  VLOG(1) << "SumFn over " << nc.window_size();

  CHECK_EQ(nc.data().size(), nc.def().size());
  CHECK_EQ(0, src_.ConsumeOffset()) << "TBD";

#define CASE(X,Y) case DataType::X: \
      AggregateTo<DataType::X, DataType::Y>(nc, dest_value_); \
      break;

  switch (nc.data_type()) {
    CASE(BOOL, INT64);
    CASE(INT64, INT64);
    CASE(UINT32, INT64);
    CASE(DOUBLE, DOUBLE);
    CASE(INT32, INT64);
    default:
      LOG(FATAL) << "Not implemented " << nc.data_type();
  }

#undef CASE
  src_.ConsumeHighMark(nc.window_size());
}


template<DataType dt> void MaxFnHelper(const NullableColumn& nc, AstExpr* dest) {
  size_t i = 0;
  using src_type = DTCpp_t<dt>;

  auto& val = get_val<dt>(*dest->mutable_variant());

  if (!dest->is_defined()) {
    for (i = nc.window_offset(); i < nc.size(); ++i) {
      if (nc.def()[i]) {
        val = nc.get<src_type>(i);
        dest->set_mask(AstExpr::DEFINED_VALUE);
        break;
      }
    }
  }

  for (; i < nc.size(); ++i) {
    if (nc.def()[i] && nc.get<src_type>(i) > val) {
      val = nc.get<src_type>(i);
    }
  }
}

void MaxFn::ApplyInternal() {
  const NullableColumn& nc = src_.column();
  CHECK_EQ(nc.data().size(), nc.def().size());
  CHECK_EQ(0, src_.ConsumeOffset()) << "TBD";

#define CASE(X) case DataType::X: \
      MaxFnHelper<DataType::X>(nc, dest_value_); \
      break;

  switch (nc.data_type()) {
    CASE(INT64);
    CASE(UINT32);
    CASE(INT32);
    CASE(DOUBLE);
    default:
      LOG(FATAL) << "Not implemented " << nc.data_type();
  }

#undef CASE
  src_.ConsumeHighMark(nc.window_size());
}


void CntFn::ApplyInternal() {
  const NullableColumn& nc = src_.column();
  VLOG(1) << "CntFn over " << nc.window_size();

  get_val<DataType::INT64>(*dest_value_->mutable_variant()) += nc.window_size();
  src_.ConsumeHighMark(nc.window_size());
}

// A table output.
// Can be 0 or 1 line but syntactically unlimited  unlike the RowBarrier below.
class SimpleTableBarrier : public MultiRowFence {
 public:
  SimpleTableBarrier(const FenceContext& bc);

  void OnIteration() override;
 private:
};

class RowBarrier : public FenceRunnerBase {
 public:
  RowBarrier(const FenceContext& bc);

  void OnIteration() override;
  void OnFinish() override;

  AstExpr GetOutExpr(size_t index) const override { return expr_arr_[index];}

 private:
  void AddAggregator(const AstExpr& e, const ColumnConsumer& src, AstExpr* dest);

  std::vector<std::unique_ptr<Aggregator>> aggregators_;
  ExprArray expr_arr_;
};


RowBarrier::RowBarrier(const FenceContext& fc)
    : FenceRunnerBase(fc), expr_arr_(input_arr_.size()) {
  VLOG(1) << "RowBarrier " << fc.fence.name;
  CHECK_EQ(ParsedFence::SINGLE_ROW, fc.fence.type);

  for (size_t i = 0; i < input_arr_.size(); ++i) {
    AstId id = fc.fence.args[i];
    const AstExpr& expr = fc.expr_array[id];
    DataType dt = expr.data_type();
    expr_arr_[i] = AstExpr(AstOp::CONST_VAL, dt);

    if (aux::IsAggregator(expr.op())) {
      AstId operand_id = expr.left();

      auto& eval = fc.eval_array[operand_id];
      CHECK(eval) << operand_id << " " << expr;
      input_arr_[i] = eval->result();
    }
  }

  for (size_t i = 0; i < input_arr_.size(); ++i) {
    AstId id = fc.fence.args[i];
    const AstExpr& expr = fc.expr_array[id];
    if (aux::IsAggregator(expr.op())) {
      AddAggregator(expr, input_arr_[i], &expr_arr_[i]);
    } else {
      CHECK(expr.is_immediate());

      expr_arr_[i] = expr;
    }
  }
}


void RowBarrier::OnIteration() {
  for (auto& agg : aggregators_) {
    agg->Apply();
  }
}

void RowBarrier::OnFinish() {}


void RowBarrier::AddAggregator(const AstExpr& e, const ColumnConsumer& src,
                               AstExpr* dest) {
  switch (e.op()) {
    case AstOp::SUM_FN:
      aggregators_.emplace_back(new SumFn(src, dest));
    break;
    case AstOp::MAX_FN:
      aggregators_.emplace_back(new MaxFn(src, dest));
    break;
    case AstOp::CNT_FN:
      aggregators_.emplace_back(new CntFn(src, dest));
    break;

    default:
      LOG(FATAL) << "Unsupported aggregator " << e;
  }
}

// Barrier evaluator implementation
SimpleTableBarrier::SimpleTableBarrier(const FenceContext& fc)
    : MultiRowFence(fc) {
  VLOG(1) << "SimpleTableBarrier " << fc.fence.name;

  CHECK_EQ(ParsedFence::OUT, fc.fence.type);

  CHECK(fc.column_writer_factory);

  for (size_t i = 0; i < input_arr_.size(); ++i) {
    AstId id = fc.fence.args[i];
    const AstExpr& expr = fc.expr_array[id];
    CHECK(!expr.is_immediate() && fc.eval_array[id]);

    auto& eval = *fc.eval_array[id];

    input_arr_[i] = eval.result();
  }
}

void SimpleTableBarrier::OnIteration() {
  size_t input_size = pass_mask_ ? pass_mask_->size() : kuint32max;

  size_t pos = input_arr_.front().column().pos();

  for (size_t i = 0; i < input_arr_.size(); ++i) {
    const ColumnConsumer& cc = input_arr_[i];
    CHECK_EQ(0, cc.ConsumeOffset()) << "TBD";
    CHECK_EQ(pos, cc.column().pos());

    input_size = std::min(input_size, cc.AvailableData());
  }
  if (!input_size)
    return;

  for (size_t i = 0; i < input_arr_.size(); ++i) {
    auto&& cc = input_arr_[i];
    out_arr_[i]->Append(input_size, pass_mask_, cc.column());
    cc.ConsumeHighMark(input_size);
  }
}

}  // namespace


FenceRunnerBase* CreateFenceRunner(const FenceContext& fc) {
  switch (fc.fence.type) {
    case ParsedFence::OUT:
      return new SimpleTableBarrier(fc);
   case ParsedFence::HASH_AGG:
      return new HashByBarrier(fc);
    case ParsedFence::SINGLE_ROW:
      return new RowBarrier(fc);
  }
  LOG(FATAL) << fc.fence.type;
  return nullptr;
}

}  // namespace puma
