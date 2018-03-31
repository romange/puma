// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/operator.h"
#include "puma/load_operator.h"

#include "base/logging.h"
#include "base/gtest.h"

#include "puma/test_utils.h"
#include "puma/query_executor_test_env.h"

using std::unique_ptr;

namespace puma {

static const char kStrVal[] = "foo";

class OperatorTest : public testing::Test {

 protected:
  static void SetUpTestCase() {
    IncreaseVlogTo("operator", 2);
    IncreaseVlogTo("column_provider", 1);
  }

  static void TearDownTestCase() {
    RestoreVlog();
  }

  OperatorTest();

  static AstExpr IntOp(AstOp op) {
    AstExpr e{op, DataType::INT64};
    if (aux::IsAggregator(op))
      e.set_mask(AstExpr::SCALAR_EXPR | AstExpr::UNARY_ARG);
    return e;
  }

  NullableColumn int_col1_, int_col2_, uint_col32_;
  NullableColumn str_col_;
  std::unique_ptr<OutputData> rd1_, rd2_, od3_, od32_;
  std::unique_ptr<Operator> def_eval_, add_const_;
  std::unique_ptr<Operator> bin_add_eval_, str_eq_eval_;

  static ColumnSlice res(Operator& e) {
    return e.result().column().viewer();
  }
};

OperatorTest::OperatorTest() : int_col1_(pmr::get_default_resource()),
    int_col2_(pmr::get_default_resource()), uint_col32_(pmr::get_default_resource()),
    str_col_(pmr::get_default_resource()) {
  int_col1_.Init(DataType::INT64, 512);
  int_col2_.Init(DataType::INT64, 512);
  uint_col32_.Init(DataType::UINT32, 512);
  str_col_.Init(DataType::STRING, 512);

  rd1_.reset(new OutputData(int_col1_));
  rd2_.reset(new OutputData(int_col2_));
  od3_.reset(new OutputData(str_col_));
  od32_.reset(new OutputData(uint_col32_));

  ColumnConsumer cc1(*rd1_), cc2(*rd2_);

  def_eval_.reset(new DefFnEvaluator(IntOp(AstOp::DEF_FN), cc1));

  AstExpr my_const = AstExpr::ConstVal<DataType::INT64>(5);
  AstExpr add_op(AstOp::ADD_OP, DataType::INT64);
  add_const_.reset(new BinOpConstVarEvaluator(add_op, cc1, my_const));

  bin_add_eval_.reset(new BinOpEvaluator(add_op, cc1, cc2));

  AstExpr eq_op(AstOp::EQ_OP, DataType::STRING);
  AstExpr cval = AstExpr::ConstVal<DataType::STRING>(kStrVal);
  str_eq_eval_.reset(new BinOpConstVarEvaluatorString(eq_op,
      ColumnConsumer{*od3_}, cval));
}

TEST_F(OperatorTest, Load) {
  unique_ptr<TestColumnProvider> data(new TestColumnProvider("a",  DataType::INT64));
  unique_ptr<TestColumnProvider> def(new TestColumnProvider("b", DataType::BOOL));

  data->Add64(17, 1_rep);
  def->AddDef(false, 5);
  def->AddDef(true, 1);

  AstExpr load_expr(AstOp::LOAD_FN, DataType::INT64);
  LoadOperator load(load_expr, data.get(), def.get());
  auto st = load.Apply();
  ASSERT_TRUE(st.ok()) << st;

  EXPECT_THAT(res(load), DefineIs({false, false, false, false, false, true}));
  EXPECT_THAT(res(load), ColumnDataIs({-1, -1, -1, -1, -1, 17}));

  data->Rewind();
  def->Rewind(); def->ClearDef();
  def->AddDef(true, 1_rep);
  def->AddDef(false, 3_rep);
  load.DiscardConsumed();

  ASSERT_TRUE(load.Apply().ok());
  EXPECT_THAT(res(load), DefineIs({true, false, false, false}));
  EXPECT_THAT(res(load), ColumnDataIs({17, -1, -1, -1}));
}


TEST_F(OperatorTest, LoadStrings) {
  TestColumnProvider* data = new TestColumnProvider("a",  DataType::STRING);
  TestColumnProvider* sz = new TestColumnProvider("a.sz", kStringLenDt);

  data->AddStrData("foo", 1);
  sz->AddU32(4, 1);
  sz->AddU32(0, 1);
  data->OnFinalize(); // To submit strings.

  AstExpr load_expr(AstOp::LOAD_FN, DataType::STRING);
  LoadOperator load(load_expr, data, sz, TAKE_OWNERSHIP);
  auto st = load.Apply();
  ASSERT_TRUE(st.ok()) << st;

  auto res_slice = res(load);

  EXPECT_THAT(res_slice, DefineIs({true, false}));

  EXPECT_EQ("foo", res_slice.get<StringPiece>(0));

  load.DiscardConsumed();
  data->Rewind();
  sz->Rewind();
}

TEST_F(OperatorTest, UndefData) {
  int_col1_.Resize(512);
  for (size_t i = 0; i < int_col1_.size(); ++i) {
    (*int_col1_.mutable_def())[i] = i < 12;
  }

  int_col1_.set_window_offset(12);

  def_eval_->Apply();

  EXPECT_THAT(res(*def_eval_), DefineIs(true, 500));
  EXPECT_THAT(res(*def_eval_), ColumnDataIs(false, 500));

  for (size_t i = 0; i < int_col1_.size(); ++i) {
    int_col1_.mutable_data()->set<int64>(i, i);
  }

  add_const_->Apply();
  EXPECT_THAT(res(*add_const_), DefineIs(false, 500));

  int_col2_.ResizeFill(500, true);
  bin_add_eval_->Apply();
  EXPECT_THAT(res(*bin_add_eval_), DefineIs(false, 500));
}


TEST_F(OperatorTest, DefData) {
  int_col1_.Resize(16);
  for (size_t i = 0; i < int_col1_.size(); ++i) {
    (*int_col1_.mutable_def())[i] = i >= 12;
  }

  int_col1_.set_window_offset(12);
  def_eval_->Apply();

  EXPECT_THAT(res(*def_eval_), DefineIs(true, 4));
  EXPECT_THAT(res(*def_eval_), ColumnDataIs(true, 4));

  for (size_t i = 0; i < int_col1_.size(); ++i) {
    int_col1_.mutable_data()->set<int64>(i, i);
  }


  add_const_->Apply();
  EXPECT_THAT(res(*add_const_), DefineIs(true, 4));
  EXPECT_THAT(res(*add_const_), ColumnDataIs({17, 18, 19, 20}));

  int_col2_.ResizeFill(4, true);
  for (size_t i = 0; i < int_col2_.size(); ++i) {
    int_col2_.mutable_data()->set<int64>(i, i + 1);
  }

  bin_add_eval_->Apply();
  EXPECT_THAT(res(*bin_add_eval_), DefineIs(true, 4));
  EXPECT_THAT(res(*bin_add_eval_), ColumnDataIs({13, 15, 17, 19}));

  bin_add_eval_->DiscardConsumed();
  int_col1_.set_pos(4);
  int_col2_.set_pos(4);
  int_col2_.Resize(8);
  for (size_t i = 0; i < int_col2_.size(); ++i) {
    int_col2_.mutable_data()->set<int64>(i, i + 1);
  }
  int_col2_.set_window_offset(4);
  bin_add_eval_->Apply();
  EXPECT_THAT(res(*bin_add_eval_), ColumnDataIs({17, 19, 21, 23}));
}

TEST_F(OperatorTest, StringEq) {
  str_col_.ResizeFill(10, true);
  for (size_t i = 0; i < str_col_.size(); ++i) {
    int_col2_.mutable_data()->set<StringPiece>(i, kStrVal);
  }
  str_eq_eval_->Apply();
  EXPECT_THAT(res(*str_eq_eval_), DefineIs(true, 10));
}

TEST_F(OperatorTest, Reduce) {
  AstExpr re(AstOp::REDUCE_FN, DataType::INT64);
  ColumnConsumer data_consumer(*rd1_), len_consumer(*od32_);

  ReduceOperator reduce(re, AstOp::SUM_FN, data_consumer, len_consumer);

  // Data
  int_col1_.ResizeFill(10, true);
  for (size_t i = 0; i < int_col1_.size(); ++i) {
    int_col1_.get<int64>(i) = i + 1;
  }

  // Length.
  uint_col32_.ResizeFill(2, true);
  uint_col32_.get<uint32>(0) = 5;
  uint_col32_.get<uint32>(1) = 6;

  rd1_->InitConsumeLimit();
  od32_->InitConsumeLimit();

  reduce.Apply();

  EXPECT_THAT(res(reduce), DefineIs(true, 1));
  EXPECT_THAT(res(reduce), ColumnDataIs({15}));
  ASSERT_EQ(10, rd1_->consume_limit());
  ASSERT_EQ(1, od32_->consume_limit());

  reduce.DiscardConsumed();

  CHECK_STATUS(reduce.Apply());
  EXPECT_EQ(0, reduce.result_size());

  int_col1_.ResizeFill(1, true);
  int_col1_.get<int64>(0) = 100;
  int_col1_.set_pos(10);

  CHECK_STATUS(reduce.Apply());
  EXPECT_THAT(res(reduce), ColumnDataIs({100 + 6 + 7 + 8 + 9 + 10}));
}

TEST_F(OperatorTest, Align) {
  AstExpr ae(AstOp::ALIGN_FN, DataType::INT64);
  ColumnConsumer data_consumer(*rd1_), align_consumer(*rd2_);

  // Data
  int_col1_.ResizeFill(4, true);
  for (size_t i = 0; i < int_col1_.size(); ++i) {
    int_col1_.get<int64>(i) = i + 1;
  }
  (*int_col1_.mutable_def())[2] = false;

  int_col2_.Resize(8);
  auto& align = *int_col2_.mutable_def();
  for (size_t i = 0; i < align.size(); ++i)
    align[i] = (i % 2) == 1;

  AlignOperator align_op(ae, align_consumer, data_consumer);
  CHECK_STATUS(align_op.Apply());
  EXPECT_THAT(res(align_op), DefineIs({false, true, false, true, false, false, false, true}));
  EXPECT_THAT(res(align_op), ColumnDataIs({-1, 1,    -1,     2,    -1,    -1,   -1,    4}));
}


#if 0
static void BM_SumFn(benchmark::State& state) {
  NullableColumn int_col(pmr::get_default_resource());

  int_col.Init(DataType::INT64, 1 << state.range_x());
  int_col.Resize(1 << state.range_x(), true);

  AstExpr sum(AstOp::SUM_FN, DataType::INT64, AstExpr::SCALAR_EXPR | AstExpr::UNARY_ARG);
  OutputData rd(int_col);

  SumFnEvaluator sum_eval(sum, ColumnConsumer(rd));

  while (state.KeepRunning()) {
    CHECK(!sum_eval.Apply());
  }
}
BENCHMARK(BM_SumFn)->Arg(10)->Arg(17);

static void BM_ReduceFn(benchmark::State& state) {
  NullableColumn int_col(pmr::get_default_resource()), len_col(pmr::get_default_resource());

  int_col.Init(DataType::INT64, 1 << state.range_x());
  int_col.Resize(1 << state.range_x(), true);

  AstExpr sum(AstOp::SUM_FN, DataType::INT64, AstExpr::SCALAR_EXPR | AstExpr::UNARY_ARG);
  OutputData rd(int_col);

  SumFnEvaluator sum_eval(sum, ColumnConsumer(rd));

  while (state.KeepRunning()) {
    sum_eval.Apply();
  }
}
#endif


}  // namespace puma
