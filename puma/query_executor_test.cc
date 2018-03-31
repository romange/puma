// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/query_executor.h"

#include <glog/stl_logging.h>
#include <gmock/gmock.h>
#include <pmr/monotonic_buffer_resource.h>

#include <atomic>
#include "base/gtest.h"
#include "base/logging.h"
#include "base/walltime.h"

#include "puma/parser/puma_scan.h"
#include "puma/query_executor_test_env.h"
#include "puma/test_utils.h"

#include "strings/strcat.h"
#include "strings/strpmr.h"

using namespace std;
using namespace base;
using namespace placeholders;

using testing::Each;
using testing::ElementsAre;
using testing::UnorderedElementsAre;
using testing::Pair;
using testing::HasSubstr;

namespace puma {
using schema::Attribute;

/**
   *********************************************************
   *********************************************************
   *********************************************************
   *********************************************************
**/

class QueryExecutorTest : public testing::Test {
 public:
  static void SetUpTestCase() {
    IncreaseVlogTo("query_executor", 2);
    IncreaseVlogTo("operator", 2);
    IncreaseVlogTo("fence", 1);
    IncreaseVlogTo("hashby", 1);
    IncreaseVlogTo("query_executor_test", 1);
    IncreaseVlogTo("file_fence_writer_factory", 1);
    IncreaseVlogTo("slice_provider", 1);
    IncreaseVlogTo("query_executor_test_env", 1);
  }

  static void TearDownTestCase() {
    RestoreVlog();
  }

 protected:

  std::pair<TestColumnProvider*, TestColumnProvider*>
    AddColumn(const string& name, DataType dt, Attribute::Ordinality ordinality,
              const Attribute* parent = nullptr) {
      return env_.AddColumn(name, dt, ordinality, parent);
    }

  Status Parse(const string& s) {
    auto st = env_.ParseAndRun(s);
    result_ = env_.writer();
    if (result_)
      first_col_ = ColumnSlice(env_.writer()->col(0));
    else
      first_col_ = ColumnSlice();
    return st;
  }

  void ReRun();

  void AddReplicated64(int64 val, int rep, TestColumnProvider* data,
                       TestColumnProvider* def) {
    data->Add64(val, rep);
    def->AddDef(true, rep);
  }

  void AddReplicated32(int32 val, int rep, TestColumnProvider* data,
                       TestColumnProvider* def) {
    data->Add32(val, rep);
    def->AddDef(true, rep);
  }

  void AddReplicatedU32(uint32 val, int rep, TestColumnProvider* data,
                        TestColumnProvider* def) {
    data->AddU32(val, rep);
    def->AddDef(true, rep);
  }

  void AddBoolReplicated(bool val, int rep, TestColumnProvider* data,
                        TestColumnProvider* def) {
    data->AddBool(val, rep);
    def->AddDef(true, rep);
  }

  QueryExecutorEnv env_;
  QueryWriter* result_ = nullptr;
  ColumnSlice first_col_;
  TestColumnProvider *a_prov_ = nullptr, *b_prov_ = nullptr;
  TestColumnProvider *a_def_ = nullptr, *b_def_ = nullptr;
  TestColumnProvider *str_sz_ = nullptr;
};



void QueryExecutorTest::ReRun() {
  env_.Run();

  result_ = env_.writer();
  first_col_ = ColumnSlice(env_.writer()->col(0));
}

TEST_F(QueryExecutorTest, Basic) {
  std::tie(a_prov_, a_def_) = AddColumn("foo", DataType::INT64, Attribute::NULLABLE);
  AddReplicated64(2, 5_rep, a_prov_, a_def_);

  Parse("x := load('foo'); fence z := row(sum(def(x)));");
  ASSERT_EQ(1, env_.handler().barrier_arr().size());

  EXPECT_THAT(first_col_, ColumnDataIs({5}));
  ASSERT_EQ(0, a_def_->index());
}

TEST_F(QueryExecutorTest, ReqDef) {
  std::tie(a_prov_, std::ignore) = AddColumn("a", DataType::INT64, Attribute::REQUIRED);
  a_prov_->Add64(10, 5);

  Parse("a := load('a'); fence z := out(def(a));");
  EXPECT_THAT(first_col_, ColumnDataIs(true, 5_rep));
}

TEST_F(QueryExecutorTest, SumBool) {
  std::tie(a_prov_, a_def_) = AddColumn("a", DataType::BOOL, Attribute::NULLABLE);
  AddBoolReplicated(true, 5_rep, a_prov_, a_def_);

  Parse("fence z := row([sum(a), sum(def(a))]);");
  EXPECT_THAT(first_col_, ColumnDataIs({5}));
}

TEST_F(QueryExecutorTest, FenceVars) {
  std::tie(a_prov_, std::ignore) = AddColumn("a", DataType::INT64, Attribute::REQUIRED);
  a_prov_->Add64(10, 5_rep);
  a_prov_->Add64(2, 5_rep);

  Parse("a := load('a'); fence x:= row(sum(a-2)); fence y := row([x$0 + 20, x$0 * 10])");
  ASSERT_EQ(2, result_->schema().size());
  EXPECT_THAT(first_col_, ColumnDataIs({60}));
  EXPECT_THAT(result_->col(1), ColumnDataIs({400}));

  Parse("fence x:= out(a-2); fence y := row(sum(x$0 + 20))");
  ASSERT_EQ(1, result_->schema().size());
  EXPECT_THAT(first_col_, ColumnDataIs({20 * 10 + 40}));

  Parse("fence x:= row(max(a));");
  ASSERT_EQ(1, result_->schema().size());
  EXPECT_THAT(first_col_, ColumnDataIs({10}));
}

TEST_F(QueryExecutorTest, BinaryConst) {
  std::tie(a_prov_, a_def_) = AddColumn("foo", DataType::INT64, Attribute::NULLABLE);

  AddReplicated64(2, 5005_rep, a_prov_, a_def_);
  a_def_->AddDef(false, 5005_rep);

  Parse("x := load('foo'); fence z := row(sum(x+6));");
  ASSERT_EQ(1, env_.handler().barrier_arr().size());

  EXPECT_THAT(first_col_, ColumnDataIs({40040}));

  Parse("x := load('foo'); fence z := row(sum(x * 6000000));");
  EXPECT_THAT(first_col_, ColumnDataIs({60060000000L}));
}

TEST_F(QueryExecutorTest, DoubleExpr) {
  std::tie(a_prov_, a_def_) = AddColumn("a", DataType::DOUBLE, Attribute::NULLABLE);

  a_prov_->AddDouble(2.5, 5);

  a_def_->AddDef(true, 5);
  a_def_->AddDef(false, 5);

  Parse("x := load('a'); fence z:= row(sum(x+6.2));");
  ASSERT_EQ(1, first_col_.size());
  ASSERT_EQ(43.5, first_col_.get<double>(0));

  Parse("fence z:= row(max(a));");
  ASSERT_EQ(2.5, first_col_.get<double>(0));

  Parse("x := load('a'); fence z:= row(sum(3.0 - x));");
  ASSERT_EQ(1, first_col_.size());
  ASSERT_EQ(2.5, first_col_.get<double>(0));
  a_prov_->AddDouble(2.5, 1000);
  a_def_->AddDef(true, 1000);

  ReRun();
  ASSERT_EQ(502.5, first_col_.get<double>(0));
}

TEST_F(QueryExecutorTest, Required) {
  std::tie(a_prov_, std::ignore) = AddColumn("d", DataType::DOUBLE, Attribute::REQUIRED);

  a_prov_->AddDouble(2.5, 5);

  Parse("x := load('d'); fence z:= out(x)");

  ASSERT_EQ(5, first_col_.size());

  for (unsigned i = 0; i < first_col_.size(); ++i) {
    EXPECT_EQ(2.5, first_col_.get<double>(i));
  }
}

TEST_F(QueryExecutorTest, MessageBase) {
  std::tie(a_prov_, a_def_) = AddColumn("m", DataType::MESSAGE, Attribute::NULLABLE);
  const Attribute* msg_m = CHECK_NOTNULL(env_.table()->LookupByPath("m"));

  // m.b
  std::tie(b_prov_, b_def_) = AddColumn("b", DataType::INT64, Attribute::NULLABLE, msg_m);

  // m: 24
  a_def_->AddDef(true, 12_rep);
  a_def_->AddDef(false, 12_rep);

  // b: 12
  AddReplicated64(2, 8_rep, b_prov_, b_def_);
  AddReplicated64(0, 2_rep, b_prov_, b_def_);
  b_def_->AddDef(false, 2);
  // m.b: 24 (8x2, 2x0, 14xNULL)

  TestColumnProvider* c_prov;
  std::tie(c_prov, std::ignore) = AddColumn("c", DataType::INT64, Attribute::REQUIRED);
  c_prov->Add64(-1, 10_rep);
  c_prov->Add64(3, 2_rep);
  c_prov->Add64(1000, 12_rep);
  ASSERT_OK(Parse("fence z:= row(sum(def(m)))"));
  EXPECT_THAT(first_col_, ColumnDataIs({12}));

  ASSERT_OK(Parse("fence z:= row(sum(def(m.b)))"));
  EXPECT_THAT(first_col_, ColumnDataIs({10}));

  ASSERT_OK(Parse("fence z:= row(sum(m.b + c))"));
  EXPECT_THAT(first_col_, ColumnDataIs({2*8 + -1 * 10}));
}

TEST_F(QueryExecutorTest, MessageWhere) {
  std::tie(a_prov_, a_def_) = AddColumn("m", DataType::MESSAGE, Attribute::NULLABLE);
  const Attribute* msg_m = CHECK_NOTNULL(env_.table()->LookupByPath("m"));
  std::tie(b_prov_, b_def_) = AddColumn("b", DataType::INT64, Attribute::NULLABLE, msg_m);

  a_def_->AddDef(false, 2_rep);
  a_def_->AddDef(true, 2_rep);
  AddReplicated64(2, 2_rep, b_prov_, b_def_);

  TestColumnProvider* c_prov;
  std::tie(c_prov, std::ignore) = AddColumn("c", DataType::INT64, Attribute::REQUIRED);
  c_prov->Add64(-1, 4_rep);
  ASSERT_OK(Parse("fence z:= out(c, where = def(m))"));
  EXPECT_THAT(first_col_, ColumnDataIs({-1, -1}));

  ASSERT_OK(Parse("fence z:= out(c, where = (m.b = 2))"));
  EXPECT_THAT(first_col_, ColumnDataIs({-1, -1}));
}


TEST_F(QueryExecutorTest, Sub) {
  std::tie(a_prov_, a_def_) = AddColumn("a", DataType::INT64, Attribute::NULLABLE);
  std::tie(b_prov_, std::ignore) = AddColumn("b", DataType::INT64, Attribute::REQUIRED);

  AddReplicated64(10, 250_rep, a_prov_, a_def_);
  a_def_->AddDef(false, 50);
  AddReplicated64(10, 200_rep, a_prov_, a_def_);

  b_prov_->Add64(-10, 500);
  Parse("a := load('a'); b := load('b'); fence z:= row(sum(a-5))");

  EXPECT_THAT(first_col_, ColumnDataIs({5 * 450}));

  Parse("a := load('a'); b := load('b'); fence z:= row(sum(10-a))");

  EXPECT_THAT(first_col_, ColumnDataIs({0}));

  a_prov_->data64.clear();
  a_def_->ClearDef();
  AddReplicated64(1LL << 35, 1_rep, a_prov_, a_def_);
  Parse("a := load('a'); fence z:= out(68719476736 - a)");

  EXPECT_THAT(first_col_, ColumnDataIs({68719476736LL - (1LL << 35)}));
}

TEST_F(QueryExecutorTest, BinVarOp) {
  std::tie(a_prov_, a_def_) = AddColumn("a", DataType::INT64, Attribute::NULLABLE);
  std::tie(b_prov_, std::ignore) = AddColumn("b", DataType::INT64, Attribute::REQUIRED);

  AddReplicated64(2, 3_rep, a_prov_, a_def_);
  b_prov_->Add64(100, 3_rep);
  ASSERT_OK(Parse("fence z:= out(a + b)"));
  EXPECT_THAT(first_col_, ColumnDataIs({102, 102, 102}));

  a_prov_->Rewind();
  a_def_->Rewind();
  b_prov_->Rewind();
  b_prov_->Add64(200, 1_rep);

  testing::internal::CaptureStderr();
  ASSERT_NOK(Parse("fence z:= out(a + b)"));
  EXPECT_THAT(testing::internal::GetCapturedStderr(), HasSubstr("discarded"));
}


TEST_F(QueryExecutorTest, BinVarOpUnalignedPage) {
  std::tie(a_prov_, a_def_) = AddColumn("a", DataType::INT64, Attribute::NULLABLE);
  std::tie(b_prov_, std::ignore) = AddColumn("b", DataType::INT64, Attribute::REQUIRED);

  for (size_t i = 1; i < 11; ++i)
    AddReplicated64(i, 1, a_prov_, a_def_);

  for (size_t i = 20; i < 30; ++i)
    b_prov_->Add64(i, 1);

  a_prov_->set_page_size(7);
  b_prov_->set_page_size(3);

  ASSERT_OK(Parse("fence z:= out(a + b)"));
  EXPECT_THAT(first_col_, ColumnDataIs({21, 23, 25, 27, 29, 31, 33, 35, 37, 39}));

  ASSERT_OK(Parse("c := b + 1; fence z:= out([a, c - 1], where = b > 22)"));
  EXPECT_THAT(first_col_, ColumnDataIs({4, 5, 6, 7, 8, 9, 10}));
  EXPECT_THAT(result_->col(1), ColumnDataIs({23, 24, 25, 26, 27, 28, 29}));
}


TEST_F(QueryExecutorTest, BinVarOpUnalignedUneven) {
  std::tie(a_prov_, a_def_) = AddColumn("a", DataType::INT64, Attribute::NULLABLE);
  std::tie(b_prov_, std::ignore) = AddColumn("b", DataType::INT64, Attribute::REQUIRED);

  AddReplicated64(10, 12_rep, a_prov_, a_def_);
  b_prov_->Add64(20, 20_rep);

  a_prov_->set_page_size(7);
  b_prov_->set_page_size(3);

  testing::internal::CaptureStderr();
  ASSERT_NOK(Parse("fence z:= out(a + b)"));
  EXPECT_THAT(testing::internal::GetCapturedStderr(), HasSubstr("discarded"));
}

TEST_F(QueryExecutorTest, Logical) {
  std::tie(a_prov_, std::ignore) = AddColumn("a", DataType::INT64, Attribute::REQUIRED);
  a_prov_->Add64(10, 5000);
  ASSERT_OK(Parse("a := load('a'); fence z:= row(sum(a > 10))"));

  EXPECT_THAT(first_col_, ColumnDataIs({0}));

  ASSERT_OK(Parse("a := load('a'); fence z:= row(sum(a = 9))"));

  EXPECT_THAT(first_col_, ColumnDataIs({0}));

  ASSERT_OK(Parse("a := load('a'); fence z:= row(sum(10 < a))"));

  EXPECT_THAT(first_col_, ColumnDataIs({0}));

  ASSERT_OK(Parse("a := load('a'); fence z:= row(sum(10 <= a))"));
  EXPECT_THAT(first_col_, ColumnDataIs({5000}));

  ASSERT_OK(Parse("a := load('a'); fence z:= row(sum(10 > a))"));
  EXPECT_THAT(first_col_, ColumnDataIs({0}));
}

TEST_F(QueryExecutorTest, Where) {
  constexpr int kRows = 5;

  std::tie(a_prov_, std::ignore) = AddColumn("a", DataType::INT64, Attribute::REQUIRED);
  a_prov_->Add64(10, kRows);
  a_prov_->Add64(2, kRows);

#if 0
  Parse("b := a + 2; fence x:= row(sum(b), where = b > 5)");
  ASSERT_TRUE(1 == result_->schema().size());
  EXPECT_THAT(first_col_, ColumnDataIs({60}));
#else
  LOG(ERROR) << "TBD: Replace row/where with restrict";
#endif

  ASSERT_OK(Parse("b := a + 2; fence x:= out(b, where = b < 5)"));
  ASSERT_TRUE(1 == result_->schema().size());

  ASSERT_EQ(kRows, first_col_.size());
  for (size_t i = 0; i < first_col_.size(); ++i) {
    ASSERT_EQ(4, first_col_.get<int64>(i));
    ASSERT_TRUE(first_col_.def(i));
  }

  TestColumnProvider* c_prov;
  TestColumnProvider* c_def;
  std::tie(c_prov, c_def) = AddColumn("c", DataType::INT64, Attribute::NULLABLE);

  AddReplicated64(2, kRows, c_prov, c_def);
  c_def->AddDef(false, kRows);
  ASSERT_OK(Parse("b := a + 2; fence x:= out(c + 8, where = b < 5)"));
  ASSERT_EQ(kRows, first_col_.size());

  for (size_t i = 0; i < first_col_.size(); ++i) {
    ASSERT_FALSE(first_col_.def(i));
  }

  ASSERT_OK(Parse("b := a + 2; fence x:= hash_by(b, [], where = a < 5)"));
  ASSERT_EQ(1, result_->schema().size());
  EXPECT_THAT(first_col_, ColumnDataIs({4}));

  ASSERT_OK(Parse("b := a + 2; fence x:= row(sum(b)); fence y:= out(b, where = x$0 < 20)"));
  ASSERT_EQ(1, result_->schema().size());
  EXPECT_THAT(first_col_, ColumnDataIs());
}

TEST_F(QueryExecutorTest, Restrict) {
  return;
  // Not sure if we keep this operator.
  std::tie(a_prov_, std::ignore) = AddColumn("a", DataType::INT64, Attribute::REQUIRED);
  a_prov_->Add64(10, 5);
  a_prov_->Add64(2, 5);

  ASSERT_OK(Parse("b := restrict(a*2, a > 2); fence z:= out([sum(b), sum(a)]);"));

  ASSERT_EQ(2, result_->schema().size());
  EXPECT_THAT(first_col_, ColumnDataIs({100}));
  EXPECT_THAT(result_->col(1), ColumnDataIs({60}));
}

TEST_F(QueryExecutorTest, Strings) {
  std::tie(a_prov_, str_sz_) = AddColumn("a", DataType::STRING, Attribute::REQUIRED);

  env_.AddString("Jessie", 5, a_prov_, str_sz_);
  env_.AddString("Roman", 4, a_prov_, str_sz_);

  ASSERT_OK(Parse("a := load('a'); fence z:= row(sum(a = 'Jessie'));"));

  ASSERT_EQ(1, result_->schema().size());
  EXPECT_THAT(first_col_, ColumnDataIs({5}));

  ASSERT_OK(Parse("a := load('a'); fence z:= out([a, def(a)])"));
  ASSERT_EQ(2, result_->schema().size());
  ASSERT_EQ(9, first_col_.size());
  ASSERT_EQ(DataType::STRING, first_col_.data_type());

  std::unordered_map<StringPiece, unsigned> keys;
  for (size_t i = 0; i < first_col_.size(); ++i) {
    keys[first_col_.get<StringPiece>(i)]++;
  }

  auto matcher = UnorderedElementsAre(Pair("Jessie", 5), Pair("Roman", 4));
  EXPECT_THAT(keys, matcher);

  ASSERT_OK(Parse("a := load('a'); fence z:= hash_by(a, sum(def(a)))"));
  ASSERT_EQ(2, result_->schema().size());
  ASSERT_EQ(2, first_col_.size());
  keys.clear();

  for (int i : {0, 1}) {
    keys[first_col_.get<StringPiece>(i)] = result_->col(1).get<int64>(i);
  }
  EXPECT_THAT(keys, matcher);

  ASSERT_OK(Parse("a := load('a'); fence z:= out(a); fence w := row(sum(z$0 = 'a'))"));
  ASSERT_EQ(1, result_->schema().size());
  EXPECT_THAT(first_col_, ColumnDataIs({0}));
}

TEST_F(QueryExecutorTest, StringHashBy) {
  std::tie(a_prov_, str_sz_) = AddColumn("a", DataType::STRING, Attribute::REQUIRED);
  env_.AddString("Jessie", 50000, a_prov_, str_sz_);
  env_.AddString("Roman", 40000, a_prov_, str_sz_);

  ASSERT_OK(Parse("a := load('a'); fence x:= hash_by(a, sum(def(a))); "
                  "fence y:= out([x$0, x$1 + 1])"));
  ASSERT_TRUE(result_);

  ASSERT_EQ(2, result_->schema().size());
  ASSERT_EQ(2, first_col_.size());

  std::vector<pair<StringPiece, int>> items;
  for (int i : {0, 1}) {
    items.emplace_back(first_col_.get<StringPiece>(i), result_->col(1).get<int64>(i));
  }

  EXPECT_THAT(items, UnorderedElementsAre(Pair("Jessie", 50001), Pair("Roman", 40001)));
 }


TEST_F(QueryExecutorTest, HashBy) {
  std::tie(a_prov_, std::ignore) = AddColumn("a", DataType::INT64, Attribute::REQUIRED);
  std::tie(b_prov_, std::ignore) = AddColumn("b", DataType::INT64, Attribute::REQUIRED);

  a_prov_->Add64(10, 5);
  a_prov_->Add64(2, 5);

  b_prov_->Add64(15, 3);
  b_prov_->Add64(20, 5);
  b_prov_->Add64(30, 2);

  ASSERT_OK(Parse("fence z:= hash_by([a,b], [sum(a), sum(2 + b)])"));

  ASSERT_EQ(4, result_->schema().size());
  ASSERT_EQ(4, first_col_.size());

  typedef pair<tuple<int64, int64>, tuple<int64, int64>> hashby_value;

  std::vector<hashby_value> vec;
  for (size_t row = 0; row < first_col_.size(); ++row) {
    tuple<int64, int64> key{result_->col(0).get<int64>(row),
                            result_->col(1).get<int64>(row)};
    tuple<int64, int64> value{result_->col(2).get<int64>(row),
                              result_->col(3).get<int64>(row)};
    vec.emplace_back(key, value);
  }
  sort(vec.begin(), vec.end());

  hashby_value e1(make_pair(2, 20), make_pair(6, 66));
  hashby_value e2{make_pair(2, 30), make_pair(4, 64)};
  hashby_value e3{make_pair(10, 15), make_pair(30, 51)};
  hashby_value e4{make_pair(10, 20), make_pair(20, 44)};

  EXPECT_THAT(vec, ElementsAre(e1, e2, e3, e4));

  TestColumnProvider* c_col;
  TestColumnProvider* c_def;

  std::tie(c_col, c_def) = AddColumn("c", DataType::INT64, Attribute::NULLABLE);

  c_def->AddDef(false, 10);
  AddReplicated64(15, 10, c_col, c_def);

  LOG(ERROR) << "TBD: null key in hash_by";
  // ASSERT_OK(Parse("fence x:= hash_by(c, c)"));
}

TEST_F(QueryExecutorTest, HashByBarrier) {
  TestColumnProvider* a_col;

  std::tie(a_col, std::ignore) = AddColumn("a", DataType::INT64, Attribute::REQUIRED);
  a_col->Add64(10, 5);
  a_col->Add64(2, 5);

  // Fences are 0-indexed.
  ASSERT_OK(Parse("fence z:= hash_by(a, sum(a+1)); fence y := out(z$1 + 2);"));

  // z - {10, 55}, {2, 15}
  ASSERT_EQ(1, result_->schema().size());
  ASSERT_EQ(2, first_col_.size());
  int64 res[] = {first_col_.get<int64>(0), first_col_.get<int64>(1)};

  EXPECT_THAT(res, UnorderedElementsAre(57, 17));
}

TEST_F(QueryExecutorTest, ManyKeysHashBug) {
  TestColumnProvider* a_col;
  std::tie(a_col, std::ignore) = AddColumn("a", DataType::INT64, Attribute::REQUIRED);
  for (int64 i = 0; i < (1 << 10); ++i) {
    a_col->Add64(i, 3);
    a_col->Add64(-i, 3);
  }
  ASSERT_OK(Parse("a := load('a'); fence z:= hash_by(a, sum(a), split=100); "
                  "fence y := row(sum(z$1));"));
  EXPECT_THAT(first_col_, ColumnDataIs({0}));
}

TEST_F(QueryExecutorTest, HashBySpill) {
  TestColumnProvider* a_col;
  std::tie(a_col, std::ignore) = AddColumn("a", DataType::INT64, Attribute::REQUIRED);

  constexpr int kPower = 15;
  for (int64 i = 0; i < (1 << kPower); ++i) {
    a_col->Add64(i, 3);
    a_col->Add64(-i, 3);
  }

  // Fences are 0-indexed.
  ASSERT_OK(Parse("fence z:= hash_by(a, sum(a+1), split=100); fence y := row(sum(z$1));"));

  ASSERT_EQ(1, result_->schema().size());
  EXPECT_THAT(first_col_, ColumnDataIs({3 * (1 << (kPower + 1))}));
}

TEST_F(QueryExecutorTest, MaxFn) {
  std::tie(a_prov_, std::ignore) = AddColumn("a", DataType::INT64, Attribute::REQUIRED);
  std::tie(b_prov_, b_def_) = AddColumn("b", DataType::DOUBLE, Attribute::NULLABLE);

  for (int64 i = 30; i > -1000; --i) {
    a_prov_->Add64(i, 3);
  }

  for (int64 i = 30; i <= 1000; ++i) {
    b_prov_->AddDouble(i*2.0 + 0.5, 2);
    b_def_->AddDef(true, 2);

    b_def_->AddDef(false, 2);
  }

  ASSERT_OK(Parse("a := load('a'); fence y := row(max(a));"));
  ASSERT_EQ(1, result_->schema().size());
  EXPECT_THAT(first_col_, ColumnDataIs({30}));

  ASSERT_OK(Parse("b := load('b'); fence y := row(max(b));"));
  ASSERT_EQ(1, result_->schema().size());
  ASSERT_EQ(1, first_col_.size());
  EXPECT_EQ(2000.5, first_col_.get<double>(0));

  ASSERT_OK(Parse("b := load('b'); fence y := row(max(b)); fence z:= row(def(y$0))"));
  EXPECT_THAT(first_col_, ColumnDataIs({true}));


  TestColumnProvider *c_prov = nullptr;
  std::tie(c_prov, std::ignore) = AddColumn("c", DataType::UINT32, Attribute::REQUIRED);
  c_prov->AddU32(10, 5);
  ASSERT_OK(Parse("fence y := row(max(c));"));
}

TEST_F(QueryExecutorTest, HashMaxFn) {
  std::tie(a_prov_, std::ignore) = AddColumn("a", DataType::INT64, Attribute::REQUIRED);
  std::tie(b_prov_, b_def_) = AddColumn("b", DataType::INT64, Attribute::NULLABLE);

  for (int64 i = 29; i > -20; --i) {
    int rep = abs(i) + 1;
    a_prov_->Add64(i, rep);

    if (i % 2 == 0) {
      b_prov_->Add64(i * 2, rep);
    }
    b_def_->AddDef(i % 2 == 0, rep);

  }

  ASSERT_OK(Parse("fence x:= hash_by(a, max(2*b+1), split=20)"));

  ASSERT_EQ(2, result_->schema().size());
  ASSERT_EQ(DataType::INT64, result_->col(1).data_type());

  for (unsigned j = 0; j < first_col_.size(); ++j) {
    ASSERT_TRUE(result_->col(0).def()[j]);
    int x = result_->col(0).get<int64>(j);

    bool is_b_def = (x % 2 == 0);
    ASSERT_EQ(is_b_def, result_->col(1).def()[j]) << j << " " << x;

    if (is_b_def) {
      int y = result_->col(1).get<int64>(j);
      ASSERT_EQ((x * 2) * 2 + 1, y);
    }
  }
}

TEST_F(QueryExecutorTest, Repeated) {
  std::tie(a_prov_, std::ignore) = AddColumn("a", DataType::INT32, Attribute::REPEATED);
  std::tie(b_prov_, b_def_) = AddColumn("b", DataType::UINT32, Attribute::NULLABLE);

  TestColumnProvider* a_sz = CHECK_NOTNULL(env_.GetLengthsBareSlice("a"));

  a_prov_->Add32(7, 10_rep * 5000);
  AddReplicatedU32(13, 10, b_prov_, b_def_);

  a_sz->AddLen(10, 5000_rep);


  ASSERT_OK(Parse("fence x:= row([sum(a), sum(b)])"));
  ASSERT_EQ(2, result_->schema().size());

  EXPECT_THAT(first_col_, ColumnDataIs({7 * 10 * 5000}));
  EXPECT_THAT(result_->col(1), ColumnDataIs({13 * 10}));

  ASSERT_OK(Parse("fence x:= out(len(a))"));

  ASSERT_EQ(1, result_->schema().size());
  ASSERT_EQ(5000, first_col_.size());

  for (unsigned j = 0; j < first_col_.size(); ++j) {
    ASSERT_TRUE(first_col_.def(j));

    int x = first_col_.get<int64>(j);
    ASSERT_EQ(10, x);
  }
}

TEST_F(QueryExecutorTest, ReduceAndRepl) {
  std::tie(a_prov_, std::ignore) = AddColumn("a", DataType::INT64, Attribute::REPEATED);
  std::tie(b_prov_, std::ignore) = AddColumn("b", DataType::INT64, Attribute::REQUIRED);

  TestColumnProvider* a_sz = CHECK_NOTNULL(env_.GetLengthsBareSlice("a"));

  a_prov_->Add64(7, 1024_rep);
  a_prov_->Add64(17, 1024_rep);

  const unsigned kTailLen = 2*1024 - 1030 - 100;
  a_sz->AddLen(1030, 1);
  a_sz->AddLen(100, 1);
  a_sz->AddLen(kTailLen, 1);

  // b
  for (unsigned i = 1; i < 4; ++i)
    b_prov_->Add64(i, 1);

  Parse("r1 := reduce(sum(a)); fence x:= out(len(a), where = r1 > 6)");
  EXPECT_THAT(first_col_, ColumnDataIs<uint32>({1030, 100, 2*1024 - 1030 - 100}));

  Parse("fence x:= row(sum(len(a)))");
  EXPECT_THAT(first_col_, ColumnDataIs({2*1024}));

  Parse("b:= reduce(sum(a)); fence x:= out(b, where = b > 7000)");
  EXPECT_THAT(first_col_, ColumnDataIs<int32>({1024 * 7 + 6 * 17, kTailLen * 17}) );

  // Replicate a
  Parse("fence x:= out(repl(len(a), b))");
  EXPECT_EQ(2*1024, first_col_.size());
  EXPECT_THAT(ColumnSlice(first_col_.col(), 0, 1030), ColumnDataIs(1, 1030));

  EXPECT_THAT(ColumnSlice(first_col_.col(), 1030, 100), ColumnDataIs(2, 100));
  EXPECT_THAT(ColumnSlice(first_col_.col(), 1130), ColumnDataIs(3, kTailLen));
}

TEST_F(QueryExecutorTest, Reduce) {
  std::tie(a_prov_, std::ignore) = AddColumn("a", DataType::INT64, Attribute::REPEATED);
  TestColumnProvider* a_sz = CHECK_NOTNULL(env_.GetLengthsBareSlice("a"));

  a_sz->AddLen(0, 1);
  a_sz->AddLen(1, 1);
  a_sz->AddLen(0, 1);

  a_prov_->Add64(2, 1);

  Parse("fence x:= out(reduce(sum(a + 1)))");
  EXPECT_THAT(first_col_, DefineIs({false, true, false}));
  EXPECT_EQ(3, first_col_.get<int64>(1));
}

TEST_F(QueryExecutorTest, HashByCnt) {
  std::tie(a_prov_, std::ignore) = AddColumn("a", DataType::INT64, Attribute::REQUIRED);
  for (int i = 0; i < 21; ++i) {
    a_prov_->Add64(i % 2, 1_rep);
  }
  Parse("fence x:= hash_by(a, cnt());");

  ASSERT_EQ(2, result_->schema().size());
  ASSERT_EQ(2, first_col_.size());

  vector<pair<int, int>> vals;
  for (size_t row = 0; row < first_col_.size(); ++row) {
    vals.emplace_back(result_->col(0).get<int64>(row),
                      result_->col(1).get<int64>(row));
  }

  EXPECT_THAT(vals, UnorderedElementsAre(Pair(0, 11), Pair(1, 10)));
}

TEST_F(QueryExecutorTest, Cnt) {
  std::tie(a_prov_, a_def_) = AddColumn("a", DataType::INT64, Attribute::NULLABLE);

  AddReplicated64(10, 250_rep, a_prov_, a_def_);
  a_def_->AddDef(false, 50);

  Parse("fence x:= row(cnt());");

  ASSERT_EQ(1, result_->schema().size());
  EXPECT_THAT(first_col_, ColumnDataIs({300}));
}

/*************** BENCHMARKING ********************************/

class BenchStateFlag {
  benchmark::State& state_;
  std::atomic_long last_ts_;
 public:
  BenchStateFlag(benchmark::State& state) :state_(state) {
    last_ts_ = -1;
  }

  bool IsFinished() {
    bool res = state_.KeepRunning();

    return !res;
  }

  int64 ts() const { return last_ts_.load(std::memory_order_relaxed); }
};

class BenchmarkBareSliceProvider : public BareSliceProvider {
  BenchStateFlag& flag_;
 public:
  BenchmarkBareSliceProvider(string bare_name, DataType dt, BenchStateFlag& flag)
    : BareSliceProvider(bare_name, dt), flag_(flag) {}

  base::Status GetPage(uint32 max_size, SlicePtr dest, uint32* fetched_size) override;
};

base::Status BenchmarkBareSliceProvider::GetPage(
  uint32 max_size, SlicePtr dest, uint32* fetched_size) {
  if (flag_.IsFinished()) {
    has_data_to_read_ = false;
    *fetched_size = 0;
  } else {
    has_data_to_read_ = true;
    *fetched_size = max_size;
    memset(dest.i64ptr, flag_.ts(), sizeof(int64) * max_size);
  }
  return Status::OK;
}

static void BM_SumAdd(benchmark::State& state) {
  QueryExecutorEnv env;
  BenchStateFlag flag(state);

  BareSliceProviderFactory factory =
    [&flag](FieldClass fc, StringPiece col, DataType dt) -> BareSliceProvider* {
    return new BenchmarkBareSliceProvider(col.as_string(), dt, flag);
  };

  env.AddColumn("a", DataType::INT64, Attribute::REQUIRED);
  env.set_bare_slice_provider_factory(factory);

  // Leave the parsing out of the loop.
  env.ParseAndRun("fence x:= row(sum(a+100));");
}
BENCHMARK(BM_SumAdd);


static void BM_HashBySum(benchmark::State& state) {
  QueryExecutorEnv env;
  TestColumnProvider* a_col;
  std::tie(a_col, std::ignore) = env.AddColumn("a", DataType::INT64, Attribute::REQUIRED);
  a_col->Add64(10, state.range_x());
  a_col->Add64(2, state.range_x());
  a_col->Add64(500, state.range_x());

  // Leave the parsing out of the loop.
  env.ParseAndRun("fence x:= hash_by(a, sum(a+100))");

  while (state.KeepRunning()) {
    env.Run(false);
  }
}
BENCHMARK(BM_HashBySum)->Arg(1 << 10)->Arg(1 << 16);

} // namespace puma
