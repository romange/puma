// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <gmock/gmock.h>
#include <cstdarg>
#include "base/gtest.h"
#include "base/logging.h"

#include "puma/parser/attribute_index.h"
#include "puma/parser/location.hh"
#include "puma/parser/puma_scan.h"
#include "puma/parser/parser_handler.h"

using namespace std;
using ::testing::Field;
using ::testing::ElementsAre;

namespace puma {
using schema::Attribute;

class TestHandler : public ParserHandler {
public:
  void Error(const location& l, const std::string& s) override {
    LOG(WARNING) << l << ": " << s;
  }

  void Debug(const location& l, const char* fmt, ...) override {
    std::stringstream s;
    s << l;
    string str = s.str();

    va_list ap;
    va_start(ap, fmt);
    string result;
    printf("%s ", str.c_str());
    vprintf(fmt, ap);
    va_end(ap);
  }

  bool AddAssignment(StringPiece str, AstExpr* expr) {
    printf("Assignment %s %p\n", str.data(), expr);
    delete expr;
    return true;
  }
};

class PumaParseTest : public testing::Test {
 protected:
  using token = puma_parser::token;

  PumaParseTest() : table_("test") {
  }

  void SetUp() override {
  }

  // Return 0 on success, -1 on failure.
  int Scan(const string& s);

  // Return 0 on success. 1 on invalid input.
  // See https://www.gnu.org/software/bison/manual/html_node/Parser-Function.html
  int Parse(const string& s);

  const AstExpr& LookupIdentifier(StringPiece str) {
    auto res = driver_.LookupIdentifier(str);
    CHECK_STATUS(res.status);
    return driver_.at(res.obj);
  }

  std::vector<int> tokens_;
  std::vector<long> intvals_;
  std::vector<double> dvals_;
  std::vector<string> strvals_;
  std::vector<AstOp> ops_;
  TestHandler driver_;
  schema::Table table_;
};


int PumaParseTest::Scan(const string& s) {
  TestHandler driver;

  tokens_.clear();
  intvals_.clear();
  strvals_.clear();


  yyscan_t scanner = driver.scanner();

  puma_scan_bytes(s.data(), s.size(), scanner);
  YYSTYPE stype;
  YYLTYPE ltype;

  // cout << ltype << endl;
  int res = 0;
  while (true) {
    res = pumalex(&stype, &ltype, scanner);
    if (res <= 0) break;
    tokens_.push_back(res);
    switch (res) {
      case token::TOK_INTNUM:
      case token::TOK_RELATION_OP:
        intvals_.push_back(stype.ival);
      break;
      case token::TOK_DOUBLENUM:
        dvals_.push_back(stype.dval);
      break;
      case token::TOK_IDENTIFIER: case token::TOK_STRING:
        strvals_.push_back(stype.strval.as_string());
      case token::TOK_MULTIPL_OP: case token::TOK_SHIFT_OP:
        ops_.push_back(stype.op);
      break;
    }
  }

  return res;
}

// returns 0 if parser succeeds.
int PumaParseTest::Parse(const string& s) {
  driver_.Reset();

  yyscan_t scanner = driver_.scanner();
  YY_BUFFER_STATE buffer_state = puma_scan_bytes(s.data(), s.size(), scanner);

  puma_parser parser(scanner, &driver_);
  int res = parser.parse();

  puma_delete_buffer(buffer_state, scanner);

  return res;
}


TEST_F(PumaParseTest, Scanner) {
  VLOG(1) << AstOp::MUL_OP;

  EXPECT_EQ(0, Scan(" --5 > 6 \n\r\n 7 < 8"));

  EXPECT_THAT(tokens_, ElementsAre(token::TOK_INTNUM, token::TOK_RELATION_OP, token::TOK_INTNUM));
  EXPECT_THAT(intvals_, ElementsAre(7, uint8(AstOp::LT_OP), 8));

  EXPECT_EQ(0, Scan("7 >= 8"));
  EXPECT_THAT(tokens_, ElementsAre(token::TOK_INTNUM, token::TOK_RELATION_OP, token::TOK_INTNUM));
  EXPECT_THAT(intvals_, ElementsAre(7, uint8(AstOp::GEQ_OP), 8));

  EXPECT_EQ(0, Scan("7 > 8"));
  EXPECT_THAT(tokens_, ElementsAre(token::TOK_INTNUM, token::TOK_RELATION_OP, token::TOK_INTNUM));
  EXPECT_THAT(intvals_, ElementsAre(7, uint8(AstOp::GT_OP), 8));


  EXPECT_EQ(0, Scan("! NOT && |"));
  EXPECT_THAT(tokens_, ElementsAre(token::TOK_NOT_OP, token::TOK_NOT_OP, token::TOK_AND_OP, '|'));

  EXPECT_EQ(0, Scan("+ - * /"));
  EXPECT_THAT(tokens_, ElementsAre('+', '-', token::TOK_MULTIPL_OP, token::TOK_MULTIPL_OP));
  EXPECT_THAT(ops_, ElementsAre(AstOp::MUL_OP, AstOp::DIV_OP));

  EXPECT_EQ(0, Scan("foo bar :="));
  EXPECT_THAT(tokens_, ElementsAre(token::TOK_IDENTIFIER, token::TOK_IDENTIFIER,
                                   token::TOK_ASSIGN_OP));
  EXPECT_EQ(0, Scan(R"(   'foo' 'bar' '\'moo\r\'')"));
  EXPECT_THAT(tokens_, ElementsAre(token::TOK_STRING, token::TOK_STRING,
                                   token::TOK_STRING));
  EXPECT_THAT(strvals_, ElementsAre("foo", "bar", "'moo\r'"));

  EXPECT_EQ(0, Scan("0.145 -1.56"));
  EXPECT_THAT(tokens_, ElementsAre(token::TOK_DOUBLENUM, '-', token::TOK_DOUBLENUM));
  EXPECT_THAT(dvals_, ElementsAre(0.145, 1.56));

  EXPECT_EQ(0, Scan("68719476736 -68719476736"));
  EXPECT_THAT(tokens_, ElementsAre(token::TOK_INTNUM, '-', token::TOK_INTNUM));
  EXPECT_THAT(intvals_, ElementsAre(68719476736, 68719476736)); // MINUS is an operator.

  EXPECT_EQ(0, Scan("a$5 a$0"));
  EXPECT_THAT(tokens_, ElementsAre(token::TOK_FENCE_IDENTIFIER, token::TOK_FENCE_IDENTIFIER));

  EXPECT_EQ(-1, Scan("a$"));
}

TEST_F(PumaParseTest, SameIdentifier) {
  EXPECT_EQ(0, Parse("a := load('a'); b := load('foo')"));
  EXPECT_EQ(1, Parse("a := load('a'); a := load('a')"));
  EXPECT_EQ(0, Parse("a := load('a'); b := 7*a"));
  EXPECT_EQ(1, Parse("a := load('a'); b := 7 + d"));
}

TEST_F(PumaParseTest, Arithmetics) {
  const auto& expr_arr = driver_.expr_arr();

  ASSERT_EQ(0, Parse(" b := -7"));

  ASSERT_EQ(2, expr_arr.size());

  {
    const AstExpr& e1 = expr_arr[1];
    ASSERT_TRUE(e1.is_immediate());
    EXPECT_EQ(-7, e1.variant().i32val);
  }

  ASSERT_EQ(0, Parse(" b := -7.0"));
  ASSERT_EQ(2, expr_arr.size());

  {
    const AstExpr& e1 = expr_arr[1];
    ASSERT_TRUE(e1.is_immediate());
    EXPECT_EQ(-7.0, e1.variant().dval);
  }

  ASSERT_EQ(0, Parse("b := -7 / 0"));
  ASSERT_EQ(4, expr_arr.size());
  const AstExpr& invalid = expr_arr[3];

  EXPECT_FALSE(invalid.is_defined());

  ASSERT_EQ(0, Parse("b := (7 / 0) + 5"));
}

TEST_F(PumaParseTest, Parser) {
  EXPECT_EQ(0, Parse("a := 5 + 6"));
  auto res = driver_.LookupIdentifier("a");

  ASSERT_TRUE(res.ok());
  AstId a_id = res.obj;

  const AstExpr& e = driver_.at(a_id);
  EXPECT_EQ(AstOp::CONST_VAL, e.op());
  EXPECT_EQ(DataType::INT32, e.data_type());
  EXPECT_EQ(11, e.variant().i32val);

  EXPECT_EQ(1, Parse("b := foo 5 + 6"));
  EXPECT_EQ(0, Parse("c := 5 + 6 / 3 + 4*5 + 2"));
  EXPECT_EQ(0, Parse("c := load('col1.name');"));
  EXPECT_EQ(0, Parse("c := 5 < 6;"));
  EXPECT_EQ(0, Parse("c := 5 = 6;"));

  EXPECT_EQ(0, Parse("c := load('c'); f:= restrict(c, c > 5);"));
  EXPECT_EQ(0, Parse("c := 'a' = 'b';"));

  EXPECT_EQ(1, Parse("c := load('a#');"));
}

TEST_F(PumaParseTest, OutFn) {
  EXPECT_EQ(1, Parse("out()"));
  ASSERT_EQ(0, Parse("a := load('a'); b := load('b'); c := load('c');"
                      "fence z := out([a, b, c])"));

  // Both aggregated and not aggregated expressions in the same out fence.
  EXPECT_EQ(1, Parse("a := load('a'); fence z := out(a, sum(a))"));

  ASSERT_EQ(0, Parse("x := load('d'); fence z := out(x)"));

  const auto& expr_arr = driver_.expr_arr();
  ASSERT_EQ(2, expr_arr.size());

  ASSERT_EQ(0, Parse("x := load('d'); fence y := row([sum(x)]); fence z := row([y$0 + 5])"));
  const auto& barrier_arr = driver_.barrier_arr();
  ASSERT_EQ(2, barrier_arr.size());

  EXPECT_EQ(ParsedFence::SINGLE_ROW, barrier_arr[0].type);
  EXPECT_EQ(kInvalidAttrId, barrier_arr[0].lca);

  EXPECT_EQ(ParsedFence::SINGLE_ROW, barrier_arr[1].type);
  EXPECT_EQ(kInvalidAttrId, barrier_arr[1].lca);

  schema::Table table("table");

  table.Add("a", DataType::INT64, Attribute::REQUIRED);
  driver_.Init(&table);

  ASSERT_EQ(0, Parse("b := a + 2; fence x:= row(sum(b)); fence y:= out(b, where = x$0 < 20)"));
}


TEST_F(PumaParseTest, CountAgg) {
  table_.Add("foo", DataType::INT64, Attribute::NULLABLE);
  driver_.Init(&table_);

  EXPECT_EQ(1, Parse("x := load('bar');"));
  EXPECT_EQ(1, Parse("fence z := row(sum(1))"));

  ASSERT_EQ(0, Parse("x := load('foo'); fence z := row(sum(def(x)));"));
  const auto& outp = driver_.barrier_arr();
  ASSERT_EQ(1, outp.size());
  const ParsedFence& b = outp.front();
  ASSERT_EQ(1, b.args.size());

  AstId out_id = b.args.front();
  ASSERT_GE(out_id, 0);

  const auto& expr_arr = driver_.expr_arr();
  EXPECT_EQ(4, expr_arr.size());

  const AstExpr& e1 = expr_arr[out_id];
  EXPECT_EQ(DataType::INT64, e1.data_type());
  EXPECT_TRUE(aux::IsAggregator(e1.op()));

  const AstExpr& load = expr_arr.front();
  ASSERT_EQ(AstOp::LOAD_FN, load.op());
  ASSERT_EQ("foo", load.str());
}

TEST_F(PumaParseTest, Fence) {
  EXPECT_EQ(1, Parse("x := load('x'); y := sum(x)"));
  EXPECT_EQ(1, Parse("x := load('x'); fence y := out(1 + sum(x))"));
  EXPECT_EQ(1, Parse("x := load('x'); fence y := out(sum(x)); fence y := out(sum(x+1))"));
  EXPECT_EQ(0, Parse("x := load('x'); a := load('a'); fence y := row(sum(x)); "
                     "fence z := out(a * y$0)"));

  // Index is out of bounds.
  EXPECT_EQ(1, Parse("x := load('x'); a := load('a'); fence y := row(sum(x)); "
                     "fence z := out(a * y$1)"));

  // TODO: Unrelated tables.
  // EXPECT_EQ(1, Parse("a := load('a'); fence x := out(a + 1); fence y := out([a, x$0]);"));
}

TEST_F(PumaParseTest, HashBy) {
  EXPECT_EQ(1, Parse("fence z := hash_by(1,2;)"));
  EXPECT_EQ(0, Parse("x := load('x'); fence z := hash_by(x)"));
  EXPECT_EQ(0, Parse("x := load('x'); fence z := hash_by(x, [])"));
  EXPECT_EQ(1, Parse("x := load('x'); fence z := hash_by(sum(x))"));
  EXPECT_EQ(1, Parse("x := load('x'); fence z := hash_by(x, x)"));
  EXPECT_EQ(0, Parse("hash_by := load('x')"));

  EXPECT_EQ(0, Parse("hash_by := load('x'); fence z := hash_by(hash_by, [])"));

  ASSERT_EQ(0, Parse("x := load('x'); y := load('y'); fence z := hash_by([x,y], sum(x + 1))"));
  ASSERT_EQ(1, driver_.barrier_arr().size());

  {
    const ParsedFence& b = driver_.barrier_arr().front();
    ASSERT_THAT(b.args, ElementsAre(0, 1));
    ASSERT_THAT(b.args2, ElementsAre(4));
    ASSERT_EQ(0, b.split_factor);
  }

  ASSERT_EQ(0, Parse("x := load('x'); fence y := hash_by([x, x+1], sum(x + 1), split=20)"));
  ASSERT_EQ(20, driver_.barrier_arr().front().split_factor);
}

TEST_F(PumaParseTest, LookupSameName) {
  table_.Add("foo", DataType::INT64, Attribute::NULLABLE);
  driver_.Init(&table_);

  ASSERT_EQ(0, Parse("fence y := out(foo)"));
}

TEST_F(PumaParseTest, FenceOpt) {
  table_.Add("foo", DataType::INT64, Attribute::NULLABLE);
  driver_.Init(&table_);

  EXPECT_EQ(0, Parse("fence y := out(foo, where = foo > 2)"));
  EXPECT_EQ(0, Parse("fence y := hash_by(foo, [], where = foo < 2, split = 2)"));

  // We can not know what are the dimensions of y with respect to z, so this is invalid.
  EXPECT_EQ(1, Parse("fence y := out(foo); fence z := out(foo, where = y$0 < 0)"));

  EXPECT_EQ(1, Parse("fence y := out(foo, where = (1 = 1)"));
  EXPECT_EQ(1, Parse("fence y := out(foo, where = (sum(foo)) > 5);"));
}

TEST_F(PumaParseTest, Rep) {
  EXPECT_EQ(0, Parse("x := load('x'); fence y := out(def(x))"));
  ASSERT_EQ(0, Parse("x := load('x'); fence y := row(max(def(x))); fence z:= row(def(y$0));"));

  table_.Add("x", DataType::INT64, Attribute::REPEATED);
  table_.Add("z", DataType::INT64, Attribute::REQUIRED);
  driver_.Init(&table_);

  EXPECT_EQ(0, Parse("x := load('x'); fence y := out(len(x))"));
  const auto& expr_arr = driver_.expr_arr();
  EXPECT_EQ(3, expr_arr.size());  // The third one is the output column of 'out' fence.

  const auto& ex = expr_arr[0];
  const auto& lenx = expr_arr[1];

  EXPECT_TRUE(ex.op() == AstOp::LOAD_FN && ex.str() == "x" &&
              ex.attr_id().local_defined());
  EXPECT_TRUE(lenx.op() == AstOp::LEN_FN && lenx.left() == 0 && !lenx.attr_id().local_defined());

  EXPECT_EQ(1, Parse("x := load('x'); fence y := out(len(x+1))"));
  EXPECT_EQ(1, Parse("x := load('x'); y := load('y'); fence x := out((len(x) + 1) < y)"));
  EXPECT_EQ(1, Parse("fence y := out([x, z])"));

  EXPECT_EQ(0, Parse("y := reduce(sum(x));"));
  EXPECT_EQ(1, Parse("y := reduce(sum(z));"));  // z is not-repeated.

  // We allow applying def() on required fields for now.
  EXPECT_EQ(0, Parse("fence y := out(def(z))"));
  EXPECT_EQ(1, Parse("fence y := out(x + z)"));

  EXPECT_EQ(0, Parse("fence y := out([z, len(x)])"));
}

TEST_F(PumaParseTest, ReplFn) {
  EXPECT_EQ(1, Parse("x := load('x'); y := repl(len(x), 1)"));

  // Replicates y into x. y belongs to parent group of x.
  EXPECT_EQ(0, Parse("x := load('x'); y := load('y'); z := repl(len(x), y);"));
  EXPECT_EQ(0, Parse("x := load('x'); y := load('y'); a := len(x); z := repl(a, a + 1);"));
  EXPECT_EQ(1, Parse("x := load('x'); y := load('y'); a := len(x); z := repl(a * 1, a + 1);"));
}

TEST_F(PumaParseTest, DuplicateStatements) {
  table_.Add("x", DataType::INT64, Attribute::REPEATED);
  driver_.Init(&table_);

  ASSERT_EQ(0, Parse("x := load('x'); y := load('x'); z := len(x); w := len(y)"));

  const auto& expr_arr = driver_.expr_arr();
  ASSERT_EQ(2, expr_arr.size());
  EXPECT_EQ(AstOp::LOAD_FN, expr_arr[0].op());
  EXPECT_EQ(AstOp::LEN_FN, expr_arr[1].op());

  EXPECT_EQ(0, driver_.LookupIdentifier("x").obj);
  EXPECT_EQ(0, driver_.LookupIdentifier("y").obj);

  EXPECT_EQ(1, driver_.LookupIdentifier("z").obj);
  EXPECT_EQ(1, driver_.LookupIdentifier("w").obj);
}

TEST_F(PumaParseTest, MessageBase) {
  const Attribute* m1 = table_.Add("m1", DataType::MESSAGE, Attribute::REPEATED);
  table_.Add("x", DataType::INT64, Attribute::REPEATED, m1);
  table_.Add("y", DataType::INT64, Attribute::REQUIRED, m1);

  table_.Add("z", DataType::INT64, Attribute::NULLABLE, m1);
  table_.Add("w", DataType::INT64, Attribute::REQUIRED, m1);

  const Attribute* m2 = table_.Add("m2", DataType::MESSAGE, Attribute::REQUIRED, m1);
  table_.Add("a", DataType::INT64, Attribute::NULLABLE, m2);
  table_.Add("b", DataType::INT64, Attribute::REPEATED, m2);

  const Attribute* m3 = table_.Add("m3", DataType::MESSAGE, Attribute::NULLABLE);
  table_.Add("c", DataType::INT64, Attribute::NULLABLE, m3);
  table_.Add("d", DataType::INT64, Attribute::REQUIRED);

  driver_.Init(&table_);

  ASSERT_EQ(0, Parse("x := load('m1.x'); fence y := row(sum(def(x)))"));
  const auto& expr_arr = driver_.expr_arr();
  EXPECT_EQ(4, expr_arr.size());  // The third one is the output column of 'out' fence.

  const auto& ex = expr_arr[0];

  ASSERT_EQ(AstOp::LOAD_FN, ex.op());
  ASSERT_EQ("m1.x", ex.str());

  ASSERT_EQ(0, Parse("x := len(m1)"));
  ASSERT_EQ(1, Parse("fence x := row(sum(m1))"));

  ASSERT_EQ(0, Parse("a := m1.z + m1.y"));
  ASSERT_EQ(0, Parse("a := m1.w + m1.z"));
  ASSERT_EQ(0, Parse("a := m1.w + m1.y"));

  ASSERT_EQ(0, Parse("a := m1.w + m1.m2.a;"));
  ASSERT_EQ(1, Parse("a := m1.w + m1.m2.b;"));
  ASSERT_EQ(0, Parse("a := reduce(max(m1.x)) + m1.m2.a"));

  ASSERT_EQ(1, Parse("a := reduce(max(m1.x)) + m1.m2.b"));
  ASSERT_EQ(0, Parse("a := reduce(max(m1.x)) + reduce(sum(m1.m2.b))"));

  ASSERT_EQ(0, Parse("fence q := row(sum(m3.c + d))"));
}

TEST_F(PumaParseTest, MessageWhere) {
  const Attribute* m_null = table_.Add("m_null", DataType::MESSAGE, Attribute::NULLABLE);
  const Attribute* m_req = table_.Add("m_req", DataType::MESSAGE, Attribute::REQUIRED);

  table_.Add("a", DataType::INT64, Attribute::NULLABLE, m_req);
  table_.Add("b", DataType::INT64, Attribute::REPEATED, m_req);
  table_.Add("c", DataType::INT64, Attribute::NULLABLE, m_null);
  driver_.Init(&table_);

  ASSERT_EQ(0, Parse("fence q := out(m_req.a, where = (reduce(sum(m_req.b)) > 1) )"));
  ASSERT_EQ(1, Parse("fence q := out(m_req.a, where = m_req.b > 1)"));
  ASSERT_EQ(0, Parse("fence q := out(m_req.a, where = m_null.c > 1)"));
}

TEST_F(PumaParseTest, Cnt) {
  table_.Add("opt_msg", DataType::MESSAGE, Attribute::NULLABLE);
  table_.Add("x", DataType::INT32, Attribute::REQUIRED);
  driver_.Init(&table_);

  EXPECT_EQ(0, Parse("fence a := row(cnt()); "));
  EXPECT_EQ(0, Parse("x := load('x'); fence a:= hash_by(x, cnt());"));
}

TEST_F(PumaParseTest, ArithmTypes) {
  table_.Add("x", DataType::INT64, Attribute::REPEATED);
  table_.Add("u32", DataType::UINT32, Attribute::REQUIRED);
  table_.Add("i32", DataType::INT32, Attribute::REQUIRED);
  table_.Add("u64", DataType::UINT64, Attribute::REQUIRED);
  table_.Add("i64", DataType::INT64, Attribute::REQUIRED);

  driver_.Init(&table_);

  EXPECT_EQ(0, Parse("fence a := out(x > 5); "));
  EXPECT_EQ(0, Parse("fence a := out(u32 > 5); fence b := out(i32 > 5);"));
  EXPECT_EQ(0, Parse("fence a := out(u64 > 5); fence b := out(i64 > 5);"));
  EXPECT_EQ(1, Parse("fence a := out(u32 > -5);"));
  EXPECT_EQ(0, Parse("fence a := out(i32 > -5);"));
  EXPECT_EQ(0, Parse("x := 1 - 2147483648; y := -0.5; fence a := out(u32);"));

  const AstExpr& x = LookupIdentifier("x");
  EXPECT_EQ(DataType::INT64, x.data_type());

  const AstExpr& y = LookupIdentifier("y");
  EXPECT_EQ(DataType::DOUBLE, y.data_type());
}

}  // namespace puma
