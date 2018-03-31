// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/parser/attribute_index.h"

#include "puma/schema.h"
#include "base/gtest.h"

namespace puma {

using namespace schema;

class AttributeIndexTest : public testing::Test {
 protected:
  void SetUp() override {
  }

  AttributeIndexTest() : table_("table"), index_(&table_) {}

  schema::Table table_;
  QueryAttributeIndex index_;
  using Info = QueryAttributeIndex::Info;
};

TEST_F(AttributeIndexTest, Basic) {
  const Attribute* m1 = table_.Add("m1", DataType::MESSAGE, Attribute::NULLABLE);
  const Attribute* m12 = table_.Add("m1", DataType::MESSAGE, Attribute::REQUIRED, m1);
  const Attribute* m2 = table_.Add("m2", DataType::MESSAGE, Attribute::REPEATED, m1);
  const Attribute* m21 = table_.Add("m21", DataType::MESSAGE, Attribute::REQUIRED, m2);
  EXPECT_EQ(2, m21->level());

  table_.Add("f1", DataType::INT64, Attribute::NULLABLE, m12);
  table_.Add("f2", DataType::INT64, Attribute::REQUIRED, m21);
  AttributeId f1_id = index_.AddVar("m1.m1.f1");
  ASSERT_NE(kInvalidAttrId, f1_id);

  {
    const Info& f1_info = index_.info(f1_id);
    EXPECT_EQ(2, f1_info.level());
  }

  AttributeId f2_id = index_.AddVar("m1.m2.m21.f2");
  ASSERT_NE(kInvalidAttrId, f1_id);

  AttributeId m21_id = index_.AddVar("m1.m2.m21");
  ASSERT_NE(kInvalidAttrId, m21_id);

  const Info& f2_info = index_.info(f2_id);
  EXPECT_EQ(3, f2_info.level());
  EXPECT_EQ(m21_id.local_id(), f2_info.parent);

  const Info& f2_parent = index_.info(AttributeId(0, f2_info.parent));
  EXPECT_EQ(m21, f2_parent.field);

  EXPECT_EQ(m21_id, index_.FindCommonAncestor(m21_id, f2_id));
  EXPECT_EQ(m21_id, index_.FindCommonAncestor(f2_id, m21_id));
}

TEST_F(AttributeIndexTest, Rep) {
  const Attribute* a_f1 = table_.Add("f1", DataType::INT64, Attribute::REPEATED);
  const Attribute* a_f2 = table_.Add("f2", DataType::INT64, Attribute::REQUIRED);

  EXPECT_EQ(0, a_f1->level());
  EXPECT_EQ(0, a_f2->level());

  AttributeId f1_id = index_.AddVar("f1");
  AttributeId f2_id = index_.AddVar("f2");
  AttributeId ca = index_.FindCommonAncestor(f2_id, f1_id);
  ASSERT_FALSE(ca.local_defined());

  AttributeId pf1_id = index_.parent(f1_id);
  ASSERT_NE(pf1_id, kInvalidAttrId);

  ca = index_.FindCommonAncestor(pf1_id, f2_id);
}

}  // namespace puma
