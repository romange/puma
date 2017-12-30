// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/schema.h"

#include <gmock/gmock.h>

#include "base/gtest.h"
#include "base/map-util.h"

using testing::ElementsAre;


namespace puma {
namespace schema {
class SchemaTest : public testing::Test {
 protected:
  SchemaTest() : pool_("table") {}

 	Table pool_;
};

TEST_F(SchemaTest, Basic) {
  const Attribute* attr1 = pool_.Add("field1", DataType::INT64, Attribute::REQUIRED);
  const Attribute* attr2 = pool_.Add("field2", DataType::MESSAGE, Attribute::NULLABLE);

  EXPECT_EQ("field1", attr1->Path());
  EXPECT_EQ("field2", attr2->Path());

  EXPECT_EQ(attr1->name(), attr1->Path());

  const char kName[] = "field2.field3";
	const Attribute* attr3 = pool_.AddWithPath(kName, DataType::INT64, Attribute::NULLABLE);
	EXPECT_EQ(attr2, attr3->parent());
	EXPECT_EQ(kName, attr3->Path());

	const auto& fields = attr2->fields();
	EXPECT_EQ(1, fields.size());
	EXPECT_EQ(attr3, FindValueWithDefault(fields, "field3", nullptr));
}

TEST_F(SchemaTest, Message) {
  const Attribute* p1 = pool_.Add("m1", DataType::MESSAGE, Attribute::REQUIRED);
  const Attribute* attr1 = pool_.Add("field1", DataType::INT64, Attribute::NULLABLE, p1);
  EXPECT_EQ("m1", p1->Path());
  EXPECT_EQ("m1.field1", attr1->Path());

  const Attribute* a1 = pool_.LookupByPath("m1.field1");
  EXPECT_TRUE(attr1 == a1);
  EXPECT_EQ(a1->parent(), p1);
  const Attribute* m1 = pool_.LookupByPath("m1");
  EXPECT_EQ(p1, m1);
  EXPECT_EQ(a1, FindValueWithDefault(m1->fields(), "field1", nullptr));
}

}  // namespace schema
}  // namespace puma
