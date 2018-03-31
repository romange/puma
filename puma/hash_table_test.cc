// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/hash_table.h"

#include "base/logging.h"
#include "base/gtest.h"

namespace puma {

template<typename T> static const char* charptr(const T& t) {
  return reinterpret_cast<const char*>(&t);
}

class HashTableTest : public testing::Test {
 protected:

};

TEST_F(HashTableTest, Basic) {
  HashTable table(pmr::get_default_resource(), 4, 4, 256);

  for (uint32 i = 0; i < 100000; ++i) {
    uint32 val = i;
    ASSERT_TRUE(nullptr == table.Lookup(charptr(val))) << i;
  }

  for (uint32 i = 0; i < 40; ++i) {
    ASSERT_EQ(HashTable::INSERT_OK, table.Insert(charptr(i), charptr(i)))
        << table.size() << " out of " << table.capacity();

    ASSERT_EQ(HashTable::KEY_EXIST, table.Insert(charptr(i), charptr(i)))
        << table.size() << " out of " << table.capacity();
  }

  for (uint32 i = 0; i < 40; ++i) {
    char* res = table.Lookup(charptr(i));
    ASSERT_TRUE(res != nullptr);
    ASSERT_EQ(i, *reinterpret_cast<uint32*>(res));

    ASSERT_EQ(HashTable::KEY_EXIST, table.Insert(charptr(i), charptr(i)))
        << table.size() << " out of " << table.capacity();
  }

  for (uint32 i = 40; i < 40000; ++i) {
    ASSERT_TRUE(nullptr == table.Lookup(charptr(i)));
  }
}

TEST_F(HashTableTest, Set) {
  HashTable table(pmr::get_default_resource(), 8, 0, 1 << 16);

  for (uint64 i = 0; i < 100000; ++i) {
    ASSERT_TRUE(nullptr == table.Lookup(charptr(i)));
  }

  for (uint64 i = 0; i < 7300; ++i) {
    ASSERT_EQ(HashTable::INSERT_OK, table.Insert(charptr(i), nullptr))
        << table.size() << " out of " << table.capacity();
  }

  for (uint64 i = 0; i < 7300; ++i) {
    ASSERT_EQ(HashTable::KEY_EXIST, table.Insert(charptr(i), nullptr));
    char* res = table.Lookup(charptr(i));
    ASSERT_TRUE(res != nullptr);
  }

  for (uint64 i = 7300; i < 173000; ++i) {
    ASSERT_TRUE(nullptr == table.Lookup(charptr(i)));
  }
}

TEST_F(HashTableTest, ChangeInline) {
  HashTable table(pmr::get_default_resource(), 8, 8, 1024);
  uint64 key = 1, val = 2;
  uint64* res_val = nullptr;

  ASSERT_EQ(HashTable::INSERT_OK, table.Insert(charptr(key), charptr(val),
                                               reinterpret_cast<char**>(&res_val)));
  ASSERT_TRUE(res_val != nullptr);
  EXPECT_EQ(2, *res_val);

  key = 3; val = 4;
  ASSERT_EQ(HashTable::INSERT_OK, table.Insert(charptr(key), charptr(val),
                                               reinterpret_cast<char**>(&res_val)));
  EXPECT_EQ(4, *res_val);

  *res_val = 15;

  uint64* res_val2 = nullptr;
  ASSERT_EQ(HashTable::KEY_EXIST, table.Insert(charptr(key), charptr(val),
                                               reinterpret_cast<char**>(&res_val2)));
  EXPECT_EQ(res_val, res_val2);
  EXPECT_EQ(15, *res_val2);
}

TEST_F(HashTableTest, Bug1) {
  HashTable table(pmr::get_default_resource(), 8, 8, 1 << 16);

  for (int64 i = 0; i < 1024; ++i) {
    HashTable::InsertResult result = table.Insert(charptr(i), charptr(i));
    ASSERT_EQ(HashTable::INSERT_OK, result);

    int64 neg = -i;
    result = table.Insert(charptr(neg), charptr(neg));
    ASSERT_NE(HashTable::CUCKOO_FAIL, result);

  }
  ASSERT_EQ(2047, table.size());



  const char* key, *val;
  for (size_t i = 0; i < table.capacity(); ++i) {
    if (!table.Get(i, &key, &val))
      continue;
    const int64* k = reinterpret_cast<const int64*>(key);
    const int64* v = reinterpret_cast<const int64*>(val);
    ASSERT_EQ(*k, *v) << i;
  }
}

TEST_F(HashTableTest, CuckooFail) {
  HashTable table(pmr::get_default_resource(), 4, 4, (1 << 7) + 10);

  int32 i = 0;
  char* val_result;
  for (; i < 128; ++i) {
    val_result = nullptr;
    HashTable::InsertResult result = table.Insert(charptr(i), charptr(i), &val_result);

    ASSERT_TRUE(val_result);
    ASSERT_EQ(i, *(int32*)val_result);
    if (result == HashTable::CUCKOO_FAIL)
      break;
  }
  ASSERT_LT(i, 127);

  const char* pulled_key, *pulled_val;
  table.GetKickedKeyValue(&pulled_key, &pulled_val);

  int32 pulled_int = *(int32*)pulled_key;
  ASSERT_EQ(pulled_int, *(int32*)pulled_val);

  for (int32 j = 0; j <= i; ++j) {
    char* val = table.Lookup(charptr(j));

    if (!val) {
      ASSERT_EQ(pulled_int, j);
      continue;
    }
    ASSERT_EQ(j, *(int32*)val);
  }
}

static void BM_InsertCuckoo(benchmark::State& state) {
  unsigned max_items = state.range_x();

  HashTable ht(pmr::get_default_resource(), 8, 0, max_items*8);

  while (state.KeepRunning()) {
    for (uint64 i = 0; i < max_items; ++i) {
      uint64 key = 1 + (i + 1)*i;
      ht.Insert(charptr(key), nullptr);
    }
  }
}
BENCHMARK(BM_InsertCuckoo)->Arg(800)->Arg(1 << 10)->Arg(1<<14);


}  // namespace puma
