// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/pb/int_compressor.h"

#include "base/gtest.h"
#include "base/logging.h"

#include "base/flit.h"

namespace puma {

using namespace base;
using namespace std;
using strings::ByteRange;

class IntCompressorTest : public testing::Test {
 public:
  IntCompressorTest() {}

 protected:
};


TEST_F(IntCompressorTest, ZigZag) {
  EXPECT_EQ(1, ZigZagEncode(1u));
  EXPECT_EQ(1ULL << 63, ZigZagEncode(1ULL << 63));

  EXPECT_EQ(2, ZigZagEncode(1));
  EXPECT_EQ(1ULL << 63, ZigZagEncode(1LL << 62));

  EXPECT_EQ(1, ZigZagEncode(-1));
  EXPECT_EQ(3, ZigZagEncode(-2));

  EXPECT_EQ(kuint32max, ZigZagEncode(kint32min));
  EXPECT_EQ(kuint64max, ZigZagEncode(kint64min));
}

TEST_F(IntCompressorTest, Int) {
  IntCompressor32 ic;
  int32_t arr[] = { 5, 5, 5, 5};

  for (unsigned i = 0; i < (1 << 16); ++i)
    ic.Add(5);
  for (unsigned i = 0; i < (1 << 14); ++i) {
    ic.Add(arr, arraysize(arr));
  }
  ic.Finalize();

  IntDecompressor<4> id;
  unique_ptr<uint32_t[]> dest(new uint32_t[IntDecompressor<4>::NUM_INTS_PER_BLOCK]);
  size_t total_size = 0;
  int res;
  for (const auto& cb : ic.compressed_blocks()) {
    uint32_t consumed = 0;
    res = id.Decompress(cb, &consumed);
    ASSERT_GE(res, 0);
    CHECK_EQ(cb.size(), consumed);

    size_t sz;
    while ((sz = id.Next(dest.get()))) {
      total_size += sz;
      for (size_t i = 0; i < sz;++i) {
        int32 decoded = base::ZigZagDecode<int32_t>(dest[i]);
        ASSERT_EQ(5, decoded);
      }
    }
  }
  EXPECT_EQ(0, res);
  EXPECT_EQ(1 << 17, total_size);

  ic.ClearCompressedData();

  ic.Add(5);
  ic.Finalize();
}

TEST_F(IntCompressorTest, Bug1) {
  IntCompressor32 ic;

  uint32_t num_items = 1 << 16;
  for (unsigned i = 0; i < num_items; ++i)
    ic.Add(5);


  while (ic.pending_size() > 0 || ic.compressed_size() == 0) {
    ic.Add(5);
    ++num_items;
  }
  ic.Finalize();

  IntDecompressor<4> id;
  int res;
  const auto& cblocks = ic.compressed_blocks();
  unsigned items = 0;
  unique_ptr<uint32_t[]> dest(new uint32_t[IntDecompressor<4>::NUM_INTS_PER_BLOCK]);

  for (const auto& cb : cblocks) {
    uint32 consumed = 0;
    res = id.Decompress(cb, &consumed);
    ASSERT_EQ(cb.size(), consumed);
    size_t sz = 0;
    while ((sz = id.Next(dest.get()))) {
      items += sz;
    }
  }
  ASSERT_EQ(0, res);
  ASSERT_EQ(num_items, items);
}


}  // namespace puma

