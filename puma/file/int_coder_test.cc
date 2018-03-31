// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/file/int_coder.h"

#include "base/gtest.h"
#include "base/logging.h"

namespace puma {

class IntCoderTest : public testing::Test {
 public:
  IntCoderTest() : encoder_(2), decoder_(1024) {
    std::fill(arr_.begin(), arr_.end(), 0x82);
  }
 protected:
  IntCoder encoder_;
  IntDecoder decoder_;

  std::array<uint32_t, 128> arr_;

  union {
    char buf[2000];
    uint32 ibuf[1];
  } u_;
};

TEST_F(IntCoderTest, Compress64Basic) {
  std::vector<uint64> v(16);
  for (size_t i = 0; i < v.size(); ++i) {
    v[i] = (1 << 20) + i;
  }

  size_t max_sz = IntCoder::MaxComprSize<uint64>(v.size());
  std::unique_ptr<uint8[]> buf(new uint8[max_sz]);

  size_t outp = encoder_.Encode64(v.data(), v.size(), buf.get());
  DCHECK_LT(outp, max_sz);
  EXPECT_EQ(28, outp);

  const auto& stats = encoder_.stats();
  EXPECT_EQ(1, stats.base_substract);
  EXPECT_EQ(1, stats.byte_pack);
  EXPECT_EQ(1, stats.raw_cnt);


  std::vector<uint64> extracted(v.size());
  for (size_t i = 0; i < 2; ++i) {
    auto result = decoder_.DecodeT(buf.get(), outp, &extracted.front());

    EXPECT_EQ(IntDecoder::FINISHED, result.first);
    EXPECT_EQ(outp, result.second);
    EXPECT_EQ(extracted.size(), decoder_.written());
    EXPECT_EQ(v, extracted);
  }
}


TEST_F(IntCoderTest, Compress64Big) {
  for (unsigned base = 0; base < 6; ++base) {
    std::vector<uint64> v(24, 0);
    for (size_t i = base; i < v.size(); ++i) {
      v[i] = (1UL << 52) + i;
    }

    size_t max_sz = IntCoder::MaxComprSize<uint64>(v.size());
    std::unique_ptr<uint8[]> buf(new uint8[max_sz]);

    IntCoder enc(5);
    size_t outp = enc.Encode64(v.data(), v.size(), buf.get());
    DCHECK_LT(outp, max_sz);
    EXPECT_LT(outp, sizeof(v[0]) * v.size() - 16);

    const auto& stats = enc.stats();
    int substract = base > 0 ? 0 : 1;
    EXPECT_EQ(substract, stats.base_substract) << base;
    EXPECT_EQ(substract, stats.raw_cnt);
    EXPECT_EQ(1, stats.byte_pack);

    std::vector<uint64> extracted(v.size());

    constexpr unsigned kDelta = 8;
    for (size_t i = 0; i < 2; ++i) {
      auto result = decoder_.DecodeT(buf.get(), outp - kDelta, &extracted.front());
      ASSERT_EQ(IntDecoder::Result(IntDecoder::NOT_FINISHED, outp - kDelta), result);

      result = decoder_.DecodeT(buf.get() + outp - kDelta, kDelta + 20,  &extracted.front());
      ASSERT_EQ(IntDecoder::Result(IntDecoder::FINISHED, kDelta), result);

      EXPECT_EQ(extracted.size(), decoder_.written());
      ASSERT_EQ(v, extracted) << i << "/" << base;
    }
  }
}


TEST_F(IntCoderTest, CompressNeg) {
  std::vector<int64> v(2, -1);

  size_t max_sz = IntCoder::MaxComprSize<uint64>(v.size());
  std::unique_ptr<uint8[]> buf(new uint8[max_sz]);

  size_t outp = encoder_.EncodeT(reinterpret_cast<const uint64*>(v.data()), v.size(), buf.get());

  std::vector<int64> extracted(v.size());

  auto result = decoder_.DecodeT(buf.get(), outp,  reinterpret_cast<uint64*>(&extracted.front()));
  ASSERT_EQ(IntDecoder::Result(IntDecoder::FINISHED, outp), result);

  EXPECT_EQ(extracted.size(), decoder_.written());
  EXPECT_EQ(v, extracted);
}


TEST_F(IntCoderTest, Compress32Basic) {
  std::vector<uint32> v(16);
  for (size_t i = 0; i < v.size(); ++i) {
    v[i] = (1 << 20) + i;
  }

  size_t max_sz = IntCoder::MaxComprSize<uint32>(v.size());
  std::unique_ptr<uint8[]> buf(new uint8[max_sz]);

  size_t outp = encoder_.EncodeT(v.data(), v.size(), buf.get());
  DCHECK_LT(outp, max_sz);
  EXPECT_EQ(24, outp);

  const auto& stats = encoder_.stats();
  EXPECT_EQ(1, stats.base_substract);
  EXPECT_EQ(1, stats.byte_pack);
  EXPECT_EQ(1, stats.raw_cnt);


  std::vector<uint32> extracted(v.size());
  for (size_t i = 0; i < 2; ++i) {
    auto result = decoder_.DecodeT<uint32>(buf.get(), outp, &extracted.front());

    EXPECT_EQ(IntDecoder::FINISHED, result.first);
    EXPECT_EQ(outp, result.second);
    EXPECT_EQ(extracted.size(), decoder_.written());
    EXPECT_EQ(v, extracted);
  }
}


TEST_F(IntCoderTest, Compress32Big) {
  for (unsigned base = 0; base < 4; ++base) {
    std::vector<uint32> v(24, 0);
    for (size_t i = base; i < v.size(); ++i) {
      v[i] = (1UL << 20) + i;
    }

    size_t max_sz = IntCoder::MaxComprSize<uint64>(v.size());
    std::unique_ptr<uint8[]> buf(new uint8[max_sz]);

    IntCoder enc(5);
    size_t outp = enc.EncodeT(v.data(), v.size(), buf.get());
    DCHECK_LT(outp, max_sz);
    EXPECT_LT(outp, sizeof(v[0]) * v.size() - 16);

    const auto& stats = enc.stats();
    int substract = base > 0 ? 0 : 1;
    EXPECT_EQ(substract, stats.base_substract) << base;
    EXPECT_EQ(substract, stats.raw_cnt);
    EXPECT_EQ(1, stats.byte_pack);

    std::vector<uint32> extracted(v.size());

    constexpr unsigned kDelta = 8;
    for (size_t i = 0; i < 2; ++i) {
      ASSERT_EQ(outp, IntDecoder::Peek(buf.get()));

      auto result = decoder_.DecodeT(buf.get(), outp - kDelta, &extracted.front());
      ASSERT_EQ(IntDecoder::Result(IntDecoder::NOT_FINISHED, outp - kDelta), result);

      result = decoder_.DecodeT(buf.get() + outp - kDelta, kDelta + 20,  &extracted.front());
      ASSERT_EQ(IntDecoder::Result(IntDecoder::FINISHED, kDelta), result);

      EXPECT_EQ(extracted.size(), decoder_.written());
      ASSERT_EQ(v, extracted) << i << "/" << base;
    }
  }
}


}  // namespace puma
