// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "util/coding/block_compressor.h"

#include "base/endian.h"
#include "base/flit.h"

namespace puma {

template <size_t INT_SIZE> class IntCompressorT {
  static_assert(INT_SIZE == 4 || INT_SIZE == 8, "");

  static constexpr uint32 int_limit(uint32_t byte_size) {
    return (byte_size - HEADER_SIZE) / INT_SIZE;
  }

 public:
  enum { HEADER_SIZE = 5};
  enum { NUM_INTS_PER_BLOCK = int_limit(BlockCompressor::BLOCK_SIZE) };

  IntCompressorT() : int_buf_(new UT[NUM_INTS_PER_BLOCK]) {}

  size_t pending_size() const { return pos_ * INT_SIZE; }
  size_t compressed_size() const { return block_compressor_.compressed_size(); }

  const std::vector<strings::ByteRange>& compressed_blocks() const {
    return block_compressor_.compressed_blocks();
  }

  template <typename V> void Add(V v);
  template <typename V> void Add(const V* src, uint32_t cnt);

  // Finalizes the frame.
  void Finalize();

  void ClearCompressedData();

 private:
  void Compress(bool final);

  BlockCompressor block_compressor_;
  uint32_t pos_ = 0;
  uint32_t limit_ = NUM_INTS_PER_BLOCK;
  uint32_t ints_in_last_block_ = 0;

  using UT = std::conditional_t<INT_SIZE == 4, uint32_t, uint64_t>;

  std::unique_ptr<UT[]> int_buf_;
  UT min_val_ = UT(-1);
};

using IntCompressor32 = IntCompressorT<sizeof(int32_t)>;
using IntCompressor64 = IntCompressorT<sizeof(int64_t)>;

template <size_t INT_SIZE> class IntDecompressor {
 public:
  using UT = std::conditional_t<INT_SIZE == 4, uint32_t, uint64_t>;

  enum {NUM_INTS_PER_BLOCK = IntCompressorT<INT_SIZE>::NUM_INTS_PER_BLOCK };
  enum {HEADER_SIZE = IntCompressorT<INT_SIZE>::HEADER_SIZE };

  using IntRange = strings::Range<UT*>;

  // Returns 0 if decompression of the frame is ended, 1 if it's still going.
  // In any case "*consumed" will hold how many bytes were consumed from br.
  // If negative number is returned - then last portion of br is too small to decompress
  // -(result) will tell how many input bytes are needed.
  // For 0,1 results it's safe to call GetIntData() to access the inflated int range.
  int Decompress(strings::ByteRange br, uint32_t* consumed);

  // Fills int data. Requires: Decompress was called and returned result >=0.
  // Returns 0 when not more data available from the decompress buffer.
  // dest must have capacity for at least NUM_INTS_PER_BLOCK integers.
  size_t Next(UT* dest);

  bool HasDecompressedData() const { return bd_next_ != bd_end_; }

 private:
  BlockDecompressor bd_;

  const uint8_t* bd_next_ = nullptr, *bd_end_ = nullptr;
  int bd_res_ = 0;
};


class IntCompressorV2 {
  enum { BLOCK_HEADER_SIZE = 3};

public:
  typedef int64_t T;
  typedef uint64_t UT;

  enum { BLOCK_MAX_BYTES = 1U << 15, BLOCK_MAX_LEN = BLOCK_MAX_BYTES / sizeof(T) };  // 2^13
  enum { COMPRESS_BLOCK_BOUND = (1U << 16) + BLOCK_HEADER_SIZE};

  constexpr static uint32_t CommitMaxSize(uint32_t sz) {
    return (sz*8 + (sz*8 / 255) + 16) + BLOCK_HEADER_SIZE;
  }

  // Can not use CommitMaxSize yet, so redefining the constant.
  enum {COMMIT_MAX_SIZE = BLOCK_MAX_LEN * 8 + BLOCK_MAX_LEN * 8 / 255 + 16 /* lz4 space */
        + BLOCK_HEADER_SIZE + sizeof(T) };


  // Dest must be at least of CommitMaxSize(sz). The simpler approach is to always use
  // COMMIT_MAX_SIZE to accomodate BLOCK_MAX_LEN.
  // Commit will finally write no more than COMPRESS_BLOCK_BOUND bytes even though uses
  // more space in between.
  uint32_t Commit(const T* src, uint32_t sz, uint8_t* dest);

 private:
  uint32_t WriteRaw(const T* src, uint32_t sz, uint8_t* dest);

  struct Aux {
    uint8_t shuffle_buf[BLOCK_MAX_BYTES];
    UT interm[BLOCK_MAX_LEN];
  };

  std::unique_ptr<Aux> aux_;
};

template<size_t INT_SIZE> template<typename V> void IntCompressorT<INT_SIZE>::Add(V v) {
  static_assert(sizeof(V) == INT_SIZE && std::is_integral<V>::value, "");

  UT u = base::ZigZagEncode(v);

  if (u < min_val_) {
    min_val_ = u;
  }

  int_buf_[pos_] = u;
  if (++pos_ >= limit_) {
    Compress(false);
  }
}


template <size_t INT_SIZE> template<typename V>
  void IntCompressorT<INT_SIZE>::Add(const V* src, uint32_t cnt) {
  static_assert(sizeof(V) == INT_SIZE && std::is_integral<V>::value, "");

  while (cnt > 0) {
    uint32_t to_fill = cnt + pos_ > limit_ ? limit_ - pos_ : cnt;
    for (uint32_t i = 0; i < to_fill; ++i) {
      UT u = base::ZigZagEncode(*src++);
      int_buf_[pos_++] = u;
      if (u < min_val_)
        min_val_ = u;
    }
    if (pos_ == limit_)
      Compress(false);
    cnt -= to_fill;
  }

}

}  // namespace puma
