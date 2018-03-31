// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "base/integral_types.h"
#include "base/pod_array.h"

namespace puma {

class IntCoder {
 public:
  enum { kMaxIntInputSize = (1UL << 20) - 1};

  explicit IntCoder(unsigned level) : level_(level) {}

  // Produces a single compressed block.
  // The block contains the compressed size information for easier decoding from the stream.
  // The destination must accomodate at least MaxComprBufSize(isize) bytes.
  // isize should be less or equal to kMaxIntInputSize.
  uint32_t Encode64(const uint64_t* source, uint32_t isize, void* dest) {
    return EncodeT(source, isize, dest);
  }

  uint32_t Encode32(const uint32_t* source, uint32_t isize, void* dest) {
    return EncodeT(source, isize, dest);
  }

  // int_cnt must be less or equal than kMaxInt32InputSize.
  template<typename T> static uint32_t MaxComprSize(uint32_t int_cnt);

  struct Stats {
   unsigned raw_cnt = 0;
   unsigned byte_pack = 0;
   unsigned base_substract = 0;
  };

  const Stats& stats() const { return stats_; }

  template<typename T> uint32_t EncodeT(const T* src, uint32_t isize, void* dest);
private:

  base::PODArray<uint8> reduced_bw_;

  unsigned level_;
  Stats stats_;
};


class IntDecoder {
 public:
  explicit IntDecoder(const uint32_t max_dest_size);

  enum Status {
    // when the the decoding was not finished. input might be not consumed fully.
    NOT_FINISHED,

    // When the decoding of the block was finished and no more calls to Continue
    // are necessary. In that case it might be that the input is not consumed fully.
    FINISHED,
  };

  typedef std::pair<Status, uint32_t> Result; // (Status, number of compressed bytes consumed).


  // header must have at least 4 bytes filled.
  // Returns the size of the block (including header) needed to pass to Decode to parse the block
  // in a single pass and without memory copies.
  static uint32 Peek(const void* header);

  // Caches "dest" and then ignores it on the next invocations of Decode until FINISHED
  // is returned.
  // Result.second is updated to how many bytes were consumed during the call.
  template<typename T> Result DecodeT(const void* src, uint32_t isize,
                                      T* const dest);

  uint32_t written() const { return written_; }

 private:
  template<typename T> Result DecodeBlock(const uint8_t* src, uint32_t isize);

  template<typename T> uint32_t DecodeRaw(const uint8_t* src, uint32_t isize);

  template<typename T> void Decompress(const uint8_t* src, uint32_t size);

  uint32_t max_dest_size_ = 0; // in integers.
  uint8* dest_ = nullptr;
  uint8 *next_val_ = nullptr;

  uint8_t* next_packed_ = nullptr;
  uint32_t compress_size_ = 0;
  uint32_t written_ = 0;

  uint64_t base_ = 0;
  unsigned bw_ = 0;

  base::PODArray<uint8> compressed_buf_;
  bool start_ = true;

  // Can be 0, 1, or 2 - undefined.
  unsigned char is_raw_ = 2;
};



}  // namespace puma
