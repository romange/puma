// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/pb/int_compressor.h"

#include <lz4.h>
#include <shuffle.h>

#include "base/endian.h"
#include "base/flit.h"

namespace puma {

using namespace std;
namespace flit = base::flit;
using strings::ByteRange;

namespace {

constexpr uint32_t kFlitBit = (1U << 23);
constexpr uint16_t kMagic = 0x7a03;
constexpr uint8_t kRawBit = 1 << 7;

template<typename UT> uint32_t DecompressFlit(const uint8_t* src, const uint8_t* end,
                                              uint32_t dest_limit, UT* dest) {
  UT min_val;
  memcpy(&min_val, src, sizeof(UT));
  src += sizeof(UT);

  UT val;
  UT* dest_begin = dest;
  while (src < end) {
    CHECK_LT(dest - dest_begin, dest_limit);

    src += flit::ParseT(src, &val);
    *dest++ = min_val + val;
  }
  DCHECK(src == end);
  return dest - dest_begin;
}


constexpr size_t kShuffleStep = 1024;

inline void bitshuffle2(const uint64_t* src, size_t count, uint8_t* dest, uint8_t* tmp) {
  const uint8_t* src_b = reinterpret_cast<const uint8_t*>(src);
  const uint8_t* src_e = src_b + count * sizeof(uint64_t);
  uint8_t* ptr = dest;
  for (; src_b + kShuffleStep <= src_e; src_b += kShuffleStep) {
    bitshuffle(sizeof(uint64_t), kShuffleStep, src_b, ptr, tmp);
    ptr+= kShuffleStep;
  }
  if (src_b != src_e) {
    bitshuffle(sizeof(uint64_t), src_e - src_b, src_b, ptr, tmp);
  }
}

inline void bitunshuffle2(const uint8_t* src, size_t sz, uint8_t* dest) {
  uint8_t tmp[kShuffleStep];

  size_t i = 0;
  uint8_t* ptr = dest;
  for (; i + kShuffleStep <= sz; i += kShuffleStep) {
    bitunshuffle(sizeof(uint64_t), kShuffleStep, src + i, ptr, tmp);
    ptr+= kShuffleStep;
  }
  if (i != sz) {
    bitunshuffle(sizeof(uint64_t), sz - i, src + i, ptr, tmp);
  }
}

}  // namespace


template <size_t INT_SIZE> void IntCompressorT<INT_SIZE>::Compress(bool is_final) {
  using strings::MutableByteRange;

  bool store_flit = false;
  if (pos_ >= 32) {
    size_t sz = 0;
    for (uint32_t i = 0; i < pos_; ++i) {
      sz += flit::Length(int_buf_[i] - min_val_);
    }

    if (sz + 2 * sizeof(UT) < sizeof(UT) * pos_ * 0.88) {
      store_flit = true;
      VLOG(1) << "Flit size is " << sz + 2 * sizeof(UT) << " vs " << sizeof(UT) * pos_;
    }
  }

  MutableByteRange mbr = block_compressor_.BlockBuffer();

  uint8_t* payload_start = mbr.begin() + HEADER_SIZE;
  LittleEndian::Store16(mbr.begin(), kMagic);
  uint32_t header = 0;
  uint32 byte_size;

  if (store_flit) {
    uint8* next = payload_start;

    LittleEndian::StoreT<UT>(min_val_, next);
    next += sizeof(min_val_);

    for(uint32_t i = 0; i < pos_; ++i) {
      next += flit::EncodeT<UT>(int_buf_[i] - min_val_, next);
    }

    header = kFlitBit | (next - payload_start);
    byte_size = next - mbr.begin();
  } else {
    byte_size = (pos_ * sizeof(UT));
    header = byte_size;
    memcpy(payload_start, int_buf_.get(), byte_size);
    byte_size += HEADER_SIZE;
  }
  mbr[2] = header & 0xFF;
  LittleEndian::Store16(mbr.begin() + 3, header >> 8);

  bool was_flushed = block_compressor_.Commit(byte_size);
  ints_in_last_block_ += pos_;

  if (is_final) {
    VLOG(1) << "IsFinal " << block_compressor_.pending_size();

    block_compressor_.Finalize();
    was_flushed = true;
  } else if (!was_flushed && byte_size + 128 > mbr.size()) {
    VLOG(1) << "BC::Compress " << block_compressor_.pending_size();
    block_compressor_.Compress();
    was_flushed = true;
  }

  if (was_flushed) {
    strings::ByteRange cblock = block_compressor_.compressed_blocks().back();
    VLOG(1) << "Compressing from " << INT_SIZE * ints_in_last_block_
            << " bytes (" << ints_in_last_block_ << " ints) to " << cblock.size() << " bytes";

    limit_ = int_limit(BlockCompressor::BLOCK_SIZE);
    ints_in_last_block_ = 0;
  } else {
    limit_ = int_limit(mbr.size() - byte_size);
  }

  // Reset the state for the next int block.
  min_val_ = UT(-1);
  pos_ = 0;
}

template <size_t INT_SIZE> void IntCompressorT<INT_SIZE>::Finalize() {
  if (pos_)
    Compress(true);
  else {
    VLOG(1) << "Block Finalize";
    block_compressor_.Finalize();
  }
}

template <size_t INT_SIZE> void IntCompressorT<INT_SIZE>::ClearCompressedData() {
  block_compressor_.ClearCompressedData();
  CHECK_EQ(0, pos_);
}


template <size_t INT_SIZE> int IntDecompressor<INT_SIZE>::Decompress(
    strings::ByteRange br, uint32_t* consumed) {
  using strings::ByteRange;

  bd_res_ = bd_.Decompress(br, consumed);
  if (bd_res_ < 0)
    return bd_res_;
  ByteRange raw = bd_.GetDecompressedBlock();

  VLOG(1) << "GetDecompressedBlock: " << raw.size() << " bd_res_ = " << bd_res_;

  // Empty blocks can happen when finalizing frame with 0 data in the last block, for example.

  bd_next_ = raw.begin();
  bd_end_ = raw.end();

  return bd_res_;
}

template <size_t INT_SIZE> size_t IntDecompressor<INT_SIZE>::Next(UT* dest) {
  if (bd_next_ == bd_end_)
    return 0;
  namespace flit = ::base::flit;

  CHECK_LT(HEADER_SIZE, bd_end_ - bd_next_) << bd_end_ - bd_next_;

  uint16_t magic = LittleEndian::Load16(bd_next_);
  CHECK_EQ(kMagic, magic);
  bd_next_ += 2;

  uint32_t header = LittleEndian::Load16(bd_next_ + 1);
  header = (header << 8) | bd_next_[0];

  UT* start = dest;
  uint32_t size = header & (~kFlitBit);
  uint32_t int_count;

  bd_next_ += 3;

  if (header & kFlitBit) {
    int_count = DecompressFlit(bd_next_, bd_next_ + size, NUM_INTS_PER_BLOCK, dest);
    bd_next_ += size;
  } else {
    CHECK_EQ(0, size % INT_SIZE);
    int_count = size / INT_SIZE;
    memcpy(start, bd_next_, size);
    bd_next_ += size;
  }
  return int_count;
}

uint32_t IntCompressorV2::Commit(const T* src, uint32_t count, uint8_t* dest) {
  if (count <= 16) {
    return WriteRaw(src, count, dest);
  }

  CHECK_LE(count, BLOCK_MAX_LEN);
  if (!aux_)
    aux_.reset(new Aux);
  T min_val = *src;

  for (uint32_t i = 1; i < count; ++i) {
    min_val = std::min(min_val, src[i]);
  }
  for (uint32_t i = 0; i < count; ++i) {
    aux_->interm[i] = UT(src[i]) - min_val;
  }

  // dest here serves as temporary buffer. aux_->shuffle is the destination.
  uint8_t* tmp_buf = dest;
  bitshuffle2(aux_->interm, count, aux_->shuffle_buf, tmp_buf);

  constexpr unsigned kHeaderOverhead = BLOCK_HEADER_SIZE + sizeof(T);
  char* const cdest = reinterpret_cast<char*>(dest);
  char* next = cdest + kHeaderOverhead; // for min_val
  uint8_t flags = 0;
  const unsigned kByteSize = sizeof(uint64_t) * count;
  int res = LZ4_compress_fast(reinterpret_cast<const char*>(aux_->shuffle_buf), next,
                              kByteSize, LZ4_COMPRESSBOUND(kByteSize), 3 /* level */);
  CHECK_GT(res, 0);
  if ((res + sizeof(T)) * 1.1 > kByteSize) {
    return WriteRaw(src, count, dest);
  }

  LittleEndian::StoreT(UT(min_val), dest + BLOCK_HEADER_SIZE);
  next += res;
  uint32_t written = next - cdest;

  CHECK_LE(written, COMPRESS_BLOCK_BOUND);
  LittleEndian::Store16(dest + 1, written - 3);
  *dest = flags;
  return written;
}

uint32_t IntCompressorV2::WriteRaw(const T* src, uint32_t count, uint8_t* dest) {
  *dest = kRawBit;
  uint16_t sz = count * sizeof(T);
  LittleEndian::Store16(dest + 1, sz);
  memcpy(dest + 3, src, sz);
  return sz + 3;
}


template class IntCompressorT<4>;
template class IntCompressorT<8>;
template class IntDecompressor<4>;
template class IntDecompressor<8>;

}  // namespace puma
