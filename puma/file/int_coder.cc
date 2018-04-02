// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/file/int_coder.h"

#define ZSTD_STATIC_LINKING_ONLY

#include <zstd.h>

#include "base/endian.h"
#include "base/logging.h"

namespace puma {

namespace {

inline unsigned byte_width(uint64 val) {
  return Bits::Bsr64NonZero(val | 1) / 8;
}

inline constexpr uint64 byte_mask(unsigned bw) {
  return (1ULL << (bw * 8)) - 1;
}


template<typename T>
  uint8* StoreReducedWidth(const T* src, uint32 cnt, T base, unsigned w, uint8* dest) {
  for(uint32 i = 0; i < cnt; ++i) {
    T v = src[i] - base;
    LittleEndian::StoreT<T>(v, dest);
    dest += w;
  }

  return dest;
}

template<typename T> constexpr uint32 MaxBufSize(uint32 cnt, uint32 base) {
  return sizeof(T) * cnt + base;
}

constexpr uint32 kSubstractBaseBit = 1U << 30;
constexpr uint32 kByteWidthBitStart = 27;
constexpr uint32 kLenMask = (1U << 23) - 1;

/* Header format:
|31                   | 30                        | 27-29                | 23-26     |
|compressed(1) or raw | Substract Base(1)/No base | actual byte width | reserved  |

| 0 - 22               |
|compressed block size |

"Compressed" - currently only zstd codec is used.
"actual bytewidth" - the real byte width of data, possibly after base substraction.
In both raw/compressed modes, dense packing is used with actual byte width.

*/

}  // namespace


template<typename T> uint32_t IntCoder::EncodeT(const T* source, uint32_t isize, void* dest) {
  DCHECK_GT(isize, 0);
  DCHECK_LE(isize, kMaxIntInputSize);

  const uint8* usrc = reinterpret_cast<const uint8*>(source);
  constexpr size_t kValSize = sizeof(T);

  size_t src_size = isize * kValSize;
  uint32* dest32 = reinterpret_cast<uint32*>(dest);
  dest32++;
  uint32 header = 0;

  unsigned bytew = kValSize - 1; // by default no byte width reduction.

  size_t base_added = 0;  // header.

  if (isize >= kValSize) {
    // Find min/max.
    T minv = *source, maxv = *source;

    for (size_t i = 1; i < isize; ++i) {
      if (source[i] > maxv) {
        maxv = source[i];
      } else {
        minv = (minv <= source[i]) ? minv : source[i];
      }
    }

    // in range: [0, kValSize).
    unsigned rangew = byte_width(maxv - minv);
    bytew = byte_width(maxv);

    if (rangew < bytew) {
      header |= kSubstractBaseBit;
      LittleEndian::StoreT<T>(minv, dest32);

      dest32 += kValSize / 4;
      bytew = rangew;
      ++stats_.base_substract;
      base_added += sizeof(minv);
    } else {
      minv = 0;
    }

    if (bytew < kValSize - 1) {
      ++stats_.byte_pack;

      src_size = isize * (bytew + 1);

      // allocate 8 byte more to allow simpler packing.
      reduced_bw_.resize(src_size + 8);
      usrc = reduced_bw_.data();

      uint8* ptr = StoreReducedWidth(source, isize, minv, 1 + bytew, reduced_bw_.data());
      DCHECK_EQ(ptr - reduced_bw_.data(), src_size);
    }
  }
  DCHECK_LE(src_size + base_added, kLenMask);

  header |= ((bytew & 7) << kByteWidthBitStart);
  size_t dst_capacity = ZSTD_compressBound(src_size);
  size_t res = ZSTD_compress(dest32, dst_capacity, usrc, src_size, level_);
  CHECK(!ZSTD_isError(res));
  VLOG(1) << "Compressing from " << src_size << " to " << res;

  if (res < src_size) {
    header |= ((res + base_added) | (1U << 31));  // zstd compressed.
    LittleEndian::Store32(dest, header);
    return res + base_added + 4;
  }

  header |= (src_size + base_added);  // raw block.
  LittleEndian::Store32(dest, header);

  memcpy(dest32, usrc, src_size);
  ++stats_.raw_cnt;

  return src_size + base_added + 4;
}

template<typename T> uint32_t IntCoder::MaxComprSize(uint32_t int_cnt) {
  return std::max<uint32_t>(ZSTD_compressBound(int_cnt * sizeof(uint32)),
                            sizeof(T) * int_cnt) + 4;
}


uint32 IntDecoder::Peek(const void *header) {
  const uint32 h = UNALIGNED_LOAD32(header);

  // header size + compressed size including possible base.
  uint32 cs = sizeof(uint32) + (h & kLenMask);

  return cs;
}


IntDecoder::IntDecoder(const uint32_t max_dest_size) : max_dest_size_(max_dest_size) {

}

template<typename T> auto IntDecoder::DecodeT(const void* src, uint32_t isize,
                                              T* const dest) -> Result {
  DCHECK_GE(isize, 4);

  const uint8* start = reinterpret_cast<const uint8*>(src);
  const uint8* block = start;
  if (!start_) {
    return DecodeBlock<T>(block, isize);
  }

  const uint32 header = UNALIGNED_LOAD32(src);
  is_raw_ = (header >> 31) == 0;
  next_packed_ = reinterpret_cast<uint8*>(dest);

  dest_ = reinterpret_cast<uint8*>(dest);
  next_val_ = dest_;

  compress_size_ = header & kLenMask;
  block += 4;
  written_ = 0;

  if (header & kSubstractBaseBit) {
    DCHECK_GT(isize, 12);
    base_ = LittleEndian::LoadT<T>(block);
    block += sizeof(T);
    compress_size_ -= sizeof(T);
  } else {
    base_ = 0;
  }
  bw_ = 1 + ((header >> kByteWidthBitStart) & 7);

  uint32 consumed = block - start;
  isize -= consumed;

  Result res;
  if (!is_raw_ && isize >= compress_size_) {
    Decompress<T>(block, compress_size_);
    res = Result{FINISHED, compress_size_};
  } else {
    start_ = false;
    if (!is_raw_) {
      compressed_buf_.reserve(compress_size_);
      compressed_buf_.clear();
    }
    res = DecodeBlock<T>(block, isize);
  }
  res.second += consumed;
  return res;
}

template<typename T> auto IntDecoder::DecodeBlock(const uint8* src, uint32_t isize) -> Result {
  uint32 to_read = std::min<uint32>(isize, compress_size_);

  Result res{NOT_FINISHED, to_read};

  if (is_raw_) {
    res.second = DecodeRaw<T>(src, to_read);
    compress_size_ -= res.second;

    if (compress_size_  == 0) {
      res.first = FINISHED;
      start_ = true;
    }
    return res;
  }

  compressed_buf_.insert(src, src + to_read);
  compress_size_ -= to_read;

  if (compress_size_ == 0) {
    Decompress<T>(compressed_buf_.data(), compressed_buf_.size());
    res.first = FINISHED;
    start_ = true;
  }

  return res;
}

template<typename T> uint32_t IntDecoder::DecodeRaw(const uint8* src, uint32_t isize) {
  DCHECK_EQ(1, is_raw_);
  uint32 cnt;
  constexpr size_t kValSize = sizeof(T);

  if (bw_ < kValSize || base_) {  // if we byte-packed the literal data.
    auto from = next_val_;
    cnt = isize / bw_;

    size_t mask = byte_mask(bw_);

    // go until we can load full 8 bytes.
    const uint8* copyend = src + isize - kValSize;
    while (src <= copyend) {
      *reinterpret_cast<T*>(next_val_) = base_ + (LittleEndian::LoadT<T>(src) & mask);
      next_val_ += kValSize;
      src += bw_;
    }

    copyend += (kValSize - bw_);

    while (src <= copyend) {
      T val = *src++;
      for (unsigned i = 1; i < bw_; ++i) {
        val |= T(*src) << (i * 8);
        ++src;
      }
      *reinterpret_cast<T*>(next_val_) = base_ + val;
      next_val_ += kValSize;
    }
    DCHECK_EQ(cnt * kValSize, next_val_ - from);
  } else {
    // just copy it.
    cnt = isize / kValSize;
    memcpy(next_val_, src, cnt * kValSize);

    next_val_ += (cnt*kValSize);
  }
  written_ += cnt;

  return cnt * bw_;
}


template<typename T>
void IntDecoder::Decompress(const uint8_t* src, uint32_t size) {
  constexpr size_t kValSize = sizeof(T);
  size_t res = ZSTD_decompress(dest_, max_dest_size_ * kValSize, src, size);

  CHECK(!ZSTD_isError(res));
  CHECK_GT(res, 0);
  CHECK_EQ(0, res % bw_);

  uint32 cnt = res / bw_;
  CHECK_LE(cnt, max_dest_size_);

  next_val_ = dest_ + cnt * kValSize;
  written_ += cnt;

  if (bw_ < kValSize) {  // realign
    uint8* start = dest_;
    T mask = byte_mask(bw_);

    for (int i = cnt - 1; i >=0; --i) {
      T val = LittleEndian::LoadT<T>(start + i * bw_) & mask;
      LittleEndian::StoreT<T>(base_ + val, start + i * kValSize);
    }
  } else if (base_) {
    T* end = reinterpret_cast<T*>(next_val_);
    for (T* ptr = reinterpret_cast<T*>(dest_); ptr != end; ++ptr) {
      *ptr += base_;
    }
  }
}

template IntDecoder::Result IntDecoder::DecodeT<uint64>(const void* src, uint32_t isize,
                                                        uint64* const dest);

template IntDecoder::Result IntDecoder::DecodeT<uint32>(const void* src, uint32_t isize,
                                                        uint32* const dest);

template uint32_t IntCoder::MaxComprSize<uint32>(uint32_t int_cnt);
template uint32_t IntCoder::MaxComprSize<uint64>(uint32_t int_cnt);
template uint32_t IntCoder::EncodeT<uint32>(const uint32* source, uint32_t isize, void* dest);
template uint32_t IntCoder::EncodeT<uint64>(const uint64* source, uint32_t isize, void* dest);

}  // namespace puma
