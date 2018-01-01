// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "puma/file/bare_slice_provider.h"
#include "util/coding/double_compressor.h"


namespace util {

template<size_t INT_SIZE> class SeqDecoder;

}  // namespace util

namespace puma {

template <size_t INT_SIZE> class IntDecompressor;

class VolumeProvider : public BareSliceProvider {
 public:
  VolumeProvider(std::string bare_name, DataType dt)
      : BareSliceProvider(std::move(bare_name), dt) {
  }

  void SetCurrentSlice(StringPiece slice) {
    cur_slice_ = slice;
    should_read_header_ = true;
    has_data_to_read_ = true;
  }

  // TBD: to fix the interface to allow multiple dictionaries per slice.
  virtual void SetDict(strings::ByteRange dict) {}

 protected:
  util::Status ParseMagicIfNeeded(uint16_t val);

  strings::ByteRange cur_slice_;

  bool should_read_header_ = false;
  uint16_t magic_val_;
};


class VolumeProviderInt2 : public VolumeProvider {
 public:
  VolumeProviderInt2(std::string bare_name, DataType dt);

  ~VolumeProviderInt2();

  util::Status GetPage(uint32 max_size, SlicePtr dest, uint32* fetched_size) override;

 protected:

  union Decoder {
    IntDecompressor<4>* d32;
    IntDecompressor<8>* d64;
  } decoder_;

  union IntBuf {
    uint32_t* u32;
    uint64_t* u64;

    template<size_t INT_SIZE> typename IntDecompressor<INT_SIZE>::UT*
        get_ptr();
  } int_buf_;



  template<size_t INT_SIZE> IntDecompressor<INT_SIZE>* get_decompressor();

  strings::ByteRange int_data_;
  unsigned dt_size_log_;
  bool decompressor_has_more_data_ = false;

  template<size_t INT_SIZE> util::Status UncompressInts(bool is_signed);
};


class VolumeProviderDouble : public VolumeProvider {
 public:
  VolumeProviderDouble(std::string bare_name);

  ~VolumeProviderDouble();

  util::Status GetPage(uint32 max_size, SlicePtr dest, uint32* fetched_size) override;

 protected:
  util::DoubleDecompressor dd_;
  double buf_[util::DoubleDecompressor::BLOCK_MAX_LEN];
  strings::Range<double*> window_;
};

class VolumeProviderBin : public VolumeProvider {
 public:
  VolumeProviderBin(std::string bare_name, DataType dt);

  ~VolumeProviderBin();

 protected:
  util::Status GetPage(uint32 max_size, SlicePtr dest, uint32* fetched_size) override;

  void /*ZSTD_DStream*/* zstd_handle_;
};


template<size_t INT_SIZE> class VolumeIntProvider : public VolumeProvider {
  static_assert(INT_SIZE == 4 || INT_SIZE == 8, "");
  using UT = std::conditional_t<INT_SIZE == 4, uint32_t, uint64_t>;

 public:
  VolumeIntProvider(std::string bare_name, DataType dt);

  ~VolumeIntProvider();

  void SetDict(strings::ByteRange dict) override;
 protected:
  util::Status GetPage(uint32 max_size, SlicePtr dest, uint32* fetched_size) override;

  typedef util::SeqDecoder<INT_SIZE> Decoder;

  std::unique_ptr<Decoder> seq_decoder_;

  strings::Range<UT*> page_;
};

}  // namespace puma
