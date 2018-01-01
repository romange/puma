// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/file/volume_provider.h"

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

#include "base/logging.h"

#include "puma/pb/int_compressor.h"
#include "puma/file/volume_format.h"
#include "util/coding/set_encoder.h"

namespace puma {

using strings::MutableByteRange;
using util::Status;
using util::StatusCode;
using std::string;
using strings::ByteRange;
using std::vector;
using util::SeqDecoder;

namespace {

inline Status ToStatus(string msg) {
  return Status(StatusCode::PARSE_ERROR, std::move(msg));
}

}  // namespace


Status VolumeProvider::ParseMagicIfNeeded(uint16_t expected) {
  if (!should_read_header_)
    return Status::OK;

  if (cur_slice_.size() < 2 || LittleEndian::Load16(cur_slice_.data()) != expected) {
    return ToStatus("Could not parse header");
  }
  should_read_header_ = false;
  cur_slice_.advance(2);
  return Status::OK;
}

VolumeProviderInt2::VolumeProviderInt2(std::string bare_name, DataType dt)
      : VolumeProvider(std::move(bare_name), dt) {
  unsigned dt_size = aux::DataTypeSize(dt);
  CHECK(dt_size == 4 || dt_size == 8);

  dt_size_log_ = dt_size == 8 ? 3 : 2;

  if (dt_size_log_ == 2) {
    decoder_.d32 = new IntDecompressor<4>;
    int_buf_.u32 = new uint32_t[IntDecompressor<4>::NUM_INTS_PER_BLOCK];
  }
  else {
    decoder_.d64 = new IntDecompressor<8>;
    int_buf_.u64 = new uint64_t[IntDecompressor<8>::NUM_INTS_PER_BLOCK];
  }
}

VolumeProviderInt2::~VolumeProviderInt2() {
  if (dt_size_log_ == 2) {
    delete decoder_.d32;
    delete[] int_buf_.u32;
  }
  else {
    delete decoder_.d64;
    delete[] int_buf_.u64;
  }
}

template<> IntDecompressor<4>* VolumeProviderInt2::get_decompressor<4>() {
  return decoder_.d32;
}

template<> IntDecompressor<8>* VolumeProviderInt2::get_decompressor<8>() {
  return decoder_.d64;
}


template<> uint32_t* VolumeProviderInt2::IntBuf::get_ptr<4>() {
  return u32;
}

template<> uint64_t* VolumeProviderInt2::IntBuf::get_ptr<8>() {
  return u64;
}



template<size_t INT_SIZE> Status VolumeProviderInt2::UncompressInts(bool is_signed) {
  IntDecompressor<INT_SIZE>* id = get_decompressor<INT_SIZE>();

  if (!decompressor_has_more_data_ && !cur_slice_.empty()) {
    RETURN_IF_ERROR(ParseMagicIfNeeded(kSliceMagic));
    uint32_t consumed = 0;

    int res = id->Decompress(cur_slice_, &consumed);
    CHECK_GE(res, 0) << "TBD";
    cur_slice_.advance(consumed);
  }
  auto* dest = int_buf_.get_ptr<INT_SIZE>();
  size_t sz = id->Next(dest);
  VLOG(1) << "Fetched " << sz << " items";

  if (is_signed) {
    using SIGNED_T = std::conditional_t<INT_SIZE == 4, int32_t, int64_t>;
    for (size_t i = 0; i < sz; ++i) {
      dest[i] = base::ZigZagDecode<SIGNED_T>(dest[i]);
    }
  }
  int_data_.reset(reinterpret_cast<uint8_t*>(dest), sz * INT_SIZE);
  decompressor_has_more_data_ = id->HasDecompressedData();

  return Status::OK;
}

Status VolumeProviderInt2::GetPage(uint32 max_size, SlicePtr dest, uint32* fetched_size) {
  if (int_data_.empty()) {
    if (dt_size_log_ == 2) {
      RETURN_IF_ERROR(UncompressInts<4>(dt() != DataType::UINT32));
    } else {
      RETURN_IF_ERROR(UncompressInts<8>(dt() != DataType::UINT64));
    }
  }

  if (!int_data_.empty()) {
    DCHECK_EQ(0, int_data_.size() & ((1 << dt_size_log_) - 1));

    size_t to_copy = std::min<uint32_t>(int_data_.size(), max_size  << dt_size_log_);
    memcpy(dest.u8ptr, int_data_.data(), to_copy);
    int_data_.advance(to_copy);

    *fetched_size = to_copy >> dt_size_log_;
  } else {
    *fetched_size = 0;
  }

  has_data_to_read_ = !cur_slice_.empty() || !int_data_.empty() || decompressor_has_more_data_;

  return Status::OK;
}


#define HANDLE ((ZSTD_DStream*)zstd_handle_)

VolumeProviderBin::VolumeProviderBin(std::string bare_name, DataType dt)
      : VolumeProvider(std::move(bare_name), dt) {
  zstd_handle_ = ZSTD_createDStream();

  size_t const res = ZSTD_initDStream(HANDLE);
  CHECK(!ZSTD_isError(res)) << ZSTD_getErrorName(res);
}

VolumeProviderBin::~VolumeProviderBin() {
  ZSTD_freeDStream(HANDLE);
}

Status VolumeProviderBin::GetPage(uint32 max_size, SlicePtr dest, uint32* fetched_size) {
  if (!cur_slice_.empty()) {
    VLOG(1) << "GetPage: " << cur_slice_.size();

    RETURN_IF_ERROR(ParseMagicIfNeeded(kSliceMagic));
  }

  if (!cur_slice_.empty()) {
    ZSTD_inBuffer input{cur_slice_.data(), cur_slice_.size(), 0};
    ZSTD_outBuffer output{dest.u8ptr, max_size, 0};

    size_t to_read = ZSTD_decompressStream(HANDLE, &output , &input);
    CHECK(!ZSTD_isError(to_read)) << ZSTD_getErrorName(to_read);

    *fetched_size = output.pos;
    cur_slice_.advance(input.pos);

    if (cur_slice_.empty()) {
      CHECK_EQ(0, to_read);
    }
  }
  has_data_to_read_ = !cur_slice_.empty();

  return Status::OK;
}

#undef HANDLE

template<size_t INT_SIZE> VolumeIntProvider<INT_SIZE>::VolumeIntProvider(
  std::string bare_name, DataType dt) :
  VolumeProvider(std::move(bare_name), dt) {
  seq_decoder_.reset(new SeqDecoder<INT_SIZE>);
}

template<size_t INT_SIZE> VolumeIntProvider<INT_SIZE>::~VolumeIntProvider() {
}

template<size_t INT_SIZE> void VolumeIntProvider<INT_SIZE>::SetDict(strings::ByteRange dict) {
  CHECK_GT(dict.size(), 2);
  uint16_t val = LittleEndian::Load16(dict.data());

  CHECK_EQ(val, kDictMagic);
  dict.advance(2);

  seq_decoder_->SetDict(dict.data(), dict.size());
}

template<size_t INT_SIZE> Status VolumeIntProvider<INT_SIZE>::GetPage(
    uint32 max_size, SlicePtr dest, uint32* fetched_size) {
  if (page_.empty()) {
    page_ = seq_decoder_->GetNextIntPage();

    if (page_.empty() && !cur_slice_.empty()) {
      RETURN_IF_ERROR(ParseMagicIfNeeded(kSliceMagic));

      uint32_t consumed = 0;
      int res = seq_decoder_->Decompress(cur_slice_, &consumed);
      CHECK_GE(res, 0);
      cur_slice_.advance(consumed);
      page_ = seq_decoder_->GetNextIntPage();
    }
  }

  if (!page_.empty()) {
    size_t to_copy = std::min<uint32_t>(page_.size(), max_size);
    memcpy(dest.u8ptr, page_.data(), to_copy * INT_SIZE);
    page_.advance(to_copy);

    *fetched_size = to_copy;
  } else {
    *fetched_size = 0;
    has_data_to_read_ = false;
  }

  return Status::OK;
}


VolumeProviderDouble::VolumeProviderDouble(std::string bare_name)
: VolumeProvider(std::move(bare_name), DataType::DOUBLE) {

}

VolumeProviderDouble::~VolumeProviderDouble() {

}

Status VolumeProviderDouble::GetPage(uint32 max_size, SlicePtr dest,
                                           uint32* fetched_size) {
  using util::DoubleDecompressor;

  while (window_.empty()) {
    if (cur_slice_.empty()) {
      *fetched_size = 0;
      has_data_to_read_ = false;

      return Status::OK;
    }

    RETURN_IF_ERROR(ParseMagicIfNeeded(kSliceMagic));
    if (cur_slice_.empty())
      continue;
    uint32 block_size;
    if (cur_slice_.size() < 3 ||
      (block_size = DoubleDecompressor::BlockSize(cur_slice_.data())) > cur_slice_.size()) {
      return ToStatus("Invalid header");
    }
    uint32_t written = dd_.Decompress(cur_slice_.data(), block_size, buf_);
    window_.reset(buf_, written);
    cur_slice_.advance(block_size);
  }

  size_t to_copy = std::min<uint32_t>(max_size, window_.size());
  std::copy(window_.begin(), window_.begin() + to_copy, dest.dptr);
  window_.advance(to_copy);
  *fetched_size = to_copy;
  has_data_to_read_ = !window_.empty() || !cur_slice_.empty();

  return Status::OK;
}

template class VolumeIntProvider<4>;
template class VolumeIntProvider<8>;

}  // namespace puma
