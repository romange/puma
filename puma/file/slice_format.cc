// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/file/slice_format.h"

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

#include "base/endian.h"
#include "base/logging.h"
#include "base/stl_util.h"

#include "file/file.h"
#include "puma/file/int_coder.h"

namespace puma {

using util::StatusObject;
using util::StatusCode;
using strings::MutableByteRange;

constexpr uint32 kMagicNum = 12051977;

FileHeader FileHeader::FromDt(DataType dt) {
  FileHeader fh;
  fh.dt = dt;
  if (base::_in(dt, {DataType::UINT32, DataType::INT32, DataType::INT64, DataType::DOUBLE})) {
    fh.cc = FileHeader::LZ4INT_CODEC;
  } else {
    fh.cc = FileHeader::ZSTD_CODEC;
  }

  return fh;
}

void FileHeader::Write(uint8* dest) const {
  LittleEndian::Store32(dest, kMagicNum);
  dest[4] = uint8(dt);
  dest[5] = uint8(cc);
}

bool FileHeader::Parse(const uint8* src) {
  uint32 val = LittleEndian::Load32(src);
  if (val != kMagicNum)
    return false;
  dt = DataType(src[4]);
  cc = CompressionCodec(src[5]);
  return true;
}


void FrameHeader::Write(uint8* dest) const {
  LittleEndian::Store32(dest,  uint32(type) << 24 | kMagic1);
  LittleEndian::Store32(dest + 4, size);
}

bool FrameHeader::Parse(const uint8* src) {
  uint32 val = LittleEndian::Load32(src);
  if ((val & 0xFFFFFF) != kMagic1)
    return false;

  type = Type(val >> 24);
  size = LittleEndian::Load32(src + 4);
  return true;
}

FrameDecoder::FrameDecoder(uint32 block_size, util::Source* src)
  : frame_size_(0), src_(src) {
  compressed_buf_.reserve(block_size);
}

void FrameDecoder::FillCompressBuf(uint32 sz) {
  if (compressed_buf_.size() >= sz)
    return;

  DCHECK_LE(sz, compressed_buf_.capacity());

  while (frame_size_ == 0) {
    uint8 buf[FrameHeader::SIZE];
    auto res = src_->Read(MutableByteRange(buf, sizeof(buf)));
    CHECK_STATUS(res.status);
    if (res.obj == 0)
      return;
    CHECK_EQ(FrameHeader::SIZE, res.obj);

    FrameHeader fh;
    CHECK(fh.Parse(buf));
    CHECK_EQ(FrameHeader::DATA, fh.type);

    frame_size_ = fh.size;
    CHECK_GT(frame_size_, 0);
  }

  uint32 to_read = std::min<size_t>(sz - compressed_buf_.size(), frame_size_);
  CHECK_GT(to_read, 0);
  auto res = src_->Read(MutableByteRange(compressed_buf_.end(), to_read));
  CHECK_STATUS(res.status);

  compressed_buf_.resize_assume_reserved(compressed_buf_.size() + res.obj);
  frame_size_ -= res.obj;
}

FileLz4Decoder::FileLz4Decoder(size_t max_items_in_block, util::Source* src)
    : FrameDecoder(1 << 16, src),
      max_items_in_block_(max_items_in_block) {
  arr_.reset(new uint32[max_items_in_block_]);
}

FileLz4Decoder::~FileLz4Decoder() {}

StatusObject<size_t> FileLz4Decoder::Read(size_t max_size, uint32* dest) {
  if (int32_range_.empty()) {
    if (compressed_buf_.size() < 4)
      FillCompressBuf(512);
    if (compressed_buf_.empty())
      return 0;

    CHECK_GE(compressed_buf_.size(), 4);

    uint32 cs = IntDecoder::Peek(compressed_buf_.begin());
    if (cs > compressed_buf_.size()) {
      compressed_buf_.reserve(cs);

      FillCompressBuf(cs);
      CHECK_EQ(cs, compressed_buf_.size());  // unexpected eof file.
    }

    IntDecoder decoder(max_items_in_block_);

    IntDecoder::Result result =
        decoder.DecodeT(compressed_buf_.begin(), compressed_buf_.size(), arr_.get());
    CHECK_EQ(IntDecoder::FINISHED, result.first);
    CHECK_EQ(cs, result.second);
    int32_range_.reset(arr_.get(), decoder.written());

    if (cs < compressed_buf_.size()) {
      memmove(compressed_buf_.begin(), compressed_buf_.begin() + cs, compressed_buf_.size() - cs);
    }
    compressed_buf_.resize_assume_reserved(compressed_buf_.size() - cs);
  }

  uint32 to_copy = std::min<uint32>(int32_range_.size(), max_size);
  std::copy(int32_range_.begin(), int32_range_.begin() + to_copy, dest);
  int32_range_.advance(to_copy);
  return to_copy;
}


FileDecoder64::FileDecoder64(size_t max_items_in_block, util::Source* src)
    : FrameDecoder(512, src), max_items_in_block_(max_items_in_block) {
    arr_.reset(new uint64[max_items_in_block_]);
}

FileDecoder64::~FileDecoder64() {
}


StatusObject<size_t> FileDecoder64::Read(size_t max_size, uint64* dest) {
  if (int64_range_.empty()) {
    if (compressed_buf_.size() < 4)
      FillCompressBuf(512);
    if (compressed_buf_.empty())
      return 0;

    CHECK_GE(compressed_buf_.size(), 4);

    uint32 cs = IntDecoder::Peek(compressed_buf_.begin());
    if (cs > compressed_buf_.size()) {
      compressed_buf_.reserve(cs);

      FillCompressBuf(cs);
      CHECK_EQ(cs, compressed_buf_.size());  // unexpected eof file.
    }
    IntDecoder decoder(max_items_in_block_);

    IntDecoder::Result result = decoder.DecodeT(compressed_buf_.begin(), cs, arr_.get());
    CHECK_EQ(IntDecoder::FINISHED, result.first);
    CHECK_EQ(cs, result.second);

    int64_range_.reset(arr_.get(), decoder.written());

    if (cs < compressed_buf_.size()) {
      memmove(compressed_buf_.begin(), compressed_buf_.begin() + cs, compressed_buf_.size() - cs);
    }
    compressed_buf_.resize_assume_reserved(compressed_buf_.size() - cs);
  }

  uint32 to_copy = std::min<uint32>(int64_range_.size(), max_size);
  std::copy(int64_range_.begin(), int64_range_.begin() + to_copy, dest);
  int64_range_.advance(to_copy);
  return to_copy;
}


#define DC_HANDLE reinterpret_cast<ZSTD_DStream*>(zstd_handle_)

constexpr unsigned kZstdCompressSize = 4096;

ZstdBlobDecoder::ZstdBlobDecoder(util::Source* src) : FrameDecoder(kZstdCompressSize, src) {
  zstd_handle_ = ZSTD_createDStream();
  size_t const res = ZSTD_initDStream(DC_HANDLE);
  CHECK(!ZSTD_isError(res)) << ZSTD_getErrorName(res);
}

ZstdBlobDecoder::~ZstdBlobDecoder() {
  ZSTD_freeDStream(DC_HANDLE);
}

StatusObject<size_t> ZstdBlobDecoder::Read(size_t max_size, uint8* dest) {
  ZSTD_outBuffer output = { dest, max_size, 0 };

   do {
    if (compress_window_.empty()) {
      compressed_buf_.clear();
      FillCompressBuf(kZstdCompressSize);
      if (compressed_buf_.empty())
        break;

      compress_window_.assign(compressed_buf_.begin(), compressed_buf_.end());
    }

    ZSTD_inBuffer input{compress_window_.begin(), compress_window_.size(), 0 };

    size_t to_read = ZSTD_decompressStream(DC_HANDLE, &output , &input);
    CHECK(!ZSTD_isError(to_read));

    compress_window_.advance(input.pos);
    if (input.pos < input.size) {
      CHECK_EQ(output.pos, output.size);
      break;
    }
  } while (output.pos < output.size);

  return output.pos;
}

}  // namespace puma
