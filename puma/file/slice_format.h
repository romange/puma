// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <memory>
#include "puma/data_types.h"
#include "util/sinksource.h"


namespace util {

class IntDecoder;

}  // namespace util

namespace puma {

/*
| `Magic_Number` | `Type`   | `Codec`    | `Frame*`  |
|:--------------:|:--------:|:----------:|:--------------:|
| 4 bytes        |  1 byte  | 1 byte     |           |

Frame:
|  Type      |  `Size`  | `Data`   | `Footer Size`              |
|:----------:|:--------:|:--------:|:--------------------------:|
| 1 byte     | 4 bytes  | n bytes  |  4 bytes for footer only.  |

Type - can be either 'data' or 'footer'.

*/

constexpr unsigned kMaxBatchSize = 1024*64;

class FileHeader {
 public:
  enum CompressionCodec { NO_CODEC = 0, ZSTD_CODEC = 1, LZ4INT_CODEC = 2};

  static constexpr unsigned kMaxLen = 6;

  FileHeader() {}

  static FileHeader FromDt(DataType dt);

  DataType dt = DataType::NONE;
  CompressionCodec cc = NO_CODEC;

  // dest must be at least kMaxLen size.
  void Write(uint8* dest) const;


  bool Parse(const uint8* src);
 private:
};

class FrameHeader {
 public:

  enum : uint32 {
    kMagic1 = 0xb179a9, /* first 3 bytes of md5(roman). Used for forward parsing */
    kMagic2 = 0x461e7a06,  // 4 bytes of: echo 'Roman&Rivi' | sha1sum Used for backward parsing.
  };
  enum { SIZE = 8 };

  enum Type : uint8 {NONE = 0, DATA = 1} type = NONE;
  uint32 size = 0;

  void Write(uint8* dest) const;

  bool Parse(const uint8* src);

  FrameHeader(Type t = NONE, uint32 s = 0) : type(t), size(s) {}
};

class FrameDecoder {
 public:
  FrameDecoder(uint32 block_size, util::Source* src);

 protected:
  void FillCompressBuf(uint32 sz);

  uint32 frame_size_;

  base::PODArray<uint8> compressed_buf_;
  util::Source* src_;
};

class FileLz4Decoder : public FrameDecoder {
  std::unique_ptr<uint32[]> arr_;
  strings::Range<const uint32*> int32_range_;

 public:
  // block_size - how much to read from src.
  // max_ints_in_block - maximum number of ints that could reside in a single compressed block.
  // Does not take ownership over src.
  FileLz4Decoder(size_t max_ints_in_block, util::Source* src);
  ~FileLz4Decoder();

  // Returns 0 on eof.
  util::StatusObject<size_t> Read(size_t max_size, uint32* dest);

 private:
  uint32 max_items_in_block_;
};


class FileDecoder64 : public FrameDecoder {
  std::unique_ptr<uint64[]> arr_;

  strings::Range<const uint64*> int64_range_;
  public:
  // max_ints_in_block - maximum number of ints that could reside in a single compressed block.
  // Does not take ownership over src.
  FileDecoder64(size_t max_ints_in_block, util::Source* src);
  ~FileDecoder64();

  // returns 0 if eof reached.
  util::StatusObject<size_t> Read(size_t max_size, uint64* dest);

 private:
  uint32 max_items_in_block_;
};

class ZstdBlobDecoder : public FrameDecoder {
 public:
  explicit ZstdBlobDecoder(util::Source* src);
  ~ZstdBlobDecoder();

  util::StatusObject<size_t> Read(size_t max_size, uint8* dest);

 private:
  void* zstd_handle_;
  strings::ByteRange compress_window_;
};


#if 0
class SliceDecoderBase {
 public:
  void Init(const strings::ByteRange& br) {
    br_ = br; is_init_ = true;
  }

 protected:
  strings::ByteRange br_;
  bool is_init_ = false;
};


class SliceDecoder64 : public SliceDecoderBase {
 public:
  SliceDecoder64(size_t max_items_in_block);
  ~SliceDecoder64();

  base::StatusObject<size_t> Decode(size_t max_size, uint64* dest);

 protected:
  std::unique_ptr<util::IntDecoder> decoder_;

  std::unique_ptr<uint64[]> arr_;
  strings::Range<const uint64*> int64_range_;

};

#endif

}  // namespace puma
