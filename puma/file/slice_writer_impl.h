// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "puma/file/slice_writer.h"

#include "puma/file/slice_format.h"
#include "util/sinksource.h"
#include "util/coding/int_coder.h"


namespace puma {

class BlobSerializer {
 public:
  void clear() { buf_ptr_->clear(); }
  size_t size() const { return buf_ptr_->size(); }
  const uint8_t* data() const { return buf_ptr_->data(); }

 protected:
  BlobSerializer(base::PODArray<uint8>* dest);

  base::PODArray<uint8>* buf_ptr_;
  base::PODArray<uint8> own_buf_;
};

class IntBlobSerializer  : public BlobSerializer {
 public:
  explicit IntBlobSerializer(base::PODArray<uint8>* dest = nullptr);
  ~IntBlobSerializer();

  void Add64(const strings::Range<const uint64*>& r) {
    AddIntChunk(r.data(), r.size());
  }

  void Add32(const strings::Range<const uint32*>& r) {
    AddIntChunk(r.data(), r.size());
  }

  template<typename T> void AddIntChunk(const T* ptr, size_t len) {
    size_t capacity = buf_ptr_->size() + util::IntCoder::MaxComprSize<T>(len);
    buf_ptr_->reserve(capacity);
    size_t sz = int_coder_->EncodeT(ptr, len, buf_ptr_->end());
    buf_ptr_->resize_assume_reserved(buf_ptr_->size() + sz);
  }

 private:
  std::unique_ptr<util::IntCoder> int_coder_;

};

class ByteBlobSerializer : public BlobSerializer {
public:
  explicit ByteBlobSerializer(base::PODArray<uint8>* dest = nullptr);
  ~ByteBlobSerializer();

  void Add(const strings::Range<const uint8*>& vec);
  void EndStream();
 private:
  void StartStream();

  void* zstd_cstream_ = nullptr;

  bool stream_started_ = false;
};


class SliceWriterImpl : public SliceWriterBase {
  std::unique_ptr<util::Sink> sink_;
  FileHeader::CompressionCodec cc_;

  std::string file_name_;

  base::PODArray<uint8> frame_buf2_;

  size_t raw_size_ = 0;
  uint32 frame_item_count_ = 0;
 public:
  SliceWriterImpl(StringPiece filename, const FileHeader& fh, util::Sink* dest = nullptr);
  ~SliceWriterImpl();

  void AddChunk(const strings::Range<const uint8*>& vec) override;
  void AddChunk64(const strings::Range<const int64*>& vec) override;
  void AddChunk32(const strings::Range<const int32*>& vec) override;

  void ResetFrame();

  void Finalize() override;
  size_t FrameSize() const;

 private:
  void FlushFrame();
  template<typename T> void AddIntChunk(const T* ptr, size_t len);

  std::unique_ptr<IntBlobSerializer> int_serializer_;
  std::unique_ptr<ByteBlobSerializer> byte_serializer_;
};

}  // namespace puma
