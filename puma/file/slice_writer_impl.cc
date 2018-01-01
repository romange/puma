// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/file/slice_writer_impl.h"

#define ZSTD_STATIC_LINKING_ONLY

#include <zstd.h>

#include "base/endian.h"
#include "base/logging.h"
#include "base/stl_util.h"

#include "file/file_util.h"

#include "puma/storage.pb.h"

#include "strings/strcat.h"

using base::StatusCode;
using strings::ByteRange;

using util::IntCoder;

namespace puma {

namespace {

// Used only when SliceWriterImpl is in framing mode.
const size_t kFrameMaxSize = (1UL << 20)* 4;   // 4MB.


}  // namespace

#define HANDLE reinterpret_cast<ZSTD_CStream*>(zstd_cstream_)

BlobSerializer::BlobSerializer(base::PODArray<uint8>* dest) : buf_ptr_(dest ? dest : &own_buf_) {
}

IntBlobSerializer::IntBlobSerializer(base::PODArray<uint8>* dest) : BlobSerializer(dest) {
  int_coder_.reset(new util::IntCoder(5));
}

IntBlobSerializer::~IntBlobSerializer() {}


ByteBlobSerializer::ByteBlobSerializer(base::PODArray<uint8>* dest) : BlobSerializer(dest) {
  zstd_cstream_ = ZSTD_createCStream();
}

ByteBlobSerializer::~ByteBlobSerializer() {
  if (HANDLE)
    ZSTD_freeCStream(HANDLE);
}

void ByteBlobSerializer::Add(const strings::Range<const uint8*>& range) {
  if (!stream_started_) {
    StartStream();
  }
  ZSTD_inBuffer input = { range.data(), range.size(), 0 };

  const size_t kOutSize = ZSTD_CStreamOutSize();
  buf_ptr_->reserve(buf_ptr_->size() + kOutSize);

  size_t available = buf_ptr_->capacity() - buf_ptr_->size();
  ZSTD_outBuffer out_buf{buf_ptr_->end(), available, 0};

  size_t cresult = ZSTD_compressStream(HANDLE, &out_buf , &input);
  CHECK(!ZSTD_isError(cresult));
  CHECK_EQ(input.pos, input.size);  //?

  buf_ptr_->resize_assume_reserved(buf_ptr_->size() + out_buf.pos);
}

void ByteBlobSerializer::StartStream() {
  ZSTD_initCStream(HANDLE, 2);
  stream_started_ = true;
}

void ByteBlobSerializer::EndStream() {
  if (!stream_started_)
    return;

  const size_t kOutSize = ZSTD_CStreamOutSize();

  buf_ptr_->reserve(buf_ptr_->size() + kOutSize);

  ZSTD_outBuffer out_buf{buf_ptr_->end(), kOutSize, 0};
  size_t res2 = ZSTD_endStream(HANDLE, &out_buf);
  CHECK(!ZSTD_isError(res2));
  CHECK_EQ(0, res2);
  buf_ptr_->resize_assume_reserved(buf_ptr_->size() + out_buf.pos);

  stream_started_ = false;
}

SliceWriterImpl::SliceWriterImpl(StringPiece filename, const FileHeader& fh, util::Sink* dest)
    : SliceWriterBase(fh.dt), sink_(dest), cc_(fh.cc)  {
  file_name_ = filename.as_string();
  frame_buf2_.reserve(1024);

  switch (fh.cc) {
    case FileHeader::ZSTD_CODEC:
      byte_serializer_.reset(new ByteBlobSerializer(&frame_buf2_));
    break;
    case FileHeader::LZ4INT_CODEC:
      int_serializer_.reset(new IntBlobSerializer(&frame_buf2_));
    break;
    default:
      LOG(FATAL) << "Not supported " << fh.cc;
  }
}

SliceWriterImpl::~SliceWriterImpl() {
}


void SliceWriterImpl::FlushFrame() {
  uint8 fh_buf[FrameHeader::SIZE];

  if (byte_serializer_) {
    byte_serializer_->EndStream();
  }
  size_t sz = frame_buf2_.size();
  FrameHeader frame_header(FrameHeader::DATA, sz);

  frame_header.Write(fh_buf);
  CHECK_STATUS(sink_->Append(ByteRange(fh_buf, FrameHeader::SIZE)));
  ByteRange br(frame_buf2_.begin(), frame_buf2_.end());

  VLOG(1) << "FlushFrame " << br.size() << " for " << file_name_;

  CHECK_STATUS(sink_->Append(br));
  ResetFrame();
}

// TODO: use  / ZSTD_compressContinue instead of stream.
// Use max-precompression size assumption like with integers.
void SliceWriterImpl::AddChunk(const strings::Range<const uint8*>& range) {
  if (range.empty())
    return;

  CHECK_EQ(FileHeader::ZSTD_CODEC, cc_);

  raw_size_ += range.size();

  VLOG(1) << "Adding " << range.size() << " bools to " << file_name_;

  frame_item_count_ += range.size();
  byte_serializer_->Add(range);

  if (frame_buf2_.size() >= kFrameMaxSize) {
    FlushFrame();
  }
}


template<typename T> void SliceWriterImpl::AddIntChunk(const T* ptr, size_t len) {
  CHECK_EQ(FileHeader::LZ4INT_CODEC, cc_);
  CHECK_LE(len, kMaxBatchSize);

  raw_size_ += len * sizeof(T);
  frame_item_count_ += len;

  int_serializer_->AddIntChunk(ptr, len);

  if (frame_buf2_.size() >= kFrameMaxSize) {
    FlushFrame();
  }
}

void SliceWriterImpl::AddChunk64(const strings::Range<const int64*>& range) {
  if (range.empty())
    return;
  CHECK(DataType::INT64 == dt() || DataType::DOUBLE == dt()) << dt() << ", " << file_name_;
  AddIntChunk(reinterpret_cast<const uint64*>(range.data()), range.size());
}

void SliceWriterImpl::AddChunk32(const strings::Range<const int32*>& range) {
  if (range.empty())
    return;

  AddIntChunk(reinterpret_cast<const uint32*>(range.data()), range.size());
}


void SliceWriterImpl::ResetFrame() {
  frame_buf2_.clear();
  frame_item_count_ = 0;
  raw_size_ = 0;
}

size_t SliceWriterImpl::FrameSize() const {
  return frame_buf2_.size();
}

void SliceWriterImpl::Finalize() {
  CHECK(sink_);

  if (frame_item_count_) {
    FlushFrame();
  }

  CHECK_STATUS(sink_->Flush());
  sink_.reset();
}

} // namespace puma
