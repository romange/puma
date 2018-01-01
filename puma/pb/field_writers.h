// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "puma/data_types.h"
#include "puma/file/volume_format.h"  // for SliceCodecId
#include "puma/pb/base.h"
#include "puma/pb/int_compressor.h"
#include "util/coding/set_encoder.h"
#include "util/coding/double_compressor.h"

namespace puma {

class VolumeWriter;


namespace pb {

class SlicerState;
class FieldWriters;

namespace internal {

template <typename T> T ValueTransform(T t) {
  return t;
};

// Template based deployment mechanism to add to IntCompressor via uniform interface.
template<size_t INT_SIZE> struct Emplacer;

inline int64 ValueTransform(double d) {
  union {
    double val;
    int64 i;
  } u{d};

  return u.i;
};

inline int64 ValueTransform(float f) {
  double d = f;
  int64 res;
  memcpy(&res, reinterpret_cast<const char*>(&d), sizeof(double));
  return res;
};

class EncoderBase {
 public:
  ~EncoderBase() {}
  uint32_t Cost() const { return pending_size() + compressed_size(); }

  void ClearCompressedData() {
    compressed_blocks_.clear();
    compressed_bufs_.clear();
    compressed_size_ = 0;
  }
  const std::vector<strings::ByteRange>& compressed_blocks() { return compressed_blocks_; }

 protected:
  void AddBuffer(const uint8_t* ptr, uint32_t sz);

  virtual size_t pending_size() const = 0;
  size_t compressed_size() const { return compressed_size_; }

  std::vector<std::unique_ptr<uint8_t[]>> compressed_bufs_;
  std::vector<strings::ByteRange> compressed_blocks_;
  size_t compressed_size_ = 0;
};

class DoubleEncoder : public EncoderBase {
 public:
  DoubleEncoder() {}

  void Add(double v);
  void Flush();

  virtual size_t pending_size() const override { return pos_ * sizeof(double); }

 private:
  util::DoubleCompressor dc_;
  double buf_[util::DoubleCompressor::BLOCK_MAX_LEN];
  uint8_t bin_buf_[util::DoubleCompressor::COMMIT_MAX_SIZE];

  unsigned pos_ = 0;
};

class Int64Encoder : public EncoderBase {
 public:
  void Add(int64_t v);
  void Flush();

  virtual size_t pending_size() const override { return pos_ * sizeof(int64_t); }

 private:
  IntCompressorV2 ic_;
  int64_t buf_[IntCompressorV2::BLOCK_MAX_LEN];
  uint8_t bin_buf_[IntCompressorV2::COMMIT_MAX_SIZE];

  unsigned pos_ = 0;
};

union EncoderCompressorInfo {
  IntCompressor32* ic32;
  IntCompressor64* ic64;

  EncoderCompressorInfo() : ic32(nullptr) {}
};

template<> struct Emplacer<4> {
  template<typename T> static void Add(T t, EncoderCompressorInfo* dest) {
    dest->ic32->Add(t);
  }
};

template<> struct Emplacer<8> {
  template<typename T> static void Add(T t, EncoderCompressorInfo* dest) {
    dest->ic64->Add(t);
  }
};

}  // namespace internal

class FieldWriters {
 public:
  explicit FieldWriters(const AnnotatedFd& fd,
                        const int slice_index[FieldClass_ARRAYSIZE],
                        SliceCodecId codec_id,
                        SlicerState* ss = nullptr);
  FieldWriters(FieldWriters&&);

  ~FieldWriters();

  size_t OutputCost() const;
  const gpb::FieldDescriptor* fd() const { return fd_; }

  void SerializeSlices(VolumeWriter* vw);

  void AddLen(uint32_t cnt) { arr_len_writer_->Add<uint32_t>(cnt); }

  void AddStr(StringPiece str) {
    bin_data_->Add(str);
    str_len_writer_->Add<arrlen_t>(str.size() + 1);
  }

  void AddBool(bool b) { bin_data_->Add(b); }

  void AddDef(bool b) {
    opt_field_->Add(b);
  }

  void AddNullStr() {
    str_len_writer_->Add<arrlen_t>(kNullStrSz);
  }

  SlicerState* msg_state() { return msg_state_.get(); }

  void AddValI64(int64_t t) {
    if (int64_v2_) {
      int64_v2_->Add(t);
    } else {
      auto v = internal::ValueTransform(t);
      internal::Emplacer<sizeof(v)>::Add(v, &encoder_);
    }
  }

  template<typename T> void AddVal(T t) {
    auto v = internal::ValueTransform(t);
    internal::Emplacer<sizeof(v)>::Add(v, &encoder_);
  }

  void AddDouble(double v);

  bool is_optional() const { return is_optional_; }
  bool is_string() const { return  str_len_writer_.operator bool(); }

  template<typename It> void AddRepeated64(It start, size_t cnt) {
    if (int64_enc_) {
      tmp_.resize(cnt);
      for (size_t i = 0; i < cnt; ++i)
        tmp_[i] = *start++;
      int64_enc_->Add(reinterpret_cast<const uint64*>(tmp_.data()), cnt);
    } else {
      for (size_t i = 0; i < cnt; ++i) {
        AddVal(*start++);
      }
    }
  }

  template<typename It> void AddRepeated32(It start, size_t cnt) {
    if (int32_enc_) {
      tmp32_.resize(cnt);
      for (size_t i = 0; i < cnt; ++i)
        tmp32_[i] = *start++;
      int32_enc_->Add(tmp32_.data(), cnt);
    } else {
      for (size_t i = 0; i < cnt; ++i) {
        AddVal(*start++);
      }
    }
  }

 private:
  const gpb::FieldDescriptor* fd_;

  std::unique_ptr<IntCompressor32> arr_len_writer_, str_len_writer_;

  std::unique_ptr<BlockCompressor> bin_data_, opt_field_;
  std::unique_ptr<SlicerState> msg_state_;
  std::unique_ptr<util::SeqEncoder<8>> int64_enc_;
  std::unique_ptr<util::SeqEncoder<4>> int32_enc_;
  std::unique_ptr<internal::DoubleEncoder> denc_;
  std::unique_ptr<internal::Int64Encoder> int64_v2_;

  internal::EncoderCompressorInfo encoder_;
  bool is64_ = false;

  const int *slice_id_arr_;

  base::PODArray<int64> tmp_;
  base::PODArray<uint32> tmp32_;

  std::string last_dict_;
  uint16_t last_dict_frame_id_ = 0;
  bool is_optional_ = false;
};


}  // namespace pb
}  // namespace puma
