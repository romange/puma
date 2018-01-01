// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/pb/field_writers.h"

#include "puma/file/volume_writer.h"
#include "puma/pb/message_slicer.h"
#include "puma/pb/util.h"

DEFINE_bool(set_enc_disable_seq, false, "");

using std::string;

namespace puma {
namespace pb {

using FD = gpb::FieldDescriptor;

namespace {

void Serialize(unsigned slice_index, BlockCompressor* bc, VolumeWriter* vw) {
  bc->Finalize();

  const std::vector<strings::ByteRange>& compressed_blocks =
      bc->compressed_blocks();

  CHECK_STATUS(vw->AppendSliceVec(slice_index, compressed_blocks.data(), compressed_blocks.size()));
  bc->ClearCompressedData();
}

void Serialize(unsigned slice_index, IntCompressor32* ic, VolumeWriter* vw) {
  VLOG(1) << "Serializing " << vw->slice_index(slice_index).name();
  ic->Finalize();

  const std::vector<strings::ByteRange>& compressed_blocks = ic->compressed_blocks();

  CHECK_STATUS(vw->AppendSliceVec(slice_index, compressed_blocks.data(), compressed_blocks.size()));
  ic->ClearCompressedData();
}

}  // namespace

namespace internal {

void EncoderBase::AddBuffer(const uint8_t* ptr, uint32_t sz) {
  std::unique_ptr<uint8_t[]> b(new uint8_t[sz]);
  memcpy(b.get(), ptr, sz);

  compressed_blocks_.emplace_back(b.get(), sz);
  compressed_bufs_.push_back(std::move(b));
  compressed_size_ += sz;
}


void DoubleEncoder::Add(double v) {
  buf_[pos_++] = v;
  if (pos_ == util::DoubleCompressor::BLOCK_MAX_LEN) {
    uint32_t sz = dc_.Commit(buf_, pos_, bin_buf_);
    AddBuffer(bin_buf_, sz);
    pos_ = 0;
  }
}

void DoubleEncoder::Flush() {
  if (!pos_)
    return;
  uint32_t sz = dc_.Commit(buf_, pos_, bin_buf_);
  AddBuffer(bin_buf_, sz);
}

void Int64Encoder::Add(int64_t v) {
  buf_[pos_++] = v;
  if (pos_ == IntCompressorV2::BLOCK_MAX_LEN) {
    uint32_t sz = ic_.Commit(buf_, pos_, bin_buf_);
    AddBuffer(bin_buf_, sz);
    pos_ = 0;
  }
}

void Int64Encoder::Flush() {
  if (!pos_)
    return;
  uint32_t sz = ic_.Commit(buf_, pos_, bin_buf_);
  AddBuffer(bin_buf_, sz);
}

}  // namespace internal

FieldWriters::FieldWriters(const AnnotatedFd& annotated_fd,
                           const int slice_id[FieldClass_ARRAYSIZE], SliceCodecId codec_id,
                           SlicerState* msg_state)
  : fd_(annotated_fd.get()), msg_state_(msg_state), slice_id_arr_(slice_id) {
  DataType dt = FromPBType(annotated_fd->cpp_type());

  if (annotated_fd->is_repeated()) {
    arr_len_writer_.reset(new IntCompressor32);
  }

  if (annotated_fd->is_optional()) {
    is_optional_ = true;
    if (dt != DataType::STRING)
      opt_field_.reset(new BlockCompressor);
  }

  if (msg_state) {
    CHECK_EQ(FD::CPPTYPE_MESSAGE, annotated_fd->cpp_type());
    return;
  }


  if (dt == DataType::BOOL || dt == DataType::STRING) {
    bin_data_.reset(new BlockCompressor);
    if (dt == DataType::STRING) {
      str_len_writer_.reset(new IntCompressor32());
    }
  } else {
    switch (dt) {
      case DataType::INT32:
      case DataType::UINT32:
        if (codec_id == kSeqEncoderCodec) {
          int32_enc_.reset(new util::SeqEncoder<4>);

          if (FLAGS_set_enc_disable_seq)
            int32_enc_->DisableSeqDictionary();
        } else {
          encoder_.ic32 = new IntCompressor32;
          is64_ = false;
        }
      break;
      case DataType::INT64:
      case DataType::UINT64:
        if (codec_id == kSeqEncoderCodec) {
          int64_enc_.reset(new util::SeqEncoder<8>);

          if (FLAGS_set_enc_disable_seq)
            int64_enc_->DisableSeqDictionary();
        } else {
          if (codec_id == kInt64V2Codec) {
            int64_v2_.reset(new internal::Int64Encoder);
          } else {
            encoder_.ic64 = new IntCompressor64;
            is64_ = true;
          }
        }
      break;
      case DataType::DOUBLE:
        if (codec_id == kDoubleEncoderCodec) {
          denc_.reset(new internal::DoubleEncoder);
        } else {
          encoder_.ic64 = new IntCompressor64;
          is64_ = true;
        }
      break;
      default:
        LOG(FATAL) << "Not supported " << dt;
    }
  }
}

FieldWriters::FieldWriters(FieldWriters&& fw)
  : fd_(fw.fd_), arr_len_writer_(std::move(fw.arr_len_writer_)),
    str_len_writer_(std::move(fw.str_len_writer_)),
    bin_data_(std::move(fw.bin_data_)),
    opt_field_(std::move(fw.opt_field_)),
    msg_state_(std::move(fw.msg_state_)),
    int64_enc_(std::move(fw.int64_enc_)),
    int32_enc_(std::move(fw.int32_enc_)),
    denc_(std::move(fw.denc_)),
    int64_v2_(std::move(fw.int64_v2_)),
    is64_(fw.is64_), slice_id_arr_(fw.slice_id_arr_), is_optional_(fw.is_optional_) {
  encoder_.ic32 = fw.encoder_.ic32;
  fw.encoder_.ic32 = nullptr;
}

FieldWriters::~FieldWriters() {
  if (is64_)
    delete encoder_.ic64;
  else
    delete encoder_.ic32;
}

size_t FieldWriters::OutputCost() const {
  size_t cost = 2;  // For magic val.

  if (arr_len_writer_) {
    cost += arr_len_writer_->pending_size() + arr_len_writer_->compressed_size();
  } else if (opt_field_) {
    cost += opt_field_->compressed_size() + opt_field_->pending_size() / 8;
  }

  if (msg_state_) {
    return cost + msg_state_->OutputCost();
  }

  if (str_len_writer_) {
    cost += str_len_writer_->pending_size() + str_len_writer_->compressed_size();
  }

  if (bin_data_) {
    cost += bin_data_->compressed_size() + bin_data_->pending_size();
  } else if (int64_enc_) {
    cost += int64_enc_->Cost();
  } else if (int32_enc_) {
    cost += int32_enc_->Cost();
  } else if (denc_) {
    cost += denc_->Cost();
  } else if (int64_v2_) {
    cost += int64_v2_->Cost();
  } else {
    CHECK(encoder_.ic32);
    if (is64_) {
      cost += encoder_.ic64->compressed_size() + encoder_.ic64->pending_size();
    } else {
      cost += encoder_.ic32->compressed_size() + encoder_.ic32->pending_size();
    }
  }

  return cost;
}

void FieldWriters::AddDouble(double v) {
  if (denc_) {
    denc_->Add(v);
  } else {
    auto v2 = internal::ValueTransform(v);
    internal::Emplacer<sizeof(v)>::Add(v2, &encoder_);
  }
}

#define SI(X) (slice_id_arr_[FieldClass::X])

void FieldWriters::SerializeSlices(VolumeWriter* vw) {
  // Meta part.
  if (arr_len_writer_) {
    Serialize(SI(ARRAY_SIZE), arr_len_writer_.get(), vw);
  } else if (opt_field_) {
    Serialize(SI(OPTIONAL), opt_field_.get(), vw);
  }

  if (msg_state_) {
    msg_state_->SerializeSlices(vw);
    return;
  }

  if (str_len_writer_) {
    Serialize(SI(STRING_LEN), str_len_writer_.get(), vw);
  }

  int data_slice = SI(DATA);

  // Data part.
  if (bin_data_) {
    Serialize(data_slice, bin_data_.get(), vw);
  } else if (int64_enc_) {
    int64_enc_->Flush();
    string cur_dict;
    bool has_dict = int64_enc_->GetDictSerialized(&cur_dict);
    if (has_dict) {
      if (cur_dict == last_dict_) {
        vw->AssignDict(data_slice, last_dict_frame_id_);
      } else {
        VLOG(1) << "Adding dictionary to slice " << data_slice << " "
                << vw->slice_index(data_slice).name();

        CHECK_STATUS(vw->AddDict(SI(DATA), StringPiece(cur_dict)));
        last_dict_frame_id_ = vw->frame_id();
        last_dict_.swap(cur_dict);
      }
    }
    const std::vector<strings::ByteRange>& slice_vec = int64_enc_->compressed_blocks();
    CHECK_STATUS(vw->AppendSliceVec(SI(DATA), slice_vec.data(), slice_vec.size()));
    int64_enc_->ClearCompressedData();
  } else if (int32_enc_) {
    int32_enc_->Flush();
    string cur_dict;
    bool has_dict = int32_enc_->GetDictSerialized(&cur_dict);
    if (has_dict) {
      if (cur_dict == last_dict_) {
        vw->AssignDict(data_slice, last_dict_frame_id_);
      } else {
        VLOG(1) << "Adding dictionary to slice " << data_slice << " "
                << vw->slice_index(data_slice).name();

        CHECK_STATUS(vw->AddDict(SI(DATA), StringPiece(cur_dict)));
        last_dict_frame_id_ = vw->frame_id();
        last_dict_.swap(cur_dict);
      }
    }
    const std::vector<strings::ByteRange>& slice_vec = int32_enc_->compressed_blocks();
    CHECK_STATUS(vw->AppendSliceVec(SI(DATA), slice_vec.data(), slice_vec.size()));
    int32_enc_->ClearCompressedData();
  } else if (denc_) {
    denc_->Flush();
    const std::vector<strings::ByteRange>& slice_vec = denc_->compressed_blocks();
    CHECK_STATUS(vw->AppendSliceVec(SI(DATA), slice_vec.data(), slice_vec.size()));
    denc_->ClearCompressedData();
  } else if (int64_v2_) {
    int64_v2_->Flush();
    const std::vector<strings::ByteRange>& slice_vec = int64_v2_->compressed_blocks();
    CHECK_STATUS(vw->AppendSliceVec(SI(DATA), slice_vec.data(), slice_vec.size()));
    int64_v2_->ClearCompressedData();
  } else {
    CHECK(encoder_.ic32);
    const std::vector<strings::ByteRange>* slice_vec = nullptr;
    if (is64_) {
      encoder_.ic64->Finalize();
      slice_vec = &encoder_.ic64->compressed_blocks();
    } else {
      encoder_.ic32->Finalize();
      slice_vec = &encoder_.ic32->compressed_blocks();
    }
    CHECK_STATUS(vw->AppendSliceVec(SI(DATA), slice_vec->data(), slice_vec->size()));

    if (is64_) {
      encoder_.ic64->ClearCompressedData();
    } else {
      encoder_.ic32->ClearCompressedData();
    }

  }
}

#undef SI


}  // namespace pb
}  // namespace puma
