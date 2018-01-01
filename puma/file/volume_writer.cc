// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/file/volume_writer.h"

#include "base/endian.h"
#include "base/logging.h"

#include "puma/file/volume_format.h"
#include "puma/pb/util.h"
#include "strings/stringprintf.h"
#include "strings/strcat.h"

namespace puma {

using util::Status;
using util::StatusCode;
using strings::ByteRange;
using strings::MutableByteRange;
using namespace std;
using storage::SliceIndex;

namespace {

string Shard(StringPiece name, unsigned shard) {
  return StrCat(name, StringPrintf("-%04d.vol", shard));
}

}  // namespace

VolumeWriter::VolumeWriter(const std::string& base_path)
  : base_path_(base_path) {
}

VolumeWriter::~VolumeWriter() {
}


Status VolumeWriter::Open() {
  CHECK(!file_);

  string name = Shard(base_path_, shard_index_);
  file_.reset(file::Open(name));
  CHECK(file_) << name;

  uint8 buf[4];
  LittleEndian::Store32(buf, kVolumeHeaderMagic);

  RETURN_IF_ERROR(file_->Write(buf, 4));
  vol_size_ = 4;

  return Status::OK;
}

unsigned VolumeWriter::AddSliceToSchema(const std::string& name, FieldClass fc, DataType dt,
                                        uint32_t codec_id) {
  unsigned index = slice_info_.size();

  slice_info_.emplace_back();

  storage::SliceIndex& last = slice_info_.back();
  last.set_name(pb::FieldClassPath(name, fc));
  last.set_data_type(dt);
  last.set_fc(fc);
  if (codec_id)
    last.set_codec_id(codec_id);
  VLOG(1) << "Adding to schema: " << last.ShortDebugString();

  return index;
}

Status VolumeWriter::StartFrame() {
  CHECK(!slice_info_.empty());

  VLOG(1) << "Starting frame " << frame_id_ << " at " << vol_size_;

  mu_.lock();
  for (auto& si : slice_info_)
    si.add_slice();

  frame_pos_ = vol_size_;

  return Status::OK;
}

Status VolumeWriter::FinalizeFrame() {
  VLOG(1) << "Frame " << frame_id_ << " took " << vol_size_ - frame_pos_ << " bytes, pos "
          << vol_size_;

  ++frame_id_;
  mu_.unlock();

  return Status::OK;
}

Status VolumeWriter::WriteMagic(uint16_t val) {
  uint8_t buf[2];
  LittleEndian::Store16(buf, val);

  StringPiece str(buf, 2);
  return file_->Write(str);
}

Status VolumeWriter::AppendSliceVec(unsigned slice_index,
                                          const ByteRange src[], uint32_t elem_size) {
  CHECK_LT(slice_index, slice_info_.size());

  size_t total_size = 0;
  for (unsigned i = 0; i < elem_size; ++i) {
    CHECK(!src[i].empty());
    total_size += src[i].size();
  }
  VLOG(2) << "Appending slice vector of size " << total_size;
  RETURN_IF_ERROR(WriteMagic(kSliceMagic));

  for (unsigned i = 0; i < elem_size; ++i) {
    RETURN_IF_ERROR(file_->Write(StringPiece(src[i])));
  }

  size_t block_size = 2 + total_size;
  SliceIndex::Slice& cur_frame = last_slice(slice_index);

  cur_frame.set_len(block_size);
  vol_size_ += block_size;

  return Status::OK;
}


Status VolumeWriter::AddDict(uint32_t slice_index, strings::ByteRange dict) {
  RETURN_IF_ERROR(WriteMagic(kDictMagic));
  RETURN_IF_ERROR(file_->Write(StringPiece(dict)));

  VLOG(2) << "Appending Dictionary of size " << dict.size();

  auto& si = slice_info_[slice_index];

  CHECK_GT(si.slice_size(), 0);
  SliceIndex::Slice& cur_slice = last_slice(slice_index);
  cur_slice.set_dict_len(dict.size() + 2);  // + Magic string.

  return Status::OK;
}

void VolumeWriter::AssignDict(uint32_t slice_index, uint16_t frame_id) {
  auto& si = slice_info_[slice_index];
  CHECK_GT(si.slice_size(), 0);

  SliceIndex::Slice& cur_slice = last_slice(slice_index);

  cur_slice.set_dict_slice_id(frame_id);
}

// See volume_format.h for footer format explanation.
Status VolumeWriter::Finalize() {
  // Sanity check.
  CHECK(slice_info_.size() > 0 && slice_info_.size() < (1 << 20));

  // Compute slice offsets for lens.
  size_t position = 0;
  std::vector<size_t> prev_pos(slice_info_.size());
  size_t frame_num = slice_info_.front().slice_size();

  for (size_t frame_index = 0; frame_index < frame_num; ++frame_index) {
    for (size_t j = 0; j < slice_info_.size(); ++j) {
      SliceIndex& si = slice_info_[j];
      SliceIndex::Slice* slice = si.mutable_slice(frame_index);

      slice->set_offset(position - prev_pos[j]);

      position += slice->len();
      if (slice->has_dict_len())
        position += slice->dict_len();
      prev_pos[j] = position;
    }
  }

  base::PODArray<uint8> buf(4 + 3 + slice_info_.size() * 5  + 4, pmr::get_default_resource());

  LittleEndian::Store32(buf.data(), kVolumeFooterMagic);
  LittleEndian::Store32(buf.data() + 4, slice_info_.size());   // number of unique slices.

  uint8* next = buf.data() + 7;  // I ignore 4'th most significant byte.

  size_t total_sz = 0;
  for (size_t j = 0; j < slice_info_.size(); ++j) {
    const storage::SliceIndex& si = slice_info_[j];
    int bs = si.ByteSize();
    CHECK(bs > 0 && bs < (1 << 24)) <<bs;
    CHECK(si.IsInitialized()) << si.ShortDebugString();
    VLOG(2) << "Adding slice " << j << ": " << si.ShortDebugString();
    LittleEndian::Store32(next, bs);

    next += 3;
    total_sz += unsigned(bs);
  }
  size_t cur_sz = next - buf.data();
  CHECK_LT(cur_sz, buf.size());

  buf.resize(cur_sz + total_sz + 4);  // +4 bytes for the offset to kMagic2 position.
  next = buf.data() + cur_sz;

  // Serialize finally.
  for (size_t j = 0; j < slice_info_.size(); ++j) {
    const storage::SliceIndex& si = slice_info_[j];
    next = si.SerializeWithCachedSizesToArray(next);
  }
  CHECK_EQ(4, end(buf) - next);

  LittleEndian::Store32(next, buf.size() - 4);
  vol_size_ += buf.size();

  RETURN_IF_ERROR(file_->Write(buf.data(), buf.size()));
  file_.reset();

  return Status::OK;
}


}  // namespace puma
