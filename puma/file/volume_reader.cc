// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/file/volume_reader.h"

#include <pmr/vector.h>

#include "base/endian.h"
#include "base/logging.h"
#include "base/stl_util.h"

using util::Status;
using util::StatusCode;
using strings::ByteRange;
using strings::MutableByteRange;
using namespace std;

namespace puma {
using storage::SliceIndex;

namespace {

inline Status ToStatus(string msg) {
  return Status(StatusCode::PARSE_ERROR, std::move(msg));
}


StatusObject<const uint8*> ReadFooter(file::ReadonlyFile* file, base::PODArray<uint8>* buf) {
  auto res = file->Read(file->Size() - buf->size(),
                        MutableByteRange(buf->data(), buf->size()));
  if (!res.ok())
    return res.status;

  CHECK_EQ(buf->size(), res.obj);

  uint32 offset = LittleEndian::Load32(buf->end() - 4);
  if (offset > buf->size() - 4) {
    base::PODArray<uint8> buf2(offset + 4, buf->mr());
    memcpy(end(buf2) - buf->size(), buf->begin(), buf->size());

    MutableByteRange mbr(buf2.begin(), buf2.size() - buf->size());

    GET_UNLESS_ERROR(read2, file->Read(file->Size() - offset - 4, mbr));

    CHECK_EQ(mbr.size(), read2);

    base::swap(buf2, *buf);
  }
  return buf->end() - 4 - offset;
}

}  // namespace


VolumeReader::VolumeReader() : mr_(pmr::get_default_resource()) {
  slice_map_.set_empty_key(StringPiece());
}

Status VolumeReader::Open(StringPiece vol_path) {
  file::ReadonlyFile::Options opts;
  opts.sequential = false;

  GET_UNLESS_ERROR(file, file::ReadonlyFile::Open(vol_path, opts));

  vol_file_.reset(file);

  size_t fl_size = file->Size();
  base::PODArray<uint8> buf(std::min<size_t>(fl_size, 1U << 16), pmr::get_default_resource());

  GET_UNLESS_ERROR(footer_start, ReadFooter(file, &buf));
  uint32 magic = LittleEndian::Load32(footer_start);
  if (magic != kVolumeFooterMagic)
    return ToStatus("Bad volume format");

  uint32 field_cnt = LittleEndian::Load32(footer_start + 4) & 0xFFFFFF;
  CHECK_GT(field_cnt, 0);

  pmr::vector<uint32> rec_data_len(field_cnt, 0, mr_);

  const uint8* next = footer_start + 7;
  for (unsigned i = 0; i < rec_data_len.size(); ++i) {
    uint32 bs = LittleEndian::Load32(next) & 0xFFFFFF; // It's offset of record i + 1.
    rec_data_len[i] = bs;
    next += 3;
  }

  sinfo_vec_.resize(field_cnt);
  uint32 agg_sz = 0;

  for (unsigned i = 0; i < field_cnt; ++i) {
    VLOG(1) << "SliceIndex " << i << " bytesize: " << rec_data_len[i];

    auto& sinfo = sinfo_vec_[i];
    CHECK(sinfo.ParseFromArray(next, rec_data_len[i])) << i << "/" << rec_data_len[i];
    next += rec_data_len[i];

    VLOG(1) << sinfo.ShortDebugString();

    agg_sz |= sinfo.slice_size();
  }
  CHECK_GT(agg_sz, 0);

  for (unsigned i = 0; i < field_cnt; ++i) {
    CHECK_EQ(agg_sz, sinfo_vec_[i].slice_size());
    slice_map_[sinfo_vec_[i].name()] = i;
  }
  frame_cnt_ = agg_sz;

  return Status::OK;
}

VolumeIterator* VolumeReader::GetSliceIterator(StringPiece name) const {
  auto it = slice_map_.find(name);
  if (it == end(slice_map_)) {
    return nullptr;
  }
  return new VolumeIterator(&sinfo_vec_[it->second], this);
}


VolumeIterator::VolumeIterator(const storage::SliceIndex* slice, const VolumeReader* vol_reader)
  : slice_index_(slice), vol_reader_(vol_reader), offset_(4) {
  const SliceIndex& si = *slice_index_;

  if (base::_in(si.data_type(), {DataType::BOOL, DataType::STRING})) {
    provider_.reset(new VolumeProviderBin(si.name(), si.data_type()));
  } else {
    if (si.codec_id() == kSeqEncoderCodec) {
      if (aux::DataTypeSize(si.data_type()) == 8)
        provider_ = std::make_unique<VolumeIntProvider<8>>(si.name(), si.data_type());
      else if (aux::DataTypeSize(si.data_type()) == 4)
        provider_ = std::make_unique<VolumeIntProvider<4>>(si.name(), si.data_type());
      else {
        LOG(FATAL) << "Unsupported data type: " << si.data_type();
      }
    } else {
      if (si.data_type() == DataType::DOUBLE && si.codec_id() == kDoubleEncoderCodec) {
        provider_.reset(new VolumeProviderDouble(si.name()));
      } else {
        provider_.reset(new VolumeProviderInt2(si.name(), si.data_type()));
      }
    }
  }

  dict_slice_offset_.set_empty_key(-1);

  for (int i = 0; i < si.slice_size(); ++i) {
    if (si.slice(i).has_dict_slice_id()) {
      dict_slice_offset_.emplace(si.slice(i).dict_slice_id(), 0);
    }
  }
}

void VolumeIterator::ReadFrame() {
  using Slice = SliceIndex::Slice;

  const Slice& slice = slice_index_->slice(frame_index_);
  VLOG(1) << "Reading slice " << frame_index_<< " <" <<slice.ShortDebugString() << ">";

  offset_ += slice.offset();

  if (slice.has_dict_len()) {
    dict_.resize(slice.dict_len());

    strings::MutableStringPiece mbr(&dict_.front(), dict_.size());

    VLOG(1) << "Reading dictionary of size " << dict_.size();

    auto res = vol_reader_->Read(offset_, mbr);
    CHECK_STATUS(res.status);

    auto it = dict_slice_offset_.find(frame_index_);
    if (it != dict_slice_offset_.end()) {
      it->second = offset_;  // Store the offset for subsequent usage.
    }
    offset_ += slice.dict_len();
    last_dict_frame_id_ = frame_index_;

  } else if (slice.has_dict_slice_id()) {
    unsigned dict_slice_id = slice.dict_slice_id();

    if (last_dict_frame_id_ != dict_slice_id) {
      size_t dict_len = slice_index_->slice(dict_slice_id).dict_len();
      DCHECK_GT(dict_len, 0);
      dict_.resize(dict_len);

      auto it = dict_slice_offset_.find(dict_slice_id);
      DCHECK(it != dict_slice_offset_.end());
      strings::MutableStringPiece mbr(&dict_.front(), dict_.size());

      auto res = vol_reader_->Read(it->second, mbr);
      CHECK_STATUS(res.status);
      last_dict_frame_id_ = dict_slice_id;
    }
  } else {
    dict_.clear();
  }

  slice_buf_.resize(slice.len());
  VLOG(1) << "Reading from offset " << offset_;

  strings::MutableByteRange mbr(slice_buf_.data(), slice_buf_.size());
  auto res2 = vol_reader_->Read(offset_, mbr);

  CHECK_STATUS(res2.status);
  CHECK_EQ(mbr.size(), res2.obj);
  offset_ += mbr.size();

  if (!dict_.empty()) {
    provider_->SetDict(StringPiece(dict_));
  }

  provider_->SetCurrentSlice(StringPiece(mbr));
}

bool VolumeIterator::NextFrame() {
  if (frame_index_ + 1 >= slice_index_->slice_size())
    return false;

  ++frame_index_;
  ReadFrame();

  return true;
}

}  // namespace puma
