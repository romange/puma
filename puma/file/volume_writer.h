// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <mutex>

#include "base/pod_array.h"
#include "util/status.h"

#include "file/file.h"
#include "puma/data_types.h"
#include "puma/storage.pb.h"
#include "strings/stringpiece.h"

namespace puma {

class VolumeWriter {
 public:
  explicit VolumeWriter(const std::string& base_path);
  ~VolumeWriter();

  util::Status Open();

  util::Status StartFrame();
  util::Status FinalizeFrame();

  util::Status AppendSliceVec(uint32_t slice_id,
                              const strings::ByteRange src[], uint32_t elem_size);

  util::Status Finalize();

  size_t volume_size() const { return vol_size_; }

  util::Status AddDict(uint32_t slice_id, strings::ByteRange dict);
  void AssignDict(uint32_t slice_id, uint16_t frame_id);


  // Is called during the schema setup by message slicer.
  // Defines the order of slices in the volume.
  // Returns the slice id of the the added slice.
  unsigned AddSliceToSchema(const std::string& name, FieldClass fc, DataType dt, uint32 codec);

  const storage::SliceIndex& slice_index(unsigned slice_id) const { return slice_info_[slice_id];}

  uint16_t frame_id() const { return frame_id_; }
  size_t num_slices() const { return slice_info_.size(); }

private:
  util::Status WriteMagic(uint16_t val);

  storage::SliceIndex::Slice& last_slice(unsigned slice_id) {
    return *std::rbegin(*slice_info_[slice_id].mutable_slice());
  }

  using SliceIndexArray = std::vector<storage::SliceIndex>;

  std::string base_path_;
  SliceIndexArray slice_info_;

  std::unique_ptr<file::WriteFile> file_;

  size_t vol_size_ = 0;
  size_t shard_index_ = 0;
  std::mutex mu_;
  size_t frame_pos_ = 0;
  uint16_t frame_id_ = 0;
};

}  // namespace puma
