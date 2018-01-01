// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <mutex>

#include "base/pod_array.h"
#include "base/pmr.h"

#include "strings/hash.h"
#include "file/file.h"

#include "puma/data_types.h"
#include "puma/file/volume_format.h"
#include "puma/file/volume_provider.h"

using util::Status;
using util::StatusObject;

namespace puma {

class VolumeIterator;

class VolumeReader {
 public:
  using SliceIndexArray = std::vector<storage::SliceIndex>;

  VolumeReader();

  Status Open(StringPiece vol_path);
  const SliceIndexArray& sinfo_array() const { return sinfo_vec_; }

  unsigned frame_count() const { return frame_cnt_; }

  StatusObject<size_t> Read(size_t offset, const strings::MutableByteRange& dest) const {
    return vol_file_->Read(offset, dest);
  }

  // Returns nullptr if name is not found.
  VolumeIterator* GetSliceIterator(StringPiece name) const;

 private:
  pmr::memory_resource* mr_;
  std::unique_ptr<file::ReadonlyFile> vol_file_;

  ::base::pmr_dense_hash_map<StringPiece, unsigned> slice_map_;

  SliceIndexArray sinfo_vec_;
  unsigned frame_cnt_ = 0;
};


class VolumeIterator {
  friend class VolumeReader;
 public:
  VolumeIterator(const storage::SliceIndex* si, const VolumeReader* vol_reader);

  Status GetPage(uint32 max_size, SlicePtr dest, uint32* fetched_size) {
    return provider_->GetPage(max_size, dest, fetched_size);
  }

  VolumeProvider* provider() { return provider_.get(); }
  const storage::SliceIndex& slice_index() const { return *slice_index_; }

  bool NextFrame();
  bool has_data_to_read() const { return provider_->has_data_to_read(); }
 private:
  void ReadFrame();

  const storage::SliceIndex* slice_index_;
  const VolumeReader* vol_reader_;

  std::unique_ptr<VolumeProvider> provider_;

  base::PODArray<uint8> slice_buf_;
  std::string dict_;
  unsigned last_dict_frame_id_ = 0;
  int frame_index_ = -1;
  size_t offset_;
  ::google::dense_hash_map<int, size_t> dict_slice_offset_;
};

}  // namespace puma
