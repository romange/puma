// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/file/volume_manager.h"

#include <pmr/vector.h>

#include "base/endian.h"
#include "base/logging.h"
#include "base/pod_array.h"

#include "file/file.h"
#include "file/file_util.h"

#include "strings/strcat.h"
#include "strings/unique_strings.h"
#include "puma/file/slice_format.h"
#include "puma/file/volume_reader.h"
#include "puma/file/volume_provider.h"

namespace puma {
using strings::MutableByteRange;
using util::Status;
using util::StatusCode;
using std::string;
using util::IntDecoder;
using std::vector;
using std::unique_ptr;

namespace {

inline Status ToStatus(string msg) {
  return Status(StatusCode::PARSE_ERROR, std::move(msg));
}

}  // namespace


struct VolumeManager::Rep {
  unique_ptr<VolumeReader> vol_reader;
  base::PODArray<uint8> slice_buffer;


  struct StageData {
    pmr::vector<unique_ptr<VolumeIterator>> iterators;
    int32 cur_frame_index = -1;

    bool has_data() const {
      for (const auto& vit : iterators) {
        if (vit->provider()->has_data_to_read()) {
          return true;
        }
      }
      return false;
    }
    bool empty() const { return iterators.empty(); }
  };

  StageData stages_[2];

  Rep(pmr::memory_resource* mr) : slice_buffer(mr) {
    vol_reader.reset(new VolumeReader);
  }
};

VolumeManager::VolumeManager(pmr::memory_resource* mr) : mr_(mr) {
  CHECK(mr_);
  rep_.reset(new Rep(mr));
}


VolumeManager::~VolumeManager() {}


// TODO: to formalize the order of fields inside the table index instead of
// loading it from volume. Also we will need to support multiple volumes with the same
// schema.
Status VolumeManager::Open(StringPiece base_path) {
  vector<string> volumes = file_util::ExpandFiles(StrCat(base_path, "*.vol"));
  CHECK_EQ(1, volumes.size()) << "TBD";
  VLOG(1) << "Opening " << volumes.front();

  RETURN_IF_ERROR(rep_->vol_reader->Open(volumes.front()));

  return Status::OK;
}

void VolumeManager::ActivateStage(unsigned stage_index) {
  stage_index_ = stage_index;

  Rep::StageData& sdata = rep_->stages_[stage_index_];

  // If current frame still has data that was not consumed, do not advance the frame.
  if (sdata.empty() || sdata.has_data())
    return;

  if (unsigned(sdata.cur_frame_index + 1) >= rep_->vol_reader->frame_count())  // end of volume?
    return;

  ++sdata.cur_frame_index;
  CHECK_STATUS(ReadFrame());
}

void VolumeManager::FreeSlices() {
  for (unsigned i = 0; i < 2; ++i)
    rep_->stages_[i].iterators.clear();
}

void VolumeManager::OnPlanStage() {
  VLOG(1) << "VolumeManager::OnPlanStage";
  rep_->stages_[stage_index_].iterators.clear();
}

void VolumeManager::FinishPlanning() {
  DCHECK_LT(stage_index_, 2);
  Rep::StageData& sdata = rep_->stages_[stage_index_];

  sdata.cur_frame_index = -1;
}

Status VolumeManager::ReadFrame() {
  DCHECK_LT(stage_index_, 2);
  Rep::StageData& sdata = rep_->stages_[stage_index_];

  DCHECK_LT(sdata.cur_frame_index, rep_->vol_reader->frame_count());

  if (!sdata.iterators.empty()) {
    VLOG(1) << "ReadFrame " << stage_index_ << " / " << sdata.cur_frame_index;

    unsigned mask = 0;
    for (auto& it : sdata.iterators) {
      int success = it->NextFrame();
      mask |= (1 << success);
    }
    CHECK_LT(mask, 3);  // Either all fail or all succeed.
  }

  return Status::OK;
}

StatusObject<BareSliceProvider*>
  VolumeManager::GetReaderInternal(std::string name, DataType dt) {

  VolumeIterator* it = rep_->vol_reader->GetSliceIterator(name);
  if (!it)
    return ToStatus("Could not find slice");

  rep_->stages_[stage_index_].iterators.emplace_back(it);

  return it->provider();
}

}  // namespace puma
