// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "puma/file/bare_slice_provider.h"

namespace file {
class ReadonlyFile;
}  // namespace file

namespace puma {

class VolumeManager : public SliceManager {
 public:
  VolumeManager(pmr::memory_resource* mr);
  ~VolumeManager();

  util::Status Open(StringPiece path);

  void ActivateStage(unsigned stage_index) override;
  void FreeSlices() override;

  size_t bytes_read() const { return bytes_read_; }
 private:
  void OnPlanStage() override;

  void FinishPlanning() override;

  // Needed by SliceManager.
  util::StatusObject<BareSliceProvider*>
    GetReaderInternal(std::string name, DataType dt) override;

  util::Status ReadFrame();

  struct Rep;

  pmr::memory_resource* mr_;
  std::unique_ptr<Rep> rep_;

  size_t bytes_read_ = 0;
};

}  // namespace puma
