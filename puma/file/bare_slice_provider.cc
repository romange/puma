// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/file/bare_slice_provider.h"

#include "base/logging.h"
#include "puma/pb/util.h"

using std::string;

namespace puma {

SliceManager::SliceManager() {
}

SliceManager::~SliceManager() {
}


void SliceManager::PlanStage(unsigned stage_index) {
  DCHECK_LT(stage_index, 2);

  stage_index_ = stage_index;
  OnPlanStage();
}

util::StatusObject<BareSliceProvider*> SliceManager::GetReader(
      FieldClass fc, StringPiece name, DataType dt) {
  DCHECK_LT(stage_index_, 2);

  string full_path = pb::FieldClassPath(name, fc);

  return GetReaderInternal(std::move(full_path), dt);
}


}  // namespace puma
