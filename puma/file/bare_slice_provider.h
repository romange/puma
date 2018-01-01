// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <functional>

#include "util/status.h"
#include "puma/data_types.h"
#include "puma/storage.pb.h"

namespace puma {

/**
   We separate logical columns (attribute fields) from physical or bare columns because
   // of repeated fields, nullability, i.e meta information that needs to be encoded along
   // with data. So we model it with bare slices.
   In addition we do not always want to load all the physical columns, i.e. it gives us
   finer granularity over what to load and in which case.

   Fills flat page with data based on its type.
   FlatPage should point to a memory block owned by the caller of GetPage and it should have
   enough space to contain 'max_size' records.
**/

class BareSliceProvider {
  DataType dt_;
  std::string name_;
 public:
  // bare_name is expected to be stored by the caller through the life of the provider.
  BareSliceProvider(std::string bare_name, DataType dt) : dt_(dt), name_(std::move(bare_name)) {}

  virtual ~BareSliceProvider() {}
  virtual util::Status GetPage(uint32 max_size, SlicePtr dest, uint32* fetched_size) = 0;

  DataType dt() const { return dt_;}
  StringPiece name() const { return name_;}

  bool has_data_to_read() const { return has_data_to_read_;}

 protected:
  bool has_data_to_read_ = false;
};


// Orchestrator for reading volumes.
class SliceManager {
 public:
  SliceManager();
  virtual ~SliceManager();

  // Resets the manager to planning state for specific stage,
  // in which it can allocate readers for accessing a specific column.
  void PlanStage(unsigned stage_index);

  // The ownership stays in the manager.
  util::StatusObject<BareSliceProvider*>
      GetReader(FieldClass fc, StringPiece name, DataType dt);

  virtual void FinishPlanning() { stage_index_ = 2; };

  // Prepare for reading the data needed for feeding 'stage_index' stage.
  virtual void ActivateStage(unsigned stage_index) {}

  // Deallocates all the allocated slices.
  virtual void FreeSlices() = 0;

 protected:
  virtual void OnPlanStage() {};

  virtual util::StatusObject<BareSliceProvider*>
    GetReaderInternal(std::string name, DataType dt) = 0;

  unsigned stage_index_ = 2;
};

// StringPiece - the name of the bare column. Valid only during the call to the factory.
typedef std::function<BareSliceProvider*(FieldClass, StringPiece, DataType)>
  BareSliceProviderFactory;

}  // namespace puma
