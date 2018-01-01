// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/pb/message_slicer.h"

#include <google/protobuf/reflection.h>

#include "base/logging.h"

#include "puma/file/volume_writer.h"
#include "puma/pb/block_compressor.h"
#include "puma/pb/util.h"

#include "strings/strcat.h"
#include "strings/util.h"

namespace puma {
namespace pb {
using base::Status;
using namespace std;
using strings::ByteRange;

namespace {

inline bool IsMessageType(const google::protobuf::FieldDescriptor* fd) {
  return fd->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE;
}

}  // namespace


SlicerState::~SlicerState() {
}

size_t SlicerState::OutputCost() const {
  size_t cost = 0;
  for (const auto& fr : field_writer_) {
    cost += fr.OutputCost();
  }

  return cost;
}


size_t SlicerState::FlushFrame(VolumeWriter* vw) {
  if (msg_cnt_ == 0)
    return 0;

  CHECK_STATUS(vw->StartFrame());

  size_t diff = vw->volume_size();
  SerializeSlices(vw);
  diff = vw->volume_size() - diff;

  CHECK_STATUS(vw->FinalizeFrame());

  return diff;
}

void SlicerState::SerializeSlices(VolumeWriter* vw) {
  for (size_t i = 0; i < field_writer_.size(); ++i) {
    auto& w = field_writer_[i];
    w.SerializeSlices(vw);
  }
  msg_cnt_ = 0;
}

MessageSlicer::MessageSlicer(const gpb::Descriptor* descr,
                             const FieldDfsNode* from, unsigned cnt, VolumeWriter* volume_writer)
  :descr_(descr), volume_writer_(CHECK_NOTNULL(volume_writer)) {
  Init(FdPath(), from, cnt);
}

MessageSlicer::MessageSlicer(const FdPath& fd_path, const FieldDfsNode* from, unsigned cnt,
                             VolumeWriter* volume_writer)
  : volume_writer_(CHECK_NOTNULL(volume_writer)) {
  CHECK(fd_path.valid());
  descr_ = fd_path.leaf()->message_type();

  Init(fd_path, from, cnt);
}


void MessageSlicer::Init(const FdPath& fd_path, const FieldDfsNode* from, unsigned cnt) {
  string prefix_path = fd_path.PathStr();
  CHECK(descr_) << prefix_path;

  for (unsigned i = 0; i < cnt; i += from[i].dfs_offset) {
    const AnnotatedFd& fd = from[i].fd;
    CHECK(fd->containing_type() == descr_) << fd->name() << " " << i;

    string field_path = prefix_path + fd->name();

    VLOG(1) << "Slicing " << fd->DebugString() << " into " << field_path;

    if (IsMessageType(fd.get())) {
      FdPath field_prefix = fd_path;
      field_prefix.push_back(fd.get());

      MessageFieldSerializer* msgs = new MessageFieldSerializer(fd, field_path, volume_writer_);
      msgs->AppendSlicesToSchema(volume_writer_);

      if (from[i].dfs_offset > 1) {
        MessageSlicer* ms = new MessageSlicer(
          field_prefix, from + i + 1, from[i].dfs_offset - 1, volume_writer_);
        msgs->SetSlicer(ms);
      }

      VLOG(1) << "Creating message_writer for " << field_prefix.PathStr();

      field_.emplace_back(msgs);
      continue;
    }

    if (fd->is_repeated()) {
      field_.emplace_back(new RepFieldLeafSerializer(fd, field_path, volume_writer_));
    } else {
      field_.emplace_back(new SingleFieldLeafSerializer(fd, field_path, volume_writer_));
    }
    field_.back()->AppendSlicesToSchema(volume_writer_);
  }

}

void MessageSlicer::Add(const gpb::Message& msg, SlicerState* dest) const {
  CHECK(msg.GetDescriptor() == descr_) << "Mine: " << descr_->name() << " theirs "
      << msg.GetDescriptor()->name();
  DCHECK_EQ(field_.size(), dest->field_writer_.size());

  for (unsigned i = 0; i < field_.size(); ++i) {
    auto& writer = dest->field_writer_[i];

    field_[i]->Add(msg, &writer);
  }

  ++dest->msg_cnt_;
}


SlicerState* MessageSlicer::CreateState() const {
  SlicerState* res = new SlicerState;
  res->field_writer_.reserve(field_.size());

  for (const auto& f : field_) {
    res->field_writer_.push_back(f->CreateWriters());
  }

  return res;
}

MessageSlicer::~MessageSlicer() {
}


}  // namespace pb
}  // namespace puma
