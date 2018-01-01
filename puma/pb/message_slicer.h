// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <memory>
#include <functional>
#include <vector>

#include "base/arena.h"
#include "base/status.h"

#include "file/file.h"

#include "puma/pb/fd_path.h"
#include "puma/pb/field_slicer.h"

namespace google {
namespace protobuf {
class FieldDescriptor;
class Message;
}  // namespace protobuf
}  // namespace google


namespace puma {

class VolumeWriter;

namespace pb {

struct FieldDfsNode;
class SlicerState;

namespace gpb = ::google::protobuf;

// Column order - DFS:
// Object traversal - from smallest tag to largest. meta data is close to field itself
// and comes before it.
// i.e. for { optional msg a { required c, repeated b}, repeated string s}, the order will be
// as follows: a.def, a.c.col, a.b.arr, a.b.col, s.arr, s.sz, s.col
//
// The order is computed once (can be done for each volume or separately) and shared among volumes.
// SliceWriter needs to report how many bytes its serialized data holds plus
// it should provide an access to its encoded data when frame is flushed to file.
//
// Fence output ? Fences release their data in pages. We should change the interface to allow
// fetching those pages simultaneously. In addition volumes should support single column
// mode as well. In fact they do not care about number of columns since they do not contain
// full schema. Volumes may contain the column type.
//
class MessageSlicer {
 public:
  MessageSlicer(const gpb::Descriptor* descr, const FieldDfsNode* from, unsigned count,
                VolumeWriter* volume_writer);

  ~MessageSlicer();

  SlicerState* CreateState() const;

  void Add(const gpb::Message& msg, SlicerState* dest) const;

 private:
  MessageSlicer(const FdPath& fd_path,
                const FieldDfsNode* from, unsigned count, VolumeWriter* volume_writer);

  void Init(const FdPath& fd_path, const FieldDfsNode* from, unsigned count);

  const gpb::Descriptor* descr_;

  std::vector<std::unique_ptr<FieldSerializerBase>> field_;

  VolumeWriter* volume_writer_ = nullptr;  // does not take ownership over it.


  MessageSlicer(const MessageSlicer&) = delete;
  void operator=(const MessageSlicer&) = delete;
};

class SlicerState {
  friend class MessageSlicer;

 public:
  SlicerState() {}
  ~SlicerState();

  size_t OutputCost() const;
  uint64 msg_cnt() const { return msg_cnt_; }

  // Returns number of serialized bytes for this frame.
  size_t FlushFrame(VolumeWriter* vw);

  // called recursively by FieldWriters as well.
  void SerializeSlices(VolumeWriter* vw);

 private:

  std::vector<FieldWriters> field_writer_;

  uint64 msg_cnt_ = 0;
};


}  // namespace pb
}  // namespace puma
