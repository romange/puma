// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <memory>
#include <string>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include "base/integral_types.h"
#include "puma/pb/field_writers.h"

namespace puma {

class VolumeWriter;

namespace pb {

class MessageSlicer;

class FieldSerializerBase {
  FieldSerializerBase(const FieldSerializerBase&) = delete;
  void operator=(const FieldSerializerBase&) = delete;

 protected:
  using FD = ::google::protobuf::FieldDescriptor;
  using PBMessage = ::google::protobuf::Message;

  AnnotatedFd fd_;

  int slice_index_[FieldClass_ARRAYSIZE];
  SliceCodecId codec_id_;

  explicit FieldSerializerBase(const AnnotatedFd& fd, const std::string& field_path);
 public:

  void AppendSlicesToSchema(VolumeWriter* vw);

  virtual void Add(const PBMessage& msg, FieldWriters* dest) const = 0;

  virtual FieldWriters CreateWriters() const {
    return FieldWriters{fd_, slice_index_, codec_id_};
  }

  virtual ~FieldSerializerBase() {}

  std::string FieldName() const;
  const FD* fd() const { return fd_.get(); }

 private:

  void AddSliceToSchema(VolumeWriter* vw, FieldClass fc, DataType dt, SliceCodecId codec_id = 0);

  std::string field_path_;
};

class MessageFieldSerializer : public FieldSerializerBase {
 public:
  MessageFieldSerializer(const AnnotatedFd& fd, const std::string& field_path, VolumeWriter* vw)
    : FieldSerializerBase(fd, field_path) {}

  void SetSlicer(MessageSlicer* ms) { slicer_.reset(ms); }

  void Add(const PBMessage& msg, FieldWriters* dest) const override;

  virtual FieldWriters CreateWriters() const override;

 private:
  std::unique_ptr<MessageSlicer> slicer_;
};


class SingleFieldLeafSerializer : public FieldSerializerBase {
 public:
  explicit SingleFieldLeafSerializer(const AnnotatedFd& fd, const std::string& field_path,
                                     VolumeWriter* vw);

  void Add(const PBMessage& msg, FieldWriters* dest) const override;
 };


class RepFieldLeafSerializer : public FieldSerializerBase {
  using FD = ::google::protobuf::FieldDescriptor;
  template<FD::CppType t> struct FD_Traits;

  template<FD::CppType t> using FD_Traits_t = typename FD_Traits<t>::type;

 public:
  RepFieldLeafSerializer(const AnnotatedFd& fd, const std::string& field_path, VolumeWriter* vw);

  void Add(const PBMessage& msg, FieldWriters* dest) const override;

 private:
  template<FD::CppType cpp_t>
    void HandleRepField(const google::protobuf::Message& msg,
                        const google::protobuf::Reflection* refl, FieldWriters* dest) const;
};

}  // namespace pb
}  // namespace puma
