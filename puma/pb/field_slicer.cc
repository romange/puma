// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/pb/field_slicer.h"

#include <google/protobuf/reflection.h>

#include "base/flags.h"
#include "base/logging.h"

#include "base/stl_util.h"
#include "puma/file/volume_writer.h"

#include "puma/pb/fd_path.h"
#include "puma/pb/message_slicer.h"
#include "puma/pb/util.h"
#include "strings/strcat.h"
#include "strings/util.h"

DEFINE_bool(enable_decimal_compressor, true, "");
DEFINE_bool(enable_int64_v2, false, "");

namespace puma {
namespace pb {

using namespace std;
namespace gpb = ::google::protobuf;
using strings::ByteRange;
using FD = gpb::FieldDescriptor;

namespace {

uint32_t DetermineCodec(const AnnotatedFd& fd, FieldClass fc) {
  uint32_t codec_id = 0;
  if (fc == FieldClass::DATA) {
    if (fd->is_repeated()) {
      if (fd.is_unordered_set &&
          base::_in(fd->cpp_type(), {FD::CPPTYPE_INT64, FD::CPPTYPE_INT32}))
        codec_id = kSeqEncoderCodec;
    } else {
      if (fd->cpp_type() == FD::CPPTYPE_DOUBLE && FLAGS_enable_decimal_compressor) {
        return kDoubleEncoderCodec;
      } else if (fd->cpp_type() == FD::CPPTYPE_INT64 && FLAGS_enable_int64_v2) {
        return kInt64V2Codec;
      }
    }
  }
  return codec_id;
}

};


FieldSerializerBase::FieldSerializerBase(const AnnotatedFd& fd, const std::string& field_path)
: fd_(fd), field_path_(field_path) {
  std::fill(slice_index_, slice_index_ + FieldClass_ARRAYSIZE, -1);
  codec_id_ = DetermineCodec(fd, FieldClass::DATA);
}

void FieldSerializerBase::AddSliceToSchema(VolumeWriter* vw, FieldClass fc, DataType dt,
                                           SliceCodecId codec_id) {
  slice_index_[fc] = vw->AddSliceToSchema(field_path_, fc, dt, codec_id);
}

void FieldSerializerBase::AppendSlicesToSchema(VolumeWriter* vw) {
  if (fd_->is_repeated()) {
    AddSliceToSchema(vw, FieldClass::ARRAY_SIZE, kArrayLenDt);
  } else if (fd_->is_optional() && fd_->cpp_type() != FD::CPPTYPE_STRING) {
    AddSliceToSchema(vw, FieldClass::OPTIONAL, DataType::BOOL);
  }
  if (fd_->cpp_type() == FD::CPPTYPE_MESSAGE)
    return;

  if (fd_->cpp_type() == FD::CPPTYPE_STRING) {
    AddSliceToSchema(vw, FieldClass::STRING_LEN, kStringLenDt);
  }

  AddSliceToSchema(vw, FieldClass::DATA, FromPBType(fd_->cpp_type()), codec_id_);
}

string FieldSerializerBase::FieldName() const {
  return fd_->full_name();
}


void MessageFieldSerializer::Add(const gpb::Message& msg, FieldWriters* dest) const {
  const gpb::Reflection* refl = msg.GetReflection();
  const gpb::Descriptor* descr = msg.GetDescriptor();
  CHECK(fd_->containing_type() == descr);

  SlicerState* ss = dest->msg_state();

  if (fd_->is_repeated()) {
    const auto& arr = refl->GetRepeatedFieldRef<gpb::Message>(msg, fd());
    dest->AddLen(arr.size());

    for (const auto& v : arr) {
      slicer_->Add(v, ss);
    }
  } else {
    if (dest->is_optional()) {
      bool exists = refl->HasField(msg, fd());
      dest->AddDef(exists);
      if (!exists)
        return;
    }

    const gpb::Message& field_msg = refl->GetMessage(msg, fd());
    slicer_->Add(field_msg, ss);
  }
}

FieldWriters MessageFieldSerializer::CreateWriters() const {
  return FieldWriters{fd(), slice_index_, codec_id_, slicer_->CreateState()};
}

SingleFieldLeafSerializer::SingleFieldLeafSerializer(
    const AnnotatedFd& fd, const std::string& field_path, VolumeWriter* vw)
  : FieldSerializerBase(fd, field_path) {
  CHECK(!fd->is_repeated()) << fd->full_name();
}

void SingleFieldLeafSerializer::Add(const gpb::Message& msg, FieldWriters* dest) const {
  const gpb::Reflection* refl = msg.GetReflection();

  string tmp;
  const string& str = dest->is_string() ? refl->GetStringReference(msg, fd(), &tmp) : tmp;

  if (dest->is_optional()) {
    bool exists = refl->HasField(msg, fd());

    if (!dest->is_string())
      dest->AddDef(exists);

    if (!exists) {
      if (dest->is_string()) {
        dest->AddNullStr();
      }
      return;
    }
  }

  switch (fd()->cpp_type()) {
    case FD::CPPTYPE_UINT32:
      dest->AddVal(refl->GetUInt32(msg, fd()));
    break;
    case FD::CPPTYPE_INT32:
      dest->AddVal(refl->GetInt32(msg, fd()));
    break;
    case FD::CPPTYPE_UINT64:
      dest->AddVal(refl->GetUInt64(msg, fd()));
    break;
    case FD::CPPTYPE_INT64:
      dest->AddValI64(refl->GetInt64(msg, fd()));
    break;
    case FD::CPPTYPE_ENUM:
      dest->AddVal<int32_t>(refl->GetEnumValue(msg, fd()));
    break;
    case FD::CPPTYPE_STRING: {
      dest->AddStr(str);
    }
    break;
    case FD::CPPTYPE_DOUBLE:
      dest->AddDouble(refl->GetDouble(msg, fd()));
    break;
    case FD::CPPTYPE_FLOAT:
      dest->AddVal(refl->GetFloat(msg, fd()));
    break;
    case FD::CPPTYPE_BOOL:
      dest->AddBool(refl->GetBool(msg, fd()));
    break;
    default:
      LOG(FATAL) << "Not implemented: " << fd_->cpp_type_name()
    ;
  }
}

RepFieldLeafSerializer::RepFieldLeafSerializer(
    const AnnotatedFd& fd, const std::string& field_path, VolumeWriter* vw)
    : FieldSerializerBase(fd, field_path) {
  CHECK(fd->is_repeated()) << fd->full_name();
}


#define DECLARE_FD_TRAITS(CPP_TYPE, src_type) \
    template<> struct RepFieldLeafSerializer::FD_Traits<gpb::FieldDescriptor::CPP_TYPE> { \
      typedef src_type type; }

DECLARE_FD_TRAITS(CPPTYPE_INT32, int32);
DECLARE_FD_TRAITS(CPPTYPE_UINT32, uint32);
DECLARE_FD_TRAITS(CPPTYPE_INT64, gpb::int64);
DECLARE_FD_TRAITS(CPPTYPE_UINT64, gpb::uint64);
DECLARE_FD_TRAITS(CPPTYPE_DOUBLE, double);
DECLARE_FD_TRAITS(CPPTYPE_FLOAT, float);

#undef DECLARE_FD_TRAITS


template<FD::CppType cpp_t>
    void RepFieldLeafSerializer::HandleRepField(
      const gpb::Message& msg, const gpb::Reflection* refl, FieldWriters* dest) const {
  using InputType = FD_Traits_t<cpp_t>;

  const auto& arr = refl->GetRepeatedFieldRef<InputType>(msg, fd());
  std::for_each(begin(arr), end(arr), [dest](InputType val) { dest->AddVal(val); });
}

template<>
    void RepFieldLeafSerializer::HandleRepField<FD::CPPTYPE_INT64> (
      const gpb::Message& msg, const gpb::Reflection* refl, FieldWriters* dest) const {
  using InputType = FD_Traits_t<FD::CPPTYPE_INT64>;

  const auto& arr = refl->GetRepeatedFieldRef<InputType>(msg, fd());
  dest->AddRepeated64(begin(arr), arr.size());
}

template<>
    void RepFieldLeafSerializer::HandleRepField<FD::CPPTYPE_INT32> (
      const gpb::Message& msg, const gpb::Reflection* refl, FieldWriters* dest) const {
  using InputType = FD_Traits_t<FD::CPPTYPE_INT32>;

  const auto& arr = refl->GetRepeatedFieldRef<InputType>(msg, fd());
  dest->AddRepeated32(begin(arr), arr.size());
}

void RepFieldLeafSerializer::Add(const gpb::Message& msg, FieldWriters* dest) const {
  const gpb::Reflection* refl = msg.GetReflection();
  int sz = refl->FieldSize(msg, fd());

  dest->AddLen(sz);

  if (sz == 0)
    return;

  switch (fd()->cpp_type()) {
    case FD::CPPTYPE_INT32:
      HandleRepField<FD::CPPTYPE_INT32>(msg, refl, dest);
    break;
    case FD::CPPTYPE_UINT32:
      HandleRepField<FD::CPPTYPE_UINT32>(msg, refl, dest);
    break;
    case FD::CPPTYPE_INT64:
      HandleRepField<FD::CPPTYPE_INT64>(msg, refl, dest);
    break;
    case FD::CPPTYPE_UINT64:
      HandleRepField<FD::CPPTYPE_UINT64>(msg, refl, dest);
    break;
    case FD::CPPTYPE_STRING: {
      const auto& arr = refl->GetRepeatedFieldRef<string>(msg, fd());
      std::for_each(begin(arr), end(arr), [dest](const auto& val) { dest->AddStr(val); } );
    }
    break;
    default:
      LOG_FIRST_N(ERROR, 100) << "Not implemented " << fd()->cpp_type_name()
                              << " "  << fd_->full_name();
  }
}

}  // namespace pb
}  // namespace puma

