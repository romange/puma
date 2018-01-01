// Copyright 2017,rshman (romange@gmail.com)
//
#pragma once

#include "puma/data_types.h"
#include "puma/schema.h"
#include "puma/storage.pb.h"
#include "puma/pb/base.h"

#include <google/protobuf/descriptor.h>

namespace puma {
namespace pb {

schema::Attribute::Ordinality FromPbLabel(gpb::FieldDescriptor::Label label);

DataType FromPBType(const gpb::FieldDescriptor::CppType t);
schema::Table* FromDescriptor(StringPiece name, const gpb::Descriptor* descr);

// i.e. "foo" and ARRAY_SIZE return "foo.arr"
std::string FieldClassPath(StringPiece base_name, FieldClass fc);
StringPiece SliceBaseName(StringPiece slice_name);
const char* FieldClassExtension(FieldClass fc);


using FieldFilterFn = std::function<bool(StringPiece)>;

struct FieldDfsNode {
  AnnotatedFd fd;

  // To skip to the next sibling - just add dfs_offset to the current index.
  size_t dfs_offset = 1;
  bool final_field = false;

  FieldDfsNode(const gpb::FieldDescriptor* fd2) : fd(fd2) {}
};

std::vector<FieldDfsNode> FromDescr(const gpb::Descriptor* descr,
                                    FieldFilterFn fn = FieldFilterFn());


std::unordered_set<const gpb::FileDescriptor*> GatherTransitiveFdSet(
  const gpb::FileDescriptor* file_descr);

}  // namespace pb
}  // namespace puma
