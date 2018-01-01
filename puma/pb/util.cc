// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/pb/util.h"

#include "base/logging.h"

#include "strings/strcat.h"

namespace puma {

namespace pb {

using FD = gpb::FieldDescriptor;
using std::string;

schema::Attribute::Ordinality FromPbLabel(FD::Label label) {
  switch (label) {
    case FD::LABEL_REPEATED:
      return schema::Attribute::REPEATED;
    case FD::LABEL_REQUIRED:
      return schema::Attribute::REQUIRED;
    case FD::LABEL_OPTIONAL:
      return schema::Attribute::NULLABLE;
  }
  LOG(FATAL) << "Won't reach it";
  return schema::Attribute::NULLABLE;
}

DataType FromPBType(const gpb::FieldDescriptor::CppType t) {
  switch(t) {
    case FD::CPPTYPE_ENUM:
    case FD::CPPTYPE_INT32:
      return DataType::INT32;

    case FD::CPPTYPE_UINT32:
      return DataType::UINT32;

    case FD::CPPTYPE_UINT64:
      return DataType::UINT64;

    case FD::CPPTYPE_INT64:
      return DataType::INT64;

    case FD::CPPTYPE_BOOL:
      return DataType::BOOL;

    case FD::CPPTYPE_STRING:
      return DataType::STRING;

    case FD::CPPTYPE_DOUBLE:
    case FD::CPPTYPE_FLOAT:
      return DataType::DOUBLE;

    case FD::CPPTYPE_MESSAGE:
      return DataType::MESSAGE;
    default:;
  }
  return DataType::NONE;
}

static void AddAttributesToTable(const gpb::Descriptor* descr, const schema::Attribute* parent,
                                 schema::Table* table) {
  for (int i = 0; i < descr->field_count(); ++i) {
    const gpb::FieldDescriptor* fd = descr->field(i);
    schema::Attribute::Ordinality ord = pb::FromPbLabel(fd->label());
    string fname = fd->name();
    DataType dt = pb::FromPBType(fd->cpp_type());
    const schema::Attribute* a = table->Add(fname, dt, ord, parent);

    if (fd->cpp_type() == FD::CPPTYPE_MESSAGE) {
      AddAttributesToTable(fd->message_type(), a, table);
    }
  }
}

// Creates a table from pb descriptor. The caller will own the returned table.
schema::Table* FromDescriptor(StringPiece name, const gpb::Descriptor* descr) {
  schema::Table* table = new schema::Table(name);
  AddAttributesToTable(descr, nullptr, table);

  return table;
}


const char* FieldClassExtension(FieldClass fc) {
  switch (fc) {
    case FieldClass::DATA:
      return ".col";
    case FieldClass::ARRAY_SIZE:
      return ".arr";
    case FieldClass::OPTIONAL:
      return ".def";
    case FieldClass::STRING_LEN:
      return ".sz";
  }
  return nullptr;
}

std::string FieldClassPath(StringPiece base_name, FieldClass fc) {
  return StrCat(base_name, FieldClassExtension(fc));
}

StringPiece SliceBaseName(StringPiece slice_name) {
  CHECK_GT(slice_name.size(), 4);

  StringPiece::size_type pos = slice_name.rfind('.');
  CHECK_EQ(slice_name.size() - 4, pos);

  slice_name.subtract(4);
  return slice_name;
}

using SIV = std::vector<FieldDfsNode>;

void FromDescr(const gpb::Descriptor* descr, const string& prefix_path, FieldFilterFn fn, SIV* res) {
  using FD = gpb::FieldDescriptor;

  int last_field_index = -1;
  for (int i = 0; i < descr->field_count(); ++i) {
    const FD* fd = descr->field(i);
    string field_path = prefix_path + fd->name();

    if (fn && !fn(field_path))
      continue;
    last_field_index = res->size();
    res->emplace_back(fd);

    if (fd->cpp_type() == FD::CPPTYPE_MESSAGE) {
      field_path.push_back('.');
      FromDescr(fd->message_type(), field_path, fn, res);
      res->at(last_field_index).dfs_offset = res->size() - last_field_index;
      // We can store empty messages.
    }
  }
  if (last_field_index >= 0) {
    (*res)[last_field_index].final_field = true;
  }
}

SIV FromDescr(const gpb::Descriptor* descr, FieldFilterFn fn) {
  SIV res;
  FromDescr(descr, "", fn, &res);
  return res;
}

std::unordered_set<const gpb::FileDescriptor*> GatherTransitiveFdSet(
  const gpb::FileDescriptor* file_descr) {
  std::unordered_set<const gpb::FileDescriptor*> res({file_descr});

  std::vector<const gpb::FileDescriptor*> stack({file_descr});
  while (!stack.empty()) {
    const gpb::FileDescriptor* fd = stack.back();
    stack.pop_back();

    for (int i = 0; i < fd->dependency_count(); ++i) {
      const gpb::FileDescriptor* child = fd->dependency(i);
      if (res.insert(child).second) {
        stack.push_back(child);
      }
    }
  }
  return res;
}

}  // namespace pb
}  // namespace puma
