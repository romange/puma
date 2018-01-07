// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// Example run:
/* ./import_volume  --output_dir ~/data/nyc_vol --schema \
    ~/project/gavroche-mr/puma/examples/schema.proto:NycRide \
    --table nyc ~/data/nyc/nyc000000000[0,1,2,3]*gz
*/
#include <mutex>
#include <iostream>
#include <experimental/filesystem>
#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/dynamic_message.h>

#include "base/init.h"
#include "base/logging.h"
#include "base/pod_array.h"
#include "base/stl_util.h"

#include "file/file_util.h"
#include "file/filesource.h"
#include "file/proto_writer.h"

#include "puma/file/volume_writer.h"
#include "puma/pb/util.h"
#include "puma/pb/message_slicer.h"
#include "puma/storage.pb.h"

#include "strings/split.h"
#include "strings/strcat.h"
#include "strings/util.h"

#include "base/map-util.h"
#include "util/sp_task_pool.h"

DEFINE_string(output_dir, "", "output col directory");
DEFINE_string(table, "", "table name");
DEFINE_string(schema, "", "<schemafile>:<msg_name>");

DEFINE_int32(frame_size, (1 << 20) * 32, "frame size");

DEFINE_bool(parallel, true, "");
DEFINE_string(unordered_set, "", "");

using std::string;
using std::cerr;
using puma::DataType;
using puma::VolumeWriter;

using util::StatusCode;
using util::Status;

namespace gpb = ::google::protobuf;
namespace gpc = gpb::compiler;
namespace fs = std::experimental::filesystem;
using puma::storage::SliceIndex;
using namespace std::placeholders;

namespace {

bool PrefixEligible(StringPiece path) {
  return true;
}

std::pair<uint32_t, const char*> ParseNum(const char* ptr) {
  uint32_t res = 0;
  for (; *ptr >= '0' && *ptr <= '9'; ++ptr) {
    res *= 10;
    res += (*ptr - '0');
  }
  return std::pair<uint32_t, const char*>(res, ptr);
}

// num is a valid <YYYY, MM, DD, H, M, S> sequence.
// Taken from: https://stackoverflow.com/questions/7960318/math-to-convert-seconds-since-1970-into-date-and-vice-versa
// http://howardhinnant.github.io/date_algorithms.html
time_t EpochTime(const uint32_t num[6]) {
  DCHECK_GE(num[0], 1970);

  unsigned year = num[0] - (num[1] <= 2);

  const unsigned era = year / 400;
  const unsigned yoe = (year - era * 400);      // [0, 399]
  const unsigned doy = (153*(num[1] + (num[1] > 2 ? -3 : 9)) + 2) / 5 + num[2]-1;  // [0, 365]
  const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;         // [0, 146096]

  time_t res = era * 146097 + doe - 719468;  // days since epoch.

  res = (res * 24 + num[3]) * 60 + num[4];  // minutes since epoch
  return res * 60 + num[5];
}

bool ParseDateTime(StringPiece str, uint32_t nums[6]) {
  const char* current = str.data();
  CHECK_EQ('\0', current[str.size()]);

  for (unsigned i = 0; i < 6; ++i) {
    auto p = ParseNum(current);
    if (p.second == current)
      return false;
    nums[i] = p.first;
    if (p.second == str.end() && i < 6)
      return false;
    current = p.second + 1;
  }

  if (nums[0] < 1900)
    return false;
  if (nums[1] == 0 || nums[1] > 12)
    return false;
  if (nums[2] == 0 || nums[2] > 31)
    return false;
  if (nums[3] >= 24)
    return false;
  if (nums[4] >= 60 || nums[5] >= 60)
    return false;

  return true;
}


inline Status ToStatus(string msg) {
  return Status(StatusCode::PARSE_ERROR, std::move(msg));
}

void PrintSchema(const gpb::Descriptor* descr, string* dest) {
  dest->append(descr->DebugString());

  for (int i = 0; i < descr->field_count(); ++i) {
    const gpb::FieldDescriptor* fd = descr->field(i);
    const gpb::Descriptor* mt = fd->message_type();
    if (mt) {
      PrintSchema(mt, dest);
    }
  }
}

void FillTableSchema(const gpb::Descriptor* descr, puma::storage::Table* dest) {
  std::unordered_set<const gpb::FileDescriptor*>  fd_set =
    puma::pb::GatherTransitiveFdSet(descr->file());

  gpb::FileDescriptorSet fd_set_proto;
  for (const gpb::FileDescriptor* fd : fd_set) {
    fd->CopyTo(fd_set_proto.add_file());
  }
  dest->set_fd_set(fd_set_proto.SerializeAsString());
  dest->set_pb_type(descr->full_name());
}


class ErrorCollector : public gpc::MultiFileErrorCollector {
  void AddError(const string& filenname, int line, int column, const string& message) {
    std::cerr << "Error File : " << filenname << " : " << message << std::endl;
  }
};

class SourceTree : public gpc::SourceTree {
 public:
  SourceTree();
  virtual ~SourceTree() {}

  gpb::io::ZeroCopyInputStream* Open(const string& filename) override;

  string GetLastErrorMessage() override {
    return disk_tree_.GetLastErrorMessage();
  }

  void MapPath(const string& virtual_path, const string& disk_path) {
    disk_tree_.MapPath(virtual_path, disk_path);
  }
 private:
  gpc::DiskSourceTree disk_tree_;
  vector<string> sl_;

  SourceTree(const SourceTree&) = delete;
  void operator=(const SourceTree&) = delete;
};

SourceTree::SourceTree() {}

gpb::io::ZeroCopyInputStream* SourceTree::Open(const string& filename) {
  gpb::io::ZeroCopyInputStream* result = disk_tree_.Open(filename);
  if (result) {
    return result;
  }

  return nullptr;
}


SourceTree source_tree;
ErrorCollector collector;

gpc::Importer importer(&source_tree, &collector);

const gpb::Descriptor* LoadDescriptor() {
  size_t pos = FLAGS_schema.find(':');
  CHECK(string::npos != pos && pos > 0);

  fs::path schema_file(FLAGS_schema.substr(0, pos));
  CHECK(fs::exists(schema_file)) << schema_file;
  schema_file = fs::canonical(schema_file);
  string msg_name = FLAGS_schema.substr(pos + 1);

  LOG(INFO) << "Parsing schema " << schema_file << ", msg: " << msg_name;

  source_tree.MapPath("/", "/");

  const gpb::FileDescriptor* fd = importer.Import(schema_file);
  CHECK(fd);

  const gpb::Descriptor* descr = fd->FindMessageTypeByName(msg_name);
  if (descr == nullptr) {
    cerr << "Could not find " << msg_name << "\n";
  }
  return descr;
}

class CsvMsgParser {
public:
  explicit CsvMsgParser(const gpb::Descriptor* descr);
  Status Parse(const std::vector<char*>& src, gpb::Message* msg);

private:

  static bool ParseUint32(StringPiece str, const gpb::FieldDescriptor* fd, gpb::Message* msg);
  static bool ParseString(StringPiece str, const gpb::FieldDescriptor* fd, gpb::Message* msg);
  static bool ParseDouble(StringPiece str, const gpb::FieldDescriptor* fd, gpb::Message* msg);
  static bool ParseInt64(StringPiece str, const gpb::FieldDescriptor* fd, gpb::Message* msg);

  typedef std::function<bool(StringPiece, gpb::Message*)> ParseCb;
  std::vector<ParseCb> parse_cb_;
};

CsvMsgParser::CsvMsgParser(const gpb::Descriptor* descr) {
  using FD = gpb::FieldDescriptor;

  parse_cb_.resize(descr->field_count());
  for (size_t i = 0; i < parse_cb_.size(); ++i) {
    const gpb::FieldDescriptor* fd = descr->field(i);
    CHECK(!fd->is_repeated());

    switch (fd->cpp_type()) {
      case FD::CPPTYPE_UINT32:
        parse_cb_[i] = std::bind(&CsvMsgParser::ParseUint32, _1, fd, _2);
      break;
      case FD::CPPTYPE_INT64:
        parse_cb_[i] = std::bind(&CsvMsgParser::ParseInt64, _1, fd, _2);
      break;
      case FD::CPPTYPE_DOUBLE:
        parse_cb_[i] = std::bind(&CsvMsgParser::ParseDouble, _1, fd, _2);
      break;
      case FD::CPPTYPE_STRING:
        parse_cb_[i] = std::bind(&CsvMsgParser::ParseString, _1, fd, _2);
      break;

      default:
        LOG(FATAL) << "Unsupported type " << fd->cpp_type_name();
    }
  }
}

Status CsvMsgParser::Parse(const std::vector<char*>& src, gpb::Message* msg) {
  if (src.size() != parse_cb_.size())
    return ToStatus("Wrong line length");
  msg->Clear();

  for (size_t i = 0; i < parse_cb_.size(); ++i) {
    if (!parse_cb_[i](src[i], msg)) {
      return ToStatus(StrCat("Could not parse field ", i, " \"", src[i], "\""));
    }
  }
  return Status::OK;
}


bool CsvMsgParser::ParseUint32(StringPiece str, const gpb::FieldDescriptor* fd,
                               gpb::Message* msg) {
  if (str.empty())
    return true;

  uint32 num;
  if (!safe_strtou32(str, &num))
    return false;
  const gpb::Reflection* refl = msg->GetReflection();
  refl->SetUInt32(msg, fd, num);
  return true;
}

bool CsvMsgParser::ParseString(StringPiece str, const gpb::FieldDescriptor* fd,
                               gpb::Message* msg) {
  const gpb::Reflection* refl = msg->GetReflection();
  if (!str.empty()) {
    refl->SetString(msg, fd, str.as_string());
  }
  return true;
}

bool CsvMsgParser::ParseDouble(StringPiece str, const gpb::FieldDescriptor* fd,
                               gpb::Message* msg) {
  if (str.empty()) {
    return true;
  }
  double num;

  // TODO: to use something like
  // https://github.com/Tencent/rapidjson/blob/master/doc/internals.md#parsing-to-double-parsingdouble
  if (!safe_strtod(str, &num))
    return false;
  if (num == 0)
    return true;
  const gpb::Reflection* refl = msg->GetReflection();
  refl->SetDouble(msg, fd, num);
  return true;
}

bool CsvMsgParser::ParseInt64(StringPiece str, const gpb::FieldDescriptor* fd,
                               gpb::Message* msg) {
  if (str.empty()) {
    return true;
  }

  int64 num;
  if (!safe_strto64(str, &num)) {
    // strptime(str.data(), "%Y-%m-%d %H:%M:%S", &tm)
    uint32_t nums[6];
    if (!ParseDateTime(str, nums))
      return false;
    // struct tm tmval = ToTm(nums);
    num = EpochTime(nums);
  }

  const gpb::Reflection* refl = msg->GetReflection();
  refl->SetInt64(msg, fd, num);
  return true;
}

using namespace puma;
class CsvTask {
 public:

  void operator()(const std::string& filename);

  CsvTask(const gpb::Message* to_clone, const pb::MessageSlicer* slicer, VolumeWriter* vw)
  : slicer_state_(slicer->CreateState()), vw_(vw) {
    local_msg_.reset(to_clone->New());

    slicer_ = slicer;
  }

  void Finalize() {
    slicer_state_->FlushFrame(vw_);
  }
 private:
  std::unique_ptr<gpb::Message> local_msg_;
  const pb::MessageSlicer* slicer_;
  std::unique_ptr<pb::SlicerState> slicer_state_;
  VolumeWriter* vw_;
};

void CsvTask::operator()(const std::string& filename) {
  StringPiece line;

  file::LineReader reader(filename);
  std::vector<char*> arr;
  CsvMsgParser parser(local_msg_->GetDescriptor());
  unsigned msg_cnt = slicer_state_->msg_cnt();

  while (reader.Next(&line)) {
    char* ptr = const_cast<char*>(line.data());
    arr.clear();
    SplitCSVLineWithDelimiter(ptr, ',', &arr);
    Status status = parser.Parse(arr, local_msg_.get());
    if (!status.ok()) {
      if (reader.line_num() == 1)
        continue;
      LOG(FATAL) << "Error parsing " << status << ", line:\n" << arr;
    }
    slicer_->Add(*local_msg_, slicer_state_.get());

    if (++msg_cnt % 10 == 0) {
      ssize_t sz = slicer_state_->OutputCost();
      if (sz >= FLAGS_frame_size) {
        size_t vol_sz = slicer_state_->FlushFrame(vw_);
        LOG(INFO) << "Flushing frame with " << msg_cnt - 1 << " records and cost "
                  << sz << " and filesize " << vol_sz;

      }
    }
  }
}

}  // namespace

int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  namespace gpb = ::google::protobuf;

  CHECK(!FLAGS_output_dir.empty());
  CHECK(!FLAGS_table.empty());
  CHECK(!FLAGS_schema.empty());
  {
    // const uint32_t nums[6] = {2010, 3, 4, 0, 0, 0};
   // CHECK_EQ(62, EpochTime(nums) / 86400);
  }
  const gpb::Descriptor* descr = LoadDescriptor();
  if (!descr)
    return -1;

  if (argc < 2)
    return 0;

  for (int i = 0; i < descr->field_count(); ++i) {
    if (descr->field(i)->cpp_type() == gpb::FieldDescriptor::CPPTYPE_MESSAGE) {
      cerr << "Can not have msg types in csv schema";
      return -1;
    }
  }
  gpb::DynamicMessageFactory msg_factory(importer.pool());
  const gpb::Message* prototype = msg_factory.GetPrototype(descr);
  CHECK(prototype);

  file_util::RecursivelyCreateDir(FLAGS_output_dir, 0777);

  string base_path = file_util::JoinPath(FLAGS_output_dir, FLAGS_table);
  std::unique_ptr<VolumeWriter> volume_writer(new VolumeWriter(base_path));
  CHECK_STATUS(volume_writer->Open());

  std::vector<pb::FieldDfsNode> node_array = pb::FromDescr(descr, PrefixEligible);
  std::unique_ptr<pb::MessageSlicer> message_slicer(
      new pb::MessageSlicer(descr, node_array.data(), node_array.size(),
                            volume_writer.get()));

  using TaskPool = util::SingleProducerTaskPool<CsvTask, string>;
  std::unique_ptr<TaskPool> pool(new TaskPool("pool", 10, 4 /* num threads*/ ));
  pool->Launch(prototype, message_slicer.get(), volume_writer.get());

  {
    string schema;
    PrintSchema(descr, &schema);

    LOG(INFO) << "Schema: " << schema;
  }

  for (int i = 1; i < argc; ++i) {
    LOG(INFO) << "Reading " << argv[i];

    StringPiece fname(argv[i]);

    if (FLAGS_parallel) {
      pool->RunTask(fname.as_string());
    } else {
      pool->RunInline(fname.as_string());
    }
  }
  pool->WaitForTasksToComplete();
  pool->Finalize();
  pool.reset();

  CHECK_STATUS(volume_writer->Finalize());

  puma::storage::Table tbl;
  FillTableSchema(descr, &tbl);
  string tbl_name = StrCat(base_path, ".idx");
  file_util::WriteStringToFileOrDie(tbl.SerializeAsString(), tbl_name);

  vector<pair<size_t, uint32_t>> slice_sizes;
  for (size_t i = 0; i < volume_writer->num_slices(); ++i) {
    const puma::storage::SliceIndex& si = volume_writer->slice_index(i);
    size_t slice_len = 0;
    for (const auto& slice : si.slice()) {
      slice_len += (slice.len() + slice.dict_len());
    }
    slice_sizes.emplace_back(slice_len, i);
  }

  std::sort(slice_sizes.rbegin(), slice_sizes.rend());
  LOG(INFO) << "TOPK heavy slices: ";
  size_t total_sz = 0;
  for (size_t j = 0; j < slice_sizes.size() && j < 20; ++j) {
    LOG(INFO) << j << ": " << volume_writer->slice_index(slice_sizes[j].second).name() << " "
              << slice_sizes[j].first << " bytes";
    total_sz += slice_sizes[j].first;
  }
  LOG(INFO) << "Total topK: " << total_sz << " bytes out of " << volume_writer->volume_size()
            << " or " << total_sz * 100 / volume_writer->volume_size() << " percent";
  return 0;
}
