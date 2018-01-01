// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <mutex>

#include <gperftools/malloc_extension.h>

#include "base/init.h"
#include "base/pod_array.h"
#include "base/stl_util.h"

#include "file/file_util.h"
#include "file/list_file.h"
#include "file/proto_writer.h"

#include "puma/file/volume_writer.h"
#include "puma/pb/message_slicer.h"
#include "puma/pb/util.h"
#include "puma/storage.pb.h"

#include "strings/split.h"
#include "strings/strcat.h"
#include "strings/util.h"

#include "util/map-util.h"
#include "util/tools/pprint_utils.h"

#include "util/sp_task_pool.h"

DEFINE_string(output_dir, "", "output col directory");
DEFINE_string(table, "", "table name");
DEFINE_string(prefix_filter, "", "if defined, writes only files that start with this prefix");
DEFINE_int32(frame_size, (1 << 20) * 32, "frame size");

DEFINE_bool(parallel, true, "");
DEFINE_string(unordered_set, "", "");

using std::string;
using puma::DataType;
using base::StatusCode;

using namespace puma::pb;

namespace puma {

using storage::SliceIndex;

bool PrefixEligible(StringPiece path) {
  if (FLAGS_prefix_filter.empty() || path.empty())
    return true;
  return path.starts_with(FLAGS_prefix_filter) ||
    strings::HasPrefixString(FLAGS_prefix_filter, path);
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

struct TaskData {
  std::atomic_long num_records = ATOMIC_VAR_INIT(0);
};

class LstTask {
 public:
  typedef TaskData* SharedData;

  void InitShared(SharedData d) { shared_data_ = d;}

  void operator()(const std::string& obj);

  LstTask(gpb::Message* to_clone, const MessageSlicer* slicer, VolumeWriter* vw)
  : slicer_state_(slicer->CreateState()), vw_(vw) {
    local_msg_.reset(to_clone->New());

    slicer_ = slicer;
  }

  void Finalize() {
    slicer_state_->FlushFrame(vw_);
  }
 private:
  std::unique_ptr<gpb::Message> local_msg_;
  const MessageSlicer* slicer_;
  std::unique_ptr<SlicerState> slicer_state_;
  VolumeWriter* vw_;
  TaskData* shared_data_ = nullptr;
};

void LstTask::operator()(const std::string& filename) {
  file::ListReader reader(filename);
  string record_buf;
  StringPiece record;

  while (reader.ReadRecord(&record, &record_buf)) {
    shared_data_->num_records.fetch_add(1, std::memory_order_relaxed);

    CHECK(local_msg_->ParseFromArray(record.data(), record.size()));

    slicer_->Add(*local_msg_, slicer_state_.get());

    unsigned msg_cnt = slicer_state_->msg_cnt() + 1;
    if (msg_cnt % 10 == 0) {
      ssize_t sz = slicer_state_->OutputCost();
      if (sz >= FLAGS_frame_size) {
        size_t vol_sz = slicer_state_->FlushFrame(vw_);
        LOG(INFO) << "Flushing frame with " << msg_cnt - 1 << " records and cost "
                  << sz << " and filesize " << vol_sz;

      }
    }
  }
}

}  // namespace puma

FdPath FromPath(const gpb::Descriptor* root, StringPiece path) {
  std::vector<StringPiece> parts = strings::Split(path, ".");
  CHECK(!parts.empty()) << path;
  const gpb::Descriptor* cur_descr = root;

  FdPath res;
  for (size_t j = 0; j < parts.size(); ++j) {
    const gpb::FieldDescriptor* field = nullptr;
    field = cur_descr->FindFieldByName(parts[j].as_string());

    CHECK(field) << "Can not find tag id " << parts[j];
    if (j + 1 < parts.size()) {
      CHECK_EQ(field->cpp_type(), gpb::FieldDescriptor::CPPTYPE_MESSAGE);
      cur_descr = field->message_type();
    }
    res.push_back(field);
  }

  return res;
}

vector<FdPath> ParseUnorderedMeta(const gpb::Descriptor* root) {
  if (FLAGS_unordered_set.empty())
    return vector<FdPath>();

  std::vector<StringPiece> parts = strings::Split(FLAGS_unordered_set, ",");
  vector<FdPath> paths;
  paths.reserve(parts.size());


  for (const StringPiece& val : parts) {
    paths.emplace_back(FromPath(root, val));
  }
  return paths;
}

typedef std::function<void(AnnotatedFd*)> AnnotatedFdCallback;

bool CallOnLeaf(const FdPath& path, vector<FieldDfsNode>* nodes,
                AnnotatedFdCallback cb) {
  size_t index = 0;
  size_t end = nodes->size();

  for (size_t depth = 0; depth < path.size(); ++depth) {
    const gpb::FieldDescriptor* desired = path.path()[depth];

    while (index < end) {
      FieldDfsNode& node = nodes->at(index);

      if (node.fd.get() != desired) {
        index += node.dfs_offset;
        continue;
      }

      if (depth == path.size() - 1) {
        cb(&node.fd);
        return true;
      }

      end = index + node.dfs_offset;
      ++index;
      break;
    }
    if (index == end)
      return false;
  }
  return false;
}

using TaskPool = util::SingleProducerTaskPool<puma::LstTask, string>;

int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  namespace gpb = ::google::protobuf;

  CHECK(!FLAGS_output_dir.empty());
  CHECK(!FLAGS_table.empty());

  file_util::RecursivelyCreateDir(FLAGS_output_dir, 0777);

  std::unique_ptr<gpb::Message> tmp_msg;
  const gpb::Descriptor* descr = nullptr;
  string ptype, fd_set;
  std::unique_ptr<puma::VolumeWriter> volume_writer;

  std::unique_ptr<TaskPool> pool;

  pool.reset(new TaskPool("pool", 10, 4 /* num threads*/ ));

  if (argc < 2)
    return 0;

  string base_path = file_util::JoinPath(FLAGS_output_dir, FLAGS_table);
  std::unique_ptr<MessageSlicer> message_slicer;
  puma::TaskData global_data;


  {
    file::ListReader reader(argv[1]);
    std::map<string, string> meta;


    CHECK(reader.GetMetaData(&meta)) << "Failed to read meta data";
    ptype = FindOrDie(meta, file::kProtoTypeKey);
    fd_set = FindOrDie(meta, file::kProtoSetKey);

    tmp_msg.reset(util::pprint::AllocateMsgByMeta(ptype, fd_set));
    descr = tmp_msg->GetDescriptor();

    std::vector<FdPath> unordered = ParseUnorderedMeta(descr);

    std::vector<FieldDfsNode> node_array = puma::pb::FromDescr(descr, &puma::PrefixEligible);

    if (node_array.empty()) {
      fprintf(stderr, "No fields are found\n");
      return -1;
    }

    for (const FdPath& path : unordered) {
      CHECK(CallOnLeaf(path, &node_array,
                       [](AnnotatedFd* dest) { dest->is_unordered_set = true;}));
    }

    volume_writer.reset(new puma::VolumeWriter(base_path));
    CHECK_STATUS(volume_writer->Open());

    message_slicer.reset(new MessageSlicer(descr, node_array.data(), node_array.size(),
                                           volume_writer.get()));
    pool->SetSharedData(&global_data);

    pool->Launch(tmp_msg.get(), message_slicer.get(), volume_writer.get());

    string schema;
    puma::PrintSchema(descr, &schema);

    LOG(INFO) << "Schema: " << schema;
  }

  LOG(INFO) << "Running in parallel " << pool->thread_count() << " threads";

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

  char buf[2048];
  MallocExtension::instance()->GetStats(buf, arraysize(buf));
  LOG(INFO) << "Memory stats: " << buf;


  CHECK_STATUS(volume_writer->Finalize());

  puma::storage::Table tbl;
  tbl.set_pb_type(ptype);
  tbl.set_fd_set(fd_set);
  string tbl_name = StrCat(base_path, ".idx");
  file_util::WriteStringToFileOrDie(tbl.SerializeAsString(), tbl_name);
  LOG(INFO) << "Processed " << global_data.num_records.load() << " records";

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
  for (size_t j = 0; j < slice_sizes.size() && j < 50; ++j) {
    LOG(INFO) << j << ": " << volume_writer->slice_index(slice_sizes[j].second).name() << " "
              << slice_sizes[j].first << " bytes";
    total_sz += slice_sizes[j].first;
  }
  LOG(INFO) << "Total topK: " << total_sz << " bytes out of " << volume_writer->volume_size()
            << " or " << total_sz * 100 / volume_writer->volume_size() << " percent";
  return 0;
}
