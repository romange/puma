// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <iostream>

#include "base/init.h"
#include "base/logging.h"
#include "base/walltime.h"

#include "file/file_util.h"

#include "puma/file/slice_provider.h"
#include "puma/file_fence_writer_factory.h"
#include "puma/load_operator.h"
#include "puma/parser/parser_handler.h"
#include "puma/parser/location.hh"  // for <<.
#include "puma/parser/puma_scan.h"

#include "puma/storage.pb.h"
#include "puma/pb/util.h"

#include "puma/schema.h"
#include "puma/query_executor.h"
#include "puma/file/volume_manager.h"

#include "strings/strcat.h"
#include "strings/strip.h"
#include "strings/util.h"

#include "util/pprint/pprint_utils.h"
#include "util/zstd_sinksource.h"

DEFINE_string(table, "", "input table idx file");
DEFINE_string(query, "", "");
DEFINE_bool(skip_print, false, "");

using util::Status;
using namespace std;
using namespace puma;
using namespace util::pprint;
using FD = util::pprint::gpb::FieldDescriptor;

class ClientDriver : public ParserHandler {
public:
  void Error(const location& l, const std::string& s) override {
    std::cerr << "Parse error at " << l << ": " << s << endl;
  }
};


using namespace std::placeholders;

string GetConstVal(const AstExpr& e) {
  DCHECK_EQ(AstOp::CONST_VAL, e.op());
  switch(e.data_type()) {
    case DataType::INT64:
    case DataType::BOOL:
      return std::to_string(e.variant().i64val);
    case DataType::STRING:
      return e.variant().strval.as_string();
    case DataType::DOUBLE:
      return std::to_string(e.variant().dval);
    case DataType::INT32:
      return std::to_string(e.variant().i32val);
    case DataType::UINT32:
      return std::to_string(e.variant().u32val);
    default:
      LOG(FATAL) << "Unsupported " << e.data_type();
    ;
  }
  return string();
}

void AppendColVal(const NullableColumn& col, size_t row, string* dest) {
  DCHECK_EQ(0, col.window_offset());

  if (!col.def()[row]) {
    dest->append("null");
    return;
  }

  switch(col.data_type()) {
    case DataType::INT64:
      StrAppend(dest, col.get<int64>(row));
    break;
    case DataType::BOOL:
      StrAppend(dest, col.get<uint8>(row) ? "true" : "false");
    break;
    case DataType::STRING:
      StrAppend(dest, col.get<StringPiece>(row));
    break;
    case DataType::DOUBLE:
      StrAppend(dest, col.get<double>(row));
    break;
    case DataType::UINT32:
      StrAppend(dest, col.get<uint32>(row));
    break;
    case DataType::INT32:
      StrAppend(dest, col.get<int32>(row));
    break;

    default:
      LOG(FATAL) << "Unsupported " << col.data_type();
    ;
  }
}

std::unique_ptr<schema::Table> BuildTable() {
  CHECK(strings::HasSuffixString(FLAGS_table, ".idx"));
  string idx;
  file_util::ReadFileToStringOrDie(FLAGS_table, &idx);

  storage::Table table_info;
  CHECK(table_info.ParseFromString(idx));
  LOG(INFO) << "PB Type: " << table_info.pb_type();

  std::unique_ptr<gpb::Message> tmp_msg(AllocateMsgByMeta(table_info.pb_type(),
                                                          table_info.fd_set()));
  const gpb::Descriptor* descr = tmp_msg->GetDescriptor();
  LOG(INFO) << "Schema: \n" << descr->DebugString();

  string table_name;
  StringPiece table_full_name = file_util::GetNameFromPath(FLAGS_table);
  TryStripSuffixString(table_full_name, ".idx", &table_name);

  return std::unique_ptr<schema::Table>(pb::FromDescriptor(table_name, descr));
}

void ParseQuery(ClientDriver* driver) {
  yyscan_t scanner = driver->scanner();
  YY_BUFFER_STATE buffer_state = puma_scan_bytes(FLAGS_query.data(), FLAGS_query.size(), scanner);

  puma_parser parser(scanner, driver);
  int res = parser.parse();

  puma_delete_buffer(buffer_state, scanner);
  CHECK_EQ(0, res) << FLAGS_query;
}

void PrintTable(const FenceRunnerBase* fence, const FileFenceWriterFactory& repo) {
  string row;
  size_t out_size = fence->OutputSize();
  vector<unique_ptr<LoadOperator>> outs(out_size);
  for (size_t i = 0; i < out_size; ++i) {
    outs[i].reset(repo.AllocateLoadOperator(fence->GetOutExpr(i)));
  }

  while (true) {
    size_t min_result = kuint32max, max_result = 0;;

    for (size_t i = 0; i < out_size; ++i) {
      CHECK_STATUS(outs[i]->Apply());
      min_result = std::min(min_result, outs[i]->result_size());
      max_result = std::max(max_result, outs[i]->result_size());
    }
    if (min_result == 0) {
      CHECK_EQ(0, max_result);
      break;
    }

    for (size_t row_id = 0; row_id < min_result; ++row_id) {
      string row;
      for (size_t j = 0; j < out_size; ++j) {
        ColumnConsumer result = outs[j]->result();
        AppendColVal(result.column(), row_id, &row);
        if (j + 1 < out_size)
          row.append(",");
      }
      puts(row.c_str());
    }

    for (size_t i = 0; i < out_size; ++i) {
      outs[i]->result().ConsumeHighMark(min_result);
      outs[i]->DiscardConsumed();
    }
  }
}


int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  if (FLAGS_query.empty()) {
    cerr << "No query was given\n";
    return 1;
  }

  std::unique_ptr<schema::Table> table = BuildTable();

  ClientDriver driver;
  driver.Init(table.get());
  ParseQuery(&driver);

  string vol_path;
  CHECK(TryStripSuffixString(FLAGS_table, ".idx", &vol_path));

  VolumeManager vmgr(pmr::get_default_resource());
  CHECK_STATUS(vmgr.Open(vol_path));

  FileFenceWriterFactory factory("/tmp");

  QueryContext qc{factory.slice(), factory.writer()};
  qc.slice_manager = &vmgr;

  MicrosecondsInt64 start = GetMonotonicMicros();
  QueryExecutor executor(qc);
  CHECK_STATUS(executor.Run(driver));
  const FenceRunnerBase* fence = executor.barrier();

  CHECK_GT(fence->OutputSize(), 0);

  MicrosecondsInt64 delta = GetMonotonicMicros() - start;

  if (!FLAGS_skip_print) {
    if (fence->type() == ParsedFence::SINGLE_ROW) {
      string row;

      for (size_t i = 0; i < fence->OutputSize(); ++i) {
        AstExpr e = fence->GetOutExpr(i);
        row.append(GetConstVal(e)).append(",");
      }
      row.pop_back();
      cout << row << endl;
    } else {
      PrintTable(fence, factory);
    }
  }
  size_t msec = (1 | (delta / 1000));
  cout << msec << "ms, processed bytes " << executor.consumed_bytes() / msec
       << " bytes/msec, file read:" << vmgr.bytes_read() / msec << " bytes/msec\n";

  return 0;
}
