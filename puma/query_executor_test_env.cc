// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/query_executor_test_env.h"

#include "base/logging.h"
#include "base/map-util.h"

#include "file/file_util.h"

#include "puma/query_executor.h"

#include "puma/file_fence_writer_factory.h"

#include "puma/file/slice_provider.h"
#include "puma/pb/util.h"

#include "strings/strcat.h"
#include "strings/strpmr.h"

#include "util/status.h"

using util::Status;
using util::StatusCode;
using util::StatusObject;
using namespace std;
using namespace placeholders;

namespace puma {
using schema::Attribute;

namespace {

const char kDataDir[] = "/tmp";

}  // namespace


void TestDriver::Error(const location& l, const std::string& s)  {
  LOG(ERROR) << "Parse error at " << l << ": " << s;
}

TestColumnProvider::~TestColumnProvider() {
  VLOG(1) << "Destroying provider " << this;
}

template <typename T> Status TestColumnProvider::GetPageT(uint32 max_size,
  const std::vector<pair<T, uint32>>& c, SlicePtr dest, uint32* fetched_size) {
  VLOG(1) << "TestColumnProvider: " << this << " " << index_
          << "/" << cur_val_offset_ << " of " << c.size();

  T* arr = reinterpret_cast<T*>(dest.u8ptr);

  *fetched_size = 0;
  for (; index_ < c.size(); ++index_) {
    unsigned left = c[index_].second - cur_val_offset_;
    unsigned page_space = max_size - *fetched_size;
    VLOG(2) << "index " << index_ << ", left: " << left << ", space: " << page_space;

    auto val = c[index_].first;
    if (left > page_space)  {
      std::fill(arr, arr + page_space, val);
      cur_val_offset_ += page_space;
      *fetched_size = max_size;
      has_data_to_read_ = true;

      return Status::OK;
    }

    std::fill(arr, arr + left, val);
    arr += left;
    *fetched_size += left;
    cur_val_offset_ = 0;
  }

  has_data_to_read_ = false;
  return Status::OK;
}

template<> Status TestColumnProvider::GetPageT<StringPiece>(uint32 max_size,
  const std::vector<pair<StringPiece, uint32>>& c, SlicePtr dest, uint32* fetched_size) {
  VLOG(1) << "TestColumnProvider: " << this << " " << index_
          << "/" << cur_val_offset_ << " of " << c.size();

  uint8_t* arr = dest.u8ptr;

  *fetched_size = 0;
  for (; index_ < c.size(); ++index_) {
    unsigned page_space = max_size - *fetched_size;
    VLOG(2) << "index " << index_ << ", space: " << page_space;

    StringPiece val = c[index_].first;
    if (!val.empty()) {
      unsigned left = c[index_].second - cur_val_offset_;
      unsigned can_fit = page_space / val.size();
      if (left < can_fit)
        can_fit = left;
      for (unsigned i = 0; i < can_fit; ++i) {
        memcpy(arr, val.data(), val.size());
        arr += val.size();
      }
      *fetched_size += val.size() * can_fit;
      cur_val_offset_ += can_fit;

      if (can_fit < left) {
        // no more data can fit.
        has_data_to_read_ = true;
        return Status::OK;
      }
    }
    cur_val_offset_ = 0;
  }

  has_data_to_read_ = false;
  return Status::OK;
}

Status TestColumnProvider::GetPage(uint32 max_size, SlicePtr dest, uint32* fetched_size) {
  max_size = std::min<uint32>(max_size, page_size_);

  switch(dt()) {
      case DataType::BOOL:
        return GetPageT(max_size, def_, dest, fetched_size);
      case DataType::INT64:
        return GetPageT<int64>(max_size, data64, dest, fetched_size);
      case DataType::UINT32:
        return GetPageT<uint32>(max_size, data_u32_, dest, fetched_size);
      case DataType::INT32:
        return GetPageT<int32>(max_size, data32_, dest, fetched_size);
      case DataType::DOUBLE:
        return GetPageT<double>(max_size, dval, dest, fetched_size);
      case DataType::STRING:
        return GetPageT(max_size, str_vals_, dest, fetched_size);

      default:
        LOG(FATAL) << "Not implemented " << dt();
  }
  return Status(StatusCode::INTERNAL_ERROR);
}

void TestColumnProvider::AddStrData(StringPiece src, uint32 count) {
  str_buf_.push_back(src.as_string());

  // final stringpiece will be set in OnFinalize function because str_buf_ can grow later.
  str_vals_.emplace_back(StringPiece(), count);
}

void TestColumnProvider::OnFinalize() {
  CHECK_EQ(str_buf_.size(), str_vals_.size());
  for (unsigned i = 0; i < str_buf_.size(); ++i) {
    str_vals_[i].first = StringPiece(str_buf_[i]);
  }
}

QueryWriter::~QueryWriter() {
  for (auto c : columns_)
    delete c;
}


void QueryWriter::InitFromFence(FileFenceWriterFactory* factory, const FenceRunnerBase& fence) {
  CHECK(schema_.empty() && fence.OutputSize() > 0);

  schema_.resize(fence.OutputSize());
  columns_.resize(schema_.size());

  for (size_t i = 0; i < schema_.size(); ++i) {
    AstExpr e = fence.GetOutExpr(i);
    DCHECK_NE(DataType::NONE, e.data_type());

    schema_[i] = e.data_type();
    columns_[i] = new Column(pmr::get_default_resource());
    columns_[i]->Init(schema_[i], 1);
  }

  if (fence.type() == ParsedFence::SINGLE_ROW) {
    for (size_t i = 0; i < schema_.size(); ++i) {
      AstExpr e = fence.GetOutExpr(i);
      columns_[i]->Append(&e.variant(), e.is_defined());
    }

    return;
  }

  for (size_t i = 0; i < schema_.size(); ++i) {
    AstExpr expr = fence.GetOutExpr(i);
    CHECK_EQ(AstOp::LOAD_FN, expr.op());

    auto cb = [this, i](const NullableColumn& src) {
      columns_[i]->AppendFrom(src, &arena_);
    };

    factory->ReadColumn(expr, cb);
  }
}

TestSliceManager::~TestSliceManager() {
  for (auto& k_v : bare_slice_map_)
    delete k_v.second;
}

TestColumnProvider* TestSliceManager::AddSlice(FieldClass fc, StringPiece name, DataType dt) {
  string full_path = pb::FieldClassPath(name, fc);
  TestColumnProvider* res = new TestColumnProvider(full_path, dt);
  auto insert_res = bare_slice_map_.emplace(res->name(), res);
  CHECK(insert_res.second);
  return res;
}

StatusObject<BareSliceProvider*>
    TestSliceManager::GetReaderInternal(std::string full_name, DataType dt) {
  auto it = bare_slice_map_.find(full_name);
  VLOG(1) << "GetBareSliceProvider: " << full_name << "/" << dt << ", found: "
          << int(it != bare_slice_map_.end());

  if (it == bare_slice_map_.end()) return nullptr;

  TestColumnProvider* provider = new TestColumnProvider(*it->second);
  provider->OnFinalize();
  allocated_providers_.emplace_back(provider);

  return provider;
}

void TestSliceManager::FreeSlices() {
  allocated_providers_.clear();
}

TestColumnProvider* TestSliceManager::FindArrayLen(StringPiece name) {
  string bare_name = pb::FieldClassPath(name, FieldClass::ARRAY_SIZE);

  return FindValueWithDefault(bare_slice_map_, bare_name, nullptr);
}

QueryExecutorEnv::QueryExecutorEnv() : pool_("test") {
  handler_.Init(&pool_);

  file_factory_.reset(new FileFenceWriterFactory(kDataDir));

  arena_.reset(new pmr::monotonic_buffer_resource);
  slice_mgr_.reset(new TestSliceManager);
}

QueryExecutorEnv::~QueryExecutorEnv() {
}


void QueryExecutorEnv::AddString(StringPiece str, uint32_t count, TestColumnProvider* data,
                                 TestColumnProvider* lengths) {
  lengths->AddU32(str.size() + 1, count);
  data->AddStrData(str, count);
}

std::pair<TestColumnProvider*, TestColumnProvider*>
  QueryExecutorEnv::AddColumn(const string& name, DataType dt,
                              Attribute::Ordinality ordinality,
                              const Attribute* parent) {
  const Attribute* attr = pool_.Add(name, dt, ordinality, parent);

  std::pair<TestColumnProvider*, TestColumnProvider*> res;
  string full_name = attr->Path();
  res.first = slice_mgr_->AddSlice(FieldClass::DATA, full_name, dt);

  if (dt == DataType::STRING) {
    res.second = slice_mgr_->AddSlice(FieldClass::STRING_LEN, full_name, kStringLenDt);
  } else if (ordinality == Attribute::NULLABLE) {
    res.second = slice_mgr_->AddSlice(FieldClass::OPTIONAL, full_name, DataType::BOOL);
  }

  if (ordinality == Attribute::REPEATED) {
    slice_mgr_->AddSlice(FieldClass::ARRAY_SIZE, full_name, kArrayLenDt);
  }

  return res;
}


Status QueryExecutorEnv::ParseAndRun(const string& s) {
  handler_.Reset();

  yyscan_t scanner = handler_.scanner();
  YY_BUFFER_STATE buffer_state = puma_scan_bytes(s.data(), s.size(), scanner);
  puma_parser parser(scanner, &handler_);
  int res = parser.parse();

  puma_delete_buffer(buffer_state, scanner);
  CHECK_EQ(0, res) << s;
  Status st = Run();
  run_status_ = st;

  return run_status_;
}

Status QueryExecutorEnv::Run(bool extract_output) {
  QueryContext qc{file_factory_->slice(),  // for loading fence columns.
                  file_factory_->writer()  // for writing fence columns.
                 };
  qc.slice_manager = slice_mgr_.get();

  executor_.reset(new QueryExecutor(qc));

  RETURN_IF_ERROR(executor_->Run(handler_));

  const FenceRunnerBase* fence = executor_->barrier();
  CHECK(fence);

  if (extract_output) {
    writer_.reset(new QueryWriter);
    writer_->InitFromFence(file_factory_.get(), *fence);
  }
  return Status::OK;
}

}  // namespace puma
