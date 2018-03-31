// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <vector>
#include <string>
#include <unordered_map>

#include <pmr/monotonic_buffer_resource.h>

#include "strings/stringpiece.h"

#include "puma/fence_runner.h"
#include "puma/query_executor.h"
#include "puma/schema.h"
#include "puma/parser/puma_scan.h"

namespace puma {

class QueryExecutor;
class MemOnlyRepo;
class FileFenceWriterFactory;


class QueryWriter {
 public:
  typedef NullableColumn Column;
  QueryWriter() {}
  ~QueryWriter();

  bool is_initialized() const { return !schema_.empty(); }

  const std::vector<DataType>& schema() const { return schema_; }
  const NullableColumn& col(size_t i) const { return *columns_[i]; }
  NullableColumn& col(size_t i) { return *columns_[i]; };
  size_t size() const { return columns_.size(); }

  void InitFromFence(FileFenceWriterFactory* factory, const FenceRunnerBase& fence);
 private:

  pmr::monotonic_buffer_resource arena_;
  std::vector<DataType> schema_;
  std::vector<NullableColumn*> columns_;
};


class TestDriver : public ParserHandler {
public:
  void Error(const location& l, const std::string& s);
};


class TestColumnProvider : public BareSliceProvider {
  unsigned index_ = 0;
  unsigned cur_val_offset_ = 0;

  template <typename T> util::Status GetPageT(uint32 max_size,
      const std::vector<std::pair<T, uint32>>& c, SlicePtr dest, uint32* fetched_size);
 public:
  unsigned index() const { return index_;}

  TestColumnProvider(const TestColumnProvider&) = default;

  TestColumnProvider(std::string bare_name, DataType dt) : BareSliceProvider(bare_name, dt) {
    has_data_to_read_ = true;
  }

  ~TestColumnProvider();

  util::Status GetPage(uint32 max_size, SlicePtr dest, uint32* fetched_size) override;

  void Rewind() {
    index_ = cur_val_offset_ = 0;
    has_data_to_read_ = true;
  }

  void AddLen(uint32 len, uint32 count) { AddU32(len, count);}
  void Add64(int64 val, uint32 count) { data64.emplace_back(val, count);}
  void Add32(int32 val, uint32 count) { data32_.emplace_back(val, count);}
  void AddU32(uint32 val, uint32 count) { data_u32_.emplace_back(val, count);}

  void AddBool(bool b, uint32 count) { AddDef(b, count); }
  void AddDouble(double d, uint32 count) { dval.emplace_back(d, count);}
  void AddDef(bool val, uint32 count) { def_.emplace_back(val, count);}
  void ClearDef() { def_.clear(); }
  void AddStrData(StringPiece src, uint32_t count);

  void set_page_size(size_t p) { page_size_ = p;}


  std::vector<std::pair<int64, uint32>> data64;   // val64, count pairs

  void OnFinalize();
 private:
  std::vector<std::string> str_buf_;
  std::vector<std::pair<bool, uint32>> def_;   // val, count pairs
  std::vector<std::pair<double, uint32>> dval;   // val, count pairs
  std::vector<std::pair<uint32, uint32>> data_u32_;
  std::vector<std::pair<int32, uint32>> data32_;
  std::vector<std::pair<StringPiece, uint32>> str_vals_;   // val64, count pairs

  size_t page_size_ = (1 << 16);
};

class TestSliceManager : public SliceManager {
 public:
  ~TestSliceManager() override;

  TestColumnProvider* AddSlice(FieldClass fc, StringPiece name, DataType dt);

  TestColumnProvider* FindArrayLen(StringPiece name);

  void FreeSlices() override;

 protected:
  util::StatusObject<BareSliceProvider*> GetReaderInternal(std::string name, DataType dt) override;

  std::unordered_map<StringPiece, TestColumnProvider*> bare_slice_map_;
  std::vector<std::unique_ptr<BareSliceProvider>> allocated_providers_;
};

class QueryExecutorEnv {
 public:
  QueryExecutorEnv();

  ~QueryExecutorEnv();

  std::pair<TestColumnProvider*, TestColumnProvider*>
      AddColumn(const std::string& name, DataType dt, schema::Attribute::Ordinality ordinality,
                const schema::Attribute* parent = nullptr);

  schema::Table* table() { return &pool_;}

  util::Status Run(bool extract_output = true);

  QueryWriter* writer() { return writer_.get(); }

  const TestDriver& handler() const { return handler_;}

  util::Status ParseAndRun(const std::string& s);

  TestColumnProvider* GetLengthsBareSlice(StringPiece name) {
    return slice_mgr_->FindArrayLen(name);
  }

  void set_bare_slice_provider_factory(BareSliceProviderFactory f) { data_provider_factory_ = f; }

  StringPiece AllocStr(StringPiece str) { return names_.Get(str); }
  util::Status run_status() const { return run_status_; }

  void AddString(StringPiece str, uint32_t count, TestColumnProvider* data,
                 TestColumnProvider* lengths);
private:
  schema::Table pool_;
  UniqueStrings names_;

  TestDriver handler_;

  BareSliceProviderFactory data_provider_factory_;

  std::unique_ptr<QueryWriter> writer_;
  std::unique_ptr<pmr::monotonic_buffer_resource> arena_;
  std::unique_ptr<QueryExecutor> executor_;
  std::unique_ptr<FileFenceWriterFactory> file_factory_;
  std::unique_ptr<TestSliceManager> slice_mgr_;
  FenceWriterFactory wf_ = nullptr;
  util::Status run_status_;
};


inline unsigned long int operator"" _rep(unsigned long long int val) { return val; }

}  // namespace puma
