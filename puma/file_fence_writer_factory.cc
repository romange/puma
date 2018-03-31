// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/file_fence_writer_factory.h"

#include "base/logging.h"

#include "file/file_util.h"
#include "file/filesource.h"

#include "puma/file/slice_provider.h"
#include "puma/file/slice_writer_impl.h"
#include "puma/load_operator.h"
#include "puma/pb/util.h"

#include "strings/strcat.h"

using namespace std::placeholders;
using namespace std;
using strings::Range;
using strings::ByteRange;

namespace puma {

class SliceWriterFactory {
 public:
  SliceWriterFactory(const std::string& root_dir) : root_dir_(root_dir) {}

  SliceWriterBase* Create(std::string file_name, FieldClass field_class, DataType dt);
  virtual ~SliceWriterFactory() {}

 protected:
  virtual SliceWriterBase* CreateInternal(std::string file_name, DataType dt);

  std::string FullPath(StringPiece file_name, FieldClass field_class) const;

  std::string root_dir_;
};


namespace {

inline Range<const int32*> IntRange(const VariantArray& arr) {
  return Range<const int32*>(
      reinterpret_cast<const int32*>(arr.column_ptr().u32ptr), arr.size());
}

inline Range<const int64*> LongRange(const VariantArray& arr) {
  return Range<const int64*>(arr.column_ptr().i64ptr, arr.size());
}

inline ByteRange BoolRange(const VariantArray& arr) {
  return ByteRange(arr.column_ptr().u8ptr, arr.size());
}


// TODO: consider bufferring similarly to field_slicer.
class FileFenceWriter : public FenceOutputWriter {
 public:
  FileFenceWriter(StringPiece name, DataType dt, SliceWriterFactory* factory,
                  pmr::memory_resource* mr);
  ~FileFenceWriter();

  void Finalize() override;

 private:
  virtual void AppendInternal(const strings::ByteRange& def, const VariantArray& arr) override;

  std::unique_ptr<SliceWriterBase> data_writer_;
  std::unique_ptr<SliceWriterBase> str_len_writer_;
  std::unique_ptr<SliceWriterBase> def_writer_;
  base::PODArray<int32> str_len_;
};


FileFenceWriter::FileFenceWriter(StringPiece name, DataType dt, SliceWriterFactory* factory,
                                 pmr::memory_resource* mr)
   : FenceOutputWriter(name, dt, mr) {

  data_writer_.reset(factory->Create(name_, FieldClass::DATA, dt));
  def_writer_.reset(factory->Create(name_, FieldClass::OPTIONAL, DataType::BOOL));

  if (DataType::STRING == dt) {
    str_len_writer_.reset(factory->Create(name_, FieldClass::STRING_LEN, kStringLenDt));
  }
}

void FileFenceWriter::Finalize() {
  VLOG(1) << "Finalizing " << name_;

  data_writer_->Finalize();
  def_writer_->Finalize();
  if (str_len_writer_)
    str_len_writer_->Finalize();
}



void FileFenceWriter::AppendInternal(const strings::ByteRange& def, const VariantArray& data) {
  def_writer_->AddChunk(def);

  switch (data_type()) {
    case DataType::BOOL:
      data_writer_->AddChunk(BoolRange(data));
    break;
    case DataType::UINT32:
    case DataType::INT32:
      data_writer_->AddChunk32(IntRange(data));
    break;
    case DataType::INT64:
    case DataType::DOUBLE:
      data_writer_->AddChunk64(LongRange(data));
    break;
    case DataType::STRING: {
      str_len_.resize(data.size());

      for (size_t i = 0; i < data.size(); ++i) {
        StringPiece str = data.at<StringPiece>(i);
        CHECK_LT(str.size(), 1 << 16);
        str_len_[i] = str.size() + 1;
        data_writer_->AddChunk(str);
      }

      str_len_writer_->AddChunk32(Range<const int32 *>{str_len_.begin(), str_len_.end()});
    }
    break;
    default:
      LOG(FATAL) << "Not supported " << data_type();
  }
}


FileFenceWriter::~FileFenceWriter() {

}


} // namespace


std::string SliceWriterFactory::FullPath(StringPiece file_name, FieldClass field_class) const {
  string path = file_util::JoinPath(root_dir_, file_name);

  return pb::FieldClassPath(path, field_class);
}

SliceWriterBase* SliceWriterFactory::Create(std::string file_name,
                                              FieldClass field_class, DataType dt) {
  string path = FullPath(file_name, field_class);

  return CreateInternal(path, dt);
}

SliceWriterBase* SliceWriterFactory::CreateInternal(
      std::string file_name, puma::DataType dt) {
  CHECK_NE(DataType::NONE, dt);
  file::WriteFile* file = file::Open(file_name);
  CHECK(file) << file_name;

  util::Sink* sink = new file::Sink(file, TAKE_OWNERSHIP);
  uint8 buf[FileHeader::kMaxLen];

  FileHeader fh = FileHeader::FromDt(dt);
  fh.Write(buf);
  CHECK_STATUS(file->Write(buf, FileHeader::kMaxLen));

  return new SliceWriterImpl(file_name, fh, sink);
}

FileFenceWriterFactory::FileFenceWriterFactory(const std::string& dir, pmr::memory_resource* mgr)
: dir_(dir), mgr_(mgr ? mgr : pmr::get_default_resource()) {
  factory_.reset(new SliceWriterFactory(dir));
}

FileFenceWriterFactory::~FileFenceWriterFactory() {

}


FenceWriterFactory FileFenceWriterFactory::writer() {
  return std::bind(&FileFenceWriterFactory::CreateWriter, this, _1, _2);
}

BareSliceProviderFactory FileFenceWriterFactory::slice() {
  return std::bind(&FileFenceWriterFactory::CreateProviderInternal, this, _1, _2, _3);
}

BareSliceProvider* FileFenceWriterFactory::CreateProviderInternal(
    FieldClass fc, StringPiece name, DataType dt) {
  BareSliceProvider* res = CreateProvider(dir_, fc, name, dt);

  allocated_providers_.emplace_back(res);
  return res;
}


FenceOutputWriter* FileFenceWriterFactory::CreateWriter(StringPiece col, DataType dt) {
  return new FileFenceWriter(col, dt, factory_.get(), mgr_);
}

void FileFenceWriterFactory::ReadColumn(const AstExpr& load_expr, ColumnCb cb) const {
  unique_ptr<LoadOperator> ld(AllocateLoadOperator(load_expr));

  ColumnConsumer consumer = ld->result();

  while (true) {
    CHECK_STATUS(ld->Apply());

    size_t available = consumer.AvailableData();
    if (available == 0)
      break;
    cb(consumer.column());

    consumer.ConsumeHighMark(available);
    ld->DiscardConsumed();
  }
}

LoadOperator* FileFenceWriterFactory::AllocateLoadOperator(const AstExpr& load_expr) const {
  BareSliceProvider* data = CreateProvider(dir_, FieldClass::DATA,
                                           load_expr.str(), load_expr.data_type());
  BareSliceProvider* def = nullptr;

  if (load_expr.data_type() == DataType::STRING) {
    def = CreateProvider(dir_, FieldClass::STRING_LEN, load_expr.str(), kStringLenDt);
  } else {
    def = CreateProvider(dir_, FieldClass::OPTIONAL, load_expr.str(), DataType::BOOL);
  }

  LoadOperator* pld = new LoadOperator(load_expr, data, def, TAKE_OWNERSHIP);
  return pld;
}

}  // namespace puma
