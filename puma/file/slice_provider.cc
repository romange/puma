// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/file/slice_provider.h"

#include <numeric>

#include "base/logging.h"
#include "base/stl_util.h"

#include "file/file_util.h"
#include "file/filesource.h"
#include "puma/pb/util.h"

#include "strings/strcat.h"
#include "strings/strip.h"
#include "strings/util.h"

using std::string;
using base::Status;

constexpr unsigned kMaxIntsInBlock = 1 << 17;

namespace puma {

namespace {

std::pair<std::unique_ptr<util::Source>, FileHeader> OpenCol(StringPiece filename) {
  auto status_obj = file::ReadonlyFile::Open(filename);
  CHECK(status_obj.ok()) << status_obj.status << " " << filename;

  std::unique_ptr<util::Source> src(new file::Source(status_obj.obj));

  std::array<uint8, FileHeader::kMaxLen> buf;
  auto res = src->Read(strings::MutableByteRange(buf));
  CHECK_STATUS(res.status);

  CHECK_EQ(res.obj, buf.size());

  FileHeader header;
  CHECK(header.Parse(buf.begin()));

  return make_pair(std::move(src), header);
}

}  // namespace

ColumnProvider::ColumnProvider(const std::string& bare_name,
                               util::Source* src, const FileHeader& fh)
  : BareSliceProvider(bare_name, fh.dt), fh_(fh), col_(src)  {

  util::Source* lz4src = col_.get();

  DataType dt = fh_.dt;

  if (base::_in(dt, {DataType::UINT32, DataType::INT32})) {
    CHECK(base::_in(fh.cc, {FileHeader::LZ4INT_CODEC, FileHeader::ZSTD_CODEC}));

    decoder_.reset(new FileLz4Decoder(kMaxIntsInBlock, lz4src));
  } else if (base::_in(dt, {DataType::INT64, DataType::DOUBLE})) {
    CHECK_EQ(FileHeader::LZ4INT_CODEC, fh.cc);

    decoder64_.reset(new FileDecoder64(kMaxIntsInBlock, lz4src));
  } else {
    blob_decoder_.reset(new ZstdBlobDecoder(col_.get()));
  }
  has_data_to_read_ = true;
}


ColumnProvider* ColumnProvider::Open(const std::string& file_name) {
  auto res = OpenCol(file_name);

  return new ColumnProvider(file_name, res.first.release(), res.second);
}


Status ColumnProvider::GetPage(uint32 max_size, SlicePtr dest, uint32* fetched_size) {
  if (decoder_) {
    // Needed to make sure since we override lengths with stringpieces.
    static_assert(sizeof(StringPiece) > sizeof(uint32), "");

    uint32* uintptr = dest.u32ptr;


    GET_UNLESS_ERROR(len_read, decoder_->Read(max_size, uintptr));
    *fetched_size = len_read;
    has_data_to_read_ = (len_read > 0);

    return Status::OK;
  }

  if (decoder64_) {
    uint64* intptr = dest.u64ptr;
    GET_UNLESS_ERROR(len_read, decoder64_->Read(max_size, intptr));
    *fetched_size = len_read;
    has_data_to_read_ = len_read > 0;

    return Status::OK;
  }

  DCHECK(blob_decoder_);
  GET_UNLESS_ERROR(len_read, blob_decoder_->Read(max_size, dest.u8ptr));
  *fetched_size = len_read;
  has_data_to_read_  = (len_read == max_size);

  return Status::OK;
}


BareSliceProvider* CreateProvider(StringPiece dir, FieldClass fc, StringPiece col, DataType dt) {
  string base_name = file_util::JoinPath(dir, col);
  string full_path = pb::FieldClassPath(base_name, fc);

  ColumnProvider* provider = ColumnProvider::Open(full_path);
  CHECK(provider);
  CHECK_EQ(dt, provider->dt());

  return provider;
}


}  // namespace puma

