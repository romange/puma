// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "puma/file/bare_slice_provider.h"

#include <pmr/monotonic_buffer_resource.h>

#include "puma/file/slice_format.h"
#include "util/sinksource.h"

namespace puma {

/* Old style file per slice format. Should be deprecated */
class ColumnProvider : public BareSliceProvider {
 public:
  ~ColumnProvider() {}

  static ColumnProvider* Open(const std::string& file_name);

  base::Status GetPage(uint32 max_size, SlicePtr dest, uint32* fetched_size) override;

  const FileHeader& file_header() const { return fh_; }

 private:
  ColumnProvider(const std::string& bare_name, util::Source* src, const FileHeader& fh);

  FileHeader fh_;
  std::unique_ptr<util::Source> col_;

  std::unique_ptr<FileLz4Decoder> decoder_;
  std::unique_ptr<FileDecoder64> decoder64_;
  std::unique_ptr<ZstdBlobDecoder> blob_decoder_;
};


BareSliceProvider* CreateProvider(StringPiece dir, FieldClass fc, StringPiece col, DataType dt);

}  // namespace puma
