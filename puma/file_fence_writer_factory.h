// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "puma/fence_runner.h"

namespace puma {

class SliceWriterFactory;
class LoadOperator;

// used to create slices coming from the fence.
class FileFenceWriterFactory {
 public:
  FileFenceWriterFactory(const std::string& dir, pmr::memory_resource* mgr = nullptr);
  ~FileFenceWriterFactory();

  FenceWriterFactory writer();
  BareSliceProviderFactory slice();

  typedef std::function<void (const NullableColumn&)> ColumnCb;

  void ReadColumn(const AstExpr& load_expr, ColumnCb cb) const;
  LoadOperator* AllocateLoadOperator(const AstExpr& load_expr) const;

private:
  BareSliceProvider* CreateProviderInternal(FieldClass fc, StringPiece name, DataType dt);

  FenceOutputWriter* CreateWriter(StringPiece col, DataType dt);

  const std::string dir_;
  pmr::memory_resource* mgr_;
  std::unique_ptr<SliceWriterFactory> factory_;
  std::vector<std::unique_ptr<BareSliceProvider>> allocated_providers_;
};


}  // namespace puma
