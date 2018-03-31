// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "puma/operator.h"

namespace puma {

class LoadOperator : public Operator {
  public:
  // The ownership over providers stays with the caller or with LoadOperator according to
  // ownership argument.
  LoadOperator(const AstExpr& e, BareSliceProvider* data, BareSliceProvider* def,
               Ownership ownership = DO_NOT_TAKE_OWNERSHIP);

  ~LoadOperator();

 protected:
  // Returns true if this batch is the last one.
  // result_column will be filled with the relevant data.
  util::Status ApplyInternal() override;

 private:
  util::Status LoadString();

  BareSliceProvider* data_provider_;
  BareSliceProvider* def_provider_;
  Ownership ownership_;

  base::pmr_unique_ptr<uint32_t[]> len_arr_;
  base::PODArray<char> str_buf_;
};

}  // namespace puma
