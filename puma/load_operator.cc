// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/load_operator.h"

#include "base/logging.h"
#include "base/simd.h"
#include "base/stl_util.h"

using util::Status;

namespace puma {

namespace {

inline void set_ptr(size_t offset, VariantArray* src, SlicePtr& dest) {
  dest.u8ptr = src->raw(offset);
}

inline void set_ptr(size_t offset, DefineVector* src, SlicePtr& dest) {
  dest.u8ptr = &src->front() + offset;
}

template<typename T> bool AppendSlice(size_t length, BareSliceProvider* provider, T* dest) {
  DCHECK(provider);
  DCHECK_GT(length, 0);
  size_t offset = dest->size();

  aux::resize(length + offset, dest);
  SlicePtr sptr;
  uint32 fetched_size;

  while (length && provider->has_data_to_read()) {
    set_ptr(offset, dest, sptr);

    CHECK_STATUS(provider->GetPage(length, sptr, &fetched_size));
    offset += fetched_size;
    length -= fetched_size;
  }
  aux::resize(offset, dest);

  bool last = !provider->has_data_to_read();
  VLOG(1) << "GetPage from " << provider->name()
          << ", last:" << last;
  return last;
}

}  // namespace

LoadOperator::LoadOperator(const AstExpr& e, BareSliceProvider* data, BareSliceProvider* def,
                           Ownership ownership)
    : Operator(e, pmr::get_default_resource()), data_provider_(data),
      def_provider_(def), ownership_(ownership), str_buf_(pmr::get_default_resource()) {
  CHECK(base::_in(e.op(), {AstOp::LOAD_FN, AstOp::LEN_FN}));
  if (e.op() == AstOp::LEN_FN) {
    DCHECK_EQ(kArrayLenDt, e.data_type());
  }
  if (data) {
    result_column_.Init(e.data_type(), kPageSize);
  }

  if (def_provider_) {
    result_column_.mutable_def()->reserve(kPageSize);
  } else {
    result_column_.mutable_def()->resize_fill(kPageSize, true);
    result_column_.mutable_def()->clear();
  }

  if (e.data_type() == DataType::STRING) {
    CHECK(def_provider_ && data_provider_);
    CHECK_EQ(kStringLenDt, def_provider_->dt());

    len_arr_ = base::make_pmr_array<uint32_t>(::pmr::get_default_resource(), kPageSize);
  }
}

LoadOperator::~LoadOperator() {
  if (ownership_ == TAKE_OWNERSHIP) {
    delete def_provider_;
    delete data_provider_;
  }
}


Status LoadOperator::ApplyInternal() {
  bool should_fetch = result_column_.MoveToBeginningIfSmaller(64);

  if (!should_fetch)
    return Status::OK;

  if (expr().data_type() == DataType::STRING) {
    return LoadString();
  }

  // if def_provider is null then this is required field.
  if (def_provider_ == nullptr) {
    DCHECK(expr_.flags() & AstExpr::ALWAYS_PRESENT) << expr_;
  }

  // size comes from def because data can be absent.
  ssize_t prev_size = result_column_.size();  // use signed for the scatter loop below.
  VariantArray* data = result_column_.mutable_data();

  if (!def_provider_) {
    // Here we ask for a maximum we can to fill up the capacity - kPageSize - prev_size.
    AppendSlice(kPageSize - prev_size, data_provider_, data);

    // It's filled with true once and only resized here.
    result_column_.mutable_def()->resize_assume_reserved(result_column_.data().size());

    return Status::OK;
  }

  auto* def = result_column_.mutable_def();
  bool def_last = AppendSlice(kPageSize - prev_size, def_provider_, def);
  VLOG(1) << "Appending " << def->size() - prev_size << " items from " << def_provider_->name();

  if (data_provider_) {
    // Count number of ones we received just now and how items were appended.
    size_t append_len = def->size() - prev_size;
    size_t def_cnt = base::CountVal8(def->begin() + prev_size, append_len, 1);

    if (def_cnt) {
      DCHECK_LE(def_cnt, append_len);

      // I ask more than def_cnt to make sure that if is_last true
      // we will still get def_cnt and data_last will be true too.
      bool data_last = AppendSlice(def_cnt + def_last, data_provider_, data);
      DCHECK_EQ(prev_size + def_cnt, result_column_.data().size()) << expr_;

      // it could be that data_last is true because data provider finished but
      // is_last is false because def provider has more nulls to fetch.
      // We still need to fetch them though.
      CHECK(!def_last || data_last);

      if (def_cnt < append_len) {
        // Scatter data values according to def column.
        data->Resize(def->size());

        ssize_t src_index = prev_size + def_cnt - 1;

        // We do not let dst_i = src_index in the loop since otherwise we reached the "dense" range
        // where we copied all the values and it should stay there.
        // TODO: To optimize by instantiating data->data_type_size().
        for (ssize_t dst_i = def->size() - 1; src_index >= prev_size && dst_i > src_index;
            --dst_i) {
          if ((*def)[dst_i]) {
            memcpy(data->raw(dst_i), data->raw(src_index), data->data_type_size());
            --src_index;
          }
        }
      }
    } else {
      if (def_last) {
        // Again, the same check. We check that data provider is depleted as well.
        bool data_last = AppendSlice(1, data_provider_, data);
        CHECK(data_last && data->size() == size_t(prev_size));
      }
      result_column_.mutable_data()->Resize(def->size());
    }
  }

  return Status::OK;
}

Status LoadOperator::LoadString() {
  DCHECK(def_provider_ && len_arr_);

  // MoveToBeginningIfSmaller moved the data to front.

  ssize_t prev_size = result_column_.size();  // use signed for the scatter loop below.
  uint32_t str_count;

  RETURN_IF_ERROR(def_provider_->GetPage(kPageSize - prev_size,
                  SlicePtr(len_arr_.get()), &str_count));
  DCHECK_LE(str_count, kPageSize - prev_size);

  result_column_.Resize(prev_size + str_count);

  VariantArray* data = result_column_.mutable_data();
  auto& def = *result_column_.mutable_def();

  // Branchless loop that fills up def and computes the total string size.
  uint32_t total_sz = 0;
  for (uint32_t i = 0; i < str_count; ++i) {
    uint8_t is_defined = (len_arr_[i] != 0);
    def[i + prev_size] = is_defined;

    total_sz += (len_arr_[i] - is_defined);
  }

  if (total_sz) {

    str_buf_.resize(total_sz);
    uint32_t str_sz;
    RETURN_IF_ERROR(data_provider_->GetPage(total_sz, puma::SlicePtr(str_buf_.data()), &str_sz));
    CHECK_EQ(str_sz, total_sz) << "TBD";

    const char* next = str_buf_.begin();
    for (uint32_t i = 0; i < str_count; ++i) {
      if (len_arr_[i]) {
        uint32_t len = len_arr_[i] - 1;
        data->column_ptr().strptr[prev_size + i] = StringPiece(next, len);
        next += len;
      }
    }
    CHECK(next == str_buf_.end());
  }

  return Status::OK;
}

}  // namespace puma
