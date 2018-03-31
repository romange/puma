// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <xxhash.h>

#include "puma/hashby.h"

#include "base/memory.h"
#include "base/pmr.h"
#include "file/file.h"

#include "puma/hash_table.h"
#include "puma/sharded_file.h"
#include "strings/escaping.h"
#include "strings/stringprintf.h"

namespace puma {
using util::Status;
using std::string;


namespace {

// computes the mask for inputs - which inputs are defined for that row.
// Supports upto K inputs. Currently K = 5 for no reason. K = 64 can be
size_t KeyMask(const std::vector<ColumnConsumer>& input_arr, size_t key_count, size_t row) {
  DCHECK(key_count > 0 && key_count < 5);

  size_t res = 0;
  for (size_t i = 0; i < key_count; ++i) {
    CHECK_EQ(0, input_arr[i].ConsumeOffset()) << "TBD";
    CHECK_EQ(0, input_arr[i].column().window_offset()) << "TBD";

    const NullableColumn& current = input_arr[i].column();
    DCHECK_LT(row, current.def().size());

    res |= size_t(current.def()[row] != 0) << i;
  }
  return res;
}

size_t ComputeKeyTupleLen(const std::vector<ColumnConsumer>& input_arr, size_t key_mask) {
  DCHECK_GT(key_mask, 0);

  size_t res = 0;

  for (size_t i = 0; key_mask; key_mask >>= 1, ++i) {
    DCHECK_LT(i, input_arr.size());

    CHECK_EQ(0, input_arr[i].ConsumeOffset()) << "TBD";

    const NullableColumn& current = input_arr[i].column();
    res += ((key_mask & 1) ? aux::DataTypeSize(current.data_type()) : 0);
  }

  return res;
}

const char* ParseHashKey2(const char* key, size_t key_mask, NullableColumn* output) {
  for (size_t i = 0; key_mask; key_mask >>= 1, ++i) {
    NullableColumn& dest = output[i];

    if (key_mask & 1) {
      size_t dt_size = dest.Append(key, true);
      key += dt_size;
#ifndef NDEBUG
      // Verification
      if (dest.data_type() == DataType::STRING) {
        StringPiece str = dest.get<StringPiece>(dest.size() - 1);
        for (char a : str) {
          (void)a;
        }
      }
#endif
    } else {
      dest.mutable_def()->push_back(false);
    }
  }
  return key;
}


}  // namespace



class HashByBarrier::AggStateDescr {
 public:
  AggStateDescr() {}

  AggStateDescr(AstOp op, DataType dt) : op_(op), dt_(dt) {
    Init();
  }

  AstOp op() const { return op_;}
  DataType dt() const { return dt_;}

  size_t ByteLen() const { return size_;}

  // Resets the buffer to init state. Returns pointer past the reset region
  // (i.e. state + ByteLen()).
  char* Reset(char* state) const;

  // Updates the aggregated state with col[row].
  // TODO: to replace Update with JIT version where all the switches are eliminated during
  // construction of the descriptor.
  char* Update(const ColumnConsumer& cc, size_t row, char* state) const;

  // Extracts a value from state and appends it to dest.
  const char* AppendTo(const char* state, NullableColumn* dest) const;

  // Merges dest and src aggregated states into dest.
  size_t Merge(const char* src, char* dest) const;

  string PrintDebug(const char* state) const;
 private:
  void Init();

  void UpdateSum(const VariantArray& arr, size_t row, char* state) const;
  void UpdateMax(const VariantArray& arr, size_t row, char* state) const;

  AstOp op_;
  DataType dt_;

  uint32 size_;
};


void HashByBarrier::AggStateDescr::Init() {
  switch (op_) {
    case AstOp::SUM_FN:
      size_ = std::max(size_t(aux::DataTypeSize(dt_)), sizeof(int64));
    break;
    case AstOp::MAX_FN:
      size_ = 1 + aux::DataTypeSize(dt_);
    break;
    case AstOp::CNT_FN:
      CHECK_EQ(DataType::INT64, dt_);
      size_ = aux::DataTypeSize(dt_);
    break;
    default:
      LOG(FATAL) << "Unsupported op " << op_;
  }
}

char* HashByBarrier::AggStateDescr::Reset(char* state) const {
  switch (op_) {
    case AstOp::SUM_FN:
    case AstOp::MAX_FN:
    case AstOp::CNT_FN:
      memset(state, 0, size_);
    break;
    default:
      LOG(FATAL) << "Unsupported op " << op_;
  }
  return state + size_;
}

char* HashByBarrier::AggStateDescr::Update(const ColumnConsumer& cc, size_t row, char* state) const {
  char* next = state + size_;

  if (op_ == AstOp::CNT_FN) {
    int64* val = reinterpret_cast<int64*>(state);
    ++(*val);
    return next;
  }

  const NullableColumn& col = cc.column();
  if (!col.def()[row])
    return next;

  switch (op_) {
    case AstOp::SUM_FN:
      UpdateSum(col.data(), row, state);
    break;
    case AstOp::MAX_FN:
      UpdateMax(col.data(), row, state);
    break;
    default:
      LOG(FATAL) << "Unsupported op " << op_;
  }

  return next;
}

const char* HashByBarrier::AggStateDescr::AppendTo(const char* state, NullableColumn* dest) const {
  switch (op_) {
    case AstOp::SUM_FN:
    case AstOp::CNT_FN:
      dest->Append(state, true);
    break;
    case AstOp::MAX_FN:
      dest->Append(state + 1, *state != 0);
    break;
    default:
      LOG(FATAL) << "Unsupported op " << op_;
  }

  VLOG(3) << "AppendTo: " << *(int64*)(state + 1);

  return state + size_;
}


size_t HashByBarrier::AggStateDescr::Merge(const char* src, char* dest) const {
  CHECK(AstOp::SUM_FN == op_ || op_ == AstOp::CNT_FN);

  switch(dt_) {
    case DataType::INT64: {
      int64_t from, to;
      memcpy(&from, src, sizeof(from));
      memcpy(&to, dest, sizeof(to));
      to += from;
      memcpy(dest, &to, sizeof(to));
    }
    break;
    default:
      LOG(FATAL) << "Unsupported datatype " << dt_;
  }

  return size_;
}

void HashByBarrier::AggStateDescr::UpdateSum(
      const VariantArray& arr, size_t row, char* state) const {
  switch(dt_) {
    case DataType::INT64: {
      int64* val = reinterpret_cast<int64*>(state);

      switch (arr.data_type()) {
        case DataType::BOOL:
          *val += arr.at<uint8>(row);
        break;
        case DataType::INT64:
          *val += arr.at<int64>(row);
        break;
        case DataType::INT32:
          *val += arr.at<int32>(row);
        break;
        default:
          LOG(FATAL) << "Unsupported datatype " << arr.data_type();
      }
    }
    break;
    default:
      LOG(FATAL) << "Unsupported datatype " << dt_;
  }
}

void HashByBarrier::AggStateDescr::UpdateMax(
    const VariantArray& arr, size_t row, char* state) const {
  switch(dt_) {
    case DataType::INT64: {
      int64 cur_val, new_val = arr.at<int64>(row);
      memcpy(&cur_val, state + 1, sizeof(cur_val));
      if (!*state || cur_val < new_val) {
        memcpy(state + 1, &new_val, sizeof(new_val));
      }
    }
    break;
    case DataType::UINT64: {
      uint64 cur_val, new_val = arr.at<uint64>(row);
      memcpy(&cur_val, state + 1, sizeof(cur_val));
      if (!*state || cur_val < new_val) {
        memcpy(state + 1, &new_val, sizeof(new_val));
      }
    }
    break;
    case DataType::UINT32: {
      uint32 cur_val, new_val = arr.at<uint32>(row);
      memcpy(&cur_val, state + 1, sizeof(cur_val));
      if (!*state || cur_val < new_val) {
        memcpy(state + 1, &new_val, sizeof(new_val));
      }
    }
    break;
    case DataType::INT32: {
      int32 cur_val, new_val = arr.at<int64>(row);
      memcpy(&cur_val, state + 1, sizeof(cur_val));
      if (!*state || cur_val < new_val) {
        memcpy(state + 1, &new_val, sizeof(new_val));
      }
    }
    break;

    case DataType::DOUBLE: {
      int64* val = reinterpret_cast<int64*>(state + 1);
      if (!*state || *val < arr.at<double>(row)) {
        *val = arr.at<double>(row);
      }
    }
    break;
    default:
      LOG(FATAL) << "Unsupported datatype " << dt_;
  }
  *state = 1;
}

string HashByBarrier::AggStateDescr::PrintDebug(const char* state) const {
  NullableColumn tmp(pmr::get_default_resource());
  tmp.Init(dt_, 1);

  AppendTo(state, &tmp);
  std::stringstream ss;
  ss << tmp;

  return ss.str();
}

HashByBarrier::HashByBarrier(const FenceContext& fc)
    : MultiRowFence(fc), sset_(fc.memory_resource) {
  split_factor_ = fc.fence.split_factor;
  keys_count_ = fc.fence.args.size();
  value_count_ = fc.fence.args2.size();
  ht_arr_size_ = 1 << keys_count_;

  CHECK_LT(keys_count_, 5) << "TBD";

  if (split_factor_ == 0) {
    split_factor_ = 10; // default value.
  }

  for (size_t i = 0; i < keys_count_; ++i) {
    AstId id = fc.fence.args[i];
    auto& eval = fc.eval_array[id];

    DCHECK(eval);

    ColumnConsumer cc = eval->result();
    input_arr_[i] = cc;
  }

  if (value_count_) {
    agg_arr_.resize(value_count_);
    // for values we should take aggregator's arguments and not the aggregators.
    // In fact, each aggregator must know how to encode/decode it's values.

    for (size_t i = 0; i < value_count_; ++i) {
      const AstExpr& e = fc.expr_array[fc.fence.args2[i]];

      agg_arr_[i] = AggStateDescr(e.op(),  e.data_type());

      if (e.is_unary()) {
        AstId ref_id = e.left();
        DCHECK_LT(ref_id, fc.eval_array.size());

        auto& eval = fc.eval_array[ref_id];
        DCHECK(eval);
        ColumnConsumer cc = eval->result();

        input_arr_[keys_count_ + i] = cc;
      }
      // Please note that src->data_type() can be different of e.data_type().
      // For example, sum(def(foo)) summarizes booleans into integer.
      val_blob_len_ += agg_arr_[i].ByteLen();
    }
  }

  htable_ = base::make_pmr_array<TableMeta>(fc.memory_resource, ht_arr_size_);
}

HashByBarrier::~HashByBarrier() {
  for (size_t i = 0; i < (ht_arr_size_); ++i) {
    delete htable_[i].table;
    delete htable_[i].sharded_file;
  }
}

void HashByBarrier::OnIteration() {
  // Create tmp_key with maximal length.
  string tmp_key(ComputeKeyTupleLen(input_arr_, ht_arr_size_ - 1), '\0');

  string init_value(val_blob_len_, '\0');

  char* init_tmp = &init_value.front();
  for (const auto& descr : agg_arr_) {
    init_tmp = descr.Reset(init_tmp);
  }
  CHECK_EQ(init_value.size(), init_tmp - init_value.data());

  CHECK_EQ(0, input_arr_.front().ConsumeOffset()) << "TBD";

  size_t rows_count = input_arr_.front().column().window_size();
  if (pass_mask_) {
    CHECK_EQ(pass_mask_->size(), rows_count);
  }
  VLOG(1) << "HashByBarrier::OnIteration: key_blob_len " << tmp_key.size() << ", val_blob_len: "
          << val_blob_len_ << ", rows: " << rows_count;

  // It is possible to break this loop into batches of 4 or 8 and to improve
  // cache locality access to columns.
  for (size_t row = 0; row < rows_count; ++row) {
    if (pass_mask_ && !(*pass_mask_)[row])
      continue;

    size_t ht_mask = KeyMask(input_arr_, keys_count_, row);
    DCHECK_LT(ht_mask, ht_arr_size_);

    if (!htable_[ht_mask].table) {
      size_t key_blob_len = ComputeKeyTupleLen(input_arr_, ht_mask);
      htable_[ht_mask].table = new HashTable(mr_, key_blob_len, val_blob_len_, 1 << 16);
    }
    HashTable* dest_ht = htable_[ht_mask].table;

    char* next = BuildHashKey(ht_mask, row, &tmp_key.front());

    CHECK_EQ(next - tmp_key.data(), dest_ht->key_size()) << "keylen:"
        << unsigned(next - tmp_key.data());

    char* val = nullptr;
    HashTable::InsertResult result = dest_ht->Insert(tmp_key.data(), init_value.data(), &val);

    next = val;
    DCHECK_EQ(agg_arr_.size(), value_count_);

    for (size_t j = 0; j < value_count_; ++j) {
      next = agg_arr_[j].Update(input_arr_[keys_count_ + j], row, next);
    }
    CHECK_EQ(next - val, dest_ht->value_size()) << "vallen: " << unsigned(next - val);

    if (VLOG_IS_ON(3)) {
      LOG(INFO) << "HashKey " << strings::CEscape(tmp_key) << ", result: ";
      const char* ptr = val;
      for (size_t j = 0; j < value_count_; ++j) {
        LOG(INFO) << "agg_" << j << ": " << agg_arr_[j].PrintDebug(ptr);
        ptr += agg_arr_[j].ByteLen();
      }
    }

    if (result == HashTable::CUCKOO_FAIL) {
      // A new key was inserted and some key/value pair was pushed out from the table.
      const char* pulled_key, *pulled_val;

      dest_ht->GetKickedKeyValue(&pulled_key, &pulled_val);
      // VLOG(1) << "Kicked key/val: " << *(int64*)pulled_key << "/" << *(int64*)val;

      SpillToFiles(ht_mask, dest_ht);
      result = dest_ht->Insert(pulled_key, pulled_val);
      CHECK_EQ(HashTable::INSERT_OK, result);
    }
  }
  for (auto& cc : input_arr_) {
    if (cc.is_defined())
      cc.ConsumeHighMark(rows_count);
  }
}

void HashByBarrier::OnFinish() {
  size_t out_size = keys_count_ + value_count_;
  CHECK_EQ(out_arr_.size(), out_size);
  VLOG(1) << "HashBy::OnFinish: out " << out_size;

  // Allocate unique_ptr array and initialize each element with mr_.
  auto out_cols = base::allocate_unique_array(out_size, std::allocator<NullableColumn>(), mr_);

  for (size_t i = 0; i < keys_count_; ++i) {
    out_cols[i].Init(input_arr_[i].column().data_type(), 0);
  }

  for (size_t i = 0; i < value_count_; ++i) {
    out_cols[keys_count_ + i].Init(agg_arr_[i].dt(), 0);
  }

  // First check if some hash tables were spilled into files.
  // ht_arr_size_ is as number of nullable key combinations (< 2^keys_count_).
  for (uint32 i = 0; i < ht_arr_size_; ++i) {
    if (!htable_[i].sharded_file)
      continue;
    VLOG(1) << "Spilling ht " << i << " into files";

    HashTable* ht = htable_[i].table;
    SpillToFiles(i, ht); // Flushes the rest of the table into files.

    CHECK_STATUS(htable_[i].sharded_file->FlushAndClose());

    ShardSpec shard_spec = htable_[i].sharded_file->spec();

    delete htable_[i].sharded_file;
    htable_[i].sharded_file = nullptr;

    DCHECK_EQ(0, ht->size());  // SpillToFiles clears it.

    auto cb = [this, ht, &out_cols, i]
        (uint32 shard_index, const std::string& filename) -> Status {
      return AppendShardToOutput(filename, i, ht, out_cols.get());
    };

    CHECK_STATUS(shard_spec.ForEach(cb));
  }

  // Writing hash tables that are still in memory
  for (uint32 ht_indx = 0; ht_indx < ht_arr_size_; ++ht_indx) {
    HashTable* src_ht = htable_[ht_indx].table;
    if (!src_ht)
      continue;
    VLOG(1) << "Processing ht_index " << ht_indx << " with size: " << src_ht->size();

    for (size_t i = 0; i < out_size; ++i) {
      out_cols[i].Clear();
      out_cols[i].Reserve(src_ht->size());
    }
    HashtableToColumns(*src_ht, ht_indx, out_cols.get());

    for (size_t i = 0; i < out_size; ++i) {
      out_arr_[i]->Append(out_cols[i].size(), nullptr, out_cols[i]);
    }
  }

  // Call the flush methods.
  MultiRowFence::OnFinish();
}


char* HashByBarrier::BuildHashKey(size_t key_mask, size_t row, char* dest) {
  // key_mask tells which of the columns are defined for this key.
  // Only non-null columns are built into key. LSB of the mask corresponds to column 0.
  for (size_t i = 0; key_mask; key_mask >>= 1, ++i) {
    DCHECK_LT(i, input_arr_.size());

    size_t dt_size;
    if (key_mask & 1) {
      const NullableColumn& col = input_arr_[i].column();
      CHECK_EQ(0, col.window_offset()) << "TBD";
      CHECK_EQ(0, input_arr_[i].ConsumeOffset()) << "TBD";

      if (col.data_type() == DataType::STRING) {
        StringPiece src = col.get<StringPiece>(row);
        if (!src.empty()) {
          src = sset_.Get(src);  // dedup & allocate it internally.
        }
        dt_size = sizeof(StringPiece);
        memcpy(dest, &src, dt_size);
      } else {
        dt_size = aux::DataTypeSize(col.data_type());

        const uint8* src = col.data().column_ptr().u8ptr + row*dt_size;
        memcpy(dest, src, dt_size);
      }

      dest += dt_size;
    }
  }
  return dest;
}

void HashByBarrier::SpillToFiles(size_t key_mask, HashTable* src) {
  for (size_t tmp = key_mask, i = 0; tmp; tmp >>= 1, ++i) {
    if (key_mask & 1) {
      const NullableColumn& current = input_arr_[i].column();
      CHECK_NE(DataType::STRING, current.data_type()) << "Does not support strings yet";
    }
  }

  ShardedFile* file = htable_[key_mask].sharded_file;
  if (!file) {
    string prefix = StringPrintf("/tmp/htable_%04ldm", key_mask);
    file = new ShardedFile(prefix, "bin", split_factor_);
    CHECK_STATUS(file->Open(false));
    htable_[key_mask].sharded_file = file;

    VLOG(1) << "Creating fileset " << file->spec().Glob();
  }
  VLOG(1) << "Spilling to " << htable_[key_mask].sharded_file->spec().Glob();

  // Push keys/values from htable to sharded files.
  const char* key;
  char* val;

  for (size_t i = 0; i < src->capacity(); ++i) {
    if (!src->Get(i, &key, &val))
      continue;

    uint32 fp = XXH32(key, src->key_size(), 10);
    CHECK_STATUS(file->Write(fp, key, src->key_size()));
    if (src->value_size()) {
      CHECK_STATUS(file->Write(fp, val, src->value_size()));
    }
  }
  src->Clear();
}

void HashByBarrier::HashtableToColumns(const HashTable& src, size_t mask,
                                       NullableColumn dest_cols[]) const {
  const char* key;
  const char* val;

  for (size_t i = 0; i < src.capacity(); ++i) {
    if (!src.Get(i, &key, &val))
      continue;
    VLOG(3) << "key/value: " << *(int64*)key << "/" << *(int64*)val;
    NullableColumn* cur_col_marker = dest_cols;
    const char* next = ParseHashKey2(key, mask, cur_col_marker);
    CHECK_EQ(next - key, src.key_size()) << "htable keysize: " << src.key_size();

    next = val;
    cur_col_marker += keys_count_;

    for (size_t j = 0; j < value_count_; ++j) {
      next = agg_arr_[j].AppendTo(next, cur_col_marker + j);
    }
    CHECK_EQ(src.value_size(), next - val);
  }
}

Status HashByBarrier::AppendShardToOutput(
      const string& filename, size_t mask, HashTable* ht, NullableColumn dest_cols[]) {
  ht->Clear();
  RETURN_IF_ERROR(LoadShard(filename, ht));

  size_t out_size = out_arr_.size();
  for (size_t i = 0; i < out_size; ++i) {
    dest_cols[i].Clear();
    dest_cols[i].Reserve(ht->size());
  }
  HashtableToColumns(*ht, mask, dest_cols);
  ht->Clear();

  // Write dest_cols into out_arr.
  for (size_t i = 0; i < out_size; ++i) {
    out_arr_[i]->Append(dest_cols[i].size(), nullptr, dest_cols[i]);
  }
  return Status::OK;
}

using strings::charptr;

Status HashByBarrier::LoadShard(const string& filename, HashTable* ht) {
  file::ReadonlyFile::Options opts;
  opts.sequential = true;
  auto res = file::ReadonlyFile::Open(filename, opts);

  RETURN_IF_ERROR(res.status);

  std::unique_ptr<file::ReadonlyFile> file(res.obj);
  size_t total_size = ht->key_size() + ht->value_size();
  std::unique_ptr<uint8[]> buf(new uint8[total_size]);
  // Reading according to SpillToFiles format.
  CHECK_EQ(0, file->Size() % total_size);

  size_t offset = 0;

  while (true) {
    GET_UNLESS_ERROR(read, file->Read(offset, strings::MutableByteRange(buf.get(), total_size)));
    if (read == 0)
      break;
    DCHECK_EQ(total_size, read);

    VLOG(3) << "LoadShard key/val: " << *(int64*)buf.get() << "/"
                                << *(int64*)(buf.get() + ht->key_size());
    char* val = nullptr;
    HashTable::InsertResult result = ht->Insert(charptr(buf.get()),
            charptr(buf.get()) + ht->key_size(), &val);

    CHECK_NE(HashTable::CUCKOO_FAIL, result);

    if (result == HashTable::KEY_EXIST) {
      const char* src = charptr(buf.get()) + ht->key_size();
      for (const AggStateDescr& descr : agg_arr_) {
        size_t delta = descr.Merge(src, val);
        src += delta;
        val += delta;
      }
    }
    offset += total_size;

  }

  CHECK_STATUS(file->Close());
  return Status::OK;
}

}  // namespace puma
