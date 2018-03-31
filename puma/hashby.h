// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "puma/fence_runner.h"
#include "strings/strpmr.h"

namespace puma {

class HashTable;
class ShardedFile;

class HashByBarrier : public MultiRowFence {
 public:
  HashByBarrier(const FenceContext& bc);
  ~HashByBarrier();
  void OnIteration() override;
  void OnFinish() override;

private:
  char* BuildHashKey(size_t key_mask, size_t row, char* dest);
  void SpillToFiles(size_t key_mask, HashTable* src);

  // writes hashtable data into nullable columns.
  void HashtableToColumns(const HashTable& src, size_t mask, NullableColumn dest_cols[]) const;

  util::Status AppendShardToOutput(const std::string& filename, size_t mask, HashTable* ht,
                             NullableColumn dest_cols[]);

  util::Status LoadShard(const std::string& filename, HashTable* ht);

  uint32 split_factor_ = 0;
  uint32 keys_count_, value_count_;
  uint32 val_blob_len_ = 0;
  uint32 ht_arr_size_;

  strings::StringPieceSet sset_;

  struct TableMeta {
    HashTable* table = nullptr;
    ShardedFile* sharded_file = nullptr;
  };

  base::pmr_unique_ptr<TableMeta[]> htable_;

  class AggStateDescr;
  std::vector<AggStateDescr> agg_arr_;
};

}  // namespace puma
