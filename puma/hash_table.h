// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <memory>
#include <vector>

#include <pmr/vector.h>

#include "base/integral_types.h"
#include "base/pmr.h"

namespace puma {

class HashTable {
  typedef size_t BucketId;
 public:
  // all arguments are in bytes.
  HashTable(pmr::memory_resource* memory_resource, uint32 key_size,
            uint32 val_size, size_t capacity);
  ~HashTable();

  enum InsertResult {INSERT_OK, KEY_EXIST, CUCKOO_FAIL};

  // Inserts key/value pair.
  // Upon returning CUCKOO_FAIL the following things happen:
  //   1. key/value pair is inserted but some other pair was pushed out during cuckoo rolling.
  //   2. The pushed pair can be fetched via GetKickedKeyValue only.
  //   3. size is not increased.
  //   4. The table should not be used for insertions anymore since it's not reallocatable.
  //   5. Lookup won't find the pushed out key/value pair.
  InsertResult Insert(const char* key, const char* value, char** val_ptr = nullptr);

  // Finds key in the table. Returns pointer to value if found, otherwise nullptr.
  char* Lookup(const char* key);

  size_t size() const { return size_; }
  size_t capacity() const { return bucket_count_*kBucketLength;}

  uint32 key_size() const { return key_size_; }
  uint32 value_size() const { return val_size_; }

  // trivial iteration getter: TODO: to add iterators.
  // index must be beween 0 and max_size.
  // Returns true if key/value at this index are found, false otherwise.
  // key/value are updated in case true is returned.
  bool Get(size_t index, const char** key, const char** value) const;

  bool Get(size_t index, const char** key, char** value) {
    return Get(index, key, const_cast<const char**>(value));
  }


  void Clear() {
    std::fill(set_mask_.begin(), set_mask_.end(), false);
    size_ = 0;
  }

  // If Insert returns CUCKOO_FAIL, it allows to fetch back the pushed out key/value pair.
  // Returns undefined results otherwise.
  void GetKickedKeyValue(const char** key, const char** value);
 private:

  static constexpr uint64 kMask1 = 0xc949d7c7509e6557ULL;
  static constexpr uint64 kMask2 = 0x9ae16a3b2f90404fULL;

  size_t LookupBucket(BucketId bid, const char* key) const;
  size_t LookupEmpty(BucketId bid) const;

  char* BucketStart(BucketId b) { return buf_.get() + b * kv_size_ * kBucketLength; }
  const char* BucketStart(BucketId b) const { return buf_.get() + b * kv_size_ * kBucketLength; }

  char* ValueStart(char* bucket_start, size_t index) const {
    return bucket_start + kBucketLength * key_size_ + index * val_size_;
  }

  BucketId SecondBucket(BucketId first_b, uint64 fp) {
    fp ^= kMask2;
    BucketId b2 = fp % bucket_count_;
    if (b2 == first_b) {
      b2 = (first_b + 1) % bucket_count_;
    }
    return b2;
  }

  void CopyKeyPair(const char* key, const char* value, char** val_ptr, char* bucket_start,
                   size_t index) const;

  void SwapCurrentAndTmp(uint8 bindex, char* offs);

  pmr::memory_resource* mr_;

  // set_mask_ says which of the cells is occupied. There are kBucketLength cells in each bucket.
  pmr::vector<bool> set_mask_;

  base::pmr_unique_ptr<char[]> buf_;  // cuckoo table.

  // Temporary space to store rolling key/value pairs. Enough to store 2 pairs.
  char* tmp_space_;
  uint8 tmp_index_ = 0; // which buffer in tmp_space is used. Can be 0 or 1.

  const uint32 key_size_, val_size_;
  size_t kv_size_;
  BucketId bucket_count_;
  uint32 shifts_limit_ = 0;

  size_t size_ = 0;
  uint32 random_bit_indx_ = 0;
  static constexpr size_t kBucketLength = 4;
};

}  // namespace puma

