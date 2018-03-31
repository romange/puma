// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/hash_table.h"

#include <xxhash.h>

#include "base/bits.h"
#include "base/logging.h"


namespace puma {

namespace {

bool IsPrime(uint64 val) {
  if (val < 4) return true;
  if (val % 2 == 0 || val % 3 == 0) return false;

  for (uint64 i = 5; i*i <= val; i+=2) {
    if (val % i == 0) return false;
  }
  return true;
}

uint64 NextHigherPrime(uint64 value) {
  if (value % 2 == 0) ++value;
  for (; value < kuint64max ; value += 2) {
    if (IsPrime(value)) {
      return value;
    }
  }
  LOG(FATAL) << "Should not happen.";
  return 1;
}

inline uint8 random_bit(uint32& bit_index) {
  return (bit_index++) & 1;
}

inline uint8 random_index(uint32& indx) {
  return (indx++) & 3;
}

}  // namespace

HashTable::HashTable(pmr::memory_resource* memory_resource, uint32 key_size, uint32 val_size,
                     size_t capacity)
  : mr_(memory_resource), set_mask_(memory_resource), key_size_(key_size), val_size_(val_size) {
  CHECK_GT(key_size, 0);
  VLOG(1) << "HashTable(" << key_size_ << "," << val_size_ << "," << capacity << ")";

  kv_size_ = key_size+val_size;
  CHECK_GT(capacity, kv_size_*16);

  size_t cnt = capacity / (kBucketLength * kv_size_);
  bucket_count_ = NextHigherPrime(cnt);
  shifts_limit_ = Bits::FindMSBSetNonZero(bucket_count_) * 2;

  buf_ = base::make_pmr_array<char>(mr_, bucket_count_ * kv_size_ * kBucketLength);
  set_mask_.resize(bucket_count_ * kBucketLength, false);

  tmp_space_ = reinterpret_cast<char*>(mr_->allocate(kv_size_*2));
}

HashTable::~HashTable() {
  mr_->deallocate(tmp_space_, kv_size_*2);
}

bool HashTable::Get(size_t index, const char** key, const char** value) const {
  if (!set_mask_[index])
    return false;

  BucketId b = index / kBucketLength;
  const char* offs = BucketStart(b);
  *key = offs + (index % kBucketLength) * key_size_;
  *value = ValueStart(const_cast<char*>(offs), index % kBucketLength);

  return true;
}

char* HashTable::Lookup(const char* key) {
  uint64 fp = XXH64(key, key_size_, kMask1);
  BucketId b = fp % bucket_count_;

  char* offs = BucketStart(b);
  size_t index = LookupBucket(b, key);
  if (index < kBucketLength)
    return ValueStart(offs, index);

  BucketId b2 = SecondBucket(b, fp);

  offs = BucketStart(b2);
  index = LookupBucket(b2, key);
  if (index < kBucketLength)
    return ValueStart(offs, index);
  return nullptr;
}

size_t HashTable::LookupBucket(BucketId b, const char* key) const {
  const char* b_start = BucketStart(b);
  for (size_t i = 0; i != kBucketLength; ++i) {
    if (set_mask_[b*kBucketLength + i] && memcmp(b_start + i*key_size_, key, key_size_) == 0) {
      return i;
    }
  }
  return kBucketLength;
}


size_t HashTable::LookupEmpty(BucketId bid) const {
  for (size_t i = 0; i != kBucketLength; ++i) {
    if (!set_mask_[bid*kBucketLength + i]) {
      return i;
    }
  }
  return kBucketLength;
}

void HashTable::CopyKeyPair(const char* key, const char* value, char** val_ptr,
                            char* bucket_start, size_t index) const {
  memcpy(bucket_start + index*key_size_, key, key_size_);

  bucket_start = ValueStart(bucket_start, index);
  if (val_size_)
    memcpy(bucket_start, value, val_size_);
  if (val_ptr)
    *val_ptr = bucket_start;
}

auto HashTable::Insert(const char* key, const char* value, char** val_ptr) -> InsertResult  {
  uint64 fp = XXH64(key, key_size_, kMask1);
  BucketId bid[2];
  char* b_start[2];

  bid[0] = fp % bucket_count_;

  VLOG(2) << "bid0 " << bid[0];

  for (unsigned j = 0; ; ++j) {
    b_start[j] = BucketStart(bid[j]);
    size_t index = LookupBucket(bid[j], key);
    if (index < kBucketLength) {
      if (val_ptr)
        *val_ptr = ValueStart(b_start[j], index);
      return KEY_EXIST;
    }
    if (j == 1)
      break;
    bid[1] = SecondBucket(bid[0], fp);
  }
  VLOG(2) << "bid1 " << bid[1];

  for (unsigned j = 0; j < 2; ++j) {
    size_t index = LookupEmpty(bid[j]);
    if (index < kBucketLength) {
      set_mask_[bid[j]*kBucketLength + index] = true;
      CopyKeyPair(key, value, val_ptr, b_start[j], index);
      ++size_;
      return INSERT_OK;
    }
  }

  uint8 rb = random_bit(random_bit_indx_);
  BucketId roll_bucket = bid[rb];
  char* roll_offs = b_start[rb];

  static_assert(kBucketLength == 4, "Other length is not supported yet");

  // We keep start_index, and start_bucket_id so that we could return val_ptr.
  // In addition we avoid loops with our original key_value pair in case our random walk
  // brought us to the same place we started from.
  // As long as there are no resizes we can store pointer to starting bucket.
  // But if we decide to reallocate here we will need to keep the index.

  char* start_bucket_ptr = roll_offs;
  uint8 start_index = random_index(random_bit_indx_);
  uint8 roll_index = start_index;

  VLOG(2) << "Starting from bucket " << roll_bucket << "/" << (void*)start_bucket_ptr
          << ", index: " << int(start_index);


  memcpy(tmp_space_, key, key_size_);
  if (val_size_)
    memcpy(tmp_space_ + key_size_, value, val_size_);
  tmp_index_ = 0;

  if (val_ptr) {
    *val_ptr = ValueStart(start_bucket_ptr, start_index);
  }
  // tmp_kv_ contains 2 kv pairs to perform swaps with cuckoo pair.
  for (uint32 j = 0; j < shifts_limit_; ++j) {
    // Store from roll_index to tmp value and from pending k_v to roll_index.
    SwapCurrentAndTmp(roll_index, roll_offs);

    tmp_index_ = 1 - tmp_index_;

    char* pending_kv_ = tmp_space_ + tmp_index_ * kv_size_;

    // Move to another bucket of the pulled key.
    fp = XXH64(pending_kv_, key_size_, kMask1);
    BucketId first_bid = fp % bucket_count_;
    if (first_bid == roll_bucket) {
      roll_bucket = SecondBucket(first_bid, fp);
    } else {
      roll_bucket = first_bid;
    }

    roll_offs = BucketStart(roll_bucket);
    size_t index = LookupEmpty(roll_bucket);
    if (index < kBucketLength) {
      set_mask_[roll_bucket*kBucketLength + index] = true;
      CopyKeyPair(pending_kv_, pending_kv_ + key_size_, nullptr, roll_offs, index);
      ++size_;

      return INSERT_OK;
    }
    roll_index = random_index(random_bit_indx_);
    if (roll_offs == start_bucket_ptr && roll_index == start_index) {
      // We made exact cycle and returned to the same place we started from.
      // Lets change the shift index to something else.
      roll_index = (roll_index + 1) % kBucketLength;
    }
  }


  return CUCKOO_FAIL;
}

inline void HashTable::SwapCurrentAndTmp(uint8 bindex, char* offs) {
  // copy key to the free tmp buffer.
  char* data = offs + key_size_*bindex;
  char* next_free = tmp_space_ + (1 - tmp_index_)*kv_size_;
  memcpy(next_free, data, key_size_);

  // copy into key from the tmp buffer.
  char* tmp_cur = tmp_space_ + tmp_index_ * kv_size_;
  memcpy(data, tmp_cur, key_size_);

  data = ValueStart(offs, bindex);
  memcpy(next_free + key_size_, data, val_size_);
  memcpy(data, tmp_cur + key_size_, val_size_);
}

void HashTable::GetKickedKeyValue(const char** key, const char** value) {
  *key = tmp_space_ + tmp_index_ * kv_size_;
  *value = tmp_space_ + tmp_index_ * kv_size_ + key_size_;
}

}  // namespace puma
