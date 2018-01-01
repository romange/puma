// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/file/volume_format.h"

#include "base/endian.h"

namespace puma {

#if 0
uint8_t* BlobHeader::Write(uint8_t* dest) {
  LittleEndian::Store16(dest, kHeaderMagic);
  dest[2] = flag_;
  dest += 3;

  if (has_id()) {
    LittleEndian::Store16(dest, id_);
    dest += 2;
  }
  if (has_type()) {
    *dest++ = type_;
  }

  LittleEndian::Store32(dest, compress_size_);
  dest += (1 + byte_size(compress_size_));

  return dest;
}

int BlobHeader::Parse(const uint8_t* src, uint32 max_size) {
  if (kHeaderMagic != *reinterpret_cast<const uint16_t*>(src) || max_size < 4)
    return -1;
  flag_ = src[2];
  int consumed = 3;

  if (has_id()) {
    if (max_size < 2 + consumed)
      return -1;
    id_ = LittleEndian::Load16(src + consumed);
    consumed += 2;
  }

  if (has_type()) {
    if (max_size < 1 + consumed)
      return -1;
    type_ = src[consumed];
    ++consumed;
  }

  uint8 bs = 1 + (flag_  >> CS_SHIFT) & 3;
  if (max_size < bs + consumed)
    return -1;

  compress_size_ = LittleEndian::Load64VariableLength(src + consumed, bs);

  consumed += bs;

  return consumed;
}


#endif

}  // namespace puma
