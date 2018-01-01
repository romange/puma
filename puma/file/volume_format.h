// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "puma/storage.pb.h"

#include "base/bits.h"

namespace puma {


// echo -n 'Roman&Rivi' | sha1sum - af8e066b3c9e53cd33a17a032ca7f1b7e3f7b309

// first 4 bytes of the number above, written as 4 first bytes of each volume.
constexpr uint32_t kVolumeHeaderMagic = 0xaf8e066b;

// next 4 bytes of the number above, written at the beginning of footer frame.
constexpr uint32_t kVolumeFooterMagic = 0x3c9e53cd;
constexpr uint16_t kSliceMagic = 0x33a1;
constexpr uint16_t kDictMagic = 0x33a2;

typedef uint32_t SliceCodecId;
constexpr SliceCodecId kSeqEncoderCodec = 0x1;
constexpr SliceCodecId kDoubleEncoderCodec = 0x2;
constexpr SliceCodecId kInt64V2Codec = 0x3;


/*
 Table format - table schema defines the order of all the fields across all volumes in the table
 and their ordered index. All the fields are referenced by this index. The table schema is
 stored in a separate file.

 There are 1 or more volumes for the table.
 Each volume is immutable and changing it - means rewriting the volume.

Logical layout:
 Each volume spans 1 or more frames and each frame contain all the slices for messages inside
 that frame. There should be no more than 256 (or some other small int) frames in the volume -
 if each frame is around 16MB it gives us at most 4GB volume size.
 *  Frame size can be bigger though producing greater volumes.
 ** 256 frames is not the hard requirement though but they should be limited to small ints
    to allow compact slice indices.

 At the end of volume file - it contains a footer information which contains the following:
  a. Positions for all slices and their frames.
  b. Other information such as global dictionaries and where they are stored.
  c. Dictionaries are just another blobs of data stored in the volume.
     They have an dedicated header.
     They should probably be stored as blocks - since global dictionaries should be loaded into RAM.
  d. Currently a volume can not be traversed sequentially without parsing its footer first.
     Footer has all the information regarding positions of all the primitives in the volume.
  e. Footer is built in a way that allows loading a single slice index into memory thus allowing
     partial reading of the volume.

 The advantage of separating volume into multiple frames is that we ease memory requirements when
 slicing objects, no need to store the whole volume in RAM.
 Volumes will also contain the slice type for each slice inside them. Optionally slice names
 as well.

 Footer format:
 kVolumeFooterMagic - 4 bytes. After this we store 'count' - number of records
 in bytesize array ( 3 bytes). This is followed by bytesize array:
 'count' byte sizes of each SliceIndex record (each bytesize is 3 bytes).
 Then the SliceIndex array blob and finally 4 bytes offset to kVolumeFooterMagic number.

*/

}  // namespace puma
