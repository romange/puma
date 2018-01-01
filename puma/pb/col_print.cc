// Copyright 2015, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <iostream>
#include "base/endian.h"
#include "base/init.h"
#include "base/logging.h"
#include "base/stl_util.h"

#include "file/file.h"
#include "strings/escaping.h"
#include "strings/strcat.h"
#include "puma/file/slice_provider.h"

#include "puma/storage.pb.h"

#include "util/coding/lz4int.h"

using std::string;
using namespace std;
using util::LZ4_RleDecoder;
using strings::MutableByteRange;

DEFINE_bool(check_compress, false, "");
DEFINE_bool(shuffle, false, "");
DEFINE_bool(skip_print, false, "");

namespace puma {

constexpr uint32 kBlockSize = (1 << 16);
constexpr uint32 kMaxIntsInBlock = 1 << 17;

static void PrintBoolArray(ColumnProvider* provider) {
  uint8 buf[1024];
  ColumnProvider::FlatPage page;
  page.ptr = buf;

  while (!page.last) {
    CHECK_STATUS(provider->GetPage(arraysize(buf), &page));

    if (FLAGS_skip_print)
      continue;

    for (unsigned i = 0; i < page.size; ++i) {
      std::cout << int(buf[i]) << endl;
    }
  }
}

typedef std::function<void(const void*, size_t)> IntArrCb;

static void PrintIntArray(ColumnProvider* provider, IntArrCb cb) {
  uint32 val[4096];

  ColumnProvider::FlatPage page;
  page.ptr = val;

  CHECK_EQ(0, sizeof(val) % traits::DataTypeSize(provider->dt()));
  size_t max_size = sizeof(val) / traits::DataTypeSize(provider->dt());

  while (!page.last) {
    CHECK_STATUS(provider->GetPage(max_size, &page));

    cb(page.ptr, page.size);
  }
}

static void PrintStringArray(ColumnProvider* provider) {
  StringPiece val[1024];

  ColumnProvider::FlatPage page;
  page.ptr = val;

  while (!page.last) {
    CHECK_STATUS(provider->GetPage(arraysize(val), &page));

    if (FLAGS_skip_print)
      continue;

    for (unsigned i = 0; i < page.size; ++i) {
      std::cout << val[i] << endl;
    }
  }
}

template<typename T> void PrintArr(const void* arr, size_t cnt) {
  if (FLAGS_skip_print)
    return;
  const T* int_arr = reinterpret_cast<const T*>(arr);
  std::cout.precision(9);
  for (size_t i = 0; i < cnt; ++i) {
    std::cout << int_arr[i] << endl;
  }
}


static size_t raw_size = 0, compress_size = 0;

using util::LZ4_RleEncoder;
using util::LZ4_RleEncoder64;

void CheckInt32Compress(const void* arr, size_t cnt) {
  const uint32* src = reinterpret_cast<const uint32*>(arr);
  size_t mem_needed = LZ4_RleEncoder::MaxComprBufSize(cnt);

  std::unique_ptr<char[]> buf(new char[mem_needed]);

  std::unique_ptr<uint32[]> shuffled;
  LZ4_RleEncoder encoder(10, 16);

  if (FLAGS_shuffle) {
    size_t quad_num = (cnt / 4) + 1;
    size_t i = 0;
    shuffled.reset(new uint32[quad_num * 4]);

    while (i + 4 <= cnt) {
      uint32* dest = shuffled.get() + i;

      for (size_t k = 0; k < 4; ++k) {
        dest[k] = 0;
        VLOG(2) << "Src (" << i + k << "): " << src[i + k];
        // take a k'th byte from each number, shift it left by j to combine into dest.
        for (size_t j = 0; j < 4; ++j) {
          uint32 v = (src[i + j] >> (k * 8)) & 0xFUL;
          dest[k] |=  v << (j * 8);
        }
      }

      for (size_t k = 0; k < 4; ++k) {
        VLOG(2) << "Dest (" << i + k << "): " << dest[k];
      }
      i += 4;
    }
    cnt = i;
    src = shuffled.get();
  }

  size_t cmprs_sz = encoder.Encode(src, cnt, buf.get());

  raw_size += (cnt*sizeof(*src));
  compress_size += cmprs_sz;
}

void CheckInt64Compress(const void* arr, size_t cnt) {
  const uint64* src = reinterpret_cast<const uint64*>(arr);
  size_t mem_needed = LZ4_RleEncoder64::MaxComprBufSize(cnt);

  std::unique_ptr<char[]> buf(new char[mem_needed]);
  LZ4_RleEncoder64 encoder(0);

  size_t cmprs_sz = encoder.Encode(src, cnt, buf.get());
  raw_size += (cnt*sizeof(*src));
  compress_size += cmprs_sz;
}

void PrintDebugFile(StringPiece filename) {
  ColumnProvider* provider = ColumnProvider::Open(filename.as_string());
  FileHeader header = provider->file_header();

  std::cout << "File " << filename << " " << header.dt << " " << header.cc << std::endl;

  switch (header.dt) {
    case DataType::BOOL:
      PrintBoolArray(provider);
    break;
    case DataType::INT64:
    if (FLAGS_check_compress) {
      PrintIntArray(provider, &CheckInt64Compress);
    } else {
      PrintIntArray(provider, &PrintArr<int64>);
    }
    break;
    case DataType::INT32:
    case DataType::UINT32:
      if (FLAGS_check_compress) {
        PrintIntArray(provider, &CheckInt32Compress);
      } else {
        PrintIntArray(provider, &PrintArr<int32>);
      }
    break;
    case DataType::DOUBLE:
      PrintIntArray(provider, &PrintArr<double>);
    break;
    case DataType::STRING:
      PrintStringArray(provider);
    break;
    default:
      LOG(ERROR) << "Unsupported " << header.dt;
  }
  delete provider;
}

}  // namespace puma

using namespace puma;

int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  for (int i = 1; i < argc; ++i) {
    StringPiece path(argv[i]);
    puma::PrintDebugFile(path);
  }

  if (FLAGS_check_compress) {
    CONSOLE_INFO << "Raw size: " << raw_size << ", compressed size: " << compress_size;
  }
  return 0;
}
