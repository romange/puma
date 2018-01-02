// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <unistd.h>

#include "base/init.h"
#include "puma/file/volume_reader.h"

#include "puma/pb/util.h"

#include "strings/numbers.h"
#include "strings/util.h"

DEFINE_string(table, "", "table name");
DEFINE_bool(skip_print, false, "");
DEFINE_bool(bin, false, "");

using namespace puma;
using namespace std;
using strings::MutableByteRange;
using util::StatusCode;
using util::Status;

namespace {
void PrintU64(const uint64* num, unsigned cnt) {
  char buf[kFastToBufferSize];

  if (FLAGS_bin) {
    write(STDOUT_FILENO, num, sizeof(*num) * cnt);
  } else {
    for (unsigned i = 0; i < cnt; ++i) {
      FastUInt64ToBuffer(num[i], buf);
      puts(buf);
    }
  }
}

void PrintI64(const int64_t* num, unsigned cnt) {
  char buf[kFastToBufferSize];

  if (FLAGS_bin) {
    ssize_t sz = sizeof(*num) * cnt;
    CHECK_EQ(sz, write(STDOUT_FILENO, num, sz));
  } else {
    for (unsigned i = 0; i < cnt; ++i) {
      FastInt64ToBuffer(num[i], buf);
      puts(buf);
    }
  }
}

void PrintI32(const int32_t* num, unsigned cnt) {
  char buf[kFastToBufferSize];

  if (FLAGS_bin) {
    write(STDOUT_FILENO, num, sizeof(*num) * cnt);
  } else {
    for (unsigned i = 0; i < cnt; ++i) {
      FastInt32ToBuffer(num[i], buf);
      puts(buf);
    }
  }
}

void PrintU32(const uint32_t* num, unsigned cnt) {
  char buf[kFastToBufferSize];

  if (FLAGS_bin) {
    write(STDOUT_FILENO, num, sizeof(*num) * cnt);
  } else {
    for (unsigned i = 0; i < cnt; ++i) {
      FastUInt32ToBuffer(num[i], buf);
      puts(buf);
    }
  }
}

void PrintDouble(const double* num, unsigned cnt) {
  char buf[kFastToBufferSize];
  // const int64* inum = reinterpret_cast<const int64*>(num);

  if (FLAGS_bin) {
    write(STDOUT_FILENO, num, sizeof(*num) * cnt);
  } else {

    for (unsigned i = 0; i < cnt; ++i) {
      //FastInt64ToBuffer(inum[i], buf);
      DoubleToBuffer(num[i], buf);

      puts(buf);
    }
  }
}

void PrintBool(const uint8* num, unsigned cnt) {
  if (FLAGS_skip_print)
    return;

  char buf[2] = {0};

  for (unsigned i = 0; i < cnt; ++i) {
    buf[0] = num[i] ? '1' : '0';
    puts(buf);
  }
}


void PrintSlice(VolumeIterator* vi) {
  VolumeProvider* provider = vi->provider();
  DataType dt = provider->dt();
  unsigned dt_size = aux::DataTypeSize(dt);

  constexpr unsigned kBufSize = 4096;
  unsigned iteration = 0;
  uint32_t fetched_size;

  while (vi->NextFrame()) {
    VLOG(1) << "Printing slice " << provider->name() << ", iteration " << iteration++;

    uint64_t items = 0;

    if (dt == DataType::BOOL) {
      uint8 buf[kBufSize];
      SlicePtr sptr(buf);

      while (provider->has_data_to_read()) {
        CHECK_STATUS(provider->GetPage(arraysize(buf), sptr, &fetched_size));

        items += fetched_size;
        PrintBool(buf, fetched_size);
      }
    } else if (dt_size == 8) {
      std::unique_ptr<uint64[]> num(new uint64[kBufSize]);
      SlicePtr sptr(num.get());

      while (provider->has_data_to_read()) {
        CHECK_STATUS(provider->GetPage(kBufSize, sptr, &fetched_size));
        items += fetched_size;

        if (!FLAGS_skip_print) {
          switch(dt) {
            case DataType::UINT64:
              PrintU64(num.get(), fetched_size);
            break;
            case DataType::INT64:
              PrintI64(reinterpret_cast<int64_t*>(num.get()), fetched_size);
            break;
            case DataType::DOUBLE:
              PrintDouble(reinterpret_cast<double*>(num.get()), fetched_size);
            break;
            default:
             LOG(FATAL) << "TBD" << dt;
          }
        }
      }
    } else if (dt_size == 4) {
      std::unique_ptr<uint32[]> num(new uint32[kBufSize]);
      SlicePtr sptr(num.get());

      while (provider->has_data_to_read()) {
        CHECK_STATUS(provider->GetPage(kBufSize, sptr, &fetched_size));
        items += fetched_size;

        if (!FLAGS_skip_print) {
          switch(dt) {
            case DataType::UINT32:
             PrintU32(num.get(), fetched_size);
            break;
            case DataType::INT32:
             PrintI32(reinterpret_cast<int32_t*>(num.get()), fetched_size);
            break;
            default:
             LOG(FATAL) << "TBD" << dt;
          }
        }
      }
    } else {
      LOG(FATAL) << "TBD: " << dt;
    }
    VLOG(1) << "Processed " << items << " items";
  }
}

void PrintStrings(VolumeIterator* str_it, VolumeIterator* sz_it) {
  VolumeProvider* sz_provider = sz_it->provider();
  CHECK_EQ(kStringLenDt, sz_provider->dt());

  VolumeProvider* str_provider = str_it->provider();
  constexpr unsigned kBufSize = 256;
  std::unique_ptr<arrlen_t[]> sz_buf(new arrlen_t[kBufSize]);
  base::PODArray<char> str_buf;
  uint32_t fetched_size;
  while (str_it->NextFrame() && sz_it->NextFrame()) {
    SlicePtr sptr(sz_buf.get());

    while (sz_provider->has_data_to_read()) {
      CHECK_STATUS(sz_provider->GetPage(kBufSize, sptr, &fetched_size));
      for (unsigned j = 0; j < fetched_size; ++j) {
        size_t len = sz_buf[j];
        if (len == kNullStrSz)
          continue;
        str_buf.reserve(len);
        --len;
        uint32_t len_fetch;
        CHECK_STATUS(str_provider->GetPage(len, SlicePtr(str_buf.begin()), &len_fetch));
        CHECK_EQ(len, len_fetch);

        if (!FLAGS_skip_print) {
          str_buf[len] = '\0';
          puts(str_buf.data());
        }
      }
    }
  };
}

}  // namespace


int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  CHECK(!FLAGS_table.empty()) << "Must be provided";

  VolumeReader vol_reader;

  CHECK_STATUS(vol_reader.Open(FLAGS_table));

  LOG(INFO) << "Opened volume with " << vol_reader.frame_count() << " frames";

  for (const auto& si : vol_reader.sinfo_array()) {
    VLOG(1) << "SI: " << si.name() << " " << si.data_type();
  }

  for (int i = 1; i < argc; ++i) {
    StringPiece slice_name(argv[i]);

    std::unique_ptr<VolumeIterator> it(vol_reader.GetSliceIterator(slice_name));
    CHECK(it) << " Could not find " << slice_name;

    DataType dt = DataType(it->provider()->dt());

    if (!FLAGS_bin) {
      fprintf(stdout, "Slice %s %s:\n", argv[i], DataType_Name(dt).c_str());
    }

    size_t disk_sz = 0;
    for (const auto& slice : it->slice_index().slice()) {
      disk_sz += (slice.len() + slice.dict_len());
    }
    LOG(INFO) << "Slice disk size: " << disk_sz;

    if (dt == DataType::STRING) {
      CHECK_EQ(it->slice_index().fc(), FieldClass::DATA);

      StringPiece base_name = pb::SliceBaseName(slice_name);
      string sz_slice_name = pb::FieldClassPath(base_name, FieldClass::STRING_LEN);
      std::unique_ptr<VolumeIterator> str_len_it(vol_reader.GetSliceIterator(sz_slice_name));
      CHECK(str_len_it) << "Could not find " << sz_slice_name;

      PrintStrings(it.get(), str_len_it.get());
    } else {
      PrintSlice(it.get());
    }
  }
  return 0;
}

