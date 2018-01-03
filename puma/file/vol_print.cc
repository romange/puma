// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "base/init.h"
#include "puma/file/volume_reader.h"

#include "puma/pb/util.h"

#include "strings/numbers.h"
#include "strings/util.h"
#include "util/sq_threadpool.h"

DEFINE_string(table, "", "table name");
DEFINE_bool(skip_print, false, "");
DEFINE_bool(bin, false, "");
DEFINE_int32(threads, 0, "Async");
DEFINE_int32(io_len, 4, "");

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

using namespace boost;
using fibers::channel_op_status;
using util::FileIOManager;
using puma::storage::SliceIndex;
typedef std::unique_ptr<uint8_t[]> ReadBuf;

struct Item {
  strings::MutableByteRange buf;
  FileIOManager::ReadResult read_result;
  ReadBuf holder;

  Item() {}
  Item(Item&&) = default;
  Item& operator=(Item&&) = default;
};

typedef fibers::buffered_channel<Item> ReadQueue;
constexpr unsigned kCompletionQueueLen = 16;

void ReadRequests(ReadQueue* q, unsigned* queue_len) {
  channel_op_status st;
  Item item;

  unsigned reads = 0;
  while(true) {
    st = q->pop(item);
    if (channel_op_status::closed == st)
      break;
    CHECK(st == channel_op_status::success) << int(st);
    item.read_result.get();

    --(*queue_len);
    ++reads;
    if (*queue_len < kCompletionQueueLen / 4)
      boost::this_fiber::yield();
  };
  LOG(INFO) << "Processed " << reads << " reads";
}

void PrintAsync(const SliceIndex& si) {
  int fd = open(FLAGS_table.c_str(), O_RDONLY, 0644);
  CHECK_GT(fd, 0);

  FileIOManager io_mgr(FLAGS_threads, FLAGS_io_len);

  ReadQueue read_channel(kCompletionQueueLen);
  size_t offset = 0;
  unsigned queue_len = 0;
  fibers::fiber read_fiber(fibers::launch::post, &ReadRequests, &read_channel, &queue_len);

  for (int i = 0; i < si.slice_size(); ++i) {
    const SliceIndex::Slice& slice = si.slice(i);
    CHECK_GT(slice.len(), 0);

    offset += slice.offset();
    Item item;
    item.holder.reset(new uint8_t[slice.len()]);
    item.buf.reset(item.holder.get(), slice.len());

    item.read_result = io_mgr.Read(fd, offset, item.buf);

    channel_op_status st = read_channel.push(std::move(item));
    CHECK(st == channel_op_status::success);
    ++queue_len;

    offset += slice.len();
  }

  read_channel.close();
  read_fiber.join();
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

int main(int argc, char** argv) {
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
      if (FLAGS_threads > 0) {
        PrintAsync(it->slice_index());
      } else {
        PrintSlice(it.get());
      }
    }
  }
  return 0;
}

