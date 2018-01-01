// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <gmock/gmock.h>
#include "base/gtest.h"

#include "base/logging.h"
#include "puma/file/volume_reader.h"
#include "puma/file/volume_writer.h"
#include "puma/file/volume_provider.h"
#include "puma/pb/message_slicer.h"
#include "puma/pb/util.h"
#include "puma/test_utils.h"
#include "strings/strcat.h"

#include "util/plang/addressbook.pb.h"


namespace puma {

using util::Status;
using namespace std;

using namespace tutorial;
using strings::ByteRange;
using testing::WhenSorted;
using testing::ElementsAreArray;

class VolumeTest : public testing::Test {
 public:
  VolumeTest();

 protected:
  void PrepareVolume();
  void PrepareReader();

  std::unique_ptr<VolumeIterator> Start(StringPiece col_name);

  std::unique_ptr<VolumeWriter> volume_writer_;
  std::unique_ptr<pb::MessageSlicer> msg_slicer_;
  std::unique_ptr<pb::SlicerState> slicer_state_;
  std::unique_ptr<VolumeReader> vol_reader_;
};

VolumeTest::VolumeTest() {
  PrepareVolume();
}

void VolumeTest::PrepareVolume() {
  volume_writer_.reset(new VolumeWriter("/tmp/vol_test"));

  CHECK_STATUS(volume_writer_->Open());

  std::vector<pb::FieldDfsNode> node_arr = pb::FromDescr(AddressBook::descriptor());
  for (auto& node : node_arr) {
    if (node.fd->name() == "ts" || node.fd->name() == "activity_id")
      node.fd.is_unordered_set = true;
  }

  msg_slicer_.reset(new pb::MessageSlicer(AddressBook::descriptor(), node_arr.data(),
                                          node_arr.size(), volume_writer_.get()));
  slicer_state_.reset(msg_slicer_->CreateState());
}


void VolumeTest::PrepareReader() {
  slicer_state_->FlushFrame(volume_writer_.get());

  CHECK_STATUS(volume_writer_->Finalize());

  vol_reader_.reset(new VolumeReader);
  CHECK_STATUS(vol_reader_->Open(("/tmp/vol_test-0000.vol")));
}

std::unique_ptr<VolumeIterator> VolumeTest::Start(StringPiece col_name) {
  CHECK(vol_reader_);
  VolumeIterator* it = vol_reader_->GetSliceIterator(col_name);
  return std::unique_ptr<VolumeIterator>(it);
}


TEST_F(VolumeTest, Basic) {
  constexpr unsigned kLen = 100;
  AddressBook ab;
  Person* person = ab.add_person();

  for (unsigned j = 0; j < kLen; ++j) {
    person->mutable_account()->add_activity_id(555 + j);
  }
  for (unsigned i = 0 ; i < kLen; ++i) {
    person->add_tag(StrCat("Rivi___", i));
  }
  person->set_name("Rivi");

  constexpr unsigned kMsgCnt = 100;
  for (unsigned i = 0; i < kMsgCnt; ++i) {
    msg_slicer_->Add(ab, slicer_state_.get());
  }

  PrepareReader();
  uint32_t fetched_size;

  {
    auto ri1 = Start("person.account.activity_id.arr");
    ASSERT_TRUE(ri1 && ri1->NextFrame());

    uint32 dest[500];
    SlicePtr sptr(dest);

    ASSERT_OK(ri1->GetPage(arraysize(dest), sptr, &fetched_size));
    ASSERT_EQ(kMsgCnt, fetched_size);
    for (unsigned i = 0; i <kMsgCnt; ++i)
      ASSERT_EQ(kLen, dest[i]);
  }

  {
    auto vol_it = Start("person.account.activity_id.col");
    ASSERT_TRUE(vol_it  && vol_it->NextFrame());

    uint32 dest[kLen];
    SlicePtr sptr(dest);

    ASSERT_OK(vol_it->GetPage(arraysize(dest), sptr, &fetched_size));
    ASSERT_EQ(kLen, fetched_size);
    std::sort(dest, dest + kLen);

    for (unsigned i = 0; i < kLen; ++i) {
      ASSERT_EQ(555 + i, dest[i]) << i;
    }
  }

  {
    uint8_t buf[128];
    SlicePtr sptr(buf);

    auto ri2 = Start("person.name.col");
    ASSERT_TRUE(ri2 && ri2->NextFrame());

    ASSERT_OK(ri2->GetPage(arraysize(buf), sptr, &fetched_size));
    ASSERT_EQ(128, fetched_size);
    EXPECT_EQ('R', buf[0]);
  }
}


TEST_F(VolumeTest, Bin) {
  constexpr unsigned kLen = 2000;
  AddressBook ab;
  Person* person = ab.add_person();

  string base(100, 'a');
  for (unsigned i = 0 ; i < kLen; ++i) {
    person->add_tag(StrCat(base, "__", i));
  }

  constexpr unsigned kMsgCnt = 50;
  for (unsigned i = 0; i < kMsgCnt; ++i) {
    msg_slicer_->Add(ab, slicer_state_.get());
    LOG(INFO) << "Output cost: " << slicer_state_->OutputCost();
  }

  PrepareReader();

  auto ri1 = Start("person.tag.col");
  ASSERT_TRUE(ri1 && ri1->NextFrame());

  uint8_t buf[128*8];

  unsigned item_cnt = 0;
  size_t delta = 0;
  uint32_t fetched_size;

  while (ri1->has_data_to_read()) {
    SlicePtr sptr(buf + delta);
    ASSERT_OK(ri1->GetPage(arraysize(buf) - delta, sptr, &fetched_size));
    const char* ptr = reinterpret_cast<char*>(buf);
    fetched_size += delta;

    while (true) {
      string expected = StrCat(base, "__", item_cnt % kLen);

      if (expected.size() > fetched_size) {
        memmove(buf, ptr, fetched_size);
        delta = fetched_size;
        break;
      }
      ASSERT_EQ(0, expected.compare(0, expected.size(), ptr, expected.size()));
      ++item_cnt;
      fetched_size -= expected.size();
      ptr += expected.size();
    }
  }
  EXPECT_EQ(kMsgCnt * kLen, item_cnt);
}


TEST_F(VolumeTest, RepeatedMsg) {
  AddressBook ab;
  Person* person = ab.add_person();

  constexpr unsigned kLen = 2000;

  for (unsigned i = 0 ; i < kLen; ++i) {
    person->add_phone()->set_number("foo");
  }

  constexpr unsigned kMsgCnt = 50;
  for (unsigned i = 0; i < kMsgCnt; ++i) {
    msg_slicer_->Add(ab, slicer_state_.get());
    LOG(INFO) << "Output cost: " << slicer_state_->OutputCost();
  }

  PrepareReader();
  auto ri1 = Start("person.phone.arr");
  ASSERT_TRUE(ri1 && ri1->NextFrame());

  uint32_t buf[512];
  SlicePtr sptr(buf);
  uint32_t fetched_size;

  ASSERT_OK(ri1->GetPage(arraysize(buf), sptr, &fetched_size));
  EXPECT_EQ(kMsgCnt, fetched_size);
  for (unsigned i = 0; i < fetched_size; ++i) {
    ASSERT_EQ(kLen, buf[i]);
  }

}

TEST_F(VolumeTest, Numbers) {
  AddressBook ab;
  Person* person = ab.add_person();

  constexpr unsigned kMsgCnt = 50;
  for (unsigned i = 0; i < kMsgCnt; ++i) {
    person->set_id(i + 10);
    person->set_dval(i * 13.5);
    msg_slicer_->Add(ab, slicer_state_.get());
  }
  PrepareReader();

  uint32_t fetched_size;
  {
    auto ri1 = Start("person.id.col");
    ASSERT_TRUE(ri1 && ri1->NextFrame());

    int64 buf[128];
    SlicePtr sptr(buf);

    ASSERT_OK(ri1->GetPage(arraysize(buf), sptr, &fetched_size));

    EXPECT_EQ(kMsgCnt, fetched_size);
    ASSERT_FALSE(ri1->has_data_to_read());

    for (unsigned i = 0; i < fetched_size; ++i) {
      ASSERT_EQ(i + 10, buf[i]);
    }
  }
  {
    auto ri1 = Start("person.dval.col");
    ASSERT_TRUE(ri1 && ri1->NextFrame());

    double buf[128];
    SlicePtr sptr(buf);

    ASSERT_OK(ri1->GetPage(arraysize(buf), sptr, &fetched_size));

    EXPECT_EQ(kMsgCnt, fetched_size);

    ASSERT_FALSE(ri1->has_data_to_read());

    for (unsigned i = 0; i < fetched_size; ++i) {
      ASSERT_FLOAT_EQ(i * 13.5, buf[i]) << i;
    }
  }
}


TEST_F(VolumeTest, RepInt64) {
  AddressBook ab;
  constexpr unsigned kMsgCnt = 50;
  constexpr unsigned kLen = 150;

  std::vector<int64> expected;

  for (unsigned i = 0; i < kLen; ++i) {
    expected.push_back((1 << 16) + i * 2);
    ab.add_ts(expected.back());
  }

  for (unsigned i = 0; i < kMsgCnt; ++i)
    msg_slicer_->Add(ab, slicer_state_.get());
  PrepareReader();
  uint32_t fetched_size;

  {
    auto ri1 = Start("ts.col");
    ASSERT_TRUE(ri1 && ri1->NextFrame());

    std::vector<int64> buf(kLen);

    SlicePtr sptr(buf.data());

    for (unsigned i = 0; i < kMsgCnt; ++i) {
      ASSERT_OK(ri1->GetPage(buf.size(), sptr, &fetched_size));
      EXPECT_EQ(kLen, fetched_size);

      ASSERT_THAT(buf, WhenSorted(ElementsAreArray(expected)));
    }
  }
}

TEST_F(VolumeTest, MultipleFrames) {
  AddressBook ab;
  constexpr unsigned kMsgCnt = 200;
  constexpr unsigned kLen = 150;
  constexpr unsigned kFrameCnt = 3;
  for (unsigned i = 0; i < kLen; ++i) {
    int64_t ts = (1 << 16) + i * 2;
    ab.add_ts(ts);
    ab.add_tmp(ts + 5);

    Person* person = ab.add_person();
    person->set_id(i + 10);
    person->set_dval(i * 13.5);

  }

  for (unsigned index = 0; index < kFrameCnt; ++index) {
    for (unsigned i = 0; i < kMsgCnt; ++i) {
      msg_slicer_->Add(ab, slicer_state_.get());
    }
    slicer_state_->FlushFrame(volume_writer_.get());
    ab.add_ts(index);
  }
  PrepareReader();
  uint32_t fetched_size;
  auto vi = Start("ts.col");
  ASSERT_TRUE(vi && vi->NextFrame());

  unsigned frame_cnt = 0;
  int64 buf[kLen];
  SlicePtr sptr(buf);
  do {
    ++frame_cnt;
    ASSERT_OK(vi->GetPage(kLen, sptr, &fetched_size));
    EXPECT_EQ(kLen, fetched_size);
  } while (vi->NextFrame());

  EXPECT_EQ(kFrameCnt, frame_cnt);
}


TEST_F(VolumeTest, Strings) {
  AddressBook ab;
  constexpr unsigned kLen = 15;
  constexpr char kName[] = "Roman";

  for (unsigned i = 0; i < kLen; ++i) {
    Person* person = ab.add_person();
    person->set_id(i + 10);
    person->set_name(kName);
    if (i % 2 == 0)
      person->set_email("email");
  }
  constexpr unsigned kMsgCnt = 1;

  for (unsigned i = 0; i < kMsgCnt; ++i) {
    msg_slicer_->Add(ab, slicer_state_.get());
  }

  PrepareReader();
  uint32 buf[256];
  uint32_t fetched_size;
  SlicePtr sptr(buf);
  {
    auto vi = Start("person.name.sz");
    ASSERT_TRUE(vi && vi->NextFrame());

    ASSERT_OK(vi->GetPage(arraysize(buf), sptr, &fetched_size));
    ASSERT_EQ(kMsgCnt * kLen, fetched_size);
    for (unsigned i = 0; i < fetched_size; ++i) {
      ASSERT_EQ(5 + 1, buf[i]);
    }
  }
  {
    auto vi = Start("person.email.sz");
    ASSERT_TRUE(vi && vi->NextFrame());
    ASSERT_OK(vi->GetPage(arraysize(buf), sptr, &fetched_size));

    ASSERT_EQ(kMsgCnt * kLen, fetched_size);
    for (unsigned i = 0; i < kLen; ++i) {
      ASSERT_EQ(i % 2 == 0 ? (5 + 1) : 0, buf[i]);
    }
  }
}

}  // namespace puma
