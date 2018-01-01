// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <glog/stl_logging.h>
#include <gmock/gmock.h>

#include <type_traits>

#include "puma/nullable_column.h"
#include "util/status.h"

namespace puma {

using ::testing::MakeMatcher;
using ::testing::Matcher;
using ::testing::MatcherInterface;
using ::testing::MatchResultListener;


::testing::AssertionResult AssertStatus(const char* s_expr, const util::Status& s);

#define ASSERT_OK(s) ASSERT_PRED_FORMAT1(AssertStatus, s)
#define ASSERT_NOK(s) ASSERT_FALSE((s).ok())

std::string RandStr(const unsigned len);

void IncreaseVlogTo(const char* module, int level);
void RestoreVlog();

template<typename T> class ColumnDataIsMatcher : public MatcherInterface<const ColumnSlice&> {
 public:
  template<typename U> ColumnDataIsMatcher(std::initializer_list<U> l) {
    values_.insert(values_.end(), l.begin(), l.end());
  }

  ColumnDataIsMatcher(T val, int32 rep) : val_(val), rep_(rep) {}

  bool MatchAndExplain(const ColumnSlice& slice, MatchResultListener* listener) const {
    if (!slice.is_initialized())
      return false;
    if (rep_ >= 0) {
      if (slice.size() != unsigned(rep_)) return false;
      for (size_t i = 0; i < slice.size(); ++i) {
        if (slice.def(i) && slice.get<T>(i) != val_)
          return false;
      }
      return true;
    }

    if (slice.size() != values_.size()) {
      return false;
    }

    for (size_t i = 0; i < values_.size(); ++i) {
       if (slice.def(i) && slice.get<T>(i) != values_[i])
         return false;
    }
    return true;
  }

  void DescribeTo(::std::ostream* os) const {
    if (rep_ >= 0)
      *os << "equals to (" << val_ << " x " << rep_ << ")";
    else
      *os << "equals to " << values_;
  }

  void DescribeNegationTo(::std::ostream* os) const {
    *os << "not equals to " << values_;
  }
 private:
  std::vector<T> values_;
  T val_ = 0;
  int32 rep_ = -1;
};

class DefineMatcher : public MatcherInterface<const ColumnSlice&> {
 public:
  DefineMatcher(bool val, int32 rep) : val_(val), rep_(rep) {}
  DefineMatcher(std::initializer_list<bool> l) {
    values_.insert(values_.end(), l.begin(), l.end());
  }

  bool MatchAndExplain(const ColumnSlice& col, MatchResultListener* listener) const {
    if (rep_ >= 0) {
      if (col.size() != unsigned(rep_)) return false;

      for (int32 i = 0; i < rep_; ++i) {
        if (col.def(i) != val_)
          return false;
      }
      return true;
    }

    if (values_.size() != col.size())
      return false;

    for (size_t i = 0; i < values_.size(); ++i) {
       if (col.def(i) != values_[i])
         return false;
    }
    return true;
  }

  void DescribeTo(::std::ostream* os) const {
    if (rep_ >= 0)
      *os << "equals to (" << val_ << " x " << rep_ << ")";
    else
      *os << "equals to null";
  }

  void DescribeNegationTo(::std::ostream* os) const {
    *os << "not equals to (" << val_ << " x " << rep_ << ")";;
  }
 private:
  bool val_ = 0;
  int32 rep_ = -1;
  std::vector<bool> values_;
};

template<typename T> using enable_if_int_t = typename
    std::enable_if<std::is_integral<T>::value && sizeof(T) >= sizeof(short),
                   Matcher<const ColumnSlice&>>::type;

template<typename T> using enable_if_not_int_t = typename
    std::enable_if<!(std::is_integral<T>::value && sizeof(T) >= sizeof(short)),
                   Matcher<const ColumnSlice&>>::type;


template<typename T> enable_if_int_t<T> ColumnDataIs(T val, int32 rep) {
  return MakeMatcher(new ColumnDataIsMatcher<T>(val, rep));
}

template<typename T> enable_if_not_int_t<T> ColumnDataIs(T val, int32 rep) {
  return MakeMatcher(new ColumnDataIsMatcher<T>(val, rep));
}

template<typename T> enable_if_int_t<T> ColumnDataIs(std::initializer_list<T> l) {
  return MakeMatcher(new ColumnDataIsMatcher<T>(l));
}

template<typename T> enable_if_not_int_t<T> ColumnDataIs(std::initializer_list<T> l) {
  return MakeMatcher(new ColumnDataIsMatcher<T>(l));
}

inline Matcher<const ColumnSlice&> ColumnDataIs() {
  return MakeMatcher(new ColumnDataIsMatcher<int64>(0, 0));
}

inline Matcher<const ColumnSlice&> DefineIs(bool val, int32 rep) {
  return MakeMatcher(new DefineMatcher(val, rep));
}

inline Matcher<const ColumnSlice&> DefineIs(std::initializer_list<bool> l) {
  return MakeMatcher(new DefineMatcher(l));
}

}  // namespace puma
