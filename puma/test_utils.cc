// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/test_utils.h"

#include "base/logging.h"

namespace puma {

using namespace std;
static std::vector<const char*> log_modules;

void IncreaseVlogTo(const char* module, int level) {
  int res = google::SetVLOGLevel(module, level);
  if (res > level) {
    google::SetVLOGLevel(module, res);
  } else {
    log_modules.push_back(module);
  }
}

void RestoreVlog() {
  for (auto m : log_modules) {
      google::SetVLOGLevel(m, 0);
  }
}

::testing::AssertionResult AssertStatus(const char* s_expr, const util::Status& s) {
  if (s.ok()) {
    return ::testing::AssertionSuccess();
  } else {
    return ::testing::AssertionFailure() << s_expr << std::endl << s.ToString();
  }
}


std::string RandStr(const unsigned len) {
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  string s(len, '\0');
  for (unsigned i = 0; i < len; ++i) {
      s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
  }

  s[len] = 0;

  return s;
}


}  // namespace puma
