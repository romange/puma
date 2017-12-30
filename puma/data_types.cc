// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <type_traits>

#include "puma/data_types.h"

#include "base/logging.h"
#include "strings/strcat.h"

namespace puma {

using std::string;

std::ostream& operator<<(std::ostream& o, DataType dt) {
  o << DataType_Name(dt);
  return o;
}


namespace {


template<DataType dt> void PromoteInt(TypedVariant* tv) {
  switch(tv->type) {
    case DataType::INT32:
      get_val<dt>(tv->variant) = get_val<DataType::INT32>(tv->variant);
    break;
    case DataType::UINT32:
      get_val<dt>(tv->variant) = get_val<DataType::UINT32>(tv->variant);
    break;
    default:
      LOG(FATAL) << "Should not get here " << tv->type;
  }
  tv->type = dt;
}

}  // namespace


namespace aux {

bool EqualizeTypes(TypedVariant* l, TypedVariant* r) {
  if (l->type == r->type)
    return true;
  if (int(l->type) > int(r->type))
    std::swap(l, r);

  // At this point we need to promote l.
  if (r->type == DataType::DOUBLE) {
    return aux::PromoteToDouble(l);
  }

  if (aux::IsInteger(r->type) && aux::IsInteger(l->type)) {
    if (l->type == DataType::INT64) {
      l->type = DataType::UINT64;  // we just make it unsigned and overflow.
    } else {
      switch(r->type) {
        case DataType::INT64:
          PromoteInt<DataType::INT64>(l);
        break;
        case DataType::UINT64:
          PromoteInt<DataType::UINT64>(l);
        break;
        default:  // must be UINT32
          PromoteInt<DataType::INT64>(l);
          PromoteInt<DataType::INT64>(r);
      }
    }
    return true;
  }
  return false;
}

bool PromoteToType(DataType dt, TypedVariant* tv) {
  DCHECK_NE(dt, tv->type);

  if (dt == DataType::DOUBLE) {
    return aux::PromoteToDouble(tv);
  }
  if (int(tv->type) > int(dt))
    return false;

  if (aux::IsInteger(dt) && aux::IsInteger(tv->type)) {
    switch(dt) {
      case DataType::INT64:
        PromoteInt<DataType::INT64>(tv);
      break;
      case DataType::UINT64:
        PromoteInt<DataType::UINT64>(tv);
      break;
      case DataType::UINT32:
        DCHECK_EQ(DataType::INT32, tv->type);
        if (tv->variant.i32val < 0)
          return false;
        tv->type = DataType::UINT32;
      break;
      default:
        return false;
    }
    return true;
  }
  return false;
}


}  // namespace traits

}  // namespace puma
