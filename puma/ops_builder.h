// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/data_types.h"

namespace puma {

typedef void (*ConstOperandFunc)(const void* src, const uint8* src_def, size_t count,
                                 const Variant& const_val, void* dest);

typedef void (*BinOperandFunc)(const void* src[2], const uint8* src_def[2], size_t count,
                               void* dest);

ConstOperandFunc BuildConstOperandFunc(AstOp op, DataType type);
void FreeJitFunc(void* func);

}  // namespace puma
