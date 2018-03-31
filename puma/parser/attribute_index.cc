// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/parser/attribute_index.h"

#include "base/logging.h"

namespace puma {
using schema::Attribute;
using std::pair;

QueryAttributeIndex::Info::Info(const schema::Attribute* a) : field(a) {
}

QueryAttributeIndex::QueryAttributeIndex(const schema::Table* table) {
  attr_map_.set_empty_key(nullptr);
  if (table)
    AddTable(table);
}

AttributeId QueryAttributeIndex::AddVar(StringPiece col_name) {
  if (table_arr_.empty())
    return kNullAttrId;

  return AddVar(0, col_name);
}

AttributeId QueryAttributeIndex::AddVar(TableId tid, StringPiece col) {
  DCHECK_LT(tid, table_arr_.size());

  const Attribute* attr = table_arr_[tid]->LookupByPath(col);
  if (!attr)
    return kInvalidAttrId;

  AttributeId::LocalId local_id = AddAttributeInfo(tid, attr);
  CHECK_NE(AttributeId::kNullLocalId, local_id);

  return AttributeId(tid, local_id);
}

void QueryAttributeIndex::SetAstId(AttributeId id, AstId ast_id) {
  CHECK(id != kInvalidAttrId && id != kNullAttrId);
  DCHECK_NE(kNullAstId, ast_id);

  attr_info_[id.table_id()]->at(id.local_id()).ast_id = ast_id;
}

AttributeId QueryAttributeIndex::GetRepeatedLeader(AttributeId id) const {
  if (!id.local_defined())
    return id;

  DCHECK_LT(id.table_id(), attr_info_.size());

  auto& arr = *attr_info_[id.table_id()];
  AttributeId::LocalId l = id.local_id();
  while (true) {
    auto it = arr.begin() + l;
    if (!it->field || it->field->is_repeated())
      break;
    l = it->parent;
  }
  return AttributeId(id.table_id(), l);
}

void QueryAttributeIndex::Reset() {
  attr_map_.clear();
  attr_info_.clear();
  attr_info_.emplace_back(new std::vector<Info>);
  attr_info_[0]->resize(1, Info(nullptr));
}

AttributeId QueryAttributeIndex::FromAttribute(const Attribute* a) const {
  if (!a)
    return kInvalidAttrId;
  auto it = attr_map_.find(a);
  return it == attr_map_.end() ? kInvalidAttrId : it->second;
}

AttributeId::LocalId QueryAttributeIndex::AddAttributeInfo(AttributeId::TableId tid,
                                                      const schema::Attribute* a) {
  if (!a)
    return AttributeId::kNullLocalId;

  auto it = attr_map_.find(a);
  if (it != attr_map_.end())
    return it->second.local_id();

  // Make sure the parent added first - parents should always have id less than of their children.
  AttributeId::LocalId lid = AddAttributeInfo(tid, a->parent());
  DCHECK_LT(tid, attr_info_.size());

  auto& arr = *attr_info_[tid];

  // Now add the attribute.
  auto res = attr_map_.emplace(a, AttributeId(tid, arr.size()));
  CHECK(res.second);
  arr.emplace_back(a);

  auto& ai = arr.back();
  ai.parent = lid;

  return res.first->second.local_id();
}

AttributeId QueryAttributeIndex::FindCommonAncestor(AttributeId u, AttributeId v) const {
  DCHECK(kInvalidAttrId != u) << u;
  DCHECK(kInvalidAttrId != v) << v;
  DCHECK_EQ(u.table_id(), v.table_id());
  DCHECK_LT(u.table_id(), attr_info_.size());

  if (!v.local_defined())
    return v;
  if (!u.local_defined())
    return u;

  AttributeId::LocalId lu = u.local_id(), lv = v.local_id();
  const auto& arr = *attr_info_[u.table_id()];

  if (arr[lu].level() < arr[lv].level())
    std::swap(lu, lv);

  // Equal levels.
  while (arr[lu].level() > arr[lv].level()) {
    lu = arr[lu].parent;
  }

  // Go up evenly.
  while (lu != lv) {
    lu = arr[lu].parent;
    lv = arr[lv].parent;

    if (lu == AttributeId::kNullLocalId)
      break;
  }
  return AttributeId(u.table_id(), lu);
}

TableId QueryAttributeIndex::AddTable(const schema::Table* table) {
  CHECK(table);
  CHECK_LT(table_arr_.size(), 1 << 15);

  AttributeId::TableId id = table_arr_.size();
  table_arr_.push_back(table);
  attr_info_.emplace_back(new std::vector<Info>);
  attr_info_.back()->emplace_back(Info(nullptr));

  return id;
}

std::ostream& operator<<(std::ostream& o, const AttributeId& e) {
  o << "(" << e.table_id() << "/" << e.local_id() << ")";
  return o;
}

}  // namespace puma
