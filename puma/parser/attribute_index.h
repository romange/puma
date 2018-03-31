// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "puma/data_types.h"
#include "puma/schema.h"
#include "puma/attribute_id.h"

namespace puma {

// Every referenced attribute in the table corresponds to some AttributeLocalId.
// kNullLocalId is reserved for a "root" node corresponding to Table itself.
class QueryAttributeIndex {
 public:
  explicit QueryAttributeIndex(const schema::Table* table = nullptr);

  struct __attribute__((aligned(4), packed)) Info {
    explicit Info(const schema::Attribute* a);

    const schema::Attribute* field = nullptr;

    AstId ast_id = kNullAstId;

    AttributeId::LocalId parent = AttributeId::kInvalidLocalId;

    bool is_repeated() const { return field && field->is_repeated(); }
    uint16_t level() const { return field->level(); }
  };

  TableId AddTable(const schema::Table* table);
  const schema::Table* table(TableId tid) { return table_arr_[tid]; }

  // Returns kInvalidAttrId if col name is unknown and can not be resolved.
  // Returns kNullAttrId if no table was assigned.
  // col is a full path inside the table.
  AttributeId AddVar(StringPiece col);
  AttributeId AddVar(TableId tid, StringPiece col);

  void SetAstId(AttributeId local_id, AstId ast_id);

  const Info& info(AttributeId id) const { return attr_info_[id.table_id()]->at(id.local_id());}

  AttributeId parent(AttributeId a) const {
    return a.local_defined() ? AttributeId(a.table_id(), info(a).parent) : a;
  }

  // This method returns lowest ancestor that is also repeated. Can return id itself.
  AttributeId GetRepeatedLeader(AttributeId id) const;

  // requires that AddVar was called first on this attribute.
  AttributeId FromAttribute(const schema::Attribute* a) const;

  // Finds Lowest Common Ancestor. if one of the nodes is parent of other then the parent
  // is returned.
  AttributeId FindCommonAncestor(AttributeId u, AttributeId v) const;

  void Reset();

  size_t num_tables() const { return attr_info_.size(); }

 private:
  AttributeId::LocalId AddAttributeInfo(AttributeId::TableId tid, const schema::Attribute* a);

  std::vector<const schema::Table*> table_arr_;

  // reverse map of Atttribute to their AttributeId.
  google::dense_hash_map<const schema::Attribute*, AttributeId> attr_map_;
  std::vector<std::unique_ptr<std::vector<Info>>> attr_info_;

  static_assert(sizeof(Info) == 16, "");
};

}  // namespace puma
