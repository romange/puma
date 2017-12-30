// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/schema.h"

#include "base/logging.h"

namespace puma {
namespace schema {

using namespace std;


void Node::AddChild(StringPiece name, const Attribute* attr) {
  CHECK(fields_.emplace(name, attr).second) << name;
  index_.push_back(attr);
}

string Attribute::Path() const {
  if (!parent_) {
    return name_.as_string();
  }

  string res = parent_->Path();
  res.append(1, '.').append(name_.data(), name_.size());
  return res;
}


Table::Table(StringPiece name, UniqueStrings* str_db) : Node(), str_db_(str_db) {
  CHECK(!name.empty());

  if (!str_db_) {
    str_db_ = new UniqueStrings();
    own_ = true;
  }
  attrs_db_.set_empty_key(nullptr);

  name_ = str_db_->Get(name);
}

Table::~Table() {
  if (own_)
    delete str_db_;
  for (const auto& ptr : attrs_db_)
    delete ptr;
}


const Attribute* Table::Add(StringPiece name, DataType type,
                            Attribute::Ordinality ordinality, const Attribute* parent) {
  CHECK(!name.empty() && name.find('.') == StringPiece::npos);

  StringPiece owned_name = str_db_->Get(name);

  Attribute* new_attr = new Attribute(owned_name, type, ordinality, parent);

  if (parent) {
    CHECK_EQ(DataType::MESSAGE, parent->type());
    auto it = attrs_db_.find(const_cast<Attribute*>(parent));
    CHECK(it != attrs_db_.end());
    (*it)->AddChild(owned_name, new_attr);
  } else {
    AddChild(owned_name, new_attr);
  }
  attrs_db_.insert(new_attr);

  return new_attr;
}

const Attribute* Table::AddWithPath(StringPiece path, DataType type,
                                    Attribute::Ordinality ordinality) {
  size_t pos = path.rfind('.');
  if (pos == StringPiece::npos)
    return Add(path, type, ordinality);

  StringPiece prefix(path, 0, pos);
  const Attribute* parent = LookupByPath(prefix);
  CHECK(parent) << prefix;

  path.advance(pos + 1);
  VLOG(1) << "Adding " << path << " to " << prefix;
  return Add(path, type, ordinality, parent);
}


const Attribute* Table::LookupByPath(StringPiece path) const {
  CHECK(!path.empty());

  const ConstAttributeMap* map = &fields();
  const Attribute* res = nullptr;
  while (true) {
    size_t pos = path.find('.');
    StringPiece name(path, 0, pos);
    auto it = map->find(name);
    if (it == map->end())
      return nullptr;
    res = it->second;
    if (pos == StringPiece::npos)
      break;
    path.advance(pos + 1);
    map = &res->fields();
  }
  return res;
}

bool Dfs(const Node* root, std::function<bool(const Attribute*)> cb) {
  std::vector<const Node*> nodes;
  nodes.push_back(root);

  while (!nodes.empty()) {
    const Node* node = nodes.back();
    nodes.pop_back();

    for (unsigned i = 0; i < node->field_count(); ++i) {
      const Attribute* attr = node->field(i);
      if (!cb(attr))
        return false;
      nodes.push_back(attr);
    }
  }

  return true;
}

}  // namespace schema
}  // namespace puma
