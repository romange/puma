// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "strings/unique_strings.h"
#include "puma/data_types.h"
#include <sparsehash/dense_hash_set>

namespace puma {
namespace schema {

class Attribute;
class Table;

typedef ::google::dense_hash_map<StringPiece, const Attribute*> ConstAttributeMap;

class Node {
  Node(const Node&) = delete;
  void operator=(const Node&) = delete;
 public:
  StringPiece name() const { return name_; }

  // Child fields in case this attribute is a message or table.
  const ConstAttributeMap& fields() const { return fields_; }

  unsigned field_count() const { return fields_.size(); }
  const Attribute* field(unsigned i) const { return index_[i]; }

  void AddChild(StringPiece name, const Attribute* attr);

 protected:
  Node(StringPiece name = StringPiece()) : name_(name) {
    fields_.set_empty_key(StringPiece());
  }

  StringPiece name_;
  ConstAttributeMap fields_;
  std::vector<const Attribute*> index_;
};


// Describes a single attribute in a tuple (its name and type).
class Attribute  : public Node {
  using FieldMap = ConstAttributeMap;

 public:
  enum Ordinality { REQUIRED, NULLABLE, REPEATED };

  std::string Path() const;

  DataType type() const { return type_; }
  Ordinality ordinality() const { return ordinality_; }

  // Tells whether this attribute can can have null values.
  bool is_nullable() const { return ordinality_ == NULLABLE; }
  bool is_repeated() const { return ordinality_ == REPEATED; }

  const Attribute* parent() const { return parent_;}

  // The length of path from the root till this attribute (not included).
  // Top attributes have level 0.
  uint16 level() const { return level_; }

  // Child fields in case this attribute is a record.
  const FieldMap& fields() const { return fields_;}

 private:
  friend class Table;

  // Creates an attribute with specified options. For ENUM attributes, you
  // probably want to use the other constructor instead, as this one would
  // create empty ENUM.
  Attribute(StringPiece name,
            DataType type, Ordinality ordinality, const Attribute* parent = nullptr)
      : Node(name), type_(type), ordinality_(ordinality), parent_(parent) {
      if (parent_)
        level_ = parent_->level_ + 1;
  }

  ~Attribute() {}

  DataType type_;
  Ordinality ordinality_;

  // Not owned by the class.
  const Attribute* parent_;
  uint16 level_ = 0;
};

class Table : public Node {
 public:
  Table(StringPiece name, UniqueStrings* str_db = nullptr);
  ~Table();

  // Adds an attribute. name is reallocated internally when stored in the attribute.
  const Attribute* Add(StringPiece name, DataType type, Attribute::Ordinality ordinality,
                       const Attribute* parent = nullptr);

  // Adds an attribute using full path. Parent node must exist in the pool.
  const Attribute* AddWithPath(StringPiece path, DataType type, Attribute::Ordinality ordinality);

  const Attribute* LookupByPath(StringPiece path) const;

 private:
  bool own_ = false;
  UniqueStrings* str_db_;

  ::google::dense_hash_set<Attribute*> attrs_db_;
};

// cb is called on each attribute in Dfs order. It should return true if the run should continue
// and false if it should stop prematurely.
// Dfs returns true if it covered the whole tree or false if it stopped prematurely.
bool Dfs(const Node* root, std::function<bool(const Attribute*)> cb);

}  // namespace schema
}  // namespace puma
