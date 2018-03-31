// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <string>
#include <pmr/vector.h>

#include "base/pmr.h"
#include "strings/stringpiece.h"
#include "strings/unique_strings.h"
#include "util/status.h"

#include "puma/parser/ast_expr.h"
#include "puma/schema.h"

namespace puma {

class location;
typedef pmr::vector<AstId> AstIdArray;

class ParsedFence {
 public:
  enum Type { OUT, SINGLE_ROW, HASH_AGG };

  typedef base::pmr_dense_hash_map<StringPiece, AstId> OptionArray;

  explicit ParsedFence(Type t) : type(t) {}

  AstIdArray args, args2; // args2 used for aggregated expressions in hashby

  Type type;
  StringPiece name;

  AstIdArray output;

  uint32 split_factor = 0;

  // If defined, this fence will output tuples that satisfy this criteria.
  AstId pass_expr = kNullAstId;

  AttributeId lca = kInvalidAttrId;
};

typedef pmr::vector<ParsedFence> FenceArray;
typedef int32 FenceId;

class QueryAttributeIndex;

class ParserHandler {
 public:
  typedef ::pmr::memory_resource* allocator_type;

  struct OptSpec {
    DataType dt;
    bool require_const;
  };

  ParserHandler();
  ParserHandler(allocator_type alloc);

  virtual ~ParserHandler();

  // The ownership over pool stays with the caller.
  void Init(const schema::Table* pool);

  void Reset(bool clear_attributes = false);

  virtual void Error(const location& l, const std::string& s) {}

  virtual void Debug(const location& l, const char* format, ...)
    __attribute__((__format__(__printf__, 3, 4))) {
  }

  util::StatusObject<AstId> LookupIdentifier(StringPiece str);

  util::StatusObject<AstId> LookupFence(StringPiece str) const;

  // Used by scanner to create STRING literals efficiently.
  StringPiece GetString(StringPiece src) { return str_db_.Get(src); }

  AstId MakeInt(uint64 val);

  AstId MakeBool(bool val) { return AddAst(AstExpr::ConstVal<DataType::BOOL>(val)); }
  AstId MakeDouble(double dval) { return AddAst(AstExpr::ConstVal<DataType::DOUBLE>(dval)); }

  // Used by grammar to create string expressions.
  AstId MakeString(StringPiece str);

  // str must be allocated via GetString method.
  util::Status AddAssignment(StringPiece str, AstId expr);
  util::Status AddFence(StringPiece name);


  util::StatusObject<AstId> AddBinOp(AstOp type, AstId left, AstId right);

  util::StatusObject<AstExpr> MakeBinOp(AstOp type, AstId left, AstId right);

  // col_name is owned by parser.
  util::StatusObject<AstId> LoadColumn(StringPiece col_name);

  util::StatusObject<AstId> AddUnaryOp(AstOp type, AstId e);

  util::StatusObject<AstId> Replicate(AstId len_expr, AstId e);

  util::StatusObject<AstId> Reduce(AstId agg_expr);
  util::StatusObject<AstId> AddCnt(StringPiece field = StringPiece());

  const AstExpr& at(AstId id) const;


  // Output state machine.
  // AddArgItem(true,...); AddArgItem(false,...); AddArgItem(false,...); ...
  // FinalizeOutput();
  void AddArgItem(AstId expr);
  void ArrayStart();

  void AddOptions(StringPiece opt, AstId e);

  util::Status FinalizeFence(ParsedFence::Type type);

  // Read methods.
  const ExprArray& expr_arr() const { return ast_arr_; }
  const FenceArray& barrier_arr() const { return fence_arr_; }
  const schema::Table* pool() const { return table_;}
  const QueryAttributeIndex& attr_index() const { return *qattr_index_;}

  void* scanner() const { return scanner_;}

 protected:
  UniqueStrings str_db_;

  // TODO: right now it's possible to define a fence 'a' and a variable 'a' since they are
  // stored in a different maps. We should fix it.
  ::google::dense_hash_map<StringPiece, AstId> ident_map_;
  ::google::dense_hash_map<StringPiece, FenceId> fence_map_;
  ExprArray ast_arr_;

  struct AstExprHash {
    size_t operator()(const AstExpr& e) const {
      return (int64(e.op()) >> 8) | int64(e.data_type()) | e.variant().i64val;
    }
  };

  ::google::dense_hash_map<AstExpr, AstId, AstExprHash> expr_map_;

  std::vector<AstIdArray> arg_list_;

  ParsedFence::OptionArray fence_options_;

 private:
  AstId AddAst(AstExpr e);

  void AddFenceColExpr(AstId arg_id, AttributeId::TableId table_id, unsigned index,
                       ParsedFence* fence);
  void AddFenceScalarExpr(AstId arg_id, unsigned index, ParsedFence* fence);

  util::Status EqualizeTypes(AstId* left, AstId* right);
  util::StatusObject<AstId> PromoteAndAdd(TypedVariant tv, DataType dt);

  util::Status ValidateOptions(const std::unordered_map<StringPiece, OptSpec>& opts);

  util::Status FinalizeHashBy();
  util::Status ValidateFenceArgs(ParsedFence::Type type, const AstIdArray& args);

  // Finds a least common ancestor for valid arguments, belonging to the same repeated group.
  // Rewrites array if needed by creating aligned expressions with the same lca.
  // Returns its attr_id.
  AttributeId AlignToCommonAncestor(AstIdArray* args);
  util::StatusObject<AstId> GetCntField(AstId field_id);

  AstId LoadFromAttrId(AttributeId id, StringPiece col_name = StringPiece());

  // For messages with defined=false, their attributes are not stored in the columns.
  // For example if instance of m is false, then m.a, m.b etc do not even appear
  // in their columns. AlignNullableChain fixes this by aligning children vectors with base
  // attribute (which can be root attribute or under it).
  // We need it for spreading children data based on the level of the output fence.
  AstId AlignNullableChain(AttributeId lca, AstId id);

  void AddFenceSchema(ParsedFence* fence);

  std::unique_ptr<QueryAttributeIndex> qattr_index_;

  const schema::Table* table_ = nullptr;

  std::vector<std::unique_ptr<schema::Table>> query_tables_;

  FenceArray fence_arr_;
  void* scanner_ = nullptr;
};


}  // namespace puma
