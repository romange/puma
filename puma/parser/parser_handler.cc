// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "puma/parser/parser_handler.h"

#include "base/logging.h"
#include "base/map-util.h"

#include "puma/schema.h"

#include "puma/parser/attribute_index.h"
#include "puma/parser/location.hh"
#include "puma/parser/puma_scan.h"

#include "strings/strcat.h"

using namespace std;
using util::StatusCode;
using util::Status;
using util::StatusObject;

#define VERIFY_AST_ID(id) CHECK_GE(id, 0); \
                          CHECK_LT(id, ast_arr_.size())

namespace puma {
using schema::Attribute;

namespace {

std::pair<StringPiece, ParserHandler::OptSpec> kWhereSpec =
    {"where",  {DataType::BOOL, false}};

inline Status ToStatus(string msg) {
  return Status(StatusCode::PARSE_ERROR, std::move(msg));
}

AttributeId BinaryLocalId(const QueryAttributeIndex& index, AttributeId l, AttributeId r) {
  if (l == kInvalidAttrId)
    return r;
  if (r == kInvalidAttrId || r == l)
    return l;

  AttributeId lr = index.GetRepeatedLeader(l);
  AttributeId rr = index.GetRepeatedLeader(r);

  // Check whether both expressions should belong to the same repeated message or root.
  if (lr != rr)
    return kInvalidAttrId;

  AttributeId lca_id = index.FindCommonAncestor(l, r);

  return lca_id;
}

string FenceVarName(StringPiece fence_name, unsigned i) {
  return StrCat(fence_name, "$", i);
}

inline AttributeId ResolveVar(AttributeId::TableId tid, StringPiece name, QueryAttributeIndex* index) {
  return (tid == AttributeId::kInvalidTableId) ? kInvalidAttrId : index->AddVar(tid, name);
}

util::StatusObject<DataType> ResolveReturnForNonArithmetic(
    AstOp op, const AstExpr& left, const AstExpr& right) {
  if (op == AstOp::RESTRICT_FN) {
    if (left.is_immediate()) {
      return ToStatus(StrCat("restrict operator does not work on ", left.ToString()));
    }

    CHECK_EQ(DataType::BOOL, right.data_type());

    return left.data_type();

  }

  if (aux::IsLogical(op)) {
    return DataType::BOOL;
  }

  return ToStatus(StrCat("Unsupported operator ", aux::ToString(op)));
}

}  // namespace


ParserHandler::ParserHandler() : ParserHandler(pmr::get_default_resource()) {
}

using OptionArray = ParsedFence::OptionArray;

ParserHandler::ParserHandler(allocator_type alloc)
    : ast_arr_(alloc),
      fence_options_(0, OptionArray::hasher(), OptionArray::key_equal(), alloc),
      fence_arr_(alloc) {
  ident_map_.set_empty_key(StringPiece());
  fence_map_.set_empty_key(StringPiece());

  AstExpr invalid = AstExpr::OpNode(AstOp::DIV_OP, DataType::NONE, -1, -1);
  expr_map_.set_empty_key(invalid);

  fence_options_.set_empty_key(StringPiece());

  qattr_index_.reset(new QueryAttributeIndex(nullptr));
  CHECK_EQ(0, pumalex_init_extra(this, &scanner_));
}

ParserHandler::~ParserHandler() {
  CHECK_EQ(0, pumalex_destroy(scanner_));
}

void ParserHandler::Init(const schema::Table* pool) {
  CHECK(table_ == nullptr);
  CHECK_EQ(0, qattr_index_->num_tables());

  table_ = pool;
  qattr_index_->AddTable(table_);
}

void ParserHandler::Reset(bool clear_attributes) {
  ident_map_.clear();
  fence_map_.clear();
  ast_arr_.clear();
  expr_map_.clear();
  arg_list_.clear();
  fence_arr_.clear();
  fence_options_.clear();

  if (clear_attributes) {
    table_ = nullptr;
  }
  qattr_index_.reset(new QueryAttributeIndex(table_));
}

AstId ParserHandler::MakeString(StringPiece str) {
  StringPiece val = str_db_.Get(str);

  AstExpr e = AstExpr::ConstVal<DataType::STRING>(val);
  return AddAst(e);
}

util::Status ParserHandler::AddAssignment(StringPiece str, AstId id) {
  auto res = ident_map_.emplace(str, id);
  VLOG(1) << "Inserted " << str << " with result " << res.second;

  if (!res.second) {
    return ToStatus(StrCat("Identifier ", str, " already defined"));
  }

  return Status::OK;
}


// The name is allocated by scanner using ParseHandler::GetString().
Status ParserHandler::AddFence(StringPiece name) {
  CHECK(!fence_arr_.empty());

  size_t last = fence_arr_.size() - 1;
  auto res = fence_map_.emplace(name, last);
  if (!res.second) {
    return ToStatus(StrCat("Fence ", name, " already defined"));
  }
  fence_arr_.back().name = name;
  AddFenceSchema(&fence_arr_.back());

  return Status::OK;
}

StatusObject<AstId> ParserHandler::LookupIdentifier(StringPiece str) {
  AstId res = FindValueWithDefault(ident_map_, str, kNullAstId);

  if (res == kNullAstId && table_) {
    // Fallback to search column with the same name and load it.
    // If that succeeds to create this variable with load expression.
    const Attribute* attr = table_->LookupByPath(str);

    if (attr) {
      auto load_col = LoadColumn(str);
      if (!load_col.ok())
        return load_col;
      RETURN_IF_ERROR(AddAssignment(str, load_col.obj));
      return load_col.obj;
    }
  }
  if (res == kNullAstId) {
    return ToStatus(StrCat("Unknown identifier ", str));
  }

  return res;
}

StatusObject<AstId> ParserHandler::LookupFence(StringPiece str) const {
  size_t pos = str.find('$');

  // Checks are here because scanner should have taken care of checking the validy of str according
  // to the agreed template.
  CHECK_NE(StringPiece::npos, pos) << str;
  CHECK_GT(pos, 0);
  CHECK_LT(pos, str.size() - 1);

  StringPiece num_str(str, pos + 1);
  uint32 index = 0;
  CHECK(safe_strtou32(num_str, &index));

  auto it = fence_map_.find(StringPiece(str, 0, pos));
  if (it == fence_map_.end())
    return ToStatus(StrCat("Can not find fence ", str));
  CHECK_LT(it->second, fence_arr_.size());

  const ParsedFence& b = fence_arr_[it->second];
  if (index >= b.output.size()) {
    return ToStatus(StrCat("Fence index is out of bounds: ", str));
  }
  return b.output[index];
}

AstId ParserHandler::MakeInt(uint64 val) {
  if (val <= kint32max)
    return AddAst(AstExpr::ConstVal<DataType::INT32>(val));

  if (val <= kuint32max)
    return AddAst(AstExpr::ConstVal<DataType::UINT32>(val));
  if (val <= kint64max)
    return AddAst(AstExpr::ConstVal<DataType::INT64>(val));
  return AddAst(AstExpr::ConstVal<DataType::UINT64>(val));
}

util::StatusObject<AstExpr> ParserHandler::MakeBinOp(AstOp op, AstId left, AstId right) {
  const AstExpr* re = &ast_arr_[right];
  const AstExpr* le = &ast_arr_[left];

  AstExpr res;
  DataType dt = le->data_type();

  if (table_) {
    if (re->data_type() != le->data_type()) {
      RETURN_IF_ERROR(EqualizeTypes(&left, &right));
      re = &ast_arr_[right];
      le = &ast_arr_[left];

      dt = le->data_type();
    }

    if (!aux::IsArithmetic(op)) {
      auto res2 = ResolveReturnForNonArithmetic(op, *re, *le);
      if (!res2.ok())
        return res2.status;
      dt = res2.obj;
    }
  }

  AstExpr::FlagsMask mask = AstExpr::BINARY_ARG;
  AttributeId lca_id = kInvalidAttrId;
  bool is_scalar = re->is_scalar() && le->is_scalar();

  if (is_scalar) {
    mask |= AstExpr::SCALAR_EXPR;
  } else {
    lca_id = BinaryLocalId(*qattr_index_, le->attr_id(), re->attr_id());
    if (lca_id == kInvalidAttrId){
      return ToStatus(StrCat("Uneven operands ", le->ToString(), " vs ", re->ToString()));
    }
    // Adds ast expressions that align data to parent levels.
    left = AlignNullableChain(lca_id, left);
    right = AlignNullableChain(lca_id, right);
  }

  res = AstExpr::OpNode(op, dt, left, right, mask);
  res.set_attr_id(lca_id);

  return res;
}

util::StatusObject<AstId> ParserHandler::AddBinOp(AstOp op, AstId left, AstId right) {
  if (op == AstOp::NEQ_OP) {
    auto res = AddBinOp(AstOp::EQ_OP, left, right);
    RETURN_IF_ERROR(res.status);
    return AddUnaryOp(AstOp::NOT_OP, res.obj);
  }

  const AstExpr& re = ast_arr_[right];
  const AstExpr& le = ast_arr_[left];
  AstExpr res;

  if (re.is_immediate() && le.is_immediate()) {
    res = AstExpr::TransformConst2(op, le, re);
  } else {
    if (op == AstOp::SUB_OP && re.is_immediate()) {
      Variant v = re.variant();
      DataType dt = UnaryMinus(re.data_type(), &v);

      right = AddAst(AstExpr{dt, v});
      op = AstOp::ADD_OP;
    }
    auto res2 = MakeBinOp(op, left, right);
    RETURN_IF_ERROR(res2.status);
    res = res2.obj;
  }

  VLOG(1) << "AddBinOp: " << res;
  return AddAst(res);
}


util::StatusObject<AstId> ParserHandler::AddUnaryOp(AstOp op, AstId left) {
  VLOG(1) << "AddUnaryOp on " << op << " " << ast_arr_[left];
  VERIFY_AST_ID(left);

  DataType dt = DataType::NONE;

  AstExpr& operand = ast_arr_[left];

  if (operand.is_immediate()) {
    if (aux::IsAggregator(op)) {
      return ToStatus("Aggregation of constant expressions is not supported");
    }
    return AddAst(AstExpr::TransformConst1(op, operand));
  }

  RETURN_IF_ERROR(AstExpr::ResolveUnaryOpType(op, operand.data_type(), &dt));

  AttributeId attr_id = operand.attr_id();
  AstExpr::FlagsMask mask = operand.flags() & AstExpr::ALWAYS_PRESENT;

  switch (op) {
    case AstOp::LEN_FN:
      CHECK_EQ(AstOp::LOAD_FN, operand.op());
      attr_id = qattr_index_->parent(attr_id);
      mask |= AstExpr::ALWAYS_PRESENT;
    break;
    case AstOp::DEF_FN:
      mask |= AstExpr::ALWAYS_PRESENT;
    break;
    default:;  // SUB/NOT/AGGREGATORS
  }

  AstExpr expr = AstExpr::OpNode(op, dt, left, kNullAstId, AstExpr::UNARY_ARG);
  if (operand.is_scalar()) {
    mask |= AstExpr::SCALAR_EXPR;
  } else {
    expr.set_attr_id(attr_id);
  }
  expr.set_mask(mask);

  return AddAst(expr);
}

util::StatusObject<AstId> ParserHandler::Replicate(AstId len_expr, AstId operand) {
  VERIFY_AST_ID(operand);
  VERIFY_AST_ID(len_expr);

  const AstExpr& len_fn = ast_arr_[len_expr];
  const AstExpr& e = ast_arr_[operand];
  if (len_fn.op() != AstOp::LEN_FN) {
    return ToStatus("first argument to repl must be len() expression");
  }
  VERIFY_AST_ID(len_fn.left());

  const AstExpr& var = ast_arr_[len_fn.left()];

  if (e.is_scalar()) {
    return ToStatus("no need to replicate scalar expressions");
  }

  AstExpr expr = AstExpr::OpNode(AstOp::REPL_FN, e.data_type(), len_expr, operand,
                                 AstExpr::BINARY_ARG);
  expr.set_attr_id(var.attr_id());

  return AddAst(expr);
}

util::StatusObject<AstId> ParserHandler::Reduce(AstId agg_expr) {
  VERIFY_AST_ID(agg_expr);
  const AstExpr agg = ast_arr_[agg_expr]; // not constref by design since we push into ast_arr_.

  if (!aux::IsAggregator(agg.op())) {
    return ToStatus("reduce expression must be aggregated");
  }

  const QueryAttributeIndex::Info& info = qattr_index_->info(agg.attr_id());
  VERIFY_AST_ID(info.ast_id);
  if (info.field && !info.field->is_repeated()) {
    return ToStatus("reduce expression must be aggregation over an array");
  }

  AttributeId parent = AttributeId(agg.attr_id().table_id(), info.parent);
  CHECK_NE(kInvalidAttrId, parent);

  GET_UNLESS_ERROR(len_ast, AddUnaryOp(AstOp::LEN_FN, info.ast_id));


  AstExpr expr = AstExpr::OpNode(AstOp::REDUCE_FN, agg.data_type(), agg_expr, len_ast,
                                 AstExpr::BINARY_ARG);
  expr.set_attr_id(parent);

  return AddAst(expr);
}


util::StatusObject<AstId> ParserHandler::AddCnt(StringPiece field) {
  static_assert(aux::IsAggregator(AstOp::CNT_FN), "");

  constexpr AstExpr::FlagsMask mask = AstExpr::ALWAYS_PRESENT;

  CHECK(field.empty()) << "TBD";

  AstExpr expr = AstExpr::OpNode(AstOp::CNT_FN, DataType::INT64, kNullAstId, kNullAstId, mask);
  return AddAst(expr);
}

const AstExpr& ParserHandler::at(AstId id) const {
  VERIFY_AST_ID(id);

  return ast_arr_[id];
}

void ParserHandler::AddArgItem(AstId id) {
  VERIFY_AST_ID(id);

  CHECK(!arg_list_.empty());
  arg_list_.back().push_back(id);
}


void ParserHandler::ArrayStart() {
  arg_list_.emplace_back();
}

void ParserHandler::AddOptions(StringPiece opt, AstId e) {
  fence_options_.emplace(opt, e);
}

Status ParserHandler::FinalizeFence(ParsedFence::Type type) {
  if (type == ParsedFence::HASH_AGG)
    return FinalizeHashBy();

  CHECK_EQ(1, arg_list_.size());

  fence_arr_.emplace_back(type);
  ParsedFence& parsed_fence = fence_arr_.back();

  RETURN_IF_ERROR(ValidateFenceArgs(type, arg_list_.back()));

  parsed_fence.args.swap(arg_list_.back());
  arg_list_.pop_back();

  if (type == ParsedFence::OUT) {
    parsed_fence.pass_expr = FindWithDefault(fence_options_, kWhereSpec.first, kNullAstId);
    bool add_pass = false;
    if (parsed_fence.pass_expr != kNullAstId) {
      const AstExpr& e = at(parsed_fence.pass_expr);
      if (e.attr_id() != kInvalidAttrId) {
        parsed_fence.args.push_back(parsed_fence.pass_expr);
        add_pass = true;
      }
    }
    parsed_fence.lca = AlignToCommonAncestor(&parsed_fence.args);
    if (add_pass) {
      parsed_fence.pass_expr = parsed_fence.args.back();
      parsed_fence.args.pop_back();
    }

    VLOG(1) << "out.lca is " << parsed_fence.lca;
  } else {
    DCHECK_EQ(ParsedFence::SINGLE_ROW, type);

    // Finalize Cnt expression.
    for (AstId id : parsed_fence.args) {
      AstExpr& e = ast_arr_[id];
      if (e.op() == AstOp::CNT_FN) {
        GET_UNLESS_ERROR(field_id, GetCntField(e.left()));
        e.set_left(field_id);
        e.set_mask(AstExpr::UNARY_ARG);
      }
    }
  }
  fence_options_.clear();

  return Status::OK;
}

Status ParserHandler::FinalizeHashBy() {
  CHECK_LE(arg_list_.size(), 2);
  CHECK_GE(arg_list_.size(), 1);

  CHECK(!arg_list_[0].empty());

  for (AstId id : arg_list_[0]) {
    AstExpr& e = ast_arr_[id];

    if (e.is_immediate())
      return ToStatus("Refuse to group by constant");
    CHECK(!aux::IsAggregator(e.op()));
  }
  fence_arr_.emplace_back(ParsedFence::HASH_AGG);
  ParsedFence& fence = fence_arr_.back();

  // Initialize key-by arguments.
  fence.args.swap(arg_list_[0]);

  // Prepare aggregation arguments if needed.
  if (arg_list_.size() > 1) {
    for (AstId id :arg_list_[1]) {
      CHECK(aux::IsAggregator(at(id).op()));
      fence.args2.push_back(id);
    }
  }
  arg_list_.clear();

  std::unordered_map<StringPiece, OptSpec> opts{ {"split", {DataType::INT32, true}},
                                                 kWhereSpec};
  RETURN_IF_ERROR(ValidateOptions(opts));

  AstId split_id = FindValueWithDefault(fence_options_, "split", kNullAstId);
  if (split_id != kNullAstId) {
    fence.split_factor = at(split_id).variant().i64val;
  }

  fence.pass_expr = FindWithDefault(fence_options_, kWhereSpec.first, kNullAstId);
  if (fence.pass_expr != kNullAstId) {
    const AstExpr& e = at(fence.pass_expr);
    if (e.is_immediate()) {
      return ToStatus("Where clause should contain only non-immediate expressions");
    }
  }

  fence_options_.clear();

  return Status::OK;
}


StatusObject<AstId> ParserHandler::GetCntField(AstId field_id) {
  if (qattr_index_->num_tables() == 0) {
    return ToStatus("No tables were found for cnt()");
  }

  CHECK_EQ(kNullAstId, field_id) << "TBD";

  const Attribute* result = nullptr;
  auto cb = [&result](const Attribute* attr) -> bool {
    if (attr->is_nullable() || attr->type() == DataType::BOOL) {
      result = attr;
      return false; // Stop the run
    }
    return true;
  };

  const schema::Table* main_table = qattr_index_->table(kNullTableId);
  CHECK(!schema::Dfs(main_table, cb)) << "TBD";

  string name = result->Path();
  AttributeId aid = qattr_index_->AddVar(kNullTableId, name);
  VLOG(1) << "Will cnt() by " << name << ", ast_id: " << aid;

  return LoadFromAttrId(aid, GetString(name));
}

AstId ParserHandler::LoadFromAttrId(AttributeId attr_id, StringPiece col_name) {
  const QueryAttributeIndex::Info* info = nullptr;
  if (attr_id.local_defined()) {
    info = &qattr_index_->info(attr_id);
    AstId ast_id = info->ast_id;
    if (ast_id != kNullAstId)
      return ast_id;  // Already created.
  }
  DataType dt = DataType::NONE;
  AstExpr::FlagsMask mask = 0;

  if (info && info->field) {
    dt = info->field->type();
    if (!info->field->is_nullable())
      mask |= AstExpr::ALWAYS_PRESENT;

    if (col_name.empty()) {
      col_name = GetString(info->field->Path());
    }
  }

  AstExpr expr(AstOp::LOAD_FN, dt, mask);
  expr.mutable_variant()->strval = col_name;
  expr.set_attr_id(attr_id);
  AstId ast_id = AddAst(expr);

  if (attr_id.local_defined()) {
    qattr_index_->SetAstId(attr_id, ast_id);
  }
  VLOG(1) << "Creating " << expr << " with " << ast_id;

  return ast_id;
}

StatusObject<AstId> ParserHandler::LoadColumn(StringPiece col_name) {
  for (char c : {'#', '$'}) {
    if (col_name.find(c) != StringPiece::npos) {
      char str[] = {c, '\0'};
      return ToStatus(StrCat("Column name can not have char '", str, "'"));
    }
  }

  AttributeId attr_id = qattr_index_->AddVar(col_name);
  if (kInvalidAttrId == attr_id) {
    return ToStatus(StrCat("Unknown column '", col_name, "'"));
  }

  return LoadFromAttrId(attr_id, col_name);
}

Status ParserHandler::EqualizeTypes(AstId* left, AstId* right) {
  const AstExpr& re = ast_arr_[*right];
  const AstExpr& le = ast_arr_[*left];

  if (!re.is_immediate() && !le.is_immediate()) {
    return ToStatus(StrCat("Can promote only constants ", re.ToString()));
  }


  if (re.is_immediate()) {
    auto res = PromoteAndAdd(TypedVariant(re.variant(), re.data_type()), le.data_type());
    if (!res.ok())
      return res.status;
    *right = res.obj;
  } else if (le.is_immediate()) {
    auto res = PromoteAndAdd(TypedVariant(le.variant(), le.data_type()), re.data_type());
    if (!res.ok())
      return res.status;
    *left = res.obj;
  } else {
    LOG(DFATAL) << "TBD";
  }
  return Status::OK;
}

StatusObject<AstId> ParserHandler::PromoteAndAdd(TypedVariant tv, DataType dt) {
  if (!aux::PromoteToType(dt, &tv))
    return ToStatus(StrCat("Could not promote ", tv.ToString(), " to ",
                           DataType_Name(dt)));

  AstId res = AddAst(AstExpr(tv));
  return res;
}

Status ParserHandler::ValidateOptions(const std::unordered_map<StringPiece, OptSpec>& opts) {
  for (const auto& k_v : fence_options_) {
    auto it = opts.find(k_v.first);
    if (it == opts.end())
      return ToStatus(StrCat("Invalid option ", k_v.first));
    const AstExpr& e = at(k_v.second);

    if (it->second.require_const && !e.is_immediate())
      return ToStatus(StrCat("option ", k_v.first, " must be constant"));

    if (e.data_type() != it->second.dt) {
      return ToStatus(StrCat("option ", k_v.first, " is of wrong type"));
    }
  }
  return Status::OK;
}

AstId ParserHandler::AddAst(AstExpr e) {
  AstId id = ast_arr_.size();
  auto res = expr_map_.emplace(e, id);
  if (!res.second) {
    return res.first->second;
  }
  ast_arr_.emplace_back(std::move(e));
  return id;
}

void ParserHandler::AddFenceColExpr(
  AstId arg_id, AttributeId::TableId table_id, unsigned index,
  ParsedFence* fence) {
  fence->output[index] = ast_arr_.size();

  ast_arr_.emplace_back(AstOp::LOAD_FN, ast_arr_[arg_id].data_type(), AstExpr::FENCE_COL);
  auto& e = ast_arr_.back();

  StringPiece var_name = str_db_.Get(FenceVarName(fence->name, index));
  AttributeId attr_id = ResolveVar(table_id, var_name, qattr_index_.get());

  e.mutable_variant()->strval = var_name;
  e.set_attr_id(attr_id);
}

AstId ParserHandler::AlignNullableChain(AttributeId lca_id, AstId id) {
  AttributeId a_id = ast_arr_[id].attr_id();
  if (a_id == lca_id || a_id == kInvalidAttrId)
    return id;
  DataType dt = ast_arr_[id].data_type();
  AttributeId::TableId tid = lca_id.table_id();

  while(true) {
    const auto& ainfo = qattr_index_->info(a_id);
    if (ainfo.parent == lca_id.local_id())
      break;
    AttributeId parent = AttributeId(tid, ainfo.parent);
    const QueryAttributeIndex::Info& parent_info = qattr_index_->info(parent);
    DCHECK(parent_info.field);
    DCHECK_EQ(DataType::MESSAGE, parent_info.field->type());
    if (parent_info.field->is_nullable()) {
      AstId parent_ast_id = LoadFromAttrId(parent);

      AstExpr expr = AstExpr::OpNode(AstOp::ALIGN_FN, dt, parent_ast_id, id);
      expr.set_attr_id(parent);
      id = AddAst(expr);
    }
    a_id = parent;
  }
  return id;
}

Status ParserHandler::ValidateFenceArgs(ParsedFence::Type type, const AstIdArray& args) {
  CHECK(!args.empty());

  AttributeId single_val = kInvalidAttrId;
  unsigned val_cnt = 0;

  for (AstId id : args) {
    const AstExpr& e = at(id);
    if (e.is_immediate()) { // Should we allow this?
      return ToStatus("Output should contain only non-immediate expressions");
    }

    bool is_single = aux::IsAggregator(e.op()) || e.is_scalar();
    if (type == ParsedFence::SINGLE_ROW && !is_single) {
      return ToStatus("row fence should contain only aggregations or scalars");
    } else if (type == ParsedFence::OUT && is_single) {
      return ToStatus("out fence should only contain columnar expressions");
    }

    if (!is_single) {
      AttributeId rep_leader = qattr_index_->GetRepeatedLeader(e.attr_id());

      DCHECK_NE(rep_leader, kInvalidAttrId);
      if (rep_leader != single_val) {
        single_val = rep_leader;
        ++val_cnt;
      }
    }
  }

  if (ParsedFence::OUT == type) {
    if (val_cnt > 1) {
      return ToStatus("out arguments with mixed repeated groups");
    }
    CHECK_EQ(1, val_cnt);   // grammar ensures it.

    std::unordered_map<StringPiece, OptSpec> opts{kWhereSpec};
    RETURN_IF_ERROR(ValidateOptions(opts));

    AstId pass_id = FindWithDefault(fence_options_, kWhereSpec.first, kNullAstId);
    if (pass_id != kNullAstId) {
      const AstExpr& e = at(pass_id);
      if (e.is_immediate()) {
        return ToStatus("'Where' clause should contain only non-immediate expressions");
      }
      if (!e.is_scalar()) {
        AttributeId leader_id = qattr_index_->GetRepeatedLeader(e.attr_id());
        if (leader_id != single_val) {
          return ToStatus("'Where' clause should be in the same repeated group as other columns");
        }
      }
    }
  } else {
    CHECK_EQ(ParsedFence::SINGLE_ROW, type);
    if (!fence_options_.empty()) {
      return ToStatus("row fence does not support options");
    }
  }
  return Status::OK;
}

AttributeId ParserHandler::AlignToCommonAncestor(AstIdArray* args) {
  DCHECK(!args->empty());
  AstIdArray& a = *args;
  AttributeId lca_id =  ast_arr_[a[0]].attr_id();

  for (size_t i = 1; lca_id.local_defined() && i < a.size(); ++i) {
    lca_id = qattr_index_->FindCommonAncestor(lca_id, ast_arr_[a[i]].attr_id());
  }
  for (size_t i = 0; i < a.size(); ++i) {
    a[i] = AlignNullableChain(lca_id, a[i]);
  }

  return lca_id;
}

void ParserHandler::AddFenceSchema(ParsedFence* fence) {
  bool is_scalar = (fence->type == ParsedFence::SINGLE_ROW);
  AttributeId::TableId table_id = AttributeId::kInvalidTableId;

  fence->output.resize(fence->args.size() + fence->args2.size());

  if (!is_scalar) {
    string tbl_name = StrCat("_fence_", fence->name);

    schema::Table* table = new schema::Table(tbl_name, &str_db_);
    for (size_t i = 0; i < fence->args.size(); ++i) {
      const AstExpr& arg = at(fence->args[i]);
      table->Add(FenceVarName(fence->name, i), arg.data_type(), Attribute::NULLABLE);
    }
    for (size_t i = 0; i < fence->args2.size(); ++i) {
      const AstExpr& arg = at(fence->args2[i]);
      table->Add(FenceVarName(fence->name, i + fence->args.size()),
                 arg.data_type(), Attribute::NULLABLE);
    }
    query_tables_.emplace_back(table);
    table_id = qattr_index_->AddTable(table);

    for (size_t i = 0; i < fence->args.size(); ++i) {
      AddFenceColExpr(fence->args[i], table_id, i, fence);
    }

    for (size_t i = 0; i < fence->args2.size(); ++i) {
      AddFenceColExpr(fence->args2[i], table_id, fence->args.size() + i, fence);
    }
  } else {
    for (size_t i = 0; i < fence->args.size(); ++i) {
      fence->output[i] = ast_arr_.size();
      ast_arr_.emplace_back(AstOp::CONST_VAL, at(fence->args[i]).data_type(), AstExpr::FENCE_COL);
    }

    for (size_t i = 0; i < fence->args2.size(); ++i) {
      size_t index = fence->args.size() + i;
      fence->output[index] = ast_arr_.size();
      ast_arr_.emplace_back(AstOp::CONST_VAL, at(fence->args2[i]).data_type(), AstExpr::FENCE_COL);
    }
  }
}

}  // namespace puma
