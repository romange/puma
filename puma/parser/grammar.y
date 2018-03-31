
/*
Copyright 2016, Beeri 15.  All rights reserved.
Author: Roman Gershman (romange@gmail.com)

  example of c-parser with modern defines:
   https://raw.githubusercontent.com/Nakrez/ucc/master/ucc/src/parse/c-parser.yy
*/

%skeleton "lalr1.cc" /* -*- C++ -*- */
%require "3.0.2"
%defines
%define parser_class_name { puma_parser }
%define api.namespace { ::puma }
%name-prefix "puma"

%define api.value.type {union ScanValue}
/*
to remove union and use clean semantic types
%define api.token.constructor
variant
*/

%define parse.assert

%expect 0

%locations
%initial-action
{
  // Initialize the initial location.
  @$.begin.filename = @$.end.filename = nullptr;
};

%define parse.trace
%define parse.error verbose
%define api.token.prefix {TOK_}

%parse-param { yyscan_t scanner_ }
%parse-param { puma::ParserHandler* handler_ }
%lex-param   { yyscan_t scanner_ }


// The following code goes into sqlgram header file.
%code requires {

#include "puma/parser/scan_value.h"

namespace puma {
class ParserHandler;
class AstExpr;
}

#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void *yyscan_t;
#endif

}

// This code goes into cc file
%code {
#include <stdio.h>
#include "puma/parser/puma_scan.h"
#include "puma/parser/parser_handler.h"
#include "strings/stringprintf.h"
#include "strings/strcat.h"


#define CHECK_PARSE_STATUS(loc, st) if (!st.ok()) { \
      error(loc, "Error " + st.ToString()); \
      YYABORT; }

#define HANDLE_ST_OBJ(loc, x, dest) \
    CHECK_PARSE_STATUS(loc, x);  \
    dest = x.obj


}  // End of cc code block.


%token END  0  "end of file"
%token <strval> STRING IDENTIFIER FENCE_IDENTIFIER
%token <uval> INTNUM
%token <uval> BOOL
%token <dval> DOUBLENUM
%token LOAD_FN DEF_FN SUM_FN MAX_FN LEN_FN CNT_FN REPL_FN REDUCE_FN RESTRICT_FN HASH_BY_FENCE OUT_FENCE ROW_FENCE FENCE_DECL

%type <ast_id> expr literal_expr arithmetic_expr logical_expr aggregation_expr

/* operators and precedence levels from lowest to highest.*/

%left <op> RELATION_OP
%left OR_OP
%left AND_OP  /* && */
%right NOT_OP

%left <op> SHIFT_OP /*<< >> */
%left '+' '-'
%left <op> MULTIPL_OP
%left <ast_expr> ASSIGN_OP
%nonassoc UMINUS

%start stmt_list

%%

stmt_list:
    | stmt_list statement
;

statement : var_assignment
    | FENCE_DECL IDENTIFIER ASSIGN_OP fence_definition { CHECK_PARSE_STATUS(@1, handler_->AddFence($2)); }
;

var_assignment : ';'
  |
  IDENTIFIER ASSIGN_OP expr {
                    CHECK_PARSE_STATUS(@1, handler_->AddAssignment($1, $3));
                 }
;

literal_expr :
      INTNUM { $$ = handler_->MakeInt($1); }
    | DOUBLENUM     { $$ = handler_->MakeDouble($1); }
    | STRING        { $$ = handler_->MakeString($1); }
    | IDENTIFIER    { auto res = handler_->LookupIdentifier($1);
                      HANDLE_ST_OBJ(@1, res, $$);
                    }
    | FENCE_IDENTIFIER { auto res = handler_->LookupFence($1); HANDLE_ST_OBJ(@1, res, $$); }
    | BOOL          { $$ = handler_->MakeBool($1 != 0); }
;

logical_expr :
      expr RELATION_OP expr { auto res = handler_->AddBinOp($2, $1, $3); HANDLE_ST_OBJ(@1, res, $$); }
    | NOT_OP expr { auto res = handler_->AddUnaryOp(AstOp::NOT_OP, $2);
                    HANDLE_ST_OBJ(@1, res, $$); }
    | expr AND_OP expr { auto res = handler_->AddBinOp(AstOp::AND_OP, $1, $3);
                         HANDLE_ST_OBJ(@1, res, $$); }
    | expr OR_OP  expr { auto res = handler_->AddBinOp(AstOp::OR_OP, $1, $3); HANDLE_ST_OBJ(@1, res, $$);  }
    | DEF_FN '(' literal_expr ')' {  auto res2 = handler_->AddUnaryOp(AstOp::DEF_FN, $3);
                                     HANDLE_ST_OBJ(@1, res2, $$);
                                }

;

arithmetic_expr :
      expr '+' expr { auto res = handler_->AddBinOp(AstOp::ADD_OP, $1, $3); HANDLE_ST_OBJ(@1, res, $$); }
    | expr '-' expr { auto res = handler_->AddBinOp(AstOp::SUB_OP, $1, $3); HANDLE_ST_OBJ(@1, res, $$);  }
    | expr MULTIPL_OP expr { auto res = handler_->AddBinOp($2, $1, $3); HANDLE_ST_OBJ(@1, res, $$); }
    | '-' expr %prec UMINUS { auto res = handler_->AddUnaryOp(AstOp::SUB_OP, $2);
                              HANDLE_ST_OBJ(@1, res, $$); }
    | expr SHIFT_OP expr { auto res = handler_->AddBinOp($2, $1, $3); HANDLE_ST_OBJ(@1, res, $$); }
;

aggregation_expr : SUM_FN '(' expr ')' { auto res = handler_->AddUnaryOp(AstOp::SUM_FN, $3);
                                         HANDLE_ST_OBJ(@1, res, $$); }
     | MAX_FN '(' expr ')' { auto res = handler_->AddUnaryOp(AstOp::MAX_FN, $3);
                                         HANDLE_ST_OBJ(@1, res, $$); }
     | CNT_FN '(' ')' { auto res = handler_->AddCnt(); HANDLE_ST_OBJ(@1, res, $$); }
;


expr : literal_expr | arithmetic_expr | logical_expr
    | '(' expr ')' {  $$ = $2; }
    | RESTRICT_FN '(' expr ',' logical_expr ')' {
      auto res = handler_->AddBinOp(AstOp::RESTRICT_FN, $3, $5); HANDLE_ST_OBJ(@1, res, $$);
    }

    | LOAD_FN '(' STRING ')' {  auto res = handler_->LoadColumn($3); HANDLE_ST_OBJ(@1, res, $$); }
    | LEN_FN '(' IDENTIFIER ')' { auto res = handler_->LookupIdentifier($3); CHECK_PARSE_STATUS(@3, res);
                                        auto res2 = handler_->AddUnaryOp(AstOp::LEN_FN, res.obj);
                                        HANDLE_ST_OBJ(@1, res2, $$);
                                }
    /* repl(len(x), y) replicates y len(x) times. x must be a repeated field and it should be a child
       of repeated leader of y. */
    | REPL_FN '(' expr ',' expr ')' { auto res = handler_->Replicate($3, $5); HANDLE_ST_OBJ(@1, res, $$); }
    | REDUCE_FN '(' aggregation_expr ')' { auto res = handler_->Reduce($3); HANDLE_ST_OBJ(@1, res, $$); }
;

// Aggregation expression list
aggregation_list :
         | non_empty_agg_list;

non_empty_agg_list : aggregation_expr { handler_->AddArgItem($1); }
         | non_empty_agg_list ',' aggregation_expr { handler_->AddArgItem($3); }

argument_list : expr { handler_->AddArgItem($1);}
         | argument_list ',' expr { handler_->AddArgItem($3); }
;


non_aggregated_array : '[' argument_list ']'
         | expr { handler_->AddArgItem($1); }
;

aggregated_array : '[' aggregation_list ']'
         | aggregation_expr { handler_->AddArgItem($1); }
;

out_list :  argument_list | aggregation_list;

expression_array :  '['  out_list ']'
         | expr { handler_->AddArgItem($1); }
         | aggregation_expr { handler_->AddArgItem($1); }
;


fence_definition : OUT_FENCE '(' { handler_->ArrayStart(); } expression_array optional_opt_list ')'
                              { auto res = handler_->FinalizeFence(ParsedFence::OUT);
                                CHECK_PARSE_STATUS(@1, res);}
    | ROW_FENCE '(' { handler_->ArrayStart(); }
        expression_array optional_opt_list ')'
                              { auto res = handler_->FinalizeFence(ParsedFence::SINGLE_ROW);
                                CHECK_PARSE_STATUS(@1, res);}
    | HASH_BY_FENCE '('  { handler_->ArrayStart(); } non_aggregated_array hash_by_body ')'
      { auto res = handler_->FinalizeFence(ParsedFence::HASH_AGG);
        CHECK_PARSE_STATUS(@1, res); }
;

hash_by_body :
    | ',' { handler_->ArrayStart(); } aggregated_array optional_opt_list

optional_opt_list :
    | ',' opt_pair optional_opt_list
;

opt_pair : IDENTIFIER RELATION_OP expr {
  handler_->AddOptions($1, $3);
}

%code provides {
#define YYSTYPE ::puma::puma_parser::semantic_type
#define YYLTYPE ::puma::puma_parser::location_type
};

%%
void puma::puma_parser::error(const location& l, std::string const& s) {
 handler_->Error(l, s);
}

