%top{
// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "puma/parser/grammar.h"
#include "puma/parser/parser_handler.h"

}

%{

// The code here is generated in cc file.

// Using yycolumn is problematic if we use puma_scan_bytes directly since it's not initialized properly.
// We just count scanned text.
#define YY_USER_ACTION yylloc->begin.column += yyleng;

#include "strings/escaping.h"


using token = ::puma::puma_parser::token;
using namespace ::puma;

#ifdef __clang__
 #pragma clang diagnostic ignored "-Wdeprecated-register"
#endif

%}

%option yylineno noyywrap reentrant
%option nodefault

/*
 * Set the type of yyextra.  All state variables used by the scanner should
 * be in yyextra, *not* statically allocated.
*/
%option extra-type="::puma::ParserHandler*"

/* remove unused functions*/
%option noinput nounput

/* %option noyyalloc noyyrealloc noyyfree */

%option warn
%option prefix="puma"
%option bison-bridge
%option bison-locations

/*
 * In order to make the world safe for Windows and Mac clients as well as
 * Unix ones, we accept either \n or \r as a newline.  A DOS-style \r\n
 * sequence will be seen as two successive newlines, but that doesn't cause
 * any problems.  Comments that start with -- and extend to the next
 * newline are treated as equivalent to a single whitespace character.
 *
 * NOTE a fine point: if there is no newline following --, we will absorb
 * everything to the end of the input as a comment.  This is correct.  Older
 * versions of Postgres failed to recognize -- as a comment if the input
 * did not end with a newline.
 *
 * XXX perhaps \f (formfeed) should be treated as a newline as well?
 *
 * XXX if you change the set of whitespace characters, fix scanner_isspace()
 * to agree, and see also the plpgsql lexer.
 */

space			[ \t\n\r\f]
horiz_space		[ \t\f]
newline			[\n\r]
non_newline		[^\n\r;]

comment			("--"{non_newline}*)

whitespace		({space}+|{comment})

/*
 * To ensure that {quotecontinue} can be scanned without having to back up
 * if the full pattern isn't matched, we include trailing whitespace in
 * {quotestop}.  This matches all cases where {quotecontinue} fails to match,
 * except for {quote} followed by whitespace and just one "-" (not two,
 * which would start a {comment}).  To cover that we have {quotefail}.
 * The actions for {quotestop} and {quotefail} must throw back characters
 * beyond the quote proper.
 */
quote			'


digit			[0-9]
ident_start		[A-Za-z]
ident_cont		[A-Za-z0-9_.]

identifier		{ident_start}{ident_cont}*

/* Assorted special-case operators and operator-like tokens */
less_equals		"<="
greater_equals	">="|"=>"
not_equals	    "<>"|"!="
not_op          "!"|"NOT"

/*
 * "self" is the set of chars that should be returned as single-character
 * tokens.  "op_chars" is the set of chars that can make up "Op" tokens,
 * which can be one or more characters long (but if a single-char token
 * appears in the "self" set, it is not to be returned as an Op).  Note
 * that the sets overlap, but each has some chars that are not in the other.
 *
 * If you change either set, adjust the character lists appearing in the
 * rule for "operator"!
 */
self			[,()|\[\].;\:\+\-\*\/\%\^\<\>\=]

/* We do not allow minus because minus is an unary expression - that allows us to
   differentiate between binary operator and negative numbers */

integer			{digit}+
decimal			(({digit}*\.{digit}+)|({digit}+\.{digit}*))
escapeseq       (\\(['"\?\\abfnrtv]|[0-7]{1,3}|x[a-fA-F0-9]+))
fence_identifier {identifier}${integer}

/*States. FENCE is inclusive (%s), which means other rules like whitespace are also active.  */

%s FENCE


/*  Rules */
%%

%{
%}

{whitespace}	{}

":="	        { return token::TOK_ASSIGN_OP;}

{less_equals}	{ yylval->op = AstOp::LEQ_OP; return token::TOK_RELATION_OP; }

{greater_equals} { yylval->op = AstOp::GEQ_OP; return token::TOK_RELATION_OP;}

{not_equals}	{ yylval->op = AstOp::NEQ_OP; return token::TOK_RELATION_OP;}

"<"             { yylval->op = AstOp::LT_OP; return token::TOK_RELATION_OP;}
">"             { yylval->op = AstOp::GT_OP; return token::TOK_RELATION_OP;}
"="             { yylval->op = AstOp::EQ_OP; return token::TOK_RELATION_OP;}
">>"            { yylval->op = AstOp::SHIFT_RIGHT_OP; return token::TOK_SHIFT_OP;}
"<<"            { yylval->op = AstOp::SHIFT_LEFT_OP; return token::TOK_SHIFT_OP;}
"true"          { yylval->uval = 1; return token::TOK_BOOL;}
"false"         { yylval->uval = 0; return token::TOK_BOOL;}

{not_op}        { return token::TOK_NOT_OP; }
"&&"            { return token::TOK_AND_OP; }

"*"             { yylval->op = AstOp::MUL_OP; return token::TOK_MULTIPL_OP; }
"/"             { yylval->op = AstOp::DIV_OP; return token::TOK_MULTIPL_OP; }

{self}			{ return yytext[0]; }

{integer}		{
	char* end = yytext + yyleng;
	yylval->uval = strtoull(yytext, &end, 10);
	return token::TOK_INTNUM;
	}
{decimal}		{ yylval->dval = atof(yytext); return token::TOK_DOUBLENUM; }
{quote}([^'\\\n]|{escapeseq})*{quote} {
					yytext[yyleng-1] = '\0';
					StringPiece src(yytext + 1, yyleng - 1);
					int dest_len = 0;
					std::string error;
					if (!strings::CUnescape(src, yytext, &dest_len, &error)) {
					  yyextra->Error(*yylloc, "Could not unescape string " + error);
					  return -1;
					}
					yylval->strval = yyextra->GetString(StringPiece(yytext, dest_len - 1));
					return token::TOK_STRING; }

"fence"         { BEGIN(FENCE); return token::TOK_FENCE_DECL; }
"load"          { return token::TOK_LOAD_FN; }
"def"           { return token::TOK_DEF_FN; }
"len"           { return token::TOK_LEN_FN; }
"repl"          { return token::TOK_REPL_FN; }
"reduce"        { return token::TOK_REDUCE_FN; }
"sum"           { return token::TOK_SUM_FN; }
"max"           { return token::TOK_MAX_FN; }
"cnt"           { return token::TOK_CNT_FN; }
"restrict"      { return token::TOK_RESTRICT_FN; }

<FENCE>"hash_by" { BEGIN(INITIAL); return token::TOK_HASH_BY_FENCE; }
<FENCE>"out"     { BEGIN(INITIAL); return token::TOK_OUT_FENCE; }
<FENCE>"row"     { BEGIN(INITIAL); return token::TOK_ROW_FENCE; }

{identifier}	{ yylval->strval = yyextra->GetString(yytext); return token::TOK_IDENTIFIER; }
{fence_identifier} {
                    // It allocates all the possible variations of fence references.
                    // TODO: to optimize this.
					yylval->strval = yyextra->GetString(yytext); return token::TOK_FENCE_IDENTIFIER;
				}
.               { string str("Invalid character '");
				  str.append(yytext, 1).append("'");
				  yyextra->Error(*yylloc, str);
				  return -1;
			    }

<<EOF>>			{ return 0; }


%%

// The code here is generated in scan.cc file.
