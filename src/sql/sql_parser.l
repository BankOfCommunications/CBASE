%option noyywrap nounput noinput nodefault yylineno case-insensitive
%option noyyalloc noyyrealloc noyyfree
%option reentrant bison-bridge bison-locations
%option header-file="sql_parser.lex.h"
%{
#include "sql_parser.tab.h"
#include <stdarg.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include <ctype.h>
#include "parse_node.h"
#include "parse_malloc.h"
#include "ob_non_reserved_keywords.h"
/*add wuna 20150907:b*/
#include <math.h>
//#include "common/utility.h"
/*add wuna 20150907:e*/
extern void yyerror(YYLTYPE* yylloc, ParseResult* p, char* s,...);

#define YY_USER_ACTION yylloc->first_line = yylloc->last_line = ((ParseResult*)yyextra)->yylineno_;\
  yylloc->first_column = ((ParseResult*)yyextra)->yycolumn_;\
  yylloc->last_column = yylloc->first_column + yyleng - 1;\
  ((ParseResult*)yyextra)->yycolumn_ += yyleng;

extern ParseNode* new_node(void *malloc_pool, ObItemType type, int num);

#define malloc_new_node(node, malloc_pool, type, num) \
do \
{ \
  if ((node = new_node(malloc_pool, type, num)) == NULL) \
  { \
    yyerror(yylloc, yyextra, "No more space for mallocing '%s'", yytext); \
    return ERROR; \
  } \
} while (0)

#define check_value(val_ptr) \
do \
{ \
  if (val_ptr == NULL) \
  { \
    yyerror(yylloc, yyextra, "No more space for mallocing string '%s'", yytext); \
    return ERROR; \
  } \
} while (0)

inline unsigned char escaped_char(unsigned char c)
{
  switch (c)
  {
    case 'b':
      return '\b';
    case 'f':
      return '\f';
    case 'n':
      return '\n';
    case 'r':
      return '\r';
    case 't':
      return '\t';
    default:
      return c;
  }
}

/* quote_type: 0 - single quotes; 1 - double quotation marks */
int64_t parse_string(const char* src, char* dest, int64_t len, int quote_type)
{
  int64_t i;
  int64_t index = 0;
  for(i = 0; i < len; ++i)
  {
    unsigned char c = src[i];
    if(c == '\\')
    {
      if (i < len - 1)
      {
        c = src[++i];
      }
      else
      {
        break;
      }
      c = escaped_char(c);
    }
    else if (quote_type == 0 && c == '\'' && i + 1 < len && src[i + 1] == '\'')
    {
      ++i;
    }
    else if (quote_type == 1 && c == '"' && i + 1 < len && src[i + 1] == '"')
    {
      ++i;
    }
    dest[index++] = c;
  }
  assert(index <= len);
  dest[index] = '\0';
  return index;
}

int64_t parse_binary(const char* src, char* dest, int64_t len)
{
  int64_t i;
  int64_t index = 0;
  for(i = 0; i < len; i += 2)
  {
    char buf[3];
    buf[0] = src[i];
    buf[1] = src[i+1];
    buf[2] = 0;
    unsigned char a = strtol(buf,0, 16);
    dest[index++] = (char)a;
  }
  return index;
}

char* str_tolower(char *buff)
{
  if (buff)
  {
    char  *ptr = buff;
    unsigned char ch = *ptr;
    while (*ptr)
    {
      ch = *ptr;
      if (ch >= 'A' && ch <= 'Z')
        ch += 'a' - 'A';
      else if (ch >= 0x80 && isupper(ch))
        ch = tolower(ch);
      *ptr = ch;
      ptr++;
    }
  }
  return buff;
}

/*add liuzy [datetime func] 20151013:b*/
int is_leap_year(int year)
{
  int leap_year = 0;
  if ((year & 3) == 0 && (year%100 || (year%400 == 0 && year)))
  {
    leap_year = 1;
  }
  return leap_year;
}

int check_datetime_validity(struct tm *time, int is_input_date)
{
  int ret = 0;
  int days_in_month[]= {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0};
  if (time->tm_min == 0 && time->tm_sec == 0)
  {
    if (time->tm_hour < 0 || time->tm_hour > 24)
    {
      ret = -1;
    }
  }
  else
  {
    if (time->tm_hour < 0 || time->tm_hour > 23 ||
        time->tm_min < 0 || time->tm_min > 59 ||
        time->tm_sec < 0 || time->tm_sec > 59)
    {
      ret = -1;
    }
  }

  if (ret == 0 && 1 == is_input_date)
  {
    int year = time->tm_year + 1900;
    int month = time->tm_mon + 1;
    if (year < 1 || year > 9999 ||
      month < 1 || month > 12)
    {
      ret = -1;
    }
    else
    {
      int mday = days_in_month[time->tm_mon];
      if (1 == is_leap_year(year) && 2 == month)
      {
        mday += 1;
      }
      if (time->tm_mday < 1 || time->tm_mday > mday)
      {
        ret = -1;
      }
    }
  }
  return ret;
}
/*add 20151013:e*/

%}

%x hint
%x sq

space         [ \t\n\r\f]
non_newline   [^\n\r]
sql_comment   ("--"{non_newline}*)
whitespace    ({space}+|{sql_comment})
hint_begin    (\/\*\+({space}*hint{space}+)?)
c_cmt_begin   \/\*(?:[^\+])
c_cmt_end     \*+\/
c_cmt_cnt     [^*/]+
c_comment     ({c_cmt_begin}{c_cmt_cnt}{c_cmt_end})
comment       ({c_comment}|{sql_comment})
identifer         ([A-Za-z_][A_Za-z0-9_]*)
system_variable  (@@[A-Za-z_][A_Za-z0-9_]*)
temp_variable (@[A-Za-z_][A_Za-z0-9_]*)

quote         '
sqbegin       {quote}
sqend         {quote}
sqdouble      {quote}{quote}
sqcontent     [^\\']+
sqescape      [\\][^0-7]
sqoctal       [\\][0-7]{1,3}
sqnewline     {quote}{whitespace}{quote}


%%
ADD               { return ADD; }
ALTER             { return ALTER; }
AND               { return AND; }
ANY               { return ANY; }
ALL               { return ALL; }
AS                { return AS; }
ASC               { return ASC; }
BEGIN             { return BEGI; }
BETWEEN           { return BETWEEN; }
BIGINT            { return BIGINT; }
BINARY            { return BINARY; }
BOOL(EAN)?       { return BOOLEAN; }
BOTH              { return BOTH; }
BY                { return BY; }
CASCADE           { return CASCADE; }
CASE              { return CASE; }
CHAR(ACTER)?      { return CHARACTER; }
CLUSTER           { return CLUSTER; }
CYCLE             { return CYCLE; } /*add lijianqiang [sequence] 20150325*/
CACHE             { return CACHE;} /*add lijianqiang [sequence] 20150325*/
CONCAT            { return CONCAT;} /*add liumz, [concat function] 20160401*/
COLUMN            { return COLUMN; }
COLUMNS           { return COLUMNS; }
COMMIT            { return COMMIT; }
CONSISTENT        { return CONSISTENT; }
CREATE            { return CREATE; }
CREATETIME        { return CREATETIME; }
CURRENT_USER      { return CURRENT_USER; }
DATE              { return DATE; }
DATETIME          { return DATETIME; }
DEALLOCATE        { return DEALLOCATE; }
DEC(IMAL)?        { return DECIMAL; }
DEFAULT           { return DEFAULT; }
DELETE            { return DELETE; }
DESC              { return DESC; }
DESCRIBE          { return DESCRIBE; }
DISTINCT          { return DISTINCT; }
DOUBLE            { return DOUBLE; }
DROP              { return DROP; }
DUAL              { return DUAL; }
ELSE              { return ELSE; }
END               { return END; }
ERROR             { return ERROR; }
EXCEPT            { return EXCEPT; }
EXECUTE           { return EXECUTE; }
EXISTS            { return EXISTS; }
EXPLAIN           { return EXPLAIN; }
FLOAT             { return FLOAT; }
FLOAT4            { return FLOAT; }
FLOAT8            { return DOUBLE; }
FOR               { return FOR; }
FROM              { return FROM; }
FULL              { return FULL; }
GRANT             { return GRANT; }
GROUP             { return GROUP; }
GLOBAL            { return GLOBAL; }
HAVING            { return HAVING; }
IDENTIFIED        { return IDENTIFIED; }
IF                { return IF; }
IN                { return IN; }
INCREMENT         { return INCREMENT; } /*add lijianqiang [sequence] 20150325*/
INNER             { return INNER; }
INT               { return INTEGER; }
INTEGER           { return INTEGER; }
INTERSECT         { return INTERSECT; }
INSERT            { return INSERT; }
INTO              { return INTO; }
IS                { return IS; }
JOIN              { return JOIN; }
KEY               { return KEY; }
LEADING           { return LEADING; }
LEFT              { return LEFT; }
LIMIT             { return LIMIT; }
LIKE              { return LIKE; }
LOCAL             { return LOCAL; }
LOCKED            { return LOCKED; }
MAXVALUE          { return MAXVALUE; } /*add lijianqiang [sequence] 20150325*/
MEDIUMINT         { return MEDIUMINT; }
MEMORY            { return MEMORY; }
MINVALUE          { return MINVALUE; } /*add lijianqiang [sequence] 20150325*/
MOD               { return MOD; }
MODIFYTIME        { return MODIFYTIME; }
NEXTVAL           { return NEXTVAL; } /*add lijianqiang [sequence] 20150330*/
NO                { return NO; } /*add lijianqiang [sequence] 20150325*/
NOT               { return NOT; }
NUMERIC           { return NUMERIC; }
OFFSET            { return OFFSET; }
ON                { return ON; }
OPTION            { return OPTION; }
OR                { return OR; }
ORDER             { return ORDER; }
OUTER             { return OUTER; }
COMMENT           { return COMMENT; }
PARAMETERS        { return PARAMETERS; }
PASSWORD          { return PASSWORD; }
PRECISION         { return PRECISION; }
PREPARE           { return PREPARE; }
PRIMARY           { return PRIMARY; }
QUICK             { return QUICK;} /*add liuzy [sequence] 20150623*/
REAL              { return REAL; }
RENAME            { return RENAME; }
REPLACE           { return REPLACE; }
RESTART			      { return RESTART; } /*add liuzy [sequence] 20150428*/
RESTRICT          { return RESTRICT; }
REVOKE            { return REVOKE; }
RIGHT             { return RIGHT; }
ROLLBACK          { return ROLLBACK; }
PRIVILEGES        { return PRIVILEGES; }
SELECT            { return SELECT; }
SEQUENCE          { return SEQUENCE; } /*add lijianqiang [sequence] 20150325*/
SCHEMA            { return SCHEMA; }
PREVVAL           { return PREVVAL; } /*add lijianqiang [sequence] 20150325*/
PROCESSLIST       { return PROCESSLIST; }
SCOPE             { return SCOPE; }
SESSION           { return SESSION; }
SET               { return SET; }
SHOW              { return SHOW; }
SMALLINT          { return SMALLINT; }
SNAPSHOT          { return SNAPSHOT; }
SPFILE            { return SPFILE; }
START             { return START; }
STATIC            { return STATIC; }
WEAK              { return WEAK;   }
STRONG            { return STRONG; }
SYSTEM            { return SYSTEM; }
TABLE             { return TABLE; }
TABLES            { return TABLES; }
THEN              { return THEN; }
TIME              { return TIME; }
TIMESTAMP         { return TIMESTAMP; }
TINYINT           { return TINYINT; }
TO                { return TO; }
TRAILING          { return TRAILING; }
TRANSACTION       { return TRANSACTION; }
UNION             { return UNION; }
UPDATE            { return UPDATE; }
USER              { return USER; }
USING             { return USING; }
VALUES            { return VALUES; }
VARBINARY         { return VARBINARY; }
VARCHAR(ACTER)?   { return VARCHAR; }
WHERE             { return WHERE; }
WHEN              { return WHEN; }
WITH              { return WITH; }
WORK              { return WORK; }
KILL              { return KILL; }
QUERY             { return QUERY; }
CONNECTION        { return CONNECTION; }
FROZEN            { return FROZEN;}
@@global          { return GLOBAL_ALIAS; }
@@session         { return SESSION_ALIAS; }
CHANGE_OBI        { return CHANGE_OBI; }
SWITCH_CLUSTER         { return SWITCH_CLUSTER; }
SET_SLAVE_CLUSTER         { return SET_SLAVE_CLUSTER;  }
MASTER            { return MASTER; }
SLAVE             { return SLAVE;  }
FORCE             { return FORCE;  }
INDEX			  { return INDEX;  }
STORING			  { return STORING;}

SEMI			  { return SEMI;} /*add by wanglei [semi join] 20151106*/
  /*add wuna 20150908:b*/
DATE_ADD          { return DATE_ADD; }
ADDDATE           { return ADDDATE; }
DATE_SUB          { return DATE_SUB; }
SUBDATE           { return SUBDATE; }
DAYS              { return DAYS;}
HOURS             { return HOURS; }
MINUTES           { return MINUTES; }
MONTHS            { return MONTHS; }
SECONDS           { return SECONDS; }
MICROSECONDS      { return MICROSECONDS; }
YEARS             { return YEARS; }
INTERVAL          { return INTERVAL; }
 /*add wuna 20150908:e*/
 /*add tianz [EXPORT_TOOL] 20141120:b*/
RANGE			  { return RANGE;}
 /*add 20141120:e*/
 /*add zhaoqiong [TRUNCATE TABLE] 20141120:b*/
TRUNCATE		  { return TRUNCATE;}
 /*add 20141120:e*/
BLOOMFILTER_JOIN  { return  BLOOMFILTER_JOIN;  }/*add hushuang[bloomfilter_join_hint] 20150422:e*/
SI  { return  SI;  }/*add wanglei[semi_join_hint] */
SIB  { return  SIB;  }/*add wanglei[semi_join_hint] */
MERGE_JOIN        { return MERGE_JOIN; }/*add hushuang[bloomfilter_join_hint] 20150422:e*/
PARTITION         { return PARTITION; }/*add liumz, [ROW_NUMBER]20150831*/
  /*add liumz, [fetch first n rows only]20150901:b*/
FETCH             { return FETCH; }
FIRST             { return FIRST; }
ROW(S)?            { return ROWS; }
ONLY              { return ONLY; }
  /*add:e*/
CURRENT_TIMESTAMP { return CURRENT_TIMESTAMP; }/*add liumz, [CURRENT_TIMESTAMP]20151021*/
CURRENT_DATE      { return CURRENT_DATE; }/*add liuzy [datetime func] 20151027*/
WITHIN            { return WITHIN; }/*add gaojt [new_agg_fun] 20141130:b*/
OVER              { return OVER;  }  /*add gaojt  20141130:e*/
I_MULTI_BATCH     { return I_MULTI_BATCH; }/* add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20151213 */
UD_MULTI_BATCH     { return UD_MULTI_BATCH; }/* add gaojt [Delete_Update_Function] [JHOBv0.1] 20160302 */
UD_ALL_ROWKEY     { return UD_ALL_ROWKEY; }/* add gaojt [Delete_Update_Function] [JHOBv0.1] 20160418 */
UD_NOT_PARALLAL     { return UD_NOT_PARALLAL; }/* add gaojt [Delete_Update_Function] [JHOBv0.1] 20160418 */
CHANGE_VALUE_SIZE    {return CHANGE_VALUE_SIZE; }/* add by maosy [Delete_Update_Function] [JHOBv0.1] 20161103 */
DATABASE          { return DATABASE;  } //add dolphin [show database] 20150604
DATABASES         { return DATABASES;  } //add zhangcd [multi_database.show_databases] 20150617
CURRENT           { return CURRENT;  } //add zhangcd [multi_database.show_databases] 20150617

NULL   {
  /* yylval->node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_NULL, 0); */
  malloc_new_node(yylval->node, ((ParseResult*)yyextra)->malloc_pool_, T_NULL, 0);
  return NULLX;
}

[0-9]+ {
  /* ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_INT, 0); */
  ParseNode* node = NULL;
  malloc_new_node(node, ((ParseResult*)yyextra)->malloc_pool_, T_INT, 0);
  yylval->node = node;
  node->str_value_ = parse_strdup(yytext, ((ParseResult*)yyextra)->malloc_pool_);
  check_value(node->str_value_);
  node->value_ = atoll(node->str_value_);
  return INTNUM;
}

[0-9]+E[-+]?[0-9]+ |
[0-9]+"."[0-9]*E[-+]?[0-9]+ |
"."[0-9]+E[-+]?[0-9]+ {
  /* ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_DOUBLE, 0); */
  ParseNode* node = NULL;
  malloc_new_node(node, ((ParseResult*)yyextra)->malloc_pool_, T_DOUBLE, 0);
  yylval->node = node;
  node->str_value_ = parse_strdup(yytext, ((ParseResult*)yyextra)->malloc_pool_);
  check_value(node->str_value_);
  return APPROXNUM;
}

[0-9]+"."[0-9]* |
"."[0-9]+ {
  /* ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_DOUBLE,  0); */
  ParseNode* node = NULL;
  malloc_new_node(node, ((ParseResult*)yyextra)->malloc_pool_, T_DECIMAL/* should be T_DECIMAL,*/, 0);
  yylval->node = node;
  node->str_value_ = parse_strdup(yytext, ((ParseResult*)yyextra)->malloc_pool_);
  check_value(node->str_value_);
  return APPROXNUM;
}

TRUE {
  /* ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_BOOL, 0); */
  malloc_new_node(yylval->node, ((ParseResult*)yyextra)->malloc_pool_, T_BOOL, 0);
  yylval->node->value_ = 1;
  return BOOL;
}
UNKNOWN {
  /* Unknown is can only appears in grammer 'expr is unknown'
     * and it is equal to NULL semanticly
     * so we set its value to to NULL directly
     */
  /* yylval->node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_NULL, 0); */
  malloc_new_node(yylval->node, ((ParseResult*)yyextra)->malloc_pool_, T_NULL, 0);
  return UNKNOWN;
}
FALSE {
  /* ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_BOOL, 0); */
  malloc_new_node(yylval->node, ((ParseResult*)yyextra)->malloc_pool_, T_BOOL, 0);
  yylval->node->value_ = 0;
  return BOOL;
}

{sqbegin} {
  BEGIN(sq);
  ((ParseResult*)yyextra)->start_col_ = yylloc->first_column;
  char ** tmp_literal = &((ParseResult*)yyextra)->tmp_literal_;
  if (*tmp_literal == NULL)
  {
    *tmp_literal = (char*) parse_malloc(
                               ((ParseResult*)yyextra)->input_sql_len_ + 1,
                               ((ParseResult*)yyextra)->malloc_pool_
                               );
    check_value(*tmp_literal);
  }
  malloc_new_node(yylval->node, ((ParseResult*)yyextra)->malloc_pool_, T_STRING, 0);
  yylval->node->value_ = 0;
}

<sq>{sqend} {
  BEGIN(INITIAL);
  yylloc->first_column = ((ParseResult*)yyextra)->start_col_;
  char * tmp_literal = ((ParseResult*)yyextra)->tmp_literal_;
  tmp_literal[yylval->node->value_] = '\0';
  yylval->node->str_value_ = parse_strndup(tmp_literal, yylval->node->value_ + 1,
                                          ((ParseResult*)yyextra)->malloc_pool_);
  return STRING;
}

<sq>{sqdouble} {
  ((ParseResult*)yyextra)->tmp_literal_[yylval->node->value_++] = '\'';
}

<sq>{sqcontent} {
  memmove(((ParseResult*)yyextra)->tmp_literal_ + yylval->node->value_, yytext, yyleng);
  yylval->node->value_ += yyleng;
}

<sq>{sqescape} {
  ((ParseResult*)yyextra)->tmp_literal_[yylval->node->value_++] = escaped_char(yytext[1]);
}

<sq>{sqoctal} {
  unsigned char c = strtoul(yytext+1, NULL, 8);
  ((ParseResult*)yyextra)->tmp_literal_[yylval->node->value_++] = c;
}

<sq>{sqnewline} { /* ignore */ }

<sq>. {
  /* Uncatched '\' before end '\'' */
  ((ParseResult*)yyextra)->tmp_literal_[yylval->node->value_++] = yytext[0];
}

<sq><<EOF>>  {
  yyerror(yylloc, yyextra, "unterminated quoted string");
  return ERROR;

}

\"(\\.|\"\"|[^"\\\n])*\" {
  ParseNode* node = NULL;
  malloc_new_node(node, ((ParseResult*)yyextra)->malloc_pool_, T_IDENT, 0);
  yylval->node = node;
  char* src = yytext+1;
  int len = strlen(src) - 1; //remove last quote charactor
  char* dest = (char*) parse_malloc(len + 1, ((ParseResult*)yyextra)->malloc_pool_);
  check_value(dest);
  node->str_value_ = dest;
  node->value_ = parse_string(src, dest, len, 1);
  return NAME;
}

\"(\\.|[^"\n])*$ {
  yyerror(yylloc, yyextra, "Unterminated string %s", yytext);
  return ERROR;
}

X'([0-9A-F]{2})+'|0X([0-9A-F]{2})+ {
  /* ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_BINARY, 0); */
  ParseNode* node = NULL;
  malloc_new_node(node, ((ParseResult*)yyextra)->malloc_pool_, T_BINARY, 0);
  char* src = yytext + 2;
  char* dest = 0;

  size_t len = strlen(src);
  if(src[len - 1] == '\'')
  {
    --len;
  }
  assert((len % 2) == 0);

  //dest = (char*) malloc(len/2);
  dest = (char*) parse_malloc(len / 2, ((ParseResult*)yyextra)->malloc_pool_);
  check_value(dest);
  node->str_value_ = dest;
  node->value_ = parse_binary(src, dest, len);

  yylval->node = node;
  return STRING;
}

Date{whitespace}?'[0-9]{4}(-[0-9]{2}){2}' {
  int year, month, day;
  struct  tm time;
  int ret = 0;

  /* ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_DATE, 0); */
  ParseNode* node = NULL;
  malloc_new_node(node, ((ParseResult*)yyextra)->malloc_pool_, T_DATE_NEW, 0);
  char* dest = strchr(yytext, '\'');
  dest =  parse_strdup(dest + 1, ((ParseResult*)yyextra)->malloc_pool_); // skip left quote
  check_value(dest);
  size_t len = strlen(dest);
  dest[len - 1] = '\0'; //remove final '

  node->str_value_ = dest;

  ret = sscanf(dest, "%4d-%2d-%2d", &year, &month, &day);
  assert(ret == 3);

  memset(&time, 0, sizeof(struct tm));
  time.tm_year = year - 1900;
  time.tm_mon = month - 1;
  time.tm_mday = day;
  time.tm_hour = 0;
  time.tm_min = 0;
  time.tm_sec = 0;
  time.tm_isdst = -1;

  //add liuzy [datetime func] 20151013:b
  int is_input_date = 1;
  ret = check_datetime_validity(&time, is_input_date);
  if (ret != 0)
  {
    return ERROR;
  }
  //add 20151013:e

  node->value_ = mktime(&time) * 1000000L;//change to micro seconds
  yylval->node = node;
  return DATE_VALUE;
}

Time{whitespace}?'[0-9]{2}(:[0-9]{2}){2}[.][0-9]{1,6}' {

  //add liuzy [datetime del microsec from TIME] 20151126:b
  //Exp: TIME must be without microsec to keep consistent with DB2
  //     so if someone input value of TIME type with microsec, return ERROR
  //     we use "return ERROR" to abandon this function
  return ERROR;
  //add 20151126:b

  //mod peiouya [DATE_TIME] 20150913:b
  int hour, minute, second, micro_sec;

  //mod liuzy [datetime func] 20151013:b
  /*Exp: delete comment*/
  //struct  tm time;
  struct  tm time;
  //mod 20151013:

  int ret = 0;

  /* ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_DATE, 0); */
  ParseNode* node = NULL;
  malloc_new_node(node, ((ParseResult*)yyextra)->malloc_pool_, T_TIME, 0);
  char* dest = strchr(yytext, '\'');
  dest =  parse_strdup(dest + 1, ((ParseResult*)yyextra)->malloc_pool_);  // skip left quote
  check_value(dest);
  size_t len = strlen(dest);
  dest[len - 1] = '\0';//remove final '
  node->str_value_ = dest;

  ret = sscanf(dest, "%2d:%2d:%2d.%d",
    &hour, &minute, &second, &micro_sec);
  assert(ret == 4);

  //mod liuzy [datetime func] 20151013:b
  /*Exp: delete comment*/
  //memset(&time, 0, sizeof(struct tm));
  //time.tm_year = 0;
  //time.tm_mon = 0;
  //time.tm_mday = 0;
  //time.tm_hour = hour;
  //time.tm_min = minute;
  //time.tm_sec = second;
  //time.tm_isdst = -1;
  memset(&time, 0, sizeof(struct tm));
  time.tm_year = 0;
  time.tm_mon = 0;
  time.tm_mday = 0;
  time.tm_hour = hour;
  time.tm_min = minute;
  time.tm_sec = second;
  time.tm_isdst = -1;
  //mod 20151013:e

  //add liuzy [datetime func] 20151013:b
  int is_input_date = 0;
  ret = check_datetime_validity(&time, is_input_date);
  if (ret != 0)
  {
    return ERROR;
  }
  //add 20151013:e

  //node->value_ = mktime(&time) * 1000000L + micro_sec;// unit is microseconds
  node->value_ = (hour * 3600 + minute * 60 + second ) * 1000000L + micro_sec;
  //mod 20150913
  yylval->node = node;
  return DATE_VALUE;
}

Time{whitespace}?'[0-9]{2}(:[0-9]{2}){2}[.]?' {
  //mod peiouya [DATE_TIME] 20150913:b
  int hour, minute, second;

  //mod liuzy [datetime func] 20151013:b
  /*Exp: delete comment*/
  //struct  tm time;
  struct  tm time;
  //mod 20151013:e

  int ret = 0;

  /* ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_DATE, 0); */
  ParseNode* node = NULL;
  malloc_new_node(node, ((ParseResult*)yyextra)->malloc_pool_, T_TIME, 0);
  char* dest = strchr(yytext, '\'');
  dest =  parse_strdup(dest + 1, ((ParseResult*)yyextra)->malloc_pool_);  // skip left quote
  check_value(dest);
  size_t len = strlen(dest);
  dest[len - 1] = '\0';//remove final '
  node->str_value_ = dest;

  ret = sscanf(dest, "%2d:%2d:%2d",
    &hour, &minute, &second);
  assert(ret == 3);

  //mod liuzy [datetime func] 20151013:b
  /*Exp: delete comment*/
  //memset(&time, 0, sizeof(struct tm));
  //time.tm_year = 0;
  //time.tm_mon = 0;
  //time.tm_mday = 0;
  //time.tm_hour = hour;
  //time.tm_min = minute;
  //time.tm_sec = second;
  //time.tm_isdst = -1;
  memset(&time, 0, sizeof(struct tm));
  time.tm_year = 0;
  time.tm_mon = 0;
  time.tm_mday = 0;
  time.tm_hour = hour;
  time.tm_min = minute;
  time.tm_sec = second;
  time.tm_isdst = -1;
  //mod 20151013:e

  //add liuzy [datetime func] 20151013:b
  int is_input_date = 0;
  ret = check_datetime_validity(&time, is_input_date);
  if (ret != 0)
  {
    return ERROR;
  }
  //add 20151013:e

  //node->value_ = mktime(&time) * 1000000L;// unit is microseconds
  node->value_ = (hour * 3600 + minute * 60 + second ) * 1000000L;
  //mod 20150913:e
  yylval->node = node;
  return DATE_VALUE;
}


Timestamp{whitespace}?'[0-9]{4}(-[0-9]{2}){2}[ ][0-9]{2}(:[0-9]{2}){2}[.][0-9]{1,6}' {
  int year, month, day, hour, minute, second, micro_sec;
  struct  tm time;
  int ret = 0;

  /* ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_DATE, 0); */
  ParseNode* node = NULL;
  malloc_new_node(node, ((ParseResult*)yyextra)->malloc_pool_, T_DATE, 0);
  char* dest = strchr(yytext, '\'');
  dest =  parse_strdup(dest + 1, ((ParseResult*)yyextra)->malloc_pool_);;  // skip left quote
  check_value(dest);
  size_t len = strlen(dest);
  dest[len - 1] = '\0';//remove final '
  node->str_value_ = dest;
  char c_usec[7]={'\0'};//add wuna 20150907
 /*mod by wuna 20150907 ret = sscanf(dest, "%4d-%2d-%2d %2d:%2d:%2d.%d",
    &year, &month, &day,
    &hour, &minute, &second, &micro_sec);*/
  ret = sscanf(dest, "%4d-%2d-%2d %2d:%2d:%2d.%6c",
      &year, &month, &day,
      &hour, &minute, &second, c_usec);
  assert(ret == 7);
  /*add wuna 20150907:b*/
  micro_sec = 0;
  int index = 0;
  for (; c_usec[index]!='\0'; index++)
  {
    if(c_usec[index] != '0')
    {
      int number = c_usec[index]-'0';
      micro_sec = micro_sec + (int)(number * pow(10,5-index));
    }
  }
  /*add wuna 20150907:e*/
  memset(&time, 0, sizeof(struct tm));
  time.tm_year = year - 1900;
  time.tm_mon = month - 1;
  time.tm_mday = day;
  time.tm_hour = hour;
  time.tm_min = minute;
  time.tm_sec = second;
  time.tm_isdst = -1;

  //add liuzy [datetime func] 20151013:b
  int is_input_date = 1;
  ret = check_datetime_validity(&time, is_input_date);
  if (ret != 0)
  {
    return ERROR;
  }
  //add 20151013:e

  //mod liuzy [datetime bug] 20150909:b
  /*Exp: before “1970-01-01 08:00:00” need to subtract micro second*/
  //node->value_ = mktime(&time) * 1000000L +  micro_sec;// unit is microseconds
  node->value_ = mktime(&time) * 1000000L + micro_sec;
  //if (node->value_ < 0)
  //{
  //  node->value_ -= micro_sec;
  //}
  //else
  //{
  //  node->value_ += micro_sec;
  //}
  //mod 20150909:e
  yylval->node = node;
  return DATE_VALUE;
}

Timestamp{whitespace}?'[0-9]{4}(-[0-9]{2}){2}[ ][0-9]{2}(:[0-9]{2}){2}[.]?' {
  int year, month, day, hour, minute, second;
  struct  tm time;
  int ret = 0;

  /* ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_DATE, 0); */
  ParseNode* node = NULL;
  malloc_new_node(node, ((ParseResult*)yyextra)->malloc_pool_, T_DATE, 0);
  char* dest = strchr(yytext, '\'');
  dest =  parse_strdup(dest + 1, ((ParseResult*)yyextra)->malloc_pool_);;  // skip left quote
  check_value(dest);
  size_t len = strlen(dest);
  --len;
  dest[len] = '\0';//remove final '
  node->str_value_ = dest;

  ret = sscanf(dest, "%4d-%2d-%2d %2d:%2d:%2d",
    &year, &month, &day,
    &hour, &minute, &second);
  assert(ret == 6);

  memset(&time, 0, sizeof(struct tm));
  time.tm_year = year - 1900;
  time.tm_mon = month - 1;
  time.tm_mday = day;
  time.tm_hour = hour;
  time.tm_min = minute;
  time.tm_sec = second;
  time.tm_isdst = -1;

  //add liuzy [datetime func] 20151013:b
  int is_input_date = 1;
  ret = check_datetime_validity(&time, is_input_date);
  if (ret != 0)
  {
    return ERROR;
  }
  //add 20151013:e

  node->value_ = mktime(&time) * 1000000L;// unit is microseconds
  yylval->node = node;
  return DATE_VALUE;
}

\/\*HINT\+[^*]+\*\/    {
  size_t len = 0;
  char* dest;
  /* ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_HINT, 0); */
  ParseNode* node = NULL;
  malloc_new_node(node, ((ParseResult*)yyextra)->malloc_pool_, T_HINT, 0);
  dest = parse_strdup(yytext + 3, ((ParseResult*)yyextra)->malloc_pool_);
  check_value(dest);
  len = strlen(dest);
  dest[len - 2] = '\0';

  node->str_value_ = dest;
  node->value_ = len - 1;

  yylval->node = node;
  return HINT_VALUE;
}

{hint_begin} {
  BEGIN hint;
  return HINT_BEGIN;
}
<hint>{c_cmt_end} {
  BEGIN INITIAL;
  return HINT_END;
}
<hint>[ \t\r\n] {}
<hint>READ_STATIC  {
  return READ_STATIC;
}
<hint>READ_CONSISTENCY  {
  return READ_CONSISTENCY;
}
<hint>INDEX {
  // add by zcd 20141216 :b
  return INDEX;
  // add :e
}
<hint>NOT_USE_INDEX {
  //add zhuyanchao secondary index 20150708
  return NOT_USE_INDEX;
  //add:e
}
<hint>WEAK  {
  return WEAK;
}
<hint>STRONG  {
  return STRONG;
}
<hint>STATIC  {
  return STATIC;
}
<hint>FROZEN  {
  return FROZEN;
}
<hint>[(),.]  {
  return yytext[0];
}
<hint>HOTSPOT  {
  return HOTSPOT;
}
<hint>JOIN  {
  return JOIN;
}
<hint>BLOOMFILTER_JOIN  {
  return BLOOMFILTER_JOIN;
}
<hint>MERGE_JOIN  {
  return MERGE_JOIN;
}
<hint>SI  {
  return SI;
}
<hint>SIB  {
  return SIB;
}
<hint>I_MULTI_BATCH  {
//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20151213:b
  return I_MULTI_BATCH;
//add gaojt 20151213:e
}
<hint>CHANGE_VALUE_SIZE {
  //add maosy [Delete_Update_Function] [JHOBv0.1] 20161103:b
  return CHANGE_VALUE_SIZE;
  // add maosy 20161103 :e
}
<hint>UD_MULTI_BATCH  {
 //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160302:b
  return UD_MULTI_BATCH;
 //add gaojt 20160302:e
}
<hint>UD_ALL_ROWKEY  {
 //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160418:b
  return UD_ALL_ROWKEY;
 //add gaojt 20160418:e
}
<hint>UD_NOT_PARALLAL  {
 //add gaojt [Delete_Update_Function_isolation] [JHOBv0.1] 20160418:b
 return UD_NOT_PARALLAL;
 //add gaojt 20160418:e
}
<hint>{identifer} {
  // add by zcd 20141216:b
  int ret = NAME;
  const NonReservedKeyword *word = NULL;
  if ((word = non_reserved_keyword_lookup(yytext)) == NULL)
  {
    ParseNode* node = NULL;
    malloc_new_node(node, ((ParseResult*)yyextra)->malloc_pool_, T_IDENT, 0);
    yylval->node = node;
    char *dup_value = parse_strdup(yytext, ((ParseResult*)yyextra)->malloc_pool_);
    check_value(dup_value);
    node->str_value_ = str_tolower(dup_value);
    node->value_ = strlen(node->str_value_);
    ret = NAME;
  }
  else
  {
    yylval->non_reserved_keyword = word;
    ret = word->keyword_type;
  }
  return ret;
  // add :e
}
<hint>[0-9]+"."[0-9]*  {
  return NAME;
}
<hint>. {}

{comment} { /* ignore */ }

[-+&~|^/%*(),.;!] { return yytext[0];}

"||" {return CNNOP;}
"=" {return COMP_EQ;}
">=" {return COMP_GE;}
">" {return COMP_GT;}
"<=" {return COMP_LE;}
"<" {return COMP_LT;}
"!="|"<>" {return COMP_NE;}

"?" {
  /* yylval->node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_QUESTIONMARK, 0); */
  malloc_new_node(yylval->node, ((ParseResult*)yyextra)->malloc_pool_, T_QUESTIONMARK, 0);
  yylval->node->value_ = 0;
  return QUESTIONMARK;
}

{system_variable} {
  /* ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_SYSTEM_VARIABLE, 0); */
  ParseNode* node = NULL;
  malloc_new_node(node, ((ParseResult*)yyextra)->malloc_pool_, T_SYSTEM_VARIABLE, 0);
  yylval->node = node;
  /* skip '@@' */
  node->str_value_ = parse_strdup(yytext + 2, ((ParseResult*)yyextra)->malloc_pool_);
  check_value(node->str_value_);
  node->value_ = strlen(node->str_value_);
  return SYSTEM_VARIABLE;
}

{temp_variable} {
  /* ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_TEMP_VARIABLE, 0); */
  ParseNode* node = NULL;
  malloc_new_node(node, ((ParseResult*)yyextra)->malloc_pool_, T_TEMP_VARIABLE, 0);
  yylval->node = node;
  /* skip '@' */
  node->str_value_ = parse_strdup(yytext + 1, ((ParseResult*)yyextra)->malloc_pool_);
  check_value(node->str_value_);
  node->value_ = strlen(node->str_value_);
  return TEMP_VARIABLE;
}

{identifer} {
  int ret = NAME;
  const NonReservedKeyword *word = NULL;
  if ((word = non_reserved_keyword_lookup(yytext)) == NULL)
  {
    ParseNode* node = NULL;
    malloc_new_node(node, ((ParseResult*)yyextra)->malloc_pool_, T_IDENT, 0);
    yylval->node = node;
    char *dup_value = parse_strdup(yytext, ((ParseResult*)yyextra)->malloc_pool_);
    check_value(dup_value);
    node->str_value_ = str_tolower(dup_value);
    node->value_ = strlen(node->str_value_);
    ret = NAME;
  }
  else
  {
    yylval->non_reserved_keyword = word;
    ret = word->keyword_type;
  }
  return ret;
}

[ \t\r\n]    {}
"--"[ \t].*;

<<EOF>>    {return END_P;}
.    {
  yyerror(yylloc, yyextra, "mystery charactor '%c'", *yytext);
  return ERROR;
}
%%

void * yyalloc (size_t bytes, void* yyscanner)
{
  ParseResult *p = yyget_extra(yyscanner);
  return parse_malloc(bytes, p->malloc_pool_);
}

void * yyrealloc (void * ptr, size_t bytes, void* yyscanner)
{
  ParseResult *p = yyget_extra(yyscanner);
  return parse_realloc(ptr, bytes, p->malloc_pool_);
}

void yyfree (void * ptr, void * yyscanner)
{
  /* Do nothing -- we leave it to the garbage collector. */
  parse_free(ptr);
}
