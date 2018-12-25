
/* A Bison parser, made by GNU Bison 2.4.1.  */

/* Skeleton implementation for Bison's Yacc-like parsers in C
   
      Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.
   
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.
   
   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output.  */
#define YYBISON 1

/* Bison version.  */
#define YYBISON_VERSION "2.4.1"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 1

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1

/* Using locations.  */
#define YYLSP_NEEDED 1



/* Copy the first part of user declarations.  */


#include <stdint.h>
#include "parse_node.h"
#include "parse_malloc.h"
#include "ob_non_reserved_keywords.h"
#include "common/ob_privilege_type.h"
#define YYDEBUG 1



/* Enabling traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 0
#endif

/* Enabling the token table.  */
#ifndef YYTOKEN_TABLE
# define YYTOKEN_TABLE 0
#endif


/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     NAME = 258,
     STRING = 259,
     INTNUM = 260,
     DATE_VALUE = 261,
     HINT_VALUE = 262,
     BOOL = 263,
     APPROXNUM = 264,
     NULLX = 265,
     UNKNOWN = 266,
     QUESTIONMARK = 267,
     SYSTEM_VARIABLE = 268,
     TEMP_VARIABLE = 269,
     EXCEPT = 270,
     UNION = 271,
     INTERSECT = 272,
     OR = 273,
     AND = 274,
     NOT = 275,
     COMP_NE = 276,
     COMP_GE = 277,
     COMP_GT = 278,
     COMP_EQ = 279,
     COMP_LT = 280,
     COMP_LE = 281,
     LIKE = 282,
     CNNOP = 283,
     BETWEEN = 284,
     IN = 285,
     IS = 286,
     MOD = 287,
     YEARS = 288,
     MICROSECONDS = 289,
     SECONDS = 290,
     MONTHS = 291,
     MINUTES = 292,
     HOURS = 293,
     DAYS = 294,
     UMINUS = 295,
     ADD = 296,
     ANY = 297,
     ALL = 298,
     ALTER = 299,
     AS = 300,
     ASC = 301,
     BEGI = 302,
     BIGINT = 303,
     BINARY = 304,
     BOOLEAN = 305,
     BOTH = 306,
     BY = 307,
     CASCADE = 308,
     CASE = 309,
     CHARACTER = 310,
     CLUSTER = 311,
     COMMENT = 312,
     COMMIT = 313,
     CONSISTENT = 314,
     COLUMN = 315,
     COLUMNS = 316,
     CREATE = 317,
     CREATETIME = 318,
     CURRENT_USER = 319,
     CHANGE_OBI = 320,
     SWITCH_CLUSTER = 321,
     INDEX = 322,
     STORING = 323,
     CYCLE = 324,
     CACHE = 325,
     CONCAT = 326,
     DATE = 327,
     DATETIME = 328,
     DEALLOCATE = 329,
     DECIMAL = 330,
     DEFAULT = 331,
     DELETE = 332,
     DESC = 333,
     DESCRIBE = 334,
     DISTINCT = 335,
     DOUBLE = 336,
     DROP = 337,
     DUAL = 338,
     TRUNCATE = 339,
     ELSE = 340,
     END = 341,
     END_P = 342,
     ERROR = 343,
     EXECUTE = 344,
     EXISTS = 345,
     EXPLAIN = 346,
     FLOAT = 347,
     FOR = 348,
     FROM = 349,
     FULL = 350,
     FROZEN = 351,
     FORCE = 352,
     I_MULTI_BATCH = 353,
     UD_MULTI_BATCH = 354,
     UD_ALL_ROWKEY = 355,
     UD_NOT_PARALLAL = 356,
     CHANGE_VALUE_SIZE = 357,
     GLOBAL = 358,
     GLOBAL_ALIAS = 359,
     GRANT = 360,
     GROUP = 361,
     HAVING = 362,
     HINT_BEGIN = 363,
     HINT_END = 364,
     HOTSPOT = 365,
     IDENTIFIED = 366,
     IF = 367,
     INNER = 368,
     INTEGER = 369,
     INSERT = 370,
     INTO = 371,
     INCREMENT = 372,
     JOIN = 373,
     KEY = 374,
     LEADING = 375,
     LEFT = 376,
     LIMIT = 377,
     LOCAL = 378,
     LOCKED = 379,
     MEDIUMINT = 380,
     MEMORY = 381,
     MODIFYTIME = 382,
     MASTER = 383,
     MINVALUE = 384,
     MAXVALUE = 385,
     NUMERIC = 386,
     NO = 387,
     NEXTVAL = 388,
     OFFSET = 389,
     ON = 390,
     ORDER = 391,
     OPTION = 392,
     OUTER = 393,
     PARAMETERS = 394,
     PASSWORD = 395,
     PRECISION = 396,
     PREPARE = 397,
     PRIMARY = 398,
     PREVVAL = 399,
     QUICK = 400,
     READ_STATIC = 401,
     REAL = 402,
     RENAME = 403,
     REPLACE = 404,
     RESTRICT = 405,
     PRIVILEGES = 406,
     REVOKE = 407,
     RIGHT = 408,
     ROLLBACK = 409,
     KILL = 410,
     READ_CONSISTENCY = 411,
     RESTART = 412,
     SCHEMA = 413,
     SCOPE = 414,
     SELECT = 415,
     SESSION = 416,
     SESSION_ALIAS = 417,
     SET = 418,
     SHOW = 419,
     SMALLINT = 420,
     SNAPSHOT = 421,
     SPFILE = 422,
     START = 423,
     STATIC = 424,
     SYSTEM = 425,
     STRONG = 426,
     SET_SLAVE_CLUSTER = 427,
     SLAVE = 428,
     SEQUENCE = 429,
     TABLE = 430,
     TABLES = 431,
     THEN = 432,
     TIME = 433,
     TIMESTAMP = 434,
     TINYINT = 435,
     TRAILING = 436,
     TRANSACTION = 437,
     TO = 438,
     UPDATE = 439,
     USER = 440,
     USING = 441,
     VALUES = 442,
     VARCHAR = 443,
     VARBINARY = 444,
     WHERE = 445,
     WHEN = 446,
     WITH = 447,
     WORK = 448,
     PROCESSLIST = 449,
     QUERY = 450,
     CONNECTION = 451,
     WEAK = 452,
     NOT_USE_INDEX = 453,
     PARTITION = 454,
     FETCH = 455,
     FIRST = 456,
     ROWS = 457,
     ONLY = 458,
     CURRENT_TIMESTAMP = 459,
     CURRENT_DATE = 460,
     BLOOMFILTER_JOIN = 461,
     MERGE_JOIN = 462,
     SI = 463,
     SIB = 464,
     WITHIN = 465,
     OVER = 466,
     SEMI = 467,
     DATABASE = 468,
     DATABASES = 469,
     CURRENT = 470,
     USE = 471,
     AUTO_INCREMENT = 472,
     CHUNKSERVER = 473,
     COMPRESS_METHOD = 474,
     CONSISTENT_MODE = 475,
     EXPIRE_INFO = 476,
     GRANTS = 477,
     JOIN_INFO = 478,
     MERGESERVER = 479,
     REPLICA_NUM = 480,
     ROOTSERVER = 481,
     ROW_COUNT = 482,
     SERVER = 483,
     SERVER_IP = 484,
     SERVER_PORT = 485,
     SERVER_TYPE = 486,
     STATUS = 487,
     TABLE_ID = 488,
     TABLET_BLOCK_SIZE = 489,
     TABLET_MAX_SIZE = 490,
     UNLOCKED = 491,
     UPDATESERVER = 492,
     USE_BLOOM_FILTER = 493,
     VARIABLES = 494,
     VERBOSE = 495,
     WARNINGS = 496,
     RANGE = 497,
     DATE_ADD = 498,
     ADDDATE = 499,
     DATE_SUB = 500,
     SUBDATE = 501,
     INTERVAL = 502
   };
#endif



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{


  struct _ParseNode *node;
  const struct _NonReservedKeyword *non_reserved_keyword;
  int    ival;



} YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
#endif

#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} YYLTYPE;
# define yyltype YYLTYPE /* obsolescent; will be withdrawn */
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif


/* Copy the second part of user declarations.  */


#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>

#include "sql_parser.lex.h"

#define YYLEX_PARAM result->yyscan_info_

extern void yyerror(YYLTYPE* yylloc, ParseResult* p, char* s,...);

extern ParseNode* merge_tree(void *malloc_pool, ObItemType node_tag, ParseNode* source_tree);

extern ParseNode* new_terminal_node(void *malloc_pool, ObItemType type);

extern ParseNode* new_non_terminal_node(void *malloc_pool, ObItemType node_tag, int num, ...);

extern char* copy_expr_string(ParseResult* p, int expr_start, int expr_end);

#define ISSPACE(c) ((c) == ' ' || (c) == '\n' || (c) == '\r' || (c) == '\t' || (c) == '\f' || (c) == '\v')

#define malloc_terminal_node(node, malloc_pool, type) \
do \
{ \
  if ((node = new_terminal_node(malloc_pool, type)) == NULL) \
  { \
    yyerror(NULL, result, "No more space for malloc"); \
    YYABORT; \
  } \
} while(0)

#define malloc_non_terminal_node(node, malloc_pool, node_tag, ...) \
do \
{ \
  if ((node = new_non_terminal_node(malloc_pool, node_tag, ##__VA_ARGS__)) == NULL) \
  { \
    yyerror(NULL, result, "No more space for malloc"); \
    YYABORT; \
  } \
} while(0)

#define merge_nodes(node, malloc_pool, node_tag, source_tree) \
do \
{ \
  if (source_tree == NULL) \
  { \
    node = NULL; \
  } \
  else if ((node = merge_tree(malloc_pool, node_tag, source_tree)) == NULL) \
  { \
    yyerror(NULL, result, "No more space for merging nodes"); \
    YYABORT; \
  } \
} while (0)

#define dup_expr_string(str_ptr, result, expr_start, expr_end) \
  do \
  { \
    str_ptr = NULL; \
    int start = expr_start; \
    while (start <= expr_end && ISSPACE(result->input_sql_[start - 1])) \
      start++; \
    if (start >= expr_start \
      && (str_ptr = copy_expr_string(result, start, expr_end)) == NULL) \
    { \
      yyerror(NULL, result, "No more space for copying expression string"); \
      YYABORT; \
    } \
  } while (0)

//add liumz, [column_alias_double_quotes]20151015:b
#define dup_expr_string_no_quotes(str_ptr, result, expr_start, expr_end) \
  do \
  { \
    str_ptr = NULL; \
    int start = expr_start; \
    int end = expr_end - 1; \
    while (start <= end && ISSPACE(result->input_sql_[start - 1])) \
      start++; \
    if (start <= end && result->input_sql_[start - 1] == '\"') \
      start++; \
    if (start >= expr_start \
      && (str_ptr = copy_expr_string(result, start, end)) == NULL) \
    { \
      yyerror(NULL, result, "No more space for copying expression string"); \
      YYABORT; \
    } \
  } while (0)
//add:e

/* add peioy, [from_subQuery_alias]20150928:b */
void random_uuid( char *buf )
{
    const char *c = "89ab";
    int n;
    for( n = 0; n < 16; ++n )
    {
        int b = rand()%255;
        switch( n )
        {
            case 6:
                sprintf(buf, "4%x", b%15 );
            break;
            case 8:
                sprintf(buf, "%c%x", c[rand()%strlen(c)], b%15 );
            break;
            default:
                sprintf(buf, "%02x", b);
            break;
        }
        buf += 2;
        switch( n )
        {
            case 3:
            case 5:
            case 7:
            case 9:
                *buf++ = '-';
                break;
        }
    }
    *buf = 0;
}
/* add peioy, [from_subQuery_alias]20150928:e */



#ifdef short
# undef short
#endif

#ifdef YYTYPE_UINT8
typedef YYTYPE_UINT8 yytype_uint8;
#else
typedef unsigned char yytype_uint8;
#endif

#ifdef YYTYPE_INT8
typedef YYTYPE_INT8 yytype_int8;
#elif (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
typedef signed char yytype_int8;
#else
typedef short int yytype_int8;
#endif

#ifdef YYTYPE_UINT16
typedef YYTYPE_UINT16 yytype_uint16;
#else
typedef unsigned short int yytype_uint16;
#endif

#ifdef YYTYPE_INT16
typedef YYTYPE_INT16 yytype_int16;
#else
typedef short int yytype_int16;
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif ! defined YYSIZE_T && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned int
# endif
#endif

#define YYSIZE_MAXIMUM ((YYSIZE_T) -1)

#ifndef YY_
# if YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(msgid) dgettext ("bison-runtime", msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(msgid) msgid
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(e) ((void) (e))
#else
# define YYUSE(e) /* empty */
#endif

/* Identity function, used to suppress warnings about constant conditions.  */
#ifndef lint
# define YYID(n) (n)
#else
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static int
YYID (int yyi)
#else
static int
YYID (yyi)
    int yyi;
#endif
{
  return yyi;
}
#endif

#if ! defined yyoverflow || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#     ifndef _STDLIB_H
#      define _STDLIB_H 1
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's `empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (YYID (0))
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined _STDLIB_H \
       && ! ((defined YYMALLOC || defined malloc) \
	     && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef _STDLIB_H
#    define _STDLIB_H 1
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
	 || (defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL \
	     && defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yytype_int16 yyss_alloc;
  YYSTYPE yyvs_alloc;
  YYLTYPE yyls_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (sizeof (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (sizeof (yytype_int16) + sizeof (YYSTYPE) + sizeof (YYLTYPE)) \
      + 2 * YYSTACK_GAP_MAXIMUM)

/* Copy COUNT objects from FROM to TO.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(To, From, Count) \
      __builtin_memcpy (To, From, (Count) * sizeof (*(From)))
#  else
#   define YYCOPY(To, From, Count)		\
      do					\
	{					\
	  YYSIZE_T yyi;				\
	  for (yyi = 0; yyi < (Count); yyi++)	\
	    (To)[yyi] = (From)[yyi];		\
	}					\
      while (YYID (0))
#  endif
# endif

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)				\
    do									\
      {									\
	YYSIZE_T yynewbytes;						\
	YYCOPY (&yyptr->Stack_alloc, Stack, yysize);			\
	Stack = &yyptr->Stack_alloc;					\
	yynewbytes = yystacksize * sizeof (*Stack) + YYSTACK_GAP_MAXIMUM; \
	yyptr += yynewbytes / sizeof (*yyptr);				\
      }									\
    while (YYID (0))

#endif

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  200
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   4241

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  259
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  199
/* YYNRULES -- Number of rules.  */
#define YYNRULES  637
/* YYNRULES -- Number of states.  */
#define YYNSTATES  1142

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   502

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint16 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,    36,     2,     2,
      47,    48,    34,    32,   258,    33,    49,    35,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   257,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,    38,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    37,    39,    40,
      41,    42,    43,    44,    45,    46,    50,    51,    52,    53,
      54,    55,    56,    57,    58,    59,    60,    61,    62,    63,
      64,    65,    66,    67,    68,    69,    70,    71,    72,    73,
      74,    75,    76,    77,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
      94,    95,    96,    97,    98,    99,   100,   101,   102,   103,
     104,   105,   106,   107,   108,   109,   110,   111,   112,   113,
     114,   115,   116,   117,   118,   119,   120,   121,   122,   123,
     124,   125,   126,   127,   128,   129,   130,   131,   132,   133,
     134,   135,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,   163,
     164,   165,   166,   167,   168,   169,   170,   171,   172,   173,
     174,   175,   176,   177,   178,   179,   180,   181,   182,   183,
     184,   185,   186,   187,   188,   189,   190,   191,   192,   193,
     194,   195,   196,   197,   198,   199,   200,   201,   202,   203,
     204,   205,   206,   207,   208,   209,   210,   211,   212,   213,
     214,   215,   216,   217,   218,   219,   220,   221,   222,   223,
     224,   225,   226,   227,   228,   229,   230,   231,   232,   233,
     234,   235,   236,   237,   238,   239,   240,   241,   242,   243,
     244,   245,   246,   247,   248,   249,   250,   251,   252,   253,
     254,   255,   256
};

#if YYDEBUG
/* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
   YYRHS.  */
static const yytype_uint16 yyprhs[] =
{
       0,     0,     3,     6,    10,    12,    14,    16,    18,    20,
      22,    24,    26,    28,    30,    32,    34,    36,    38,    40,
      42,    44,    46,    48,    50,    52,    54,    56,    58,    60,
      62,    64,    66,    68,    70,    72,    74,    76,    78,    79,
      81,    83,    87,    89,    93,    99,   101,   105,   111,   113,
     115,   117,   119,   121,   123,   125,   127,   129,   133,   135,
     137,   141,   147,   149,   151,   153,   155,   158,   160,   162,
     165,   168,   172,   176,   180,   184,   188,   192,   196,   198,
     201,   204,   208,   213,   217,   222,   226,   230,   234,   238,
     242,   246,   250,   254,   258,   262,   266,   270,   275,   279,
     283,   286,   290,   295,   299,   304,   308,   313,   319,   326,
     330,   335,   339,   346,   348,   352,   358,   360,   361,   363,
     366,   371,   374,   375,   377,   379,   381,   383,   385,   387,
     389,   391,   393,   395,   397,   402,   408,   418,   427,   432,
     437,   446,   453,   462,   467,   471,   473,   477,   480,   483,
     485,   489,   494,   499,   504,   509,   511,   513,   515,   517,
     519,   521,   523,   527,   531,   538,   546,   548,   552,   556,
     565,   569,   570,   572,   576,   578,   584,   588,   590,   592,
     594,   596,   598,   601,   604,   606,   609,   611,   614,   617,
     619,   622,   625,   628,   631,   633,   635,   637,   640,   646,
     650,   651,   655,   656,   658,   659,   663,   664,   668,   669,
     672,   673,   676,   678,   681,   683,   686,   688,   692,   693,
     697,   701,   705,   709,   713,   717,   721,   725,   729,   733,
     735,   736,   744,   746,   750,   753,   754,   758,   764,   766,
     770,   772,   777,   778,   781,   783,   787,   793,   801,   806,
     814,   815,   818,   820,   822,   826,   827,   829,   833,   837,
     843,   845,   849,   852,   854,   858,   862,   864,   867,   871,
     876,   882,   891,   893,   895,   906,   911,   916,   921,   922,
     925,   929,   930,   941,   949,   957,   962,   967,   972,   975,
     978,   983,   988,   994,   995,   999,  1001,  1005,  1006,  1008,
    1011,  1013,  1015,  1017,  1022,  1028,  1033,  1037,  1042,  1044,
    1046,  1048,  1050,  1052,  1054,  1058,  1060,  1062,  1064,  1066,
    1069,  1070,  1072,  1074,  1076,  1078,  1080,  1082,  1083,  1085,
    1086,  1089,  1093,  1098,  1103,  1108,  1112,  1116,  1120,  1121,
    1125,  1126,  1130,  1132,  1133,  1137,  1139,  1143,  1146,  1147,
    1149,  1151,  1152,  1155,  1156,  1158,  1160,  1162,  1165,  1169,
    1171,  1173,  1177,  1179,  1183,  1185,  1189,  1192,  1194,  1196,
    1202,  1204,  1208,  1211,  1213,  1217,  1221,  1228,  1234,  1237,
    1240,  1243,  1245,  1247,  1250,  1253,  1255,  1256,  1260,  1262,
    1264,  1266,  1268,  1270,  1271,  1275,  1281,  1287,  1293,  1298,
    1303,  1308,  1311,  1316,  1320,  1324,  1328,  1332,  1336,  1340,
    1344,  1347,  1351,  1355,  1360,  1363,  1364,  1366,  1369,  1374,
    1376,  1378,  1379,  1380,  1383,  1386,  1387,  1389,  1390,  1392,
    1396,  1398,  1402,  1407,  1409,  1411,  1415,  1417,  1421,  1423,
    1427,  1429,  1433,  1435,  1439,  1445,  1450,  1453,  1455,  1457,
    1459,  1461,  1463,  1465,  1467,  1469,  1471,  1473,  1476,  1477,
    1479,  1482,  1486,  1490,  1493,  1496,  1499,  1502,  1504,  1507,
    1510,  1513,  1515,  1518,  1520,  1523,  1527,  1532,  1535,  1537,
    1539,  1541,  1543,  1545,  1547,  1549,  1551,  1553,  1555,  1559,
    1565,  1572,  1575,  1576,  1580,  1584,  1586,  1590,  1595,  1597,
    1599,  1601,  1602,  1606,  1607,  1610,  1614,  1617,  1620,  1625,
    1626,  1628,  1629,  1631,  1633,  1640,  1642,  1646,  1649,  1651,
    1653,  1656,  1658,  1660,  1662,  1665,  1667,  1669,  1671,  1673,
    1675,  1676,  1678,  1682,  1686,  1690,  1692,  1698,  1701,  1702,
    1707,  1709,  1711,  1713,  1715,  1717,  1720,  1722,  1726,  1730,
    1734,  1739,  1744,  1750,  1756,  1760,  1762,  1764,  1766,  1770,
    1773,  1774,  1776,  1780,  1784,  1786,  1788,  1793,  1800,  1806,
    1808,  1812,  1816,  1821,  1826,  1832,  1834,  1835,  1837,  1839,
    1840,  1844,  1848,  1852,  1855,  1860,  1868,  1876,  1883,  1884,
    1886,  1888,  1892,  1902,  1905,  1906,  1910,  1914,  1918,  1919,
    1921,  1923,  1925,  1927,  1931,  1938,  1939,  1941,  1943,  1945,
    1947,  1949,  1951,  1953,  1955,  1957,  1959,  1961,  1963,  1965,
    1967,  1969,  1971,  1973,  1975,  1977,  1979,  1981,  1983,  1985,
    1987,  1989,  1991,  1993,  1995,  1997,  1999,  2001
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     260,     0,    -1,   261,    96,    -1,   261,   257,   262,    -1,
     262,    -1,   323,    -1,   316,    -1,   288,    -1,   285,    -1,
     284,    -1,   312,    -1,   362,    -1,   365,    -1,   426,    -1,
     429,    -1,   434,    -1,   439,    -1,   445,    -1,   437,    -1,
     372,    -1,   383,    -1,   385,    -1,   399,    -1,   400,    -1,
     404,    -1,   406,    -1,   409,    -1,   419,    -1,   424,    -1,
     413,    -1,   414,    -1,   415,    -1,   416,    -1,   304,    -1,
     309,    -1,   377,    -1,   379,    -1,   381,    -1,    -1,   315,
      -1,   269,    -1,   263,   258,   269,    -1,   265,    -1,   454,
      49,    34,    -1,   454,    49,   454,    49,    34,    -1,   453,
      -1,   454,    49,   453,    -1,   454,    49,   454,    49,   453,
      -1,     4,    -1,     6,    -1,     5,    -1,     9,    -1,     8,
      -1,    10,    -1,    12,    -1,    14,    -1,    13,    -1,   171,
      49,   453,    -1,   264,    -1,   266,    -1,    47,   269,    48,
      -1,    47,   263,   258,   269,    48,    -1,   271,    -1,   278,
      -1,   279,    -1,   324,    -1,    99,   324,    -1,   283,    -1,
     267,    -1,    32,   268,    -1,    33,   268,    -1,   268,    32,
     268,    -1,   268,    33,   268,    -1,   268,    34,   268,    -1,
     268,    35,   268,    -1,   268,    36,   268,    -1,   268,    38,
     268,    -1,   268,    37,   268,    -1,   267,    -1,    32,   269,
      -1,    33,   269,    -1,   269,    32,   269,    -1,   269,    32,
     269,   277,    -1,   269,    33,   269,    -1,   269,    33,   269,
     277,    -1,   269,    34,   269,    -1,   269,    35,   269,    -1,
     269,    36,   269,    -1,   269,    38,   269,    -1,   269,    37,
     269,    -1,   269,    26,   269,    -1,   269,    25,   269,    -1,
     269,    24,   269,    -1,   269,    22,   269,    -1,   269,    23,
     269,    -1,   269,    21,   269,    -1,   269,    27,   269,    -1,
     269,    20,    27,   269,    -1,   269,    19,   269,    -1,   269,
      18,   269,    -1,    20,   269,    -1,   269,    31,    10,    -1,
     269,    31,    20,    10,    -1,   269,    31,     8,    -1,   269,
      31,    20,     8,    -1,   269,    31,    11,    -1,   269,    31,
      20,    11,    -1,   269,    29,   268,    19,   268,    -1,   269,
      20,    29,   268,    19,   268,    -1,   269,    30,   270,    -1,
     269,    20,    30,   270,    -1,   269,    28,   269,    -1,    80,
      47,   269,   258,   269,    48,    -1,   324,    -1,    47,   263,
      48,    -1,    63,   272,   273,   275,    95,    -1,   269,    -1,
      -1,   274,    -1,   273,   274,    -1,   200,   269,   186,   269,
      -1,    94,   269,    -1,    -1,   252,    -1,   253,    -1,   254,
      -1,   255,    -1,    45,    -1,    44,    -1,    43,    -1,    42,
      -1,    41,    -1,    40,    -1,    39,    -1,   455,    47,    34,
      48,    -1,   455,    47,   282,   269,    48,    -1,   455,    47,
     263,    48,   219,   115,    47,   347,    48,    -1,   455,    47,
      48,   220,    47,   345,   346,    48,    -1,   455,    47,   263,
      48,    -1,    45,    47,   263,    48,    -1,   276,    47,   269,
     258,   256,   269,   277,    48,    -1,   455,    47,   269,    54,
     293,    48,    -1,    84,    47,   269,   258,     5,   258,     5,
      48,    -1,   455,    47,   343,    48,    -1,   455,    47,    48,
      -1,   213,    -1,   213,    47,    48,    -1,   224,   188,    -1,
     224,    81,    -1,   214,    -1,   214,    47,    48,    -1,   158,
      47,   263,    48,    -1,    81,    47,   269,    48,    -1,   187,
      47,   269,    48,    -1,   280,    47,   281,    48,    -1,   236,
      -1,   323,    -1,   316,    -1,   285,    -1,   284,    -1,    52,
      -1,    89,    -1,   142,   102,   389,    -1,   153,   102,   389,
      -1,    86,   332,   103,   358,   329,   317,    -1,   193,   332,
     357,   172,   286,   329,   317,    -1,   287,    -1,   286,   258,
     287,    -1,   265,    24,   269,    -1,    71,   184,   289,   358,
      47,   290,    48,   301,    -1,   121,    20,    99,    -1,    -1,
     291,    -1,   290,   258,   291,    -1,   292,    -1,   152,   128,
      47,   320,    48,    -1,   453,   293,   299,    -1,   189,    -1,
     174,    -1,   134,    -1,   123,    -1,    57,    -1,    84,   294,
      -1,   140,   294,    -1,    59,    -1,   101,   295,    -1,   156,
      -1,    90,   296,    -1,   188,   297,    -1,    82,    -1,    64,
     298,    -1,    58,   298,    -1,   197,   298,    -1,   198,   298,
      -1,    72,    -1,   136,    -1,    81,    -1,   187,   297,    -1,
      47,     5,   258,     5,    48,    -1,    47,     5,    48,    -1,
      -1,    47,     5,    48,    -1,    -1,   150,    -1,    -1,    47,
       5,    48,    -1,    -1,    47,     5,    48,    -1,    -1,   299,
     300,    -1,    -1,    20,    10,    -1,    10,    -1,    85,   266,
      -1,   226,    -1,   152,   128,    -1,   302,    -1,   301,   258,
     302,    -1,    -1,   232,   303,     4,    -1,   230,   303,     4,
      -1,   244,   303,     5,    -1,   243,   303,     5,    -1,   242,
     303,     5,    -1,   234,   303,     5,    -1,   228,   303,     4,
      -1,   247,   303,     8,    -1,   229,   303,   178,    -1,    66,
     303,     4,    -1,    24,    -1,    -1,    71,    76,   305,   144,
     358,   306,   307,    -1,     3,    -1,    47,   320,    48,    -1,
      77,   308,    -1,    -1,    47,   320,    48,    -1,    91,    76,
     310,   144,   358,    -1,   311,    -1,   310,   258,   305,    -1,
       3,    -1,    91,   184,   313,   314,    -1,    -1,   121,    99,
      -1,   356,    -1,   314,   258,   356,    -1,    93,   184,   313,
     314,   449,    -1,   318,   125,   358,   319,   196,   321,   317,
      -1,   318,   125,   358,   323,    -1,   318,   125,   358,    47,
     320,    48,   323,    -1,    -1,   200,   269,    -1,   158,    -1,
     124,    -1,    47,   320,    48,    -1,    -1,   453,    -1,   320,
     258,   453,    -1,    47,   322,    48,    -1,   321,   258,    47,
     322,    48,    -1,   269,    -1,   322,   258,   269,    -1,   325,
     317,    -1,   324,    -1,    47,   325,    48,    -1,    47,   324,
      48,    -1,   326,    -1,   328,   342,    -1,   327,   347,   342,
      -1,   327,   346,   331,   342,    -1,   169,   332,   352,   354,
     341,    -1,   169,   332,   352,   354,   103,    92,   329,   341,
      -1,   328,    -1,   324,    -1,   169,   332,   352,   354,   103,
     355,   329,   344,   351,   330,    -1,   327,    16,   352,   327,
      -1,   327,    17,   352,   327,    -1,   327,    15,   352,   327,
      -1,    -1,   199,   269,    -1,   199,     7,   269,    -1,    -1,
     251,    47,    47,   322,    48,   258,    47,   322,    48,    48,
      -1,   251,    47,   258,    47,   322,    48,    48,    -1,   251,
      47,    47,   322,    48,   258,    48,    -1,   251,    47,   258,
      48,    -1,   131,   340,   143,   340,    -1,   143,   340,   131,
     340,    -1,   131,   340,    -1,   143,   340,    -1,   131,   340,
     258,   340,    -1,   209,   210,   211,   212,    -1,   209,   210,
     340,   211,   212,    -1,    -1,   117,   333,   118,    -1,   334,
      -1,   333,   258,   334,    -1,    -1,   335,    -1,   334,   335,
      -1,   155,    -1,   207,    -1,   119,    -1,   165,    47,   339,
      48,    -1,    76,    47,   358,   358,    48,    -1,     3,    47,
       3,    48,    -1,    47,   338,    48,    -1,   127,    47,   336,
      48,    -1,   107,    -1,   108,    -1,   109,    -1,   110,    -1,
     111,    -1,   337,    -1,   336,   258,   337,    -1,   215,    -1,
     216,    -1,   217,    -1,   218,    -1,   338,   258,    -1,    -1,
     206,    -1,   180,    -1,   178,    -1,   105,    -1,     5,    -1,
      12,    -1,    -1,   331,    -1,    -1,   102,   193,    -1,   269,
     103,   269,    -1,    60,   269,   103,   269,    -1,   129,   269,
     103,   269,    -1,   190,   269,   103,   269,    -1,    60,   103,
     269,    -1,   129,   103,   269,    -1,   190,   103,   269,    -1,
      -1,   115,    61,   263,    -1,    -1,   208,    61,   263,    -1,
     347,    -1,    -1,   145,    61,   348,    -1,   349,    -1,   348,
     258,   349,    -1,   269,   350,    -1,    -1,    55,    -1,    87,
      -1,    -1,   116,   269,    -1,    -1,    52,    -1,    89,    -1,
     269,    -1,   269,   456,    -1,   269,    54,   456,    -1,    34,
      -1,   353,    -1,   354,   258,   353,    -1,   356,    -1,   355,
     258,   356,    -1,   357,    -1,   324,    54,   454,    -1,   324,
     454,    -1,   324,    -1,   359,    -1,    47,   359,    48,    54,
     454,    -1,   358,    -1,   358,    54,   454,    -1,   358,   454,
      -1,   454,    -1,   454,    49,   454,    -1,    47,   359,    48,
      -1,   356,   360,   127,   356,   144,   269,    -1,   356,   127,
     356,   144,   269,    -1,   104,   361,    -1,   130,   361,    -1,
     162,   361,    -1,   122,    -1,   221,    -1,   130,   221,    -1,
     162,   221,    -1,   147,    -1,    -1,   100,   364,   363,    -1,
     323,    -1,   284,    -1,   316,    -1,   285,    -1,   249,    -1,
      -1,   173,   185,   369,    -1,   173,    76,   144,   358,   369,
      -1,   173,    70,   103,   358,   369,    -1,   173,    70,    30,
     358,   369,    -1,   173,   184,   241,   369,    -1,   173,   237,
     241,   369,    -1,   173,   368,   248,   369,    -1,   173,   167,
      -1,   173,    71,   184,   358,    -1,    88,   358,   370,    -1,
      87,   358,   370,    -1,   173,   250,   366,    -1,   173,   278,
     250,    -1,   173,   231,   367,    -1,   173,   148,   369,    -1,
     173,   371,   203,    -1,   173,   223,    -1,   173,   224,   222,
      -1,   173,   179,   185,    -1,   131,     5,   258,     5,    -1,
     131,     5,    -1,    -1,   405,    -1,   102,    73,    -1,   102,
      73,    47,    48,    -1,   112,    -1,   170,    -1,    -1,    -1,
      27,     4,    -1,   199,   269,    -1,    -1,     4,    -1,    -1,
     104,    -1,    71,   194,   373,    -1,   374,    -1,   373,   258,
     374,    -1,   375,   120,    61,   376,    -1,     4,    -1,     4,
      -1,    71,   222,   378,    -1,   454,    -1,   195,   222,   380,
      -1,   454,    -1,    91,   222,   382,    -1,   454,    -1,    91,
     194,   384,    -1,   375,    -1,   384,   258,   375,    -1,    71,
     388,   183,   389,   386,    -1,    71,   388,   183,   389,    -1,
     386,   387,    -1,   387,    -1,   390,    -1,   391,    -1,   392,
      -1,   393,    -1,   394,    -1,   395,    -1,   396,    -1,   397,
      -1,   398,    -1,    18,   158,    -1,    -1,   454,    -1,    54,
     293,    -1,   177,   201,   269,    -1,   126,    61,   269,    -1,
     138,   269,    -1,   141,   138,    -1,   139,   269,    -1,   141,
     139,    -1,    78,    -1,   141,    78,    -1,    79,     5,    -1,
     141,    79,    -1,   145,    -1,   141,   145,    -1,   154,    -1,
     141,   154,    -1,    91,   183,   389,    -1,    53,   183,   389,
     401,    -1,   401,   402,    -1,   402,    -1,   403,    -1,   392,
      -1,   393,    -1,   394,    -1,   395,    -1,   396,    -1,   397,
      -1,   398,    -1,   166,    -1,   166,   201,   269,    -1,   172,
     149,   405,    24,   376,    -1,    53,   194,   375,   120,    61,
     376,    -1,   102,   375,    -1,    -1,   157,   194,   408,    -1,
     375,   192,   375,    -1,   407,    -1,   408,   258,   407,    -1,
      53,   194,   375,   410,    -1,   133,    -1,   245,    -1,   202,
      -1,    -1,   201,    68,   175,    -1,    -1,    56,   411,    -1,
     177,   191,   412,    -1,    67,   411,    -1,   163,   411,    -1,
     164,   417,   418,     5,    -1,    -1,   112,    -1,    -1,   204,
      -1,   205,    -1,   114,   420,   144,   423,   192,   384,    -1,
     421,    -1,   420,   258,   421,    -1,    52,   422,    -1,    53,
      -1,    71,    -1,    71,   194,    -1,    86,    -1,    91,    -1,
      93,    -1,   114,   146,    -1,   124,    -1,   193,    -1,   169,
      -1,   158,    -1,   160,    -1,    -1,    34,    -1,    34,    49,
      34,    -1,   454,    49,    34,    -1,   454,    49,   454,    -1,
     454,    -1,   161,   420,   425,   103,   384,    -1,   144,   423,
      -1,    -1,   151,   427,   103,   428,    -1,   456,    -1,   323,
      -1,   316,    -1,   285,    -1,   284,    -1,   172,   430,    -1,
     431,    -1,   430,   258,   431,    -1,    14,   432,   269,    -1,
     453,   432,   269,    -1,   112,   453,   432,   269,    -1,   170,
     453,   432,   269,    -1,   113,    49,   453,   432,   269,    -1,
     171,    49,   453,   432,   269,    -1,    13,   432,   269,    -1,
     192,    -1,    24,    -1,    14,    -1,    98,   427,   435,    -1,
     195,   436,    -1,    -1,   433,    -1,   436,   258,   433,    -1,
     438,   151,   427,    -1,    83,    -1,    91,    -1,    53,   184,
     358,   440,    -1,    53,   184,   358,   157,   192,   358,    -1,
     157,   184,   358,   192,   358,    -1,   441,    -1,   440,   258,
     441,    -1,    50,   442,   292,    -1,    91,   442,   453,   443,
      -1,    53,   442,   453,   444,    -1,   157,   442,   453,   192,
     456,    -1,    69,    -1,    -1,    62,    -1,   159,    -1,    -1,
     172,    20,    10,    -1,    91,    20,    10,    -1,   172,    85,
     266,    -1,    91,    85,    -1,    53,   179,   172,   447,    -1,
      53,   179,   446,    74,   137,    24,     4,    -1,    53,   179,
     446,    75,   137,    24,     4,    -1,    53,   179,   181,   182,
      24,     4,    -1,    -1,   106,    -1,   448,    -1,   447,   258,
     448,    -1,   453,    24,   266,   449,   450,   240,    24,   451,
     452,    -1,    66,     4,    -1,    -1,   168,    24,   135,    -1,
     168,    24,   176,    -1,   168,    24,    60,    -1,    -1,   235,
      -1,   246,    -1,   227,    -1,   233,    -1,    65,    24,     5,
      -1,   238,    24,     4,   239,    24,     5,    -1,    -1,     3,
      -1,   457,    -1,     3,    -1,   457,    -1,     3,    -1,     3,
      -1,   457,    -1,   226,    -1,   227,    -1,   228,    -1,   229,
      -1,   230,    -1,   231,    -1,   232,    -1,   233,    -1,   234,
      -1,   235,    -1,   236,    -1,   237,    -1,   238,    -1,   239,
      -1,   240,    -1,   241,    -1,   243,    -1,   242,    -1,   244,
      -1,   245,    -1,   246,    -1,   247,    -1,   248,    -1,   249,
      -1,   250,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   345,   345,   354,   361,   368,   369,   370,   371,   372,
     373,   374,   375,   376,   377,   378,   379,   380,   381,   382,
     383,   384,   385,   386,   387,   388,   389,   390,   391,   392,
     393,   394,   395,   397,   398,   400,   401,   402,   403,   405,
     416,   420,   476,   481,   492,   508,   515,   525,   538,   539,
     540,   541,   542,   543,   544,   545,   546,   547,   551,   553,
     555,   557,   563,   571,   575,   579,   583,   588,   597,   598,
     602,   606,   607,   608,   609,   610,   611,   612,   615,   616,
     620,   624,   628,   680,   681,   733,   734,   735,   736,   737,
     738,   739,   740,   741,   742,   743,   744,   745,   746,   750,
     754,   758,   762,   766,   770,   774,   778,   782,   786,   790,
     794,   798,   803,   811,   815,   820,   828,   829,   833,   835,
     840,   847,   848,   853,   854,   855,   856,   859,   860,   861,
     862,   863,   864,   865,   870,   884,   913,   929,   942,  1039,
    1051,  1197,  1222,  1239,  1243,  1273,  1282,  1293,  1302,  1311,
    1320,  1331,  1342,  1356,  1374,  1382,  1389,  1390,  1391,  1392,
    1396,  1400,  1408,  1413,  1435,  1452,  1463,  1467,  1476,  1491,
    1508,  1511,  1515,  1519,  1526,  1530,  1539,  1548,  1550,  1552,
    1554,  1556,  1561,  1570,  1579,  1581,  1583,  1585,  1590,  1597,
    1599,  1606,  1613,  1620,  1627,  1629,  1631,  1641,  1657,  1659,
    1662,  1666,  1667,  1671,  1672,  1676,  1677,  1681,  1682,  1686,
    1689,  1693,  1698,  1703,  1705,  1707,  1712,  1716,  1721,  1727,
    1732,  1737,  1742,  1747,  1752,  1757,  1762,  1767,  1773,  1781,
    1782,  1791,  1805,  1817,  1824,  1830,  1835,  1847,  1856,  1860,
    1867,  1886,  1896,  1897,  1902,  1906,  1920,  1936,  1950,  1961,
    1979,  1980,  1987,  1992,  2000,  2005,  2009,  2010,  2017,  2021,
    2027,  2028,  2042,  2052,  2057,  2058,  2062,  2066,  2071,  2081,
    2102,  2127,  2156,  2157,  2161,  2190,  2215,  2240,  2269,  2270,
    2274,  2282,  2283,  2290,  2295,  2300,  2307,  2315,  2323,  2327,
    2331,  2340,  2347,  2356,  2359,  2373,  2377,  2382,  2388,  2392,
    2399,  2403,  2407,  2411,  2416,  2421,  2428,  2433,  2439,  2445,
    2449,  2453,  2459,  2467,  2471,  2479,  2484,  2489,  2493,  2502,
    2507,  2513,  2517,  2521,  2525,  2531,  2533,  2539,  2540,  2546,
    2547,  2555,  2562,  2569,  2576,  2583,  2594,  2605,  2620,  2621,
    2630,  2631,  2639,  2640,  2644,  2651,  2653,  2658,  2666,  2667,
    2669,  2675,  2676,  2684,  2687,  2691,  2698,  2709,  2723,  2737,
    2747,  2751,  2758,  2760,  2779,  2783,  2787,  2792,  2802,  2806,
    2816,  2820,  2824,  2838,  2842,  2853,  2857,  2861,  2870,  2876,
    2882,  2888,  2893,  2897,  2901,  2908,  2909,  2919,  2927,  2928,
    2929,  2930,  2934,  2935,  2945,  2947,  2949,  2951,  2953,  2955,
    2957,  2962,  2964,  2966,  2968,  2970,  2974,  2987,  2991,  2995,
    3001,  3007,  3013,  3021,  3030,  3040,  3044,  3046,  3048,  3053,
    3054,  3055,  3060,  3061,  3063,  3069,  3070,  3075,  3076,  3085,
    3091,  3095,  3101,  3107,  3113,  3125,  3131,  3143,  3149,  3162,
    3168,  3179,  3185,  3189,  3203,  3212,  3222,  3227,  3234,  3238,
    3242,  3246,  3250,  3254,  3258,  3262,  3266,  3273,  3278,  3284,
    3289,  3296,  3303,  3310,  3314,  3321,  3325,  3332,  3337,  3345,
    3349,  3356,  3361,  3368,  3373,  3386,  3401,  3412,  3416,  3423,
    3427,  3431,  3435,  3439,  3443,  3447,  3451,  3458,  3462,  3475,
    3479,  3485,  3490,  3500,  3506,  3512,  3516,  3527,  3533,  3538,
    3551,  3556,  3560,  3565,  3569,  3575,  3586,  3598,  3610,  3617,
    3621,  3629,  3633,  3638,  3652,  3663,  3667,  3673,  3679,  3684,
    3689,  3694,  3699,  3705,  3710,  3715,  3720,  3725,  3730,  3737,
    3742,  3761,  3766,  3771,  3777,  3781,  3793,  3833,  3838,  3849,
    3856,  3861,  3863,  3865,  3867,  3878,  3886,  3890,  3897,  3903,
    3910,  3917,  3924,  3931,  3938,  3947,  3948,  3952,  3963,  3970,
    3975,  3980,  3984,  3997,  4005,  4007,  4018,  4024,  4032,  4042,
    4046,  4053,  4058,  4064,  4069,  4078,  4079,  4083,  4084,  4085,
    4089,  4094,  4099,  4103,  4117,  4123,  4130,  4146,  4156,  4159,
    4167,  4171,  4178,  4193,  4196,  4200,  4202,  4204,  4207,  4211,
    4216,  4221,  4226,  4234,  4238,  4243,  4254,  4256,  4273,  4275,
    4292,  4296,  4298,  4311,  4312,  4313,  4314,  4315,  4316,  4317,
    4318,  4319,  4320,  4321,  4322,  4323,  4324,  4325,  4326,  4327,
    4328,  4329,  4330,  4331,  4332,  4333,  4334,  4335
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || YYTOKEN_TABLE
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "NAME", "STRING", "INTNUM", "DATE_VALUE",
  "HINT_VALUE", "BOOL", "APPROXNUM", "NULLX", "UNKNOWN", "QUESTIONMARK",
  "SYSTEM_VARIABLE", "TEMP_VARIABLE", "EXCEPT", "UNION", "INTERSECT", "OR",
  "AND", "NOT", "COMP_NE", "COMP_GE", "COMP_GT", "COMP_EQ", "COMP_LT",
  "COMP_LE", "LIKE", "CNNOP", "BETWEEN", "IN", "IS", "'+'", "'-'", "'*'",
  "'/'", "'%'", "MOD", "'^'", "YEARS", "MICROSECONDS", "SECONDS", "MONTHS",
  "MINUTES", "HOURS", "DAYS", "UMINUS", "'('", "')'", "'.'", "ADD", "ANY",
  "ALL", "ALTER", "AS", "ASC", "BEGI", "BIGINT", "BINARY", "BOOLEAN",
  "BOTH", "BY", "CASCADE", "CASE", "CHARACTER", "CLUSTER", "COMMENT",
  "COMMIT", "CONSISTENT", "COLUMN", "COLUMNS", "CREATE", "CREATETIME",
  "CURRENT_USER", "CHANGE_OBI", "SWITCH_CLUSTER", "INDEX", "STORING",
  "CYCLE", "CACHE", "CONCAT", "DATE", "DATETIME", "DEALLOCATE", "DECIMAL",
  "DEFAULT", "DELETE", "DESC", "DESCRIBE", "DISTINCT", "DOUBLE", "DROP",
  "DUAL", "TRUNCATE", "ELSE", "END", "END_P", "ERROR", "EXECUTE", "EXISTS",
  "EXPLAIN", "FLOAT", "FOR", "FROM", "FULL", "FROZEN", "FORCE",
  "I_MULTI_BATCH", "UD_MULTI_BATCH", "UD_ALL_ROWKEY", "UD_NOT_PARALLAL",
  "CHANGE_VALUE_SIZE", "GLOBAL", "GLOBAL_ALIAS", "GRANT", "GROUP",
  "HAVING", "HINT_BEGIN", "HINT_END", "HOTSPOT", "IDENTIFIED", "IF",
  "INNER", "INTEGER", "INSERT", "INTO", "INCREMENT", "JOIN", "KEY",
  "LEADING", "LEFT", "LIMIT", "LOCAL", "LOCKED", "MEDIUMINT", "MEMORY",
  "MODIFYTIME", "MASTER", "MINVALUE", "MAXVALUE", "NUMERIC", "NO",
  "NEXTVAL", "OFFSET", "ON", "ORDER", "OPTION", "OUTER", "PARAMETERS",
  "PASSWORD", "PRECISION", "PREPARE", "PRIMARY", "PREVVAL", "QUICK",
  "READ_STATIC", "REAL", "RENAME", "REPLACE", "RESTRICT", "PRIVILEGES",
  "REVOKE", "RIGHT", "ROLLBACK", "KILL", "READ_CONSISTENCY", "RESTART",
  "SCHEMA", "SCOPE", "SELECT", "SESSION", "SESSION_ALIAS", "SET", "SHOW",
  "SMALLINT", "SNAPSHOT", "SPFILE", "START", "STATIC", "SYSTEM", "STRONG",
  "SET_SLAVE_CLUSTER", "SLAVE", "SEQUENCE", "TABLE", "TABLES", "THEN",
  "TIME", "TIMESTAMP", "TINYINT", "TRAILING", "TRANSACTION", "TO",
  "UPDATE", "USER", "USING", "VALUES", "VARCHAR", "VARBINARY", "WHERE",
  "WHEN", "WITH", "WORK", "PROCESSLIST", "QUERY", "CONNECTION", "WEAK",
  "NOT_USE_INDEX", "PARTITION", "FETCH", "FIRST", "ROWS", "ONLY",
  "CURRENT_TIMESTAMP", "CURRENT_DATE", "BLOOMFILTER_JOIN", "MERGE_JOIN",
  "SI", "SIB", "WITHIN", "OVER", "SEMI", "DATABASE", "DATABASES",
  "CURRENT", "USE", "AUTO_INCREMENT", "CHUNKSERVER", "COMPRESS_METHOD",
  "CONSISTENT_MODE", "EXPIRE_INFO", "GRANTS", "JOIN_INFO", "MERGESERVER",
  "REPLICA_NUM", "ROOTSERVER", "ROW_COUNT", "SERVER", "SERVER_IP",
  "SERVER_PORT", "SERVER_TYPE", "STATUS", "TABLE_ID", "TABLET_BLOCK_SIZE",
  "TABLET_MAX_SIZE", "UNLOCKED", "UPDATESERVER", "USE_BLOOM_FILTER",
  "VARIABLES", "VERBOSE", "WARNINGS", "RANGE", "DATE_ADD", "ADDDATE",
  "DATE_SUB", "SUBDATE", "INTERVAL", "';'", "','", "$accept", "sql_stmt",
  "stmt_list", "stmt", "expr_list", "column_ref", "base_column_ref",
  "expr_const", "simple_expr", "arith_expr", "expr", "in_expr",
  "case_expr", "case_arg", "when_clause_list", "when_clause",
  "case_default", "add_sub_date_name", "interval_time_stamp", "func_expr",
  "when_func", "when_func_name", "when_func_stmt", "distinct_or_all",
  "sequence_expr", "delete_stmt", "update_stmt", "update_asgn_list",
  "update_asgn_factor", "create_table_stmt", "opt_if_not_exists",
  "table_element_list", "table_element", "column_definition", "data_type",
  "opt_decimal", "opt_float", "opt_precision", "opt_time_precision",
  "opt_char_length", "opt_column_attribute_list", "column_attribute",
  "opt_table_option_list", "table_option", "opt_equal_mark",
  "create_index_stmt", "opt_index_name", "opt_index_columns",
  "opt_storing", "opt_storing_columns", "drop_index_stmt", "index_list",
  "drp_index_name", "drop_table_stmt", "opt_if_exists", "table_list",
  "truncate_table_stmt", "insert_stmt", "opt_when", "replace_or_insert",
  "opt_insert_columns", "column_list", "insert_vals_list", "insert_vals",
  "select_stmt", "select_with_parens", "select_no_parens",
  "no_table_select", "select_clause", "simple_select", "opt_where",
  "opt_range", "select_limit", "opt_hint", "opt_hint_list", "hint_options",
  "hint_option", "join_type_list", "joins_type", "opt_comma_list",
  "consistency_level", "limit_expr", "opt_select_limit", "opt_for_update",
  "parameterized_trim", "opt_groupby", "opt_partition_by", "opt_order_by",
  "order_by", "sort_list", "sort_key", "opt_asc_desc", "opt_having",
  "opt_distinct", "projection", "select_expr_list", "from_list",
  "table_factor", "base_table_factor", "relation_factor", "joined_table",
  "join_type", "join_outer", "explain_stmt", "explainable_stmt",
  "opt_verbose", "show_stmt", "opt_limit", "opt_for_grant_user",
  "opt_scope", "opt_show_condition", "opt_like_condition", "opt_full",
  "create_user_stmt", "user_specification_list", "user_specification",
  "user", "password", "create_database_stmt", "crt_database_specification",
  "use_database_stmt", "use_database_specification", "drop_database_stmt",
  "drop_database_specification", "drop_user_stmt", "user_list",
  "create_sequence_stmt", "sequence_label_list", "sequence_label",
  "opt_or_replace", "sequence_name", "opt_data_type", "opt_start_with",
  "opt_increment", "opt_minvalue", "opt_maxvalue", "opt_cycle",
  "opt_cache", "opt_order", "opt_use_quick_path", "drop_sequence_stmt",
  "alter_sequence_stmt", "sequence_label_list_alter",
  "sequence_label_alter", "opt_restart", "set_password_stmt",
  "opt_for_user", "rename_user_stmt", "rename_info", "rename_list",
  "lock_user_stmt", "lock_spec", "opt_work",
  "opt_with_consistent_snapshot", "begin_stmt", "commit_stmt",
  "rollback_stmt", "kill_stmt", "opt_is_global", "opt_flag", "grant_stmt",
  "priv_type_list", "priv_type", "opt_privilege", "priv_level",
  "revoke_stmt", "opt_on_priv_level", "prepare_stmt", "stmt_name",
  "preparable_stmt", "variable_set_stmt", "var_and_val_list",
  "var_and_val", "to_or_eq", "argument", "execute_stmt", "opt_using_args",
  "argument_list", "deallocate_prepare_stmt", "deallocate_or_drop",
  "alter_table_stmt", "alter_column_actions", "alter_column_action",
  "opt_column", "opt_drop_behavior", "alter_column_behavior",
  "alter_system_stmt", "opt_force", "alter_system_actions",
  "alter_system_action", "opt_comment", "opt_config_scope", "server_type",
  "opt_cluster_or_address", "column_name", "relation_name",
  "function_name", "column_label", "unreserved_keyword", 0
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[YYLEX-NUM] -- Internal token number corresponding to
   token YYLEX-NUM.  */
static const yytype_uint16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,    43,    45,    42,    47,    37,   287,    94,   288,
     289,   290,   291,   292,   293,   294,   295,    40,    41,    46,
     296,   297,   298,   299,   300,   301,   302,   303,   304,   305,
     306,   307,   308,   309,   310,   311,   312,   313,   314,   315,
     316,   317,   318,   319,   320,   321,   322,   323,   324,   325,
     326,   327,   328,   329,   330,   331,   332,   333,   334,   335,
     336,   337,   338,   339,   340,   341,   342,   343,   344,   345,
     346,   347,   348,   349,   350,   351,   352,   353,   354,   355,
     356,   357,   358,   359,   360,   361,   362,   363,   364,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,   421,   422,   423,   424,   425,
     426,   427,   428,   429,   430,   431,   432,   433,   434,   435,
     436,   437,   438,   439,   440,   441,   442,   443,   444,   445,
     446,   447,   448,   449,   450,   451,   452,   453,   454,   455,
     456,   457,   458,   459,   460,   461,   462,   463,   464,   465,
     466,   467,   468,   469,   470,   471,   472,   473,   474,   475,
     476,   477,   478,   479,   480,   481,   482,   483,   484,   485,
     486,   487,   488,   489,   490,   491,   492,   493,   494,   495,
     496,   497,   498,   499,   500,   501,   502,    59,    44
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   259,   260,   261,   261,   262,   262,   262,   262,   262,
     262,   262,   262,   262,   262,   262,   262,   262,   262,   262,
     262,   262,   262,   262,   262,   262,   262,   262,   262,   262,
     262,   262,   262,   262,   262,   262,   262,   262,   262,   262,
     263,   263,   264,   264,   264,   265,   265,   265,   266,   266,
     266,   266,   266,   266,   266,   266,   266,   266,   267,   267,
     267,   267,   267,   267,   267,   267,   267,   267,   268,   268,
     268,   268,   268,   268,   268,   268,   268,   268,   269,   269,
     269,   269,   269,   269,   269,   269,   269,   269,   269,   269,
     269,   269,   269,   269,   269,   269,   269,   269,   269,   269,
     269,   269,   269,   269,   269,   269,   269,   269,   269,   269,
     269,   269,   269,   270,   270,   271,   272,   272,   273,   273,
     274,   275,   275,   276,   276,   276,   276,   277,   277,   277,
     277,   277,   277,   277,   278,   278,   278,   278,   278,   278,
     278,   278,   278,   278,   278,   278,   278,   278,   278,   278,
     278,   278,   278,   278,   279,   280,   281,   281,   281,   281,
     282,   282,   283,   283,   284,   285,   286,   286,   287,   288,
     289,   289,   290,   290,   291,   291,   292,   293,   293,   293,
     293,   293,   293,   293,   293,   293,   293,   293,   293,   293,
     293,   293,   293,   293,   293,   293,   293,   293,   294,   294,
     294,   295,   295,   296,   296,   297,   297,   298,   298,   299,
     299,   300,   300,   300,   300,   300,   301,   301,   301,   302,
     302,   302,   302,   302,   302,   302,   302,   302,   302,   303,
     303,   304,   305,   306,   307,   307,   308,   309,   310,   310,
     311,   312,   313,   313,   314,   314,   315,   316,   316,   316,
     317,   317,   318,   318,   319,   319,   320,   320,   321,   321,
     322,   322,   323,   323,   324,   324,   325,   325,   325,   325,
     326,   326,   327,   327,   328,   328,   328,   328,   329,   329,
     329,   330,   330,   330,   330,   330,   331,   331,   331,   331,
     331,   331,   331,   332,   332,   333,   333,   333,   334,   334,
     335,   335,   335,   335,   335,   335,   335,   335,   335,   335,
     335,   335,   335,   336,   336,   337,   337,   337,   337,   338,
     338,   339,   339,   339,   339,   340,   340,   341,   341,   342,
     342,   343,   343,   343,   343,   343,   343,   343,   344,   344,
     345,   345,   346,   346,   347,   348,   348,   349,   350,   350,
     350,   351,   351,   352,   352,   352,   353,   353,   353,   353,
     354,   354,   355,   355,   356,   356,   356,   356,   356,   356,
     357,   357,   357,   358,   358,   359,   359,   359,   360,   360,
     360,   360,   360,   360,   360,   361,   361,   362,   363,   363,
     363,   363,   364,   364,   365,   365,   365,   365,   365,   365,
     365,   365,   365,   365,   365,   365,   365,   365,   365,   365,
     365,   365,   365,   366,   366,   366,   367,   367,   367,   368,
     368,   368,   369,   369,   369,   370,   370,   371,   371,   372,
     373,   373,   374,   375,   376,   377,   378,   379,   380,   381,
     382,   383,   384,   384,   385,   385,   386,   386,   387,   387,
     387,   387,   387,   387,   387,   387,   387,   388,   388,   389,
     390,   391,   392,   393,   393,   394,   394,   395,   395,   396,
     396,   397,   397,   398,   398,   399,   400,   401,   401,   402,
     402,   402,   402,   402,   402,   402,   402,   403,   403,   404,
     404,   405,   405,   406,   407,   408,   408,   409,   410,   410,
     411,   411,   412,   412,   413,   413,   414,   415,   416,   417,
     417,   418,   418,   418,   419,   420,   420,   421,   421,   421,
     421,   421,   421,   421,   421,   421,   421,   421,   421,   422,
     422,   423,   423,   423,   423,   423,   424,   425,   425,   426,
     427,   428,   428,   428,   428,   429,   430,   430,   431,   431,
     431,   431,   431,   431,   431,   432,   432,   433,   434,   435,
     435,   436,   436,   437,   438,   438,   439,   439,   439,   440,
     440,   441,   441,   441,   441,   442,   442,   443,   443,   443,
     444,   444,   444,   444,   445,   445,   445,   445,   446,   446,
     447,   447,   448,   449,   449,   450,   450,   450,   450,   451,
     451,   451,   451,   452,   452,   452,   453,   453,   454,   454,
     455,   456,   456,   457,   457,   457,   457,   457,   457,   457,
     457,   457,   457,   457,   457,   457,   457,   457,   457,   457,
     457,   457,   457,   457,   457,   457,   457,   457
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     2,     3,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     0,     1,
       1,     3,     1,     3,     5,     1,     3,     5,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     3,     1,     1,
       3,     5,     1,     1,     1,     1,     2,     1,     1,     2,
       2,     3,     3,     3,     3,     3,     3,     3,     1,     2,
       2,     3,     4,     3,     4,     3,     3,     3,     3,     3,
       3,     3,     3,     3,     3,     3,     3,     4,     3,     3,
       2,     3,     4,     3,     4,     3,     4,     5,     6,     3,
       4,     3,     6,     1,     3,     5,     1,     0,     1,     2,
       4,     2,     0,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     4,     5,     9,     8,     4,     4,
       8,     6,     8,     4,     3,     1,     3,     2,     2,     1,
       3,     4,     4,     4,     4,     1,     1,     1,     1,     1,
       1,     1,     3,     3,     6,     7,     1,     3,     3,     8,
       3,     0,     1,     3,     1,     5,     3,     1,     1,     1,
       1,     1,     2,     2,     1,     2,     1,     2,     2,     1,
       2,     2,     2,     2,     1,     1,     1,     2,     5,     3,
       0,     3,     0,     1,     0,     3,     0,     3,     0,     2,
       0,     2,     1,     2,     1,     2,     1,     3,     0,     3,
       3,     3,     3,     3,     3,     3,     3,     3,     3,     1,
       0,     7,     1,     3,     2,     0,     3,     5,     1,     3,
       1,     4,     0,     2,     1,     3,     5,     7,     4,     7,
       0,     2,     1,     1,     3,     0,     1,     3,     3,     5,
       1,     3,     2,     1,     3,     3,     1,     2,     3,     4,
       5,     8,     1,     1,    10,     4,     4,     4,     0,     2,
       3,     0,    10,     7,     7,     4,     4,     4,     2,     2,
       4,     4,     5,     0,     3,     1,     3,     0,     1,     2,
       1,     1,     1,     4,     5,     4,     3,     4,     1,     1,
       1,     1,     1,     1,     3,     1,     1,     1,     1,     2,
       0,     1,     1,     1,     1,     1,     1,     0,     1,     0,
       2,     3,     4,     4,     4,     3,     3,     3,     0,     3,
       0,     3,     1,     0,     3,     1,     3,     2,     0,     1,
       1,     0,     2,     0,     1,     1,     1,     2,     3,     1,
       1,     3,     1,     3,     1,     3,     2,     1,     1,     5,
       1,     3,     2,     1,     3,     3,     6,     5,     2,     2,
       2,     1,     1,     2,     2,     1,     0,     3,     1,     1,
       1,     1,     1,     0,     3,     5,     5,     5,     4,     4,
       4,     2,     4,     3,     3,     3,     3,     3,     3,     3,
       2,     3,     3,     4,     2,     0,     1,     2,     4,     1,
       1,     0,     0,     2,     2,     0,     1,     0,     1,     3,
       1,     3,     4,     1,     1,     3,     1,     3,     1,     3,
       1,     3,     1,     3,     5,     4,     2,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     2,     0,     1,
       2,     3,     3,     2,     2,     2,     2,     1,     2,     2,
       2,     1,     2,     1,     2,     3,     4,     2,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     3,     5,
       6,     2,     0,     3,     3,     1,     3,     4,     1,     1,
       1,     0,     3,     0,     2,     3,     2,     2,     4,     0,
       1,     0,     1,     1,     6,     1,     3,     2,     1,     1,
       2,     1,     1,     1,     2,     1,     1,     1,     1,     1,
       0,     1,     3,     3,     3,     1,     5,     2,     0,     4,
       1,     1,     1,     1,     1,     2,     1,     3,     3,     3,
       4,     4,     5,     5,     3,     1,     1,     1,     3,     2,
       0,     1,     3,     3,     1,     1,     4,     6,     5,     1,
       3,     3,     4,     4,     5,     1,     0,     1,     1,     0,
       3,     3,     3,     2,     4,     7,     7,     6,     0,     1,
       1,     3,     9,     2,     0,     3,     3,     3,     0,     1,
       1,     1,     1,     3,     6,     0,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
      38,     0,     0,   501,   501,   458,   564,   293,     0,     0,
     565,     0,     0,   393,     0,   253,     0,     0,   252,     0,
     501,   509,   293,     0,   421,     0,   293,     0,     0,     0,
       4,     9,     8,     7,    33,    34,    10,    39,     6,     0,
       5,   273,   250,   266,   343,   272,    11,    12,    19,    35,
      36,    37,    20,    21,    22,    23,    24,    25,    26,    29,
      30,    31,    32,    27,    28,    13,    14,    15,    18,     0,
      16,    17,   273,     0,   588,     0,     0,     0,   500,   504,
     506,     0,     0,   171,     0,     0,     0,   297,     0,   608,
     613,   614,   615,   616,   617,   618,   619,   620,   621,   622,
     623,   624,   625,   626,   627,   628,   630,   629,   631,   632,
     633,   634,   635,   636,   637,   425,   373,   609,   425,     0,
       0,   242,     0,     0,   242,   611,   560,   540,   612,   392,
       0,   530,   518,   519,   521,   522,   523,     0,   525,   528,
     527,   526,     0,   515,     0,     0,     0,   538,   507,   510,
     511,   353,   606,     0,     0,     0,     0,   492,     0,     0,
     545,   546,     0,   607,   610,     0,     0,     0,     0,     0,
       0,   428,   419,   422,     0,   401,   420,     0,     0,   422,
       0,   145,   149,   410,     0,   492,     0,   415,   123,   124,
     125,   126,     0,     0,     0,     0,     0,   503,     0,     0,
       1,     2,    38,     0,     0,   262,   353,   353,   353,     0,
       0,   329,     0,   267,     0,   265,   264,   589,     0,     0,
       0,     0,   459,     0,   433,     0,   457,   232,     0,     0,
       0,   429,   430,     0,   435,   436,     0,     0,   320,     0,
     308,   309,   310,   311,   312,   302,     0,   300,     0,   301,
       0,   295,   298,     0,   426,   404,     0,   403,   240,     0,
     238,   475,     0,     0,   442,   441,   439,   440,     0,     0,
     558,   389,   391,   390,   388,   387,   529,   517,   520,   524,
       0,     0,     0,     0,     0,   495,   493,     0,     0,   512,
     513,     0,   354,   355,     0,   556,   555,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   408,     0,   412,   422,
     394,     0,     0,     0,   148,   147,   411,     0,   407,   416,
     422,     0,   405,     0,   406,   422,   409,     0,     0,   505,
       0,   370,   437,   438,     3,   255,   606,    48,    50,    49,
      52,    51,    53,    54,    56,    55,     0,     0,     0,     0,
     117,     0,     0,     0,     0,     0,     0,   623,    58,    42,
      59,    78,   251,    62,    63,    64,     0,    67,    65,    45,
       0,   607,     0,     0,     0,     0,     0,     0,     0,   329,
     268,   330,   563,   584,   590,     0,     0,     0,     0,   467,
       0,     0,     0,     0,     0,   471,   473,   487,   480,   481,
     482,   483,   484,   485,   486,   476,   478,   479,   576,   576,
     576,   576,   566,   569,     0,   498,   499,   497,     0,     0,
       0,     0,     0,   445,     0,     0,     0,     0,     0,   294,
       0,   299,   278,   374,     0,     0,   243,     0,   241,   367,
     244,   364,   368,     0,   594,   557,   561,   559,   531,     0,
     535,   516,   544,   543,   542,   541,   539,     0,     0,     0,
     537,     0,   508,   359,   356,   360,   327,   554,   548,     0,
       0,   491,     0,     0,     0,   547,   549,     0,    40,   422,
     422,   402,   422,     0,     0,   423,   424,     0,   398,     0,
     146,   150,   417,   399,   414,     0,   400,     0,   144,   160,
       0,   161,     0,     0,     0,    40,     0,     0,     0,     0,
       0,   372,     0,     0,   248,   100,    79,    80,     0,    40,
      65,   116,     0,     0,    66,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   293,   273,   277,   272,   275,   276,   348,   344,   345,
     325,   326,   288,   289,     0,   269,     0,     0,     0,     0,
       0,   469,     0,   463,   465,   468,   470,   464,   466,   472,
     474,     0,   477,   575,     0,     0,     0,     0,     0,     0,
       0,     0,   170,     0,   431,     0,     0,     0,   444,   447,
     448,   449,   450,   451,   452,   453,   454,   455,   456,     0,
     306,   319,     0,   315,   316,   317,   318,     0,   313,   324,
     323,   322,   321,     0,   296,     0,   250,   237,   239,   273,
       0,   368,     0,     0,   366,   386,   381,     0,   386,   386,
     382,     0,   443,     0,   246,     0,     0,     0,     0,   568,
     494,   496,   536,     0,   357,     0,     0,   328,   270,   550,
       0,   434,   489,   551,     0,   139,     0,   397,   396,   395,
     152,     0,   151,   153,     0,     0,     0,   134,     0,     0,
       0,     0,     0,     0,     0,   138,     0,     0,     0,   143,
     502,   606,     0,   278,   166,     0,   371,     0,   256,     0,
       0,    60,     0,   122,   118,     0,   162,   163,    57,    99,
      98,     0,     0,     0,    95,    93,    94,    92,    91,    90,
      96,   111,     0,     0,    68,     0,     0,   109,   113,   103,
     101,   105,     0,    81,    83,    85,    86,    87,    89,    88,
       0,   159,   158,   157,   156,    43,    46,     0,   353,   349,
     350,   347,     0,     0,     0,     0,     0,     0,   591,   594,
     587,     0,     0,   462,   488,   571,     0,     0,   579,   567,
       0,   576,   570,   490,     0,   235,     0,     0,   172,   174,
     432,   181,   208,   184,   208,   194,   196,   189,   200,   204,
     202,   180,   179,   195,   200,   186,   178,   206,   206,   177,
     208,   208,   460,     0,   446,   305,     0,   307,     0,   303,
       0,   279,   164,   375,   245,   365,   385,   378,     0,   383,
     379,   384,   380,     0,   593,   562,   532,   514,   533,   534,
     358,   278,   278,   362,   361,   552,   553,    41,     0,   418,
     413,     0,   340,   335,     0,   336,     0,   337,     0,     0,
       0,   331,   135,     0,     0,   250,     0,   254,     0,     0,
     250,    41,     0,     0,   119,     0,     0,    97,     0,   110,
      69,    70,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   104,   102,   106,   133,   132,   131,   130,   129,   128,
     127,    82,    84,   154,     0,     0,   346,   286,   290,   287,
     291,     0,   598,   585,   586,   210,     0,     0,   573,   577,
     578,   572,     0,     0,     0,   231,     0,   218,     0,     0,
     191,   190,     0,   182,   203,   187,     0,   185,   183,     0,
     197,   188,   192,   193,   461,   304,   314,   280,     0,     0,
       0,   327,     0,   338,     0,     0,     0,   343,   332,   333,
     334,     0,   141,   168,   167,   165,     0,   249,   257,   260,
       0,     0,   247,    61,     0,   121,   115,     0,     0,   107,
      71,    72,    73,    74,    75,    77,    76,   114,    44,    47,
       0,   292,     0,     0,   176,     0,   583,     0,     0,   574,
     233,     0,   234,     0,   230,   230,   230,   230,   230,   230,
     230,   230,   230,   230,   169,   216,   173,     0,     0,     0,
       0,   369,   377,     0,   271,   363,     0,   351,     0,     0,
       0,     0,   342,     0,     0,   258,     0,     0,   120,   112,
     108,     0,     0,     0,   212,     0,     0,     0,   214,   209,
     581,   580,   582,     0,     0,   229,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   207,   199,     0,
     201,   205,   376,     0,     0,   281,   142,   140,   341,   137,
       0,   261,     0,   597,   595,   596,     0,   211,   213,   215,
     236,   175,   228,   225,   227,   220,   219,   224,   223,   222,
     221,   226,   217,     0,   339,   352,     0,   274,   136,   259,
     601,   602,   599,   600,   605,   198,     0,     0,     0,   592,
       0,     0,     0,     0,     0,     0,   285,   603,     0,     0,
       0,     0,     0,     0,     0,     0,   284,   283,   604,     0,
       0,   282
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    28,    29,    30,   487,   368,   369,   370,   371,   735,
     488,   737,   373,   532,   713,   714,   875,   192,   901,   374,
     375,   376,   750,   516,   377,    31,    32,   703,   704,    33,
     230,   787,   788,   789,   812,   933,   937,   935,   940,   930,
     994,  1049,  1014,  1015,  1056,    34,   228,   785,   925,  1002,
      35,   259,   260,    36,   263,   448,    37,    38,   205,    39,
     523,   707,   870,   970,    40,   378,    42,    43,    44,    45,
     636,  1107,   667,    88,   250,   251,   252,   627,   628,   435,
     633,   572,   668,   213,   517,  1027,   957,   210,   211,   568,
     569,   761,  1075,   294,   475,   476,   842,   450,   451,   341,
     452,   651,   827,    46,   275,   130,    47,   332,   328,   194,
     316,   255,   195,    48,   231,   232,   264,   672,    49,   234,
      50,   342,    51,   266,    52,   265,    53,   608,   609,    86,
     221,   610,   611,   408,   409,   410,   411,   412,   413,   414,
      54,    55,   415,   416,   417,    56,   302,    57,   285,   286,
      58,   427,    79,   339,    59,    60,    61,    62,   150,   291,
      63,   142,   143,   277,   459,    64,   288,    65,   126,   466,
      66,   160,   161,   297,   456,    67,   270,   457,    68,    69,
      70,   422,   423,   598,   921,   918,    71,   220,   393,   394,
     654,   993,  1114,  1119,   379,   380,   196,   127,   381
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -886
static const yytype_int16 yypact[] =
{
    3683,    40,   145,  -100,  -100,   129,  -886,     5,  3350,  3350,
     256,   -51,  3392,  -112,  1345,  -886,  3392,   -58,  -886,  1345,
    -100,    23,     5,   952,  3009,   -45,     5,   -72,   156,   -25,
    -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,   190,
    -886,     1,   130,  -886,    91,    38,  -886,  -886,  -886,  -886,
    -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,
    -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,   191,
    -886,  -886,   326,   338,   -18,  3350,  3350,   393,  -886,  -886,
    -886,   246,   414,   299,   393,  3350,   238,    34,   339,  -886,
    -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,
    -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,
    -886,  -886,  -886,  -886,  -886,   448,   380,  -886,   448,   451,
    3350,   334,   393,  3350,   334,  -886,   274,  -886,  -886,  -886,
     324,   311,  -886,   281,  -886,  -886,  -886,   333,  -886,  -886,
    -886,  -886,   -55,  -886,   378,  3350,   393,    11,  -886,  -886,
     107,    49,  -886,    42,    42,  3443,   441,   389,  3443,   450,
     249,  -886,    42,  -886,  -886,   468,    48,   341,   391,   486,
     489,  -886,  -886,    57,   502,  -886,  -886,   367,   312,    57,
     508,   509,   514,  -886,    88,   461,   321,   434,  -886,  -886,
    -886,  -886,   520,   319,   323,   369,   528,   383,  3350,  3350,
    -886,  -886,  3683,  3350,  2687,  -886,    49,    49,    49,   524,
     176,   271,   394,  -886,  3392,  -886,  -886,  -886,  3443,   412,
     263,   734,  -886,   417,  -886,   -38,  -886,  -886,   454,   575,
    3350,   343,  -886,   479,  -886,  -886,  3350,   555,  -886,   556,
    -886,  -886,  -886,  -886,  -886,  -886,   557,  -886,   558,  -886,
     -42,    34,  -886,  3350,  -886,  -886,  3350,  -886,  -886,    67,
    -886,  -886,   507,  3102,  -886,   357,  -886,  -886,  3102,   603,
    -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,
    3144,  1345,   324,   426,   427,  -886,   362,  3144,   518,  -886,
    -886,   617,  -886,  -886,  1578,  -886,  -886,  2687,  2687,    42,
    3443,   393,   600,    42,  3443,  2896,  2687,  2687,  3350,  3350,
    3350,  3350,  2687,  2687,   623,  2687,  -886,  2687,  -886,    57,
    -886,  2687,   580,   581,  -886,  -886,  -886,   100,  -886,  -886,
      57,   630,  -886,  2687,  -886,    57,  -886,  1323,   570,  -886,
     472,  1658,  -886,  -886,  -886,    46,    72,  -886,  -886,  -886,
    -886,  -886,  -886,  -886,  -886,  -886,  2687,  2687,  2687,  1696,
    2687,   593,   599,   546,   547,   602,   227,   607,  -886,  -886,
    -886,  -886,  4203,  -886,  -886,  -886,   609,  -886,  -886,  -886,
     611,   612,   111,   111,   111,  2687,   309,   309,   452,   561,
    -886,  -886,  -886,   400,  -886,   640,   645,   533,   534,  -886,
     668,   613,  2687,  2687,   469,  -886,  -886,   474,  -886,  -886,
    -886,  -886,  -886,  -886,  -886,   734,  -886,  -886,   619,   619,
     619,   -16,   418,  -886,   616,  -886,  -886,  -886,  3350,   587,
     642,   393,   629,   589,   710,   -13,  3350,   327,   360,  -886,
      34,  -886,   515,  -886,  3350,   414,  -886,   608,   458,  3170,
     379,  -886,  -886,   393,   -30,  -886,  -886,   462,   669,   527,
     672,  -886,  -886,  -886,  -886,  -886,  -886,  3350,   393,   393,
    -886,   393,  -886,  -886,   772,  -886,   244,  4203,  4203,  2687,
      42,  -886,   718,  2687,    42,  -886,  4203,    -9,  4203,    57,
      57,  -886,    57,  3989,  2235,  -886,  4203,    -5,  -886,  4020,
    -886,  -886,   677,  -886,   467,  2604,  -886,   678,   511,  -886,
    1949,  -886,  2065,  2318,     3,  3791,  2687,   687,   563,  3479,
    3350,  -886,  1074,   543,  -886,  2767,  -886,  -886,   482,  4051,
     301,  4203,   541,  2687,  -886,  3350,  3350,  3443,  2687,  2687,
     304,  2687,  2687,  2687,  2687,  2687,  2687,  2687,  2687,  2803,
     697,   368,  2687,  2687,  2687,  2687,  2687,  2687,  2687,   324,
    3195,     5,  -886,   728,  -886,   728,  -886,  3919,   491,  -886,
    -886,  -886,   -60,   620,    80,  -886,  3443,   173,   743,   726,
     729,  -886,  2687,  4203,  4203,  -886,  -886,  -886,  -886,  -886,
    -886,  2687,  -886,  -886,  3443,  3443,  3443,  3350,  3443,   436,
     718,   705,  -886,  3304,  -886,   718,  1926,   560,   589,  -886,
    -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,   708,
    -886,  -886,  3350,  -886,  -886,  -886,  -886,     6,  -886,  -886,
    -886,  -886,  -886,   709,    34,  2434,   130,  -886,  -886,   982,
     379,   711,  3102,  3350,  -886,   615,  -886,  3102,   -31,   159,
    -886,   631,  -886,   760,  -886,   603,   731,   393,  3231,  -886,
    -886,  -886,   357,  3392,  -886,  3056,  1578,  -886,  -886,  4203,
    2687,  -886,  -886,  4203,  2687,  -886,  2687,  -886,  -886,  -886,
    -886,   758,  -886,  -886,   763,   769,   559,  -886,   767,  2687,
    3628,  2687,  3877,  2687,  3898,   597,  1926,  2687,  4082,  -886,
    -886,   768,   794,   -99,  -886,   770,  -886,     7,  -886,   774,
    2687,  -886,  2687,    -4,  -886,  3537,  -886,  -886,  -886,  3962,
    2767,  2687,  2803,   697,  1062,  1062,  1062,  1062,  1062,  1062,
     751,  1135,  2803,  2803,  -886,   647,  1696,  -886,  -886,  -886,
    -886,  -886,   425,  1098,  1098,   784,   784,   784,   784,  -886,
     775,  -886,  -886,  -886,  -886,  -886,  -886,   776,    49,  -886,
    -886,  -886,  2687,   309,   309,   309,   618,   621,  -886,   762,
    -886,   820,   825,  4203,  4203,  -886,  1926,   184,    47,  -886,
     639,   619,  -886,  -886,  3443,   787,   733,     9,  -886,  -886,
    -886,  -886,   818,  -886,   818,  -886,  -886,  -886,   821,   721,
     827,  -886,  -886,  -886,   821,  -886,  -886,   829,   829,  -886,
     818,   818,  -886,  2687,  -886,  -886,   832,  -886,   327,  -886,
    2687,  4203,  -886,   823,   379,  -886,  -886,  -886,   424,  -886,
    -886,  -886,  -886,  3102,  -886,  -886,  -886,   357,  -886,  -886,
    -886,   515,   -94,   379,  -886,  4203,  4203,  4203,   624,  -886,
    -886,  2687,   675,  4203,  2687,  4203,  2687,  4203,  2687,   777,
     842,  4203,  -886,  2687,  3479,   130,  3479,    40,  3443,  2687,
      92,  4113,   673,  2687,  -886,   796,  2687,   751,   735,  -886,
    -886,  -886,  2803,  2803,  2803,  2803,  2803,  2803,  2803,  2803,
      12,  -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,
    -886,  -886,  -886,  -886,  3274,  1578,  -886,  -886,  -886,  -886,
    -886,   682,   727,  -886,  -886,  -886,    95,   103,  -886,  -886,
    -886,  -886,  3392,    13,   849,  -886,   850,   230,  3304,   894,
    -886,  -886,   896,  -886,  -886,  -886,   902,  -886,  -886,   903,
    -886,  -886,  -886,  -886,  4203,  -886,  -886,  4203,  3350,  2687,
     759,   176,  3102,   791,   905,  4175,   851,   766,  4203,  4203,
    4203,   866,  -886,  4203,  -886,  -886,   865,  -886,  -886,  4203,
      14,   868,  -886,  -886,  2687,  4203,  -886,  4144,  2803,   901,
     460,   460,   879,   879,   879,   879,  -886,  -886,  -886,  -886,
      21,  -886,   895,   680,    18,   908,  -886,   914,   173,  -886,
    -886,  3443,  -886,  3443,   917,   917,   917,   917,   917,   917,
     917,   917,   917,   917,   667,  -886,  -886,   878,    15,   897,
     899,  -886,  4203,  2687,  -886,   379,   882,   811,   900,   910,
    2687,   911,  -886,   766,  3443,  -886,  2687,  2687,  4203,  -886,
     901,  3102,   237,   920,  -886,   939,   173,   822,  -886,  -886,
    -886,  -886,  -886,    20,    25,  -886,   947,   949,   782,   957,
     958,   962,   963,   964,   965,   955,   230,  -886,  -886,   966,
    -886,  -886,  4203,  2687,  2687,   722,  -886,  -886,   714,  -886,
     928,  4203,    26,  -886,  -886,  -886,  -115,  -886,  -886,  -886,
    -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,  -886,
    -886,  -886,  -886,   929,   714,  4203,   931,  -886,  -886,  -886,
    -886,  -886,  -886,  -886,   -17,  -886,   -15,   959,   960,  -886,
    2687,   313,   974,   977,    27,  2687,  -886,  -886,   757,   724,
      29,   999,   364,   976,  1020,  2687,  -886,  -886,  -886,    32,
     978,  -886
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -886,  -886,  -886,   830,  -313,  -886,  -501,  -570,  -493,  -306,
     572,   306,  -886,  -886,  -886,   314,  -886,  -886,  -703,  1007,
    -886,  -886,  -886,  -886,  -886,  -122,  -114,  -886,   169,  -886,
    -886,  -886,   106,   446,  -602,   239,  -886,  -886,   236,  -683,
    -886,  -886,  -886,   -21,   -19,  -886,   601,  -886,  -886,  -886,
    -886,  -886,  -886,  -886,   923,   780,  -886,  -110,  -610,  -886,
    -886,  -755,  -886,  -669,  -125,     0,    10,  -886,   121,   137,
    -541,  -886,   839,    -1,  -886,   610,  -228,  -886,   234,  -886,
    -886,  -356,   102,  -166,  -886,  -886,  -886,    97,  -885,  -886,
     294,  -886,  -886,  -194,   392,   152,  -886,  -428,   861,    56,
     614,  -886,  -161,  -886,  -886,  -886,  -886,  -886,  -886,  -886,
    -135,   942,  -886,  -886,  -886,   632,   -50,  -278,  -886,  -886,
    -886,  -886,  -886,  -886,  -886,  -431,  -886,  -886,   459,  -886,
    -111,  -886,  -886,  -391,  -386,  -383,  -381,  -375,  -366,  -342,
    -886,  -886,  -886,   651,  -886,  -886,   877,  -886,   604,  -886,
    -886,  -886,   109,  -886,  -886,  -886,  -886,  -886,  -886,  -886,
    -886,  1049,   788,  -886,   785,  -886,  -886,  -886,    17,  -886,
    -886,  -886,   771,  -132,   416,  -886,  -886,  -886,  -886,  -886,
    -886,  -886,   475,    93,  -886,  -886,  -886,  -886,  -886,   499,
     310,  -886,  -886,  -886,    36,    90,  -886,  -459,    -6
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -611
static const yytype_int16 yytable[] =
{
      41,    72,   117,   117,   497,   274,   128,   769,   271,   261,
     128,    73,   382,   383,   384,   664,   272,   163,   702,   640,
     273,   151,   298,   441,   514,   198,   822,   225,  1044,   923,
     306,   573,  1120,   144,   233,   620,   653,   237,  1045,   675,
     662,   902,   612,   682,   320,   390,   528,   613,  1117,  -263,
     614,   695,   615,   593,   817,   867,   734,   927,   616,   162,
     987,  1000,  1035,  1068,   115,   118,   295,   617,  1090,   117,
     117,   201,  1032,  1091,  1109,  1129,   439,  1133,   308,   117,
    1140,   238,   424,   763,   314,   570,  -329,     1,   217,   280,
     873,   618,   571,   522,   860,   425,   284,  -263,   116,   116,
     635,   292,    78,  1046,   224,   635,   206,   207,   208,   919,
     239,   931,  1110,    80,   117,   995,   826,   117,  1111,  -610,
    1112,  -608,    87,   997,  1041,   433,   145,   942,   943,   148,
      41,  1113,   223,   124,  -329,   149,   146,   129,   293,   117,
     212,   240,   241,   242,   243,   244,   197,    81,  1080,   163,
     199,   309,   163,   245,   218,   287,   200,   465,     1,   864,
     462,   246,   865,   219,   952,   222,   116,   479,   463,   324,
    1047,   483,   464,   502,   915,   235,   597,   347,   348,   349,
     996,   350,   351,   352,   498,   353,   354,   355,   998,   247,
     829,   299,   117,   117,   303,   503,   712,   117,   764,   248,
     506,   283,    41,   281,   840,    82,   920,   426,   128,    22,
     222,   444,   163,   267,   824,    22,   440,   612,   767,   828,
     524,  1118,   613,   575,   117,   614,   837,   615,   642,   734,
     117,   392,   202,   616,   296,   116,   209,   843,  -329,   734,
     734,   249,   617,  1121,  1048,   621,  1053,   117,  1054,   676,
     117,   481,  1029,   676,   395,   965,   315,   117,  -263,   345,
     972,   676,   117,   449,   818,   868,   618,   928,   449,   281,
     676,   868,  1036,  1069,   117,   916,   325,   481,   868,   666,
     561,   117,    41,   868,  1036,  1036,   430,  1036,   116,   343,
    1036,   766,   204,   116,   163,  -329,  1004,  1083,   163,   163,
     951,   953,   117,   117,   117,   117,   826,   386,   324,   442,
     326,   289,   290,    83,   570,   203,  -273,  -273,  -273,   387,
     116,   571,   783,    84,    74,   445,   222,   790,    75,    76,
     204,   721,   119,   722,   723,   117,   480,   397,   398,    77,
     484,   162,   214,   116,   365,    41,   443,   665,   670,   215,
     971,    85,   674,   116,   677,   678,   917,   679,   116,   530,
    1125,  1126,   534,   702,   489,   490,   491,   492,  1082,    73,
     460,     1,  1084,   212,   215,   386,   739,   460,   740,   741,
     831,   233,   562,   562,   562,   388,   216,   387,   742,   734,
     734,   734,   734,   734,   734,   734,   734,   224,   116,   116,
     116,   116,  -342,   652,   226,   950,   441,   907,   908,   909,
       7,  1135,  1136,  1085,  -342,   325,   878,   227,   660,   284,
     229,   236,   117,   890,   716,   717,   880,   881,  1052,   256,
     117,   521,  -273,   891,   754,   892,   893,   751,   117,   120,
     121,   117,   253,   117,  -273,   752,  -273,   639,    15,   753,
     122,  1124,   254,   388,   258,   262,  1130,    73,  1005,  1006,
    1007,   117,  1008,   999,  1009,   629,  1139,   418,   128,   269,
     419,   276,  1010,  1011,  1012,   278,  1088,  1013,   123,   279,
    -342,   282,    18,   645,   601,   734,   418,   830,   832,   419,
     300,   301,   622,    22,   885,   886,   887,   888,   889,   304,
     637,   646,   666,   563,   565,   566,   647,   305,   420,   648,
    -273,   594,   595,   596,   117,   307,   163,    26,   116,   564,
     564,   564,    72,   659,  1025,   310,   116,   420,   645,   117,
     117,   163,    73,   312,   116,   311,   313,   116,   630,   644,
     631,   649,   623,   624,   625,   626,   646,   585,   586,   317,
     738,   647,   318,   319,   648,   321,   322,   116,   708,    41,
     758,   323,   330,   327,   905,   331,   632,   333,   949,   334,
     163,   335,   336,   718,   421,   337,   979,   980,   981,   982,
     983,   984,   985,   986,   338,   385,   649,   391,   163,   163,
     163,   117,   163,   781,   396,   429,   756,   163,   428,   432,
     650,   431,   434,   436,   437,   438,   446,   587,   588,   705,
     706,    89,   395,   843,   589,   453,   117,   455,   467,   468,
     469,   471,   472,   590,   482,   222,   222,   495,   500,   501,
     776,   777,   778,   117,   780,   504,   117,   117,   518,   776,
     533,   117,   449,   606,   519,   650,     1,   449,   535,   536,
     757,   537,   117,   779,  -155,   447,   559,   128,   576,   117,
     560,  -609,   574,   212,   577,   449,   882,   399,   400,   578,
     579,   580,  1040,   581,   582,   591,   599,   600,   816,   883,
     884,   885,   886,   887,   888,   889,   602,   116,   593,   603,
     605,   538,   539,   540,   541,   542,   543,   544,   545,   546,
     547,   548,   549,   550,   551,   552,   553,   554,   555,   556,
     557,   558,   116,   619,   635,   401,   642,  1078,   656,   657,
     655,   658,   671,   738,   684,   685,   687,   402,   403,   644,
     404,   688,   116,   825,   405,   699,   530,   116,   700,   709,
     710,   712,   967,   406,   736,   208,    73,   770,   839,   762,
     771,   765,   784,   772,   978,   116,   815,   819,   833,   823,
    1104,   813,   826,   848,   834,   836,   607,   883,   884,   885,
     886,   887,   888,   889,   850,   125,   372,    22,   163,   548,
     549,   550,   551,   552,   553,   554,   555,   556,   557,   558,
     538,   539,   540,   541,   542,   543,   544,   545,   546,   547,
     548,   549,   550,   551,   552,   553,   554,   555,   556,   557,
     558,   849,   399,   400,   852,   851,   859,  -608,   863,   866,
     708,   869,   558,   903,   913,   904,   663,   117,   653,   914,
     910,   922,   911,   449,    90,    91,    92,    93,    94,    95,
      96,    97,    98,    99,   100,   101,   102,   103,   104,   105,
     106,   107,   108,   109,   110,   111,   112,   113,   114,   974,
     401,   926,   163,   645,   924,   929,   474,    41,   932,   477,
     478,   934,   402,   403,   936,   404,   939,   948,   486,   405,
     945,   646,   954,   956,   493,   494,   647,   496,   406,   648,
     962,   976,   961,   499,   991,   992,  1001,  1003,   163,  1017,
     407,  1018,   756,  1023,   968,   505,  1026,  1019,  1020,   515,
    1028,   209,  1030,  1033,  1034,  1037,   128,   889,  1050,  1042,
    1043,   649,   163,   116,  1051,  1066,  1067,  1074,   525,   526,
     527,   529,   531,   883,   884,   885,   886,   887,   888,   889,
     989,  1055,   117,  1073,  1086,  1070,   117,  1071,  1076,  1087,
    1089,  1092,   449,  1093,   705,   152,   966,   567,  1077,  1079,
    1094,  1095,  1096,  1101,   776,   153,   154,  1097,  1098,  1099,
    1100,  1103,   676,  1106,   583,   584,  1108,  1115,  1116,  1127,
     650,  1128,  1132,  1122,  1123,    89,  1057,  1058,  1059,  1060,
    1061,  1062,  1063,  1064,  1065,   163,  1131,   163,    90,    91,
      92,    93,    94,    95,    96,    97,    98,    99,   100,   101,
     102,   103,   104,   105,   106,   107,   108,   109,   110,   111,
     112,   113,   114,  1134,  1137,  1138,  1141,   874,   163,   879,
     215,   193,   344,   964,  1016,   117,   643,   708,  1021,   708,
     775,   449,   116,   938,   941,  1102,   638,   268,   454,   389,
     634,   669,   946,  1024,  1031,   673,   906,   990,   844,   340,
     257,   641,   329,   604,   155,   156,   592,   814,   147,   461,
     989,   835,   470,   661,   782,   768,   485,   152,     0,   912,
       0,     0,   690,     0,   692,   694,  -367,     0,   698,   547,
     548,   549,   550,   551,   552,   553,   554,   555,   556,   557,
     558,   157,     0,     0,  -367,   715,     0,     0,     0,  -367,
     719,   720,  -367,   724,   725,   726,   727,   728,   729,   730,
     731,     1,   158,   159,   743,   744,   745,   746,   747,   748,
     749,   116,   554,   555,   556,   557,   558,   894,   895,   896,
     897,   898,   899,   900,  -367,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   773,     0,     0,     0,     0,     0,
       0,     0,     0,   774,   549,   550,   551,   552,   553,   554,
     555,   556,   557,   558,     0,     0,     0,     0,    90,    91,
      92,    93,    94,    95,    96,    97,    98,    99,   100,   101,
     102,   103,   104,   105,   106,   107,   108,   109,   110,   111,
     112,   113,   114,  -367,     0,     0,     0,   821,    90,    91,
      92,    93,    94,    95,    96,    97,    98,    99,   100,   101,
     102,   103,   104,   105,   106,   107,   108,   109,   110,   111,
     112,   113,   114,     0,     0,     0,     0,     0,   474,     0,
       0,     0,   845,    22,     0,     0,   846,     0,   847,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   853,     0,   855,     0,   857,     0,     0,     0,   861,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   871,     0,   872,     0,     0,     0,     0,     0,
       0,     0,     0,   877,     0,     0,     0,     0,     0,     0,
      90,    91,    92,    93,    94,    95,    96,    97,    98,    99,
     100,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   112,   113,   114,     0,   346,   347,   348,   349,
       0,   350,   351,   352,   567,   353,   354,   355,     0,     0,
       0,     0,     0,   356,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   357,   358,   507,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   165,     0,
     359,   508,     0,     0,     0,   509,     0,     0,     0,     0,
       0,     0,     0,   510,     0,   944,   360,     0,     0,     0,
       0,     0,   947,     0,     0,     0,     0,   131,   132,     0,
       0,     0,     0,   361,   169,     0,     0,   170,     0,     0,
       0,     0,   511,     0,     0,     0,   133,     0,     0,     0,
       0,     0,   362,   955,     0,     0,   958,     0,   959,     0,
     960,   134,     0,     0,     0,   963,   135,     0,   136,     0,
       0,   969,     0,     0,     0,   975,     0,     0,   977,     0,
       0,     0,   512,     0,     0,     0,     0,     0,     0,   137,
       0,     0,     0,     0,     0,   363,     0,     0,     0,   138,
       0,     0,     0,     0,     0,     0,   364,   474,     0,     0,
       0,   174,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   365,     0,     0,     0,     0,     0,
       0,     0,     0,   139,     0,     0,     0,     0,     0,     0,
     180,     0,     0,   513,   140,     0,     0,     0,     0,     0,
       0,  1022,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   181,   182,   141,     0,
       0,     0,     0,     0,     0,     0,  1038,   366,     0,    90,
      91,    92,    93,    94,    95,    96,    97,    98,    99,   367,
     101,   102,   103,   104,   105,   106,   107,   108,   109,   110,
     111,   112,   113,   114,     0,   188,   189,   190,   191,     0,
       0,   346,   347,   348,   349,     0,   350,   351,   352,     0,
     353,   354,   355,     0,     0,  1072,     0,     0,   356,     0,
       0,     0,     0,     0,     0,     0,     0,     0,  1081,   969,
     357,   358,   473,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   165,     0,   359,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   360,     0,     0,     0,     0,  1105,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   361,   169,
       0,    89,   170,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   362,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   969,     0,     0,     0,     0,   969,     0,   346,
     347,   348,   349,     0,   350,   351,   352,   969,   353,   354,
     355,     0,   520,     0,     0,     0,   356,     0,     0,     0,
     363,     0,     0,     0,     0,     0,     0,     0,   357,   358,
       0,   364,     0,     0,     0,     0,   174,     0,     0,     0,
       0,   165,     0,   359,     0,     0,     0,     0,     0,   365,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   360,
       0,     0,     0,     0,     0,   180,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   361,   169,     0,     0,
     170,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   181,   182,     0,     0,   362,     0,     0,     0,     0,
       0,     0,   366,     0,    90,    91,    92,    93,    94,    95,
      96,    97,    98,    99,   367,   101,   102,   103,   104,   105,
     106,   107,   108,   109,   110,   111,   112,   113,   114,     0,
     188,   189,   190,   191,     0,     0,     0,     0,   363,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   364,
       0,     0,     0,     0,   174,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    22,     0,   365,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   180,    90,    91,    92,    93,    94,    95,
      96,    97,    98,    99,   100,   101,   102,   103,   104,   105,
     106,   107,   108,   109,   110,   111,   112,   113,   114,   181,
     182,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     366,     0,    90,    91,    92,    93,    94,    95,    96,    97,
      98,    99,   367,   101,   102,   103,   104,   105,   106,   107,
     108,   109,   110,   111,   112,   113,   114,     0,   188,   189,
     190,   191,   346,   347,   348,   349,     0,   350,   351,   352,
       0,   353,   354,   355,     0,     0,     0,     0,     0,   356,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   357,   358,   791,   792,   793,     0,     0,     0,     0,
     794,     0,     0,     0,   165,     0,   359,     0,   795,     0,
       0,     0,     0,     0,     0,     0,     0,   796,   797,     0,
     798,     0,   360,     0,     0,     0,   799,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   800,     0,   361,
     169,     0,     0,   170,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   362,   801,
       0,     0,   689,     0,     0,     0,     0,     0,     0,     0,
     802,     0,   803,     0,     0,     0,   804,     0,   346,   347,
     348,   349,     0,   350,   351,   352,     0,   353,   354,   355,
       0,     0,   805,     0,     0,   356,     0,     0,     0,     0,
       0,   363,     0,     0,     0,     0,     0,   357,   358,     0,
     806,     0,   364,     0,     0,     0,     0,   174,     0,     0,
     165,     0,   359,   807,   808,   809,     0,     0,     0,     0,
     365,     0,     0,   810,   811,     0,     0,     0,   360,     0,
       0,     0,     0,     0,     0,     0,   180,     0,     0,     0,
       0,     0,     0,     0,     0,   361,   169,     0,     0,   170,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   181,   182,   362,     0,     0,     0,   691,     0,
       0,     0,     0,   366,     0,    90,    91,    92,    93,    94,
      95,    96,    97,    98,    99,   367,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   112,   113,   114,
       0,   188,   189,   190,   191,     0,     0,   363,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   364,     0,
       0,     0,     0,   174,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   365,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   180,   538,   539,   540,   541,   542,   543,   544,
     545,   546,   547,   548,   549,   550,   551,   552,   553,   554,
     555,   556,   557,   558,     0,     0,     0,     0,   181,   182,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   366,
       0,    90,    91,    92,    93,    94,    95,    96,    97,    98,
      99,   367,   101,   102,   103,   104,   105,   106,   107,   108,
     109,   110,   111,   112,   113,   114,     0,   188,   189,   190,
     191,   346,   347,   348,   349,     0,   350,   351,   352,     0,
     353,   354,   355,     0,     0,     0,     0,     0,   356,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     357,   358,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   165,     0,   359,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   360,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   361,   169,
       0,     0,   170,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   362,     0,     0,
       0,   693,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   346,   347,   348,
     349,   820,   350,   351,   352,     0,   353,   354,   355,     0,
       0,     0,     0,     0,   356,     0,     0,     0,     0,     0,
     363,     0,     0,     0,     0,     0,   357,   358,     0,     0,
       0,   364,     0,     0,     0,     0,   174,     0,     0,   165,
       0,   359,     0,     0,     0,     0,     0,     0,     0,   365,
       0,     0,     0,   681,     0,     0,     0,   360,     0,     0,
       0,     0,     0,     0,     0,   180,     0,     0,     0,     0,
       0,     0,     0,     0,   361,   169,     0,     0,   170,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   181,   182,   362,     0,     0,     0,     0,     0,     0,
       0,     0,   366,     0,    90,    91,    92,    93,    94,    95,
      96,    97,    98,    99,   367,   101,   102,   103,   104,   105,
     106,   107,   108,   109,   110,   111,   112,   113,   114,     0,
     188,   189,   190,   191,     0,     0,   363,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   364,     0,     0,
       0,     0,   174,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   365,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   180,   538,   539,   540,   541,   542,   543,   544,   545,
     546,   547,   548,   549,   550,   551,   552,   553,   554,   555,
     556,   557,   558,     0,     0,     0,     0,   181,   182,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   366,     0,
      90,    91,    92,    93,    94,    95,    96,    97,    98,    99,
     367,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   112,   113,   114,     0,   188,   189,   190,   191,
     346,   347,   348,   349,     0,   350,   351,   352,     0,   353,
     354,   355,     0,     0,     0,     0,     0,   356,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   357,
     358,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   165,     0,   359,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     360,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   361,   169,     0,
       0,   170,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   362,   540,   541,   542,
     543,   544,   545,   546,   547,   548,   549,   550,   551,   552,
     553,   554,   555,   556,   557,   558,   346,   347,   348,   349,
       0,   350,   351,   352,     0,   353,   354,   355,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   363,
       0,     0,     0,     0,     0,   732,   733,     0,     0,     0,
     364,     0,     0,     0,     0,   174,     0,     0,   165,     0,
     359,     0,     0,     0,     0,     0,     0,     0,   365,     0,
       0,     0,   686,     0,     0,     0,   360,     0,     0,     0,
       0,     0,     0,     0,   180,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   169,     0,     0,   170,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   152,
     181,   182,   362,     0,     0,     0,     0,     0,     0,   153,
     154,   366,     0,    90,    91,    92,    93,    94,    95,    96,
      97,    98,    99,   367,   101,   102,   103,   104,   105,   106,
     107,   108,   109,   110,   111,   112,   113,   114,     0,   188,
     189,   190,   191,     0,     0,   363,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   364,     0,     0,     0,
       0,   174,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   365,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     180,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   155,   156,
       0,     0,   164,     0,     0,     0,   181,   182,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   366,     0,    90,
      91,    92,    93,    94,    95,    96,    97,    98,    99,   367,
     101,   102,   103,   104,   105,   106,   107,   108,   109,   110,
     111,   112,   113,   114,   165,   188,   189,   190,   191,    89,
       0,     0,     0,     0,     0,     0,   158,   159,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   166,
     167,     0,     0,     0,     0,   168,     0,     0,     0,     0,
     169,     0,     0,   170,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   447,     0,    89,     0,     0,     0,     0,
       0,     0,     0,   171,     0,     0,     0,     0,     0,     0,
       0,   172,    90,    91,    92,    93,    94,    95,    96,    97,
      98,    99,   100,   101,   102,   103,   104,   105,   106,   107,
     108,   109,   110,   111,   112,   113,   114,    89,   841,   447,
       0,     0,     0,     0,     0,     0,     0,   173,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   174,     0,     0,
       0,     0,     0,    89,     0,     0,   175,     0,   458,   176,
       0,     0,     0,     0,     0,     0,     0,     0,   177,     0,
       0,     0,     0,   178,   179,     0,   180,     0,   701,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,  -427,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   181,   182,   643,     0,     0,     0,     0,   755,
       0,     0,   183,   184,    89,     0,     0,     0,     0,     0,
     185,     0,     0,     0,     0,     0,   186,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   187,
       0,   188,   189,   190,   191,   838,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   152,     0,     0,
       0,     0,    90,    91,    92,    93,    94,    95,    96,    97,
      98,    99,   100,   101,   102,   103,   104,   105,   106,   107,
     108,   109,   110,   111,   112,   113,   114,   152,   988,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    90,    91,
      92,    93,    94,    95,    96,    97,    98,    99,   100,   101,
     102,   103,   104,   105,   106,   107,   108,   109,   110,   111,
     112,   113,   114,    89,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      90,    91,    92,    93,    94,    95,    96,    97,    98,    99,
     100,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   112,   113,   114,   125,    90,    91,    92,    93,
      94,    95,    96,    97,    98,    99,   100,   101,   102,   103,
     104,   105,   106,   107,   108,   109,   110,   111,   112,   113,
     114,    90,    91,    92,    93,    94,    95,    96,    97,    98,
      99,   100,   101,   102,   103,   104,   105,   106,   107,   108,
     109,   110,   111,   112,   113,   114,   152,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   786,    90,    91,    92,
      93,    94,    95,    96,    97,    98,    99,   100,   101,   102,
     103,   104,   105,   106,   107,   108,   109,   110,   111,   112,
     113,   114,   701,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      90,    91,    92,    93,    94,    95,    96,    97,    98,    99,
     100,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   112,   113,   114,     0,     0,     0,     0,     0,
      90,    91,    92,    93,    94,    95,    96,    97,    98,    99,
     100,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   112,   113,   114,   538,   539,   540,   541,   542,
     543,   544,   545,   546,   547,   548,   549,   550,   551,   552,
     553,   554,   555,   556,   557,   558,    90,    91,    92,    93,
      94,    95,    96,    97,    98,    99,   100,   101,   102,   103,
     104,   105,   106,   107,   108,   109,   110,   111,   112,   113,
     114,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    90,    91,
      92,    93,    94,    95,    96,    97,    98,    99,   100,   101,
     102,   103,   104,   105,   106,   107,   108,   109,   110,   111,
     112,   113,   114,     0,     0,     0,   538,   539,   540,   541,
     542,   543,   544,   545,   546,   547,   548,   549,   550,   551,
     552,   553,   554,   555,   556,   557,   558,     0,     0,    90,
      91,    92,    93,    94,    95,    96,    97,    98,    99,   100,
     101,   102,   103,   104,   105,   106,   107,   108,   109,   110,
     111,   112,   113,   114,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    90,    91,    92,    93,    94,
      95,    96,    97,    98,    99,   100,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   112,   113,   114,
       1,   854,     0,     0,     0,     0,     2,     0,     0,     3,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       4,     0,     0,     0,     5,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     6,     0,     0,     7,
       8,     9,     0,     0,    10,     0,    11,     0,     0,     0,
       0,    12,     0,    13,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   876,     0,    14,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    15,     0,   538,
     539,   540,   541,   542,   543,   544,   545,   546,   547,   548,
     549,   550,   551,   552,   553,   554,   555,   556,   557,   558,
       0,     0,     0,     0,    16,     0,     0,     0,     0,     0,
      17,    18,     0,     0,    19,   696,    20,    21,     0,     0,
       0,     0,    22,     0,     0,    23,    24,     0,     0,     0,
      25,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    26,     0,    27,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   697,   538,   539,   540,   541,   542,
     543,   544,   545,   546,   547,   548,   549,   550,   551,   552,
     553,   554,   555,   556,   557,   558,   538,   539,   540,   541,
     542,   543,   544,   545,   546,   547,   548,   549,   550,   551,
     552,   553,   554,   555,   556,   557,   558,   538,   539,   540,
     541,   542,   543,   544,   545,   546,   547,   548,   549,   550,
     551,   552,   553,   554,   555,   556,   557,   558,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   759,     0,     0,     0,     0,     0,
     856,   539,   540,   541,   542,   543,   544,   545,   546,   547,
     548,   549,   550,   551,   552,   553,   554,   555,   556,   557,
     558,   858,     0,     0,     0,     0,   760,   538,   539,   540,
     541,   542,   543,   544,   545,   546,   547,   548,   549,   550,
     551,   552,   553,   554,   555,   556,   557,   558,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   680,   538,   539,
     540,   541,   542,   543,   544,   545,   546,   547,   548,   549,
     550,   551,   552,   553,   554,   555,   556,   557,   558,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   683,   538,
     539,   540,   541,   542,   543,   544,   545,   546,   547,   548,
     549,   550,   551,   552,   553,   554,   555,   556,   557,   558,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   711,
     538,   539,   540,   541,   542,   543,   544,   545,   546,   547,
     548,   549,   550,   551,   552,   553,   554,   555,   556,   557,
     558,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     862,   538,   539,   540,   541,   542,   543,   544,   545,   546,
     547,   548,   549,   550,   551,   552,   553,   554,   555,   556,
     557,   558,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   973,   538,   539,   540,   541,   542,   543,   544,   545,
     546,   547,   548,   549,   550,   551,   552,   553,   554,   555,
     556,   557,   558,     0,     0,     0,     0,     0,     0,     0,
       0,     0,  1039,   538,   539,   540,   541,   542,   543,   544,
     545,   546,   547,   548,   549,   550,   551,   552,   553,   554,
     555,   556,   557,   558,   894,   895,   896,   897,   898,   899,
     900,   538,   539,   540,   541,   542,   543,   544,   545,   546,
     547,   548,   549,   550,   551,   552,   553,   554,   555,   556,
     557,   558
};

static const yytype_int16 yycheck[] =
{
       0,     1,     8,     9,   317,   130,    12,   577,   130,   120,
      16,     1,   206,   207,   208,   474,   130,    23,   519,   447,
     130,    22,   154,   251,   337,    26,   636,    77,    10,   784,
     162,   387,    47,    16,    84,    48,    66,     3,    20,    48,
     471,   744,   433,    48,   179,   211,   359,   433,    65,    48,
     433,    48,   433,    69,    48,    48,   549,    48,   433,    23,
      48,    48,    48,    48,     8,     9,    24,   433,    48,    75,
      76,    96,   957,    48,    48,    48,   118,    48,    30,    85,
      48,    47,   120,   143,    27,     5,    48,    47,   106,   144,
      94,   433,    12,    47,   696,   133,   146,    96,     8,     9,
     199,    52,   202,    85,     4,   199,    15,    16,    17,    62,
      76,   794,   227,     4,   120,    20,   147,   123,   233,    47,
     235,    49,   117,    20,   103,   236,   184,   810,   811,    20,
     130,   246,    76,   184,    96,   112,   194,   249,    89,   145,
     102,   107,   108,   109,   110,   111,   191,    18,  1033,   155,
     222,   103,   158,   119,   172,   144,     0,   282,    47,   258,
     282,   127,   703,   181,   258,    75,    76,   299,   282,    81,
     152,   303,   282,    73,   776,    85,   192,     4,     5,     6,
      85,     8,     9,    10,   319,    12,    13,    14,    85,   155,
     221,   155,   198,   199,   158,   330,   200,   203,   258,   165,
     335,   145,   202,   258,   663,    76,   159,   245,   214,   169,
     120,   144,   218,   123,   642,   169,   258,   608,   574,   647,
     345,   238,   608,   389,   230,   608,   657,   608,   258,   722,
     236,   214,   257,   608,   192,   145,   145,   665,   200,   732,
     733,   207,   608,   258,   226,   258,  1001,   253,  1003,   258,
     256,   301,   955,   258,   218,   865,   199,   263,   257,   203,
     870,   258,   268,   263,   258,   258,   608,   258,   268,   258,
     258,   258,   258,   258,   280,    91,   188,   327,   258,   258,
     169,   287,   282,   258,   258,   258,   230,   258,   198,   199,
     258,   211,   200,   203,   300,   257,    66,    60,   304,   305,
     841,   842,   308,   309,   310,   311,   147,   131,    81,   253,
     222,   204,   205,   184,     5,   125,    15,    16,    17,   143,
     230,    12,   600,   194,   179,   258,   236,   605,   183,   184,
     200,    27,    76,    29,    30,   341,   300,    74,    75,   194,
     304,   305,   151,   253,   171,   345,   256,   103,   480,    48,
     258,   222,   484,   263,   489,   490,   172,   492,   268,   359,
      47,    48,   362,   864,   308,   309,   310,   311,  1037,   359,
     280,    47,   135,   102,    48,   131,     8,   287,    10,    11,
     221,   431,   382,   383,   384,   209,    48,   143,    20,   882,
     883,   884,   885,   886,   887,   888,   889,     4,   308,   309,
     310,   311,   131,   453,   158,   833,   634,   763,   764,   765,
      86,    47,    48,   176,   143,   188,   722,     3,   468,   469,
     121,   183,   428,   736,   535,   536,   732,   733,   998,    49,
     436,   341,   131,     8,   559,    10,    11,   559,   444,   183,
     184,   447,   103,   449,   143,   559,   145,   447,   124,   559,
     194,  1120,     4,   209,     3,   121,  1125,   447,   228,   229,
     230,   467,   232,   922,   234,   105,  1135,    50,   474,   195,
      53,   160,   242,   243,   244,   194,  1046,   247,   222,   146,
     209,   103,   158,   104,   428,   978,    50,   648,   649,    53,
      49,   102,   436,   169,    34,    35,    36,    37,    38,    49,
     444,   122,   258,   382,   383,   384,   127,   258,    91,   130,
     209,   418,   419,   420,   520,    47,   522,   193,   428,   382,
     383,   384,   522,   467,   952,   184,   436,    91,   104,   535,
     536,   537,   522,    47,   444,   144,    47,   447,   178,   449,
     180,   162,   215,   216,   217,   218,   122,    78,    79,    47,
     550,   127,   185,   241,   130,    47,    47,   467,   522,   559,
     561,    47,   241,   102,   758,   131,   206,    47,   144,   250,
     576,   248,   203,   537,   157,    47,   882,   883,   884,   885,
     886,   887,   888,   889,   201,    61,   162,   193,   594,   595,
     596,   597,   598,   157,   182,    20,   560,   603,   144,   120,
     221,   258,    47,    47,    47,    47,    99,   138,   139,   519,
     520,     3,   576,  1041,   145,   258,   622,    14,   192,   192,
     258,   103,     5,   154,    24,   535,   536,     4,    48,    48,
     594,   595,   596,   639,   598,     5,   642,   643,    68,   603,
      47,   647,   642,    54,   172,   221,    47,   647,   102,   102,
     560,    49,   658,   597,    47,    47,    47,   663,   258,   665,
      49,    49,   210,   102,    24,   665,    19,    78,    79,    24,
     137,   137,   978,     5,    61,   201,   258,    61,   622,    32,
      33,    34,    35,    36,    37,    38,    99,   597,    69,    47,
      61,    18,    19,    20,    21,    22,    23,    24,    25,    26,
      27,    28,    29,    30,    31,    32,    33,    34,    35,    36,
      37,    38,   622,     3,   199,   126,   258,  1030,    49,   192,
     258,    49,     4,   723,    47,   258,    48,   138,   139,   639,
     141,   220,   642,   643,   145,    48,   736,   647,   175,   196,
     258,   200,   867,   154,    47,    17,   736,     4,   658,   258,
      24,   131,    47,    24,    19,   665,    48,    48,   127,    48,
    1073,   201,   147,     5,     4,    34,   177,    32,    33,    34,
      35,    36,    37,    38,     5,     3,   204,   169,   784,    28,
      29,    30,    31,    32,    33,    34,    35,    36,    37,    38,
      18,    19,    20,    21,    22,    23,    24,    25,    26,    27,
      28,    29,    30,    31,    32,    33,    34,    35,    36,    37,
      38,    48,    78,    79,    47,   256,   219,    49,    24,    49,
     784,    47,    38,    48,     4,    49,    54,   833,    66,     4,
     212,   192,   211,   833,   226,   227,   228,   229,   230,   231,
     232,   233,   234,   235,   236,   237,   238,   239,   240,   241,
     242,   243,   244,   245,   246,   247,   248,   249,   250,   186,
     126,   128,   868,   104,    77,    47,   294,   867,    47,   297,
     298,   150,   138,   139,    47,   141,    47,    54,   306,   145,
      48,   122,   258,   208,   312,   313,   127,   315,   154,   130,
      48,    95,   115,   321,   212,   168,    47,    47,   904,     5,
     166,     5,   866,   144,   868,   333,   115,     5,     5,   337,
       5,   145,    61,    47,    49,    47,   922,    38,    10,    24,
     240,   162,   928,   833,    10,   258,    48,   116,   356,   357,
     358,   359,   360,    32,    33,    34,    35,    36,    37,    38,
     904,    24,   948,    61,    24,    48,   952,    48,    48,    10,
     128,     4,   952,     4,   864,     3,   866,   385,    48,    48,
     178,     4,     4,     8,   928,    13,    14,     5,     5,     5,
       5,     5,   258,   251,   402,   403,    48,    48,    47,     5,
     221,     4,   258,    24,    24,     3,  1005,  1006,  1007,  1008,
    1009,  1010,  1011,  1012,  1013,  1001,   239,  1003,   226,   227,
     228,   229,   230,   231,   232,   233,   234,   235,   236,   237,
     238,   239,   240,   241,   242,   243,   244,   245,   246,   247,
     248,   249,   250,    24,    48,     5,    48,   713,  1034,   723,
      48,    24,   202,   864,   928,  1041,    54,  1001,   948,  1003,
     594,  1041,   952,   804,   808,  1066,   445,   124,   268,   210,
     440,   479,   818,   951,   957,   483,   762,   905,   666,   198,
     118,   447,   185,   431,   112,   113,   415,   608,    19,   281,
    1034,   655,   287,   469,   599,   576,   305,     3,    -1,   769,
      -1,    -1,   510,    -1,   512,   513,   104,    -1,   516,    27,
      28,    29,    30,    31,    32,    33,    34,    35,    36,    37,
      38,   149,    -1,    -1,   122,   533,    -1,    -1,    -1,   127,
     538,   539,   130,   541,   542,   543,   544,   545,   546,   547,
     548,    47,   170,   171,   552,   553,   554,   555,   556,   557,
     558,  1041,    34,    35,    36,    37,    38,    39,    40,    41,
      42,    43,    44,    45,   162,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   582,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   591,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    -1,    -1,    -1,    -1,   226,   227,
     228,   229,   230,   231,   232,   233,   234,   235,   236,   237,
     238,   239,   240,   241,   242,   243,   244,   245,   246,   247,
     248,   249,   250,   221,    -1,    -1,    -1,   635,   226,   227,
     228,   229,   230,   231,   232,   233,   234,   235,   236,   237,
     238,   239,   240,   241,   242,   243,   244,   245,   246,   247,
     248,   249,   250,    -1,    -1,    -1,    -1,    -1,   666,    -1,
      -1,    -1,   670,   169,    -1,    -1,   674,    -1,   676,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   689,    -1,   691,    -1,   693,    -1,    -1,    -1,   697,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   710,    -1,   712,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   721,    -1,    -1,    -1,    -1,    -1,    -1,
     226,   227,   228,   229,   230,   231,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,   244,   245,
     246,   247,   248,   249,   250,    -1,     3,     4,     5,     6,
      -1,     8,     9,    10,   762,    12,    13,    14,    -1,    -1,
      -1,    -1,    -1,    20,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    32,    33,    34,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    45,    -1,
      47,    48,    -1,    -1,    -1,    52,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    60,    -1,   813,    63,    -1,    -1,    -1,
      -1,    -1,   820,    -1,    -1,    -1,    -1,    52,    53,    -1,
      -1,    -1,    -1,    80,    81,    -1,    -1,    84,    -1,    -1,
      -1,    -1,    89,    -1,    -1,    -1,    71,    -1,    -1,    -1,
      -1,    -1,    99,   851,    -1,    -1,   854,    -1,   856,    -1,
     858,    86,    -1,    -1,    -1,   863,    91,    -1,    93,    -1,
      -1,   869,    -1,    -1,    -1,   873,    -1,    -1,   876,    -1,
      -1,    -1,   129,    -1,    -1,    -1,    -1,    -1,    -1,   114,
      -1,    -1,    -1,    -1,    -1,   142,    -1,    -1,    -1,   124,
      -1,    -1,    -1,    -1,    -1,    -1,   153,   905,    -1,    -1,
      -1,   158,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   171,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   158,    -1,    -1,    -1,    -1,    -1,    -1,
     187,    -1,    -1,   190,   169,    -1,    -1,    -1,    -1,    -1,
      -1,   949,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   213,   214,   193,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   974,   224,    -1,   226,
     227,   228,   229,   230,   231,   232,   233,   234,   235,   236,
     237,   238,   239,   240,   241,   242,   243,   244,   245,   246,
     247,   248,   249,   250,    -1,   252,   253,   254,   255,    -1,
      -1,     3,     4,     5,     6,    -1,     8,     9,    10,    -1,
      12,    13,    14,    -1,    -1,  1023,    -1,    -1,    20,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,  1036,  1037,
      32,    33,    34,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    45,    -1,    47,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    63,    -1,    -1,    -1,    -1,  1074,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    80,    81,
      -1,     3,    84,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    99,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,  1120,    -1,    -1,    -1,    -1,  1125,    -1,     3,
       4,     5,     6,    -1,     8,     9,    10,  1135,    12,    13,
      14,    -1,    54,    -1,    -1,    -1,    20,    -1,    -1,    -1,
     142,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    32,    33,
      -1,   153,    -1,    -1,    -1,    -1,   158,    -1,    -1,    -1,
      -1,    45,    -1,    47,    -1,    -1,    -1,    -1,    -1,   171,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    63,
      -1,    -1,    -1,    -1,    -1,   187,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    80,    81,    -1,    -1,
      84,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   213,   214,    -1,    -1,    99,    -1,    -1,    -1,    -1,
      -1,    -1,   224,    -1,   226,   227,   228,   229,   230,   231,
     232,   233,   234,   235,   236,   237,   238,   239,   240,   241,
     242,   243,   244,   245,   246,   247,   248,   249,   250,    -1,
     252,   253,   254,   255,    -1,    -1,    -1,    -1,   142,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   153,
      -1,    -1,    -1,    -1,   158,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   169,    -1,   171,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   187,   226,   227,   228,   229,   230,   231,
     232,   233,   234,   235,   236,   237,   238,   239,   240,   241,
     242,   243,   244,   245,   246,   247,   248,   249,   250,   213,
     214,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     224,    -1,   226,   227,   228,   229,   230,   231,   232,   233,
     234,   235,   236,   237,   238,   239,   240,   241,   242,   243,
     244,   245,   246,   247,   248,   249,   250,    -1,   252,   253,
     254,   255,     3,     4,     5,     6,    -1,     8,     9,    10,
      -1,    12,    13,    14,    -1,    -1,    -1,    -1,    -1,    20,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    32,    33,    57,    58,    59,    -1,    -1,    -1,    -1,
      64,    -1,    -1,    -1,    45,    -1,    47,    -1,    72,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    81,    82,    -1,
      84,    -1,    63,    -1,    -1,    -1,    90,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   101,    -1,    80,
      81,    -1,    -1,    84,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    99,   123,
      -1,    -1,   103,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     134,    -1,   136,    -1,    -1,    -1,   140,    -1,     3,     4,
       5,     6,    -1,     8,     9,    10,    -1,    12,    13,    14,
      -1,    -1,   156,    -1,    -1,    20,    -1,    -1,    -1,    -1,
      -1,   142,    -1,    -1,    -1,    -1,    -1,    32,    33,    -1,
     174,    -1,   153,    -1,    -1,    -1,    -1,   158,    -1,    -1,
      45,    -1,    47,   187,   188,   189,    -1,    -1,    -1,    -1,
     171,    -1,    -1,   197,   198,    -1,    -1,    -1,    63,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   187,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    80,    81,    -1,    -1,    84,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   213,   214,    99,    -1,    -1,    -1,   103,    -1,
      -1,    -1,    -1,   224,    -1,   226,   227,   228,   229,   230,
     231,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,   244,   245,   246,   247,   248,   249,   250,
      -1,   252,   253,   254,   255,    -1,    -1,   142,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   153,    -1,
      -1,    -1,    -1,   158,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   171,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   187,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    -1,    -1,    -1,    -1,   213,   214,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   224,
      -1,   226,   227,   228,   229,   230,   231,   232,   233,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,   246,   247,   248,   249,   250,    -1,   252,   253,   254,
     255,     3,     4,     5,     6,    -1,     8,     9,    10,    -1,
      12,    13,    14,    -1,    -1,    -1,    -1,    -1,    20,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      32,    33,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    45,    -1,    47,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    63,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    80,    81,
      -1,    -1,    84,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    99,    -1,    -1,
      -1,   103,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,     3,     4,     5,
       6,     7,     8,     9,    10,    -1,    12,    13,    14,    -1,
      -1,    -1,    -1,    -1,    20,    -1,    -1,    -1,    -1,    -1,
     142,    -1,    -1,    -1,    -1,    -1,    32,    33,    -1,    -1,
      -1,   153,    -1,    -1,    -1,    -1,   158,    -1,    -1,    45,
      -1,    47,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   171,
      -1,    -1,    -1,   258,    -1,    -1,    -1,    63,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   187,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    80,    81,    -1,    -1,    84,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   213,   214,    99,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   224,    -1,   226,   227,   228,   229,   230,   231,
     232,   233,   234,   235,   236,   237,   238,   239,   240,   241,
     242,   243,   244,   245,   246,   247,   248,   249,   250,    -1,
     252,   253,   254,   255,    -1,    -1,   142,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   153,    -1,    -1,
      -1,    -1,   158,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   171,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   187,    18,    19,    20,    21,    22,    23,    24,    25,
      26,    27,    28,    29,    30,    31,    32,    33,    34,    35,
      36,    37,    38,    -1,    -1,    -1,    -1,   213,   214,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   224,    -1,
     226,   227,   228,   229,   230,   231,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,   244,   245,
     246,   247,   248,   249,   250,    -1,   252,   253,   254,   255,
       3,     4,     5,     6,    -1,     8,     9,    10,    -1,    12,
      13,    14,    -1,    -1,    -1,    -1,    -1,    20,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    32,
      33,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    45,    -1,    47,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      63,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    80,    81,    -1,
      -1,    84,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    99,    20,    21,    22,
      23,    24,    25,    26,    27,    28,    29,    30,    31,    32,
      33,    34,    35,    36,    37,    38,     3,     4,     5,     6,
      -1,     8,     9,    10,    -1,    12,    13,    14,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   142,
      -1,    -1,    -1,    -1,    -1,    32,    33,    -1,    -1,    -1,
     153,    -1,    -1,    -1,    -1,   158,    -1,    -1,    45,    -1,
      47,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   171,    -1,
      -1,    -1,   258,    -1,    -1,    -1,    63,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   187,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    81,    -1,    -1,    84,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,     3,
     213,   214,    99,    -1,    -1,    -1,    -1,    -1,    -1,    13,
      14,   224,    -1,   226,   227,   228,   229,   230,   231,   232,
     233,   234,   235,   236,   237,   238,   239,   240,   241,   242,
     243,   244,   245,   246,   247,   248,   249,   250,    -1,   252,
     253,   254,   255,    -1,    -1,   142,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   153,    -1,    -1,    -1,
      -1,   158,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   171,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     187,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   112,   113,
      -1,    -1,     3,    -1,    -1,    -1,   213,   214,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   224,    -1,   226,
     227,   228,   229,   230,   231,   232,   233,   234,   235,   236,
     237,   238,   239,   240,   241,   242,   243,   244,   245,   246,
     247,   248,   249,   250,    45,   252,   253,   254,   255,     3,
      -1,    -1,    -1,    -1,    -1,    -1,   170,   171,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    70,
      71,    -1,    -1,    -1,    -1,    76,    -1,    -1,    -1,    -1,
      81,    -1,    -1,    84,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    47,    -1,     3,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   104,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   112,   226,   227,   228,   229,   230,   231,   232,   233,
     234,   235,   236,   237,   238,   239,   240,   241,   242,   243,
     244,   245,   246,   247,   248,   249,   250,     3,    92,    47,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   148,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   158,    -1,    -1,
      -1,    -1,    -1,     3,    -1,    -1,   167,    -1,    34,   170,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   179,    -1,
      -1,    -1,    -1,   184,   185,    -1,   187,    -1,     3,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   203,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   213,   214,    54,    -1,    -1,    -1,    -1,    34,
      -1,    -1,   223,   224,     3,    -1,    -1,    -1,    -1,    -1,
     231,    -1,    -1,    -1,    -1,    -1,   237,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   250,
      -1,   252,   253,   254,   255,    34,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,     3,    -1,    -1,
      -1,    -1,   226,   227,   228,   229,   230,   231,   232,   233,
     234,   235,   236,   237,   238,   239,   240,   241,   242,   243,
     244,   245,   246,   247,   248,   249,   250,     3,    34,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   226,   227,
     228,   229,   230,   231,   232,   233,   234,   235,   236,   237,
     238,   239,   240,   241,   242,   243,   244,   245,   246,   247,
     248,   249,   250,     3,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     226,   227,   228,   229,   230,   231,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,   244,   245,
     246,   247,   248,   249,   250,     3,   226,   227,   228,   229,
     230,   231,   232,   233,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,   246,   247,   248,   249,
     250,   226,   227,   228,   229,   230,   231,   232,   233,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,   246,   247,   248,   249,   250,     3,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   152,   226,   227,   228,
     229,   230,   231,   232,   233,   234,   235,   236,   237,   238,
     239,   240,   241,   242,   243,   244,   245,   246,   247,   248,
     249,   250,     3,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     226,   227,   228,   229,   230,   231,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,   244,   245,
     246,   247,   248,   249,   250,    -1,    -1,    -1,    -1,    -1,
     226,   227,   228,   229,   230,   231,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,   244,   245,
     246,   247,   248,   249,   250,    18,    19,    20,    21,    22,
      23,    24,    25,    26,    27,    28,    29,    30,    31,    32,
      33,    34,    35,    36,    37,    38,   226,   227,   228,   229,
     230,   231,   232,   233,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,   246,   247,   248,   249,
     250,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   226,   227,
     228,   229,   230,   231,   232,   233,   234,   235,   236,   237,
     238,   239,   240,   241,   242,   243,   244,   245,   246,   247,
     248,   249,   250,    -1,    -1,    -1,    18,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29,    30,    31,
      32,    33,    34,    35,    36,    37,    38,    -1,    -1,   226,
     227,   228,   229,   230,   231,   232,   233,   234,   235,   236,
     237,   238,   239,   240,   241,   242,   243,   244,   245,   246,
     247,   248,   249,   250,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   226,   227,   228,   229,   230,
     231,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,   244,   245,   246,   247,   248,   249,   250,
      47,   103,    -1,    -1,    -1,    -1,    53,    -1,    -1,    56,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      67,    -1,    -1,    -1,    71,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    83,    -1,    -1,    86,
      87,    88,    -1,    -1,    91,    -1,    93,    -1,    -1,    -1,
      -1,    98,    -1,   100,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   258,    -1,   114,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   124,    -1,    18,
      19,    20,    21,    22,    23,    24,    25,    26,    27,    28,
      29,    30,    31,    32,    33,    34,    35,    36,    37,    38,
      -1,    -1,    -1,    -1,   151,    -1,    -1,    -1,    -1,    -1,
     157,   158,    -1,    -1,   161,    54,   163,   164,    -1,    -1,
      -1,    -1,   169,    -1,    -1,   172,   173,    -1,    -1,    -1,
     177,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   193,    -1,   195,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   103,    18,    19,    20,    21,    22,
      23,    24,    25,    26,    27,    28,    29,    30,    31,    32,
      33,    34,    35,    36,    37,    38,    18,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29,    30,    31,
      32,    33,    34,    35,    36,    37,    38,    18,    19,    20,
      21,    22,    23,    24,    25,    26,    27,    28,    29,    30,
      31,    32,    33,    34,    35,    36,    37,    38,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    55,    -1,    -1,    -1,    -1,    -1,
     103,    19,    20,    21,    22,    23,    24,    25,    26,    27,
      28,    29,    30,    31,    32,    33,    34,    35,    36,    37,
      38,   103,    -1,    -1,    -1,    -1,    87,    18,    19,    20,
      21,    22,    23,    24,    25,    26,    27,    28,    29,    30,
      31,    32,    33,    34,    35,    36,    37,    38,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    48,    18,    19,
      20,    21,    22,    23,    24,    25,    26,    27,    28,    29,
      30,    31,    32,    33,    34,    35,    36,    37,    38,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    48,    18,
      19,    20,    21,    22,    23,    24,    25,    26,    27,    28,
      29,    30,    31,    32,    33,    34,    35,    36,    37,    38,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    48,
      18,    19,    20,    21,    22,    23,    24,    25,    26,    27,
      28,    29,    30,    31,    32,    33,    34,    35,    36,    37,
      38,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      48,    18,    19,    20,    21,    22,    23,    24,    25,    26,
      27,    28,    29,    30,    31,    32,    33,    34,    35,    36,
      37,    38,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    48,    18,    19,    20,    21,    22,    23,    24,    25,
      26,    27,    28,    29,    30,    31,    32,    33,    34,    35,
      36,    37,    38,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    48,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    18,    19,    20,    21,    22,    23,    24,    25,    26,
      27,    28,    29,    30,    31,    32,    33,    34,    35,    36,
      37,    38
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
       0,    47,    53,    56,    67,    71,    83,    86,    87,    88,
      91,    93,    98,   100,   114,   124,   151,   157,   158,   161,
     163,   164,   169,   172,   173,   177,   193,   195,   260,   261,
     262,   284,   285,   288,   304,   309,   312,   315,   316,   318,
     323,   324,   325,   326,   327,   328,   362,   365,   372,   377,
     379,   381,   383,   385,   399,   400,   404,   406,   409,   413,
     414,   415,   416,   419,   424,   426,   429,   434,   437,   438,
     439,   445,   324,   325,   179,   183,   184,   194,   202,   411,
     411,    18,    76,   184,   194,   222,   388,   117,   332,     3,
     226,   227,   228,   229,   230,   231,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,   244,   245,
     246,   247,   248,   249,   250,   358,   454,   457,   358,    76,
     183,   184,   194,   222,   184,     3,   427,   456,   457,   249,
     364,    52,    53,    71,    86,    91,    93,   114,   124,   158,
     169,   193,   420,   421,   427,   184,   194,   420,   411,   112,
     417,   332,     3,    13,    14,   112,   113,   149,   170,   171,
     430,   431,   453,   457,     3,    45,    70,    71,    76,    81,
      84,   104,   112,   148,   158,   167,   170,   179,   184,   185,
     187,   213,   214,   223,   224,   231,   237,   250,   252,   253,
     254,   255,   276,   278,   368,   371,   455,   191,   332,   222,
       0,    96,   257,   125,   200,   317,    15,    16,    17,   145,
     346,   347,   102,   342,   151,    48,    48,   106,   172,   181,
     446,   389,   454,   358,     4,   375,   158,     3,   305,   121,
     289,   373,   374,   375,   378,   454,   183,     3,    47,    76,
     107,   108,   109,   110,   111,   119,   127,   155,   165,   207,
     333,   334,   335,   103,     4,   370,    49,   370,     3,   310,
     311,   389,   121,   313,   375,   384,   382,   454,   313,   195,
     435,   284,   285,   316,   323,   363,   160,   422,   194,   146,
     144,   258,   103,   358,   375,   407,   408,   144,   425,   204,
     205,   418,    52,    89,   352,    24,   192,   432,   432,   453,
      49,   102,   405,   453,    49,   258,   432,    47,    30,   103,
     184,   144,    47,    47,    27,   199,   369,    47,   185,   241,
     369,    47,    47,    47,    81,   188,   222,   102,   367,   405,
     241,   131,   366,    47,   250,   248,   203,    47,   201,   412,
     357,   358,   380,   454,   262,   358,     3,     4,     5,     6,
       8,     9,    10,    12,    13,    14,    20,    32,    33,    47,
      63,    80,    99,   142,   153,   171,   224,   236,   264,   265,
     266,   267,   269,   271,   278,   279,   280,   283,   324,   453,
     454,   457,   352,   352,   352,    61,   131,   143,   209,   331,
     342,   193,   427,   447,   448,   453,   182,    74,    75,    78,
      79,   126,   138,   139,   141,   145,   154,   166,   392,   393,
     394,   395,   396,   397,   398,   401,   402,   403,    50,    53,
      91,   157,   440,   441,   120,   133,   245,   410,   144,    20,
     358,   258,   120,   389,    47,   338,    47,    47,    47,   118,
     258,   335,   358,   454,   144,   258,    99,    47,   314,   324,
     356,   357,   359,   258,   314,    14,   433,   436,    34,   423,
     454,   421,   284,   285,   316,   323,   428,   192,   192,   258,
     423,   103,     5,    34,   269,   353,   354,   269,   269,   432,
     453,   375,    24,   432,   453,   431,   269,   263,   269,   358,
     358,   358,   358,   269,   269,     4,   269,   263,   369,   269,
      48,    48,    73,   369,     5,   269,   369,    34,    48,    52,
      60,    89,   129,   190,   263,   269,   282,   343,    68,   172,
      54,   454,    47,   319,   323,   269,   269,   269,   263,   269,
     324,   269,   272,    47,   324,   102,   102,    49,    18,    19,
      20,    21,    22,    23,    24,    25,    26,    27,    28,    29,
      30,    31,    32,    33,    34,    35,    36,    37,    38,    47,
      49,   169,   324,   327,   328,   327,   327,   269,   348,   349,
       5,    12,   340,   340,   210,   342,   258,    24,    24,   137,
     137,     5,    61,   269,   269,    78,    79,   138,   139,   145,
     154,   201,   402,    69,   442,   442,   442,   192,   442,   258,
      61,   358,    99,    47,   374,    61,    54,   177,   386,   387,
     390,   391,   392,   393,   394,   395,   396,   397,   398,     3,
      48,   258,   358,   215,   216,   217,   218,   336,   337,   105,
     178,   180,   206,   339,   334,   199,   329,   358,   305,   324,
     356,   359,   258,    54,   454,   104,   122,   127,   130,   162,
     221,   360,   375,    66,   449,   258,    49,   192,    49,   358,
     375,   407,   384,    54,   456,   103,   258,   331,   341,   269,
     432,     4,   376,   269,   432,    48,   258,   369,   369,   369,
      48,   258,    48,    48,    47,   258,   258,    48,   220,   103,
     269,   103,   269,   103,   269,    48,    54,   103,   269,    48,
     175,     3,   265,   286,   287,   454,   454,   320,   453,   196,
     258,    48,   200,   273,   274,   269,   389,   389,   453,   269,
     269,    27,    29,    30,   269,   269,   269,   269,   269,   269,
     269,   269,    32,    33,   267,   268,    47,   270,   324,     8,
      10,    11,    20,   269,   269,   269,   269,   269,   269,   269,
     281,   284,   285,   316,   323,    34,   453,   454,   332,    55,
      87,   350,   258,   143,   258,   131,   211,   340,   448,   266,
       4,    24,    24,   269,   269,   292,   453,   453,   453,   358,
     453,   157,   441,   376,    47,   306,   152,   290,   291,   292,
     376,    57,    58,    59,    64,    72,    81,    82,    84,    90,
     101,   123,   134,   136,   140,   156,   174,   187,   188,   189,
     197,   198,   293,   201,   387,    48,   358,    48,   258,    48,
       7,   269,   317,    48,   356,   454,   147,   361,   356,   221,
     361,   221,   361,   127,     4,   433,    34,   384,    34,   454,
     456,    92,   355,   356,   353,   269,   269,   269,     5,    48,
       5,   256,    47,   269,   103,   269,   103,   269,   103,   219,
     293,   269,    48,    24,   258,   329,    49,    48,   258,    47,
     321,   269,   269,    94,   274,   275,   258,   269,   268,   270,
     268,   268,    19,    32,    33,    34,    35,    36,    37,    38,
     263,     8,    10,    11,    39,    40,    41,    42,    43,    44,
      45,   277,   277,    48,    49,   352,   349,   340,   340,   340,
     212,   211,   449,     4,     4,   293,    91,   172,   444,    62,
     159,   443,   192,   320,    77,   307,   128,    48,   258,    47,
     298,   298,    47,   294,   150,   296,    47,   295,   294,    47,
     297,   297,   298,   298,   269,    48,   337,   269,    54,   144,
     356,   329,   258,   329,   258,   269,   208,   345,   269,   269,
     269,   115,    48,   269,   287,   317,   454,   323,   453,   269,
     322,   258,   317,    48,   186,   269,    95,   269,    19,   268,
     268,   268,   268,   268,   268,   268,   268,    48,    34,   453,
     354,   212,   168,   450,   299,    20,    85,    20,    85,   456,
      48,    47,   308,    47,    66,   228,   229,   230,   232,   234,
     242,   243,   244,   247,   301,   302,   291,     5,     5,     5,
       5,   454,   269,   144,   341,   356,   115,   344,     5,   277,
      61,   346,   347,    47,    49,    48,   258,    47,   269,    48,
     268,   103,    24,   240,    10,    20,    85,   152,   226,   300,
      10,    10,   266,   320,   320,    24,   303,   303,   303,   303,
     303,   303,   303,   303,   303,   303,   258,    48,    48,   258,
      48,    48,   269,    61,   116,   351,    48,    48,   263,    48,
     347,   269,   322,    60,   135,   176,    24,    10,   266,   128,
      48,    48,     4,     4,   178,     4,     4,     5,     5,     5,
       5,     8,   302,     5,   263,   269,   251,   330,    48,    48,
     227,   233,   235,   246,   451,    48,    47,    65,   238,   452,
      47,   258,    24,    24,   322,    47,    48,     5,     4,    48,
     322,   239,   258,    48,    24,    47,    48,    48,     5,   322,
      48,    48
};

#define yyerrok		(yyerrstatus = 0)
#define yyclearin	(yychar = YYEMPTY)
#define YYEMPTY		(-2)
#define YYEOF		0

#define YYACCEPT	goto yyacceptlab
#define YYABORT		goto yyabortlab
#define YYERROR		goto yyerrorlab


/* Like YYERROR except do call yyerror.  This remains here temporarily
   to ease the transition to the new meaning of YYERROR, for GCC.
   Once GCC version 2 has supplanted version 1, this can go.  */

#define YYFAIL		goto yyerrlab

#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)					\
do								\
  if (yychar == YYEMPTY && yylen == 1)				\
    {								\
      yychar = (Token);						\
      yylval = (Value);						\
      yytoken = YYTRANSLATE (yychar);				\
      YYPOPSTACK (1);						\
      goto yybackup;						\
    }								\
  else								\
    {								\
      yyerror (&yylloc, result, YY_("syntax error: cannot back up")); \
      YYERROR;							\
    }								\
while (YYID (0))


#define YYTERROR	1
#define YYERRCODE	256


/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#define YYRHSLOC(Rhs, K) ((Rhs)[K])
#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)				\
    do									\
      if (YYID (N))                                                    \
	{								\
	  (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;	\
	  (Current).first_column = YYRHSLOC (Rhs, 1).first_column;	\
	  (Current).last_line    = YYRHSLOC (Rhs, N).last_line;		\
	  (Current).last_column  = YYRHSLOC (Rhs, N).last_column;	\
	}								\
      else								\
	{								\
	  (Current).first_line   = (Current).last_line   =		\
	    YYRHSLOC (Rhs, 0).last_line;				\
	  (Current).first_column = (Current).last_column =		\
	    YYRHSLOC (Rhs, 0).last_column;				\
	}								\
    while (YYID (0))
#endif


/* YY_LOCATION_PRINT -- Print the location on the stream.
   This macro was not mandated originally: define only if we know
   we won't break user code: when these are the locations we know.  */

#ifndef YY_LOCATION_PRINT
# if YYLTYPE_IS_TRIVIAL
#  define YY_LOCATION_PRINT(File, Loc)			\
     fprintf (File, "%d.%d-%d.%d",			\
	      (Loc).first_line, (Loc).first_column,	\
	      (Loc).last_line,  (Loc).last_column)
# else
#  define YY_LOCATION_PRINT(File, Loc) ((void) 0)
# endif
#endif


/* YYLEX -- calling `yylex' with the right arguments.  */

#ifdef YYLEX_PARAM
# define YYLEX yylex (&yylval, &yylloc, YYLEX_PARAM)
#else
# define YYLEX yylex (&yylval, &yylloc)
#endif

/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)			\
do {						\
  if (yydebug)					\
    YYFPRINTF Args;				\
} while (YYID (0))

# define YY_SYMBOL_PRINT(Title, Type, Value, Location)			  \
do {									  \
  if (yydebug)								  \
    {									  \
      YYFPRINTF (stderr, "%s ", Title);					  \
      yy_symbol_print (stderr,						  \
		  Type, Value, Location, result); \
      YYFPRINTF (stderr, "\n");						  \
    }									  \
} while (YYID (0))


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_value_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, ParseResult* result)
#else
static void
yy_symbol_value_print (yyoutput, yytype, yyvaluep, yylocationp, result)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
    YYLTYPE const * const yylocationp;
    ParseResult* result;
#endif
{
  if (!yyvaluep)
    return;
  YYUSE (yylocationp);
  YYUSE (result);
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyoutput, yytoknum[yytype], *yyvaluep);
# else
  YYUSE (yyoutput);
# endif
  switch (yytype)
    {
      default:
	break;
    }
}


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, ParseResult* result)
#else
static void
yy_symbol_print (yyoutput, yytype, yyvaluep, yylocationp, result)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
    YYLTYPE const * const yylocationp;
    ParseResult* result;
#endif
{
  if (yytype < YYNTOKENS)
    YYFPRINTF (yyoutput, "token %s (", yytname[yytype]);
  else
    YYFPRINTF (yyoutput, "nterm %s (", yytname[yytype]);

  YY_LOCATION_PRINT (yyoutput, *yylocationp);
  YYFPRINTF (yyoutput, ": ");
  yy_symbol_value_print (yyoutput, yytype, yyvaluep, yylocationp, result);
  YYFPRINTF (yyoutput, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_stack_print (yytype_int16 *yybottom, yytype_int16 *yytop)
#else
static void
yy_stack_print (yybottom, yytop)
    yytype_int16 *yybottom;
    yytype_int16 *yytop;
#endif
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)				\
do {								\
  if (yydebug)							\
    yy_stack_print ((Bottom), (Top));				\
} while (YYID (0))


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_reduce_print (YYSTYPE *yyvsp, YYLTYPE *yylsp, int yyrule, ParseResult* result)
#else
static void
yy_reduce_print (yyvsp, yylsp, yyrule, result)
    YYSTYPE *yyvsp;
    YYLTYPE *yylsp;
    int yyrule;
    ParseResult* result;
#endif
{
  int yynrhs = yyr2[yyrule];
  int yyi;
  unsigned long int yylno = yyrline[yyrule];
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %lu):\n",
	     yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr, yyrhs[yyprhs[yyrule] + yyi],
		       &(yyvsp[(yyi + 1) - (yynrhs)])
		       , &(yylsp[(yyi + 1) - (yynrhs)])		       , result);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)		\
do {					\
  if (yydebug)				\
    yy_reduce_print (yyvsp, yylsp, Rule, result); \
} while (YYID (0))

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args)
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef	YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif



#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined __GLIBC__ && defined _STRING_H
#   define yystrlen strlen
#  else
/* Return the length of YYSTR.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static YYSIZE_T
yystrlen (const char *yystr)
#else
static YYSIZE_T
yystrlen (yystr)
    const char *yystr;
#endif
{
  YYSIZE_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static char *
yystpcpy (char *yydest, const char *yysrc)
#else
static char *
yystpcpy (yydest, yysrc)
    char *yydest;
    const char *yysrc;
#endif
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

# ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYSIZE_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYSIZE_T yyn = 0;
      char const *yyp = yystr;

      for (;;)
	switch (*++yyp)
	  {
	  case '\'':
	  case ',':
	    goto do_not_strip_quotes;

	  case '\\':
	    if (*++yyp != '\\')
	      goto do_not_strip_quotes;
	    /* Fall through.  */
	  default:
	    if (yyres)
	      yyres[yyn] = *yyp;
	    yyn++;
	    break;

	  case '"':
	    if (yyres)
	      yyres[yyn] = '\0';
	    return yyn;
	  }
    do_not_strip_quotes: ;
    }

  if (! yyres)
    return yystrlen (yystr);

  return yystpcpy (yyres, yystr) - yyres;
}
# endif

/* Copy into YYRESULT an error message about the unexpected token
   YYCHAR while in state YYSTATE.  Return the number of bytes copied,
   including the terminating null byte.  If YYRESULT is null, do not
   copy anything; just return the number of bytes that would be
   copied.  As a special case, return 0 if an ordinary "syntax error"
   message will do.  Return YYSIZE_MAXIMUM if overflow occurs during
   size calculation.  */
static YYSIZE_T
yysyntax_error (char *yyresult, int yystate, int yychar)
{
  int yyn = yypact[yystate];

  if (! (YYPACT_NINF < yyn && yyn <= YYLAST))
    return 0;
  else
    {
      int yytype = YYTRANSLATE (yychar);
      YYSIZE_T yysize0 = yytnamerr (0, yytname[yytype]);
      YYSIZE_T yysize = yysize0;
      YYSIZE_T yysize1;
      int yysize_overflow = 0;
      enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
      char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
      int yyx;

# if 0
      /* This is so xgettext sees the translatable formats that are
	 constructed on the fly.  */
      YY_("syntax error, unexpected %s");
      YY_("syntax error, unexpected %s, expecting %s");
      YY_("syntax error, unexpected %s, expecting %s or %s");
      YY_("syntax error, unexpected %s, expecting %s or %s or %s");
      YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s");
# endif
      char *yyfmt;
      char const *yyf;
      static char const yyunexpected[] = "syntax error, unexpected %s";
      static char const yyexpecting[] = ", expecting %s";
      static char const yyor[] = " or %s";
      char yyformat[sizeof yyunexpected
		    + sizeof yyexpecting - 1
		    + ((YYERROR_VERBOSE_ARGS_MAXIMUM - 2)
		       * (sizeof yyor - 1))];
      char const *yyprefix = yyexpecting;

      /* Start YYX at -YYN if negative to avoid negative indexes in
	 YYCHECK.  */
      int yyxbegin = yyn < 0 ? -yyn : 0;

      /* Stay within bounds of both yycheck and yytname.  */
      int yychecklim = YYLAST - yyn + 1;
      int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
      int yycount = 1;

      yyarg[0] = yytname[yytype];
      yyfmt = yystpcpy (yyformat, yyunexpected);

      for (yyx = yyxbegin; yyx < yyxend; ++yyx)
	if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR)
	  {
	    if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
	      {
		yycount = 1;
		yysize = yysize0;
		yyformat[sizeof yyunexpected - 1] = '\0';
		break;
	      }
	    yyarg[yycount++] = yytname[yyx];
	    yysize1 = yysize + yytnamerr (0, yytname[yyx]);
	    yysize_overflow |= (yysize1 < yysize);
	    yysize = yysize1;
	    yyfmt = yystpcpy (yyfmt, yyprefix);
	    yyprefix = yyor;
	  }

      yyf = YY_(yyformat);
      yysize1 = yysize + yystrlen (yyf);
      yysize_overflow |= (yysize1 < yysize);
      yysize = yysize1;

      if (yysize_overflow)
	return YYSIZE_MAXIMUM;

      if (yyresult)
	{
	  /* Avoid sprintf, as that infringes on the user's name space.
	     Don't have undefined behavior even if the translation
	     produced a string with the wrong number of "%s"s.  */
	  char *yyp = yyresult;
	  int yyi = 0;
	  while ((*yyp = *yyf) != '\0')
	    {
	      if (*yyp == '%' && yyf[1] == 's' && yyi < yycount)
		{
		  yyp += yytnamerr (yyp, yyarg[yyi++]);
		  yyf += 2;
		}
	      else
		{
		  yyp++;
		  yyf++;
		}
	    }
	}
      return yysize;
    }
}
#endif /* YYERROR_VERBOSE */


/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep, YYLTYPE *yylocationp, ParseResult* result)
#else
static void
yydestruct (yymsg, yytype, yyvaluep, yylocationp, result)
    const char *yymsg;
    int yytype;
    YYSTYPE *yyvaluep;
    YYLTYPE *yylocationp;
    ParseResult* result;
#endif
{
  YYUSE (yyvaluep);
  YYUSE (yylocationp);
  YYUSE (result);

  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  switch (yytype)
    {
      case 3: /* "NAME" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 4: /* "STRING" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 5: /* "INTNUM" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 6: /* "DATE_VALUE" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 7: /* "HINT_VALUE" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 8: /* "BOOL" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 9: /* "APPROXNUM" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 10: /* "NULLX" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 11: /* "UNKNOWN" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 12: /* "QUESTIONMARK" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 13: /* "SYSTEM_VARIABLE" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 14: /* "TEMP_VARIABLE" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 260: /* "sql_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 261: /* "stmt_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 262: /* "stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 263: /* "expr_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 264: /* "column_ref" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 265: /* "base_column_ref" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 266: /* "expr_const" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 267: /* "simple_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 268: /* "arith_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 269: /* "expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 270: /* "in_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 271: /* "case_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 272: /* "case_arg" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 273: /* "when_clause_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 274: /* "when_clause" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 275: /* "case_default" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 276: /* "add_sub_date_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 277: /* "interval_time_stamp" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 278: /* "func_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 279: /* "when_func" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 280: /* "when_func_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 281: /* "when_func_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 282: /* "distinct_or_all" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 283: /* "sequence_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 284: /* "delete_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 285: /* "update_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 286: /* "update_asgn_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 287: /* "update_asgn_factor" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 288: /* "create_table_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 289: /* "opt_if_not_exists" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 290: /* "table_element_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 291: /* "table_element" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 292: /* "column_definition" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 293: /* "data_type" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 294: /* "opt_decimal" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 295: /* "opt_float" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 296: /* "opt_precision" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 297: /* "opt_time_precision" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 298: /* "opt_char_length" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 299: /* "opt_column_attribute_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 300: /* "column_attribute" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 301: /* "opt_table_option_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 302: /* "table_option" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 303: /* "opt_equal_mark" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 304: /* "create_index_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 305: /* "opt_index_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 306: /* "opt_index_columns" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 307: /* "opt_storing" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 308: /* "opt_storing_columns" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 309: /* "drop_index_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 310: /* "index_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 311: /* "drp_index_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 312: /* "drop_table_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 313: /* "opt_if_exists" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 314: /* "table_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 315: /* "truncate_table_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 316: /* "insert_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 317: /* "opt_when" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 318: /* "replace_or_insert" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 319: /* "opt_insert_columns" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 320: /* "column_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 321: /* "insert_vals_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 322: /* "insert_vals" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 323: /* "select_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 324: /* "select_with_parens" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 325: /* "select_no_parens" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 326: /* "no_table_select" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 327: /* "select_clause" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 328: /* "simple_select" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 329: /* "opt_where" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 330: /* "opt_range" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 331: /* "select_limit" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 332: /* "opt_hint" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 333: /* "opt_hint_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 334: /* "hint_options" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 335: /* "hint_option" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 336: /* "join_type_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 337: /* "joins_type" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 338: /* "opt_comma_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 340: /* "limit_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 341: /* "opt_select_limit" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 342: /* "opt_for_update" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 343: /* "parameterized_trim" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 344: /* "opt_groupby" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 345: /* "opt_partition_by" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 346: /* "opt_order_by" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 347: /* "order_by" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 348: /* "sort_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 349: /* "sort_key" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 350: /* "opt_asc_desc" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 351: /* "opt_having" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 352: /* "opt_distinct" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 353: /* "projection" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 354: /* "select_expr_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 355: /* "from_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 356: /* "table_factor" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 357: /* "base_table_factor" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 358: /* "relation_factor" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 359: /* "joined_table" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 360: /* "join_type" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 361: /* "join_outer" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 362: /* "explain_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 363: /* "explainable_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 364: /* "opt_verbose" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 365: /* "show_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 366: /* "opt_limit" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 367: /* "opt_for_grant_user" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 369: /* "opt_show_condition" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 370: /* "opt_like_condition" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 372: /* "create_user_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 373: /* "user_specification_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 374: /* "user_specification" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 375: /* "user" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 376: /* "password" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 377: /* "create_database_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 378: /* "crt_database_specification" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 379: /* "use_database_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 380: /* "use_database_specification" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 381: /* "drop_database_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 382: /* "drop_database_specification" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 383: /* "drop_user_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 384: /* "user_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 385: /* "create_sequence_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 386: /* "sequence_label_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 387: /* "sequence_label" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 388: /* "opt_or_replace" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 389: /* "sequence_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 390: /* "opt_data_type" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 391: /* "opt_start_with" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 392: /* "opt_increment" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 393: /* "opt_minvalue" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 394: /* "opt_maxvalue" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 395: /* "opt_cycle" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 396: /* "opt_cache" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 397: /* "opt_order" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 398: /* "opt_use_quick_path" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 399: /* "drop_sequence_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 400: /* "alter_sequence_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 401: /* "sequence_label_list_alter" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 402: /* "sequence_label_alter" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 403: /* "opt_restart" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 404: /* "set_password_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 405: /* "opt_for_user" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 406: /* "rename_user_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 407: /* "rename_info" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 408: /* "rename_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 409: /* "lock_user_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 410: /* "lock_spec" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 411: /* "opt_work" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 413: /* "begin_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 414: /* "commit_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 415: /* "rollback_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 416: /* "kill_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 417: /* "opt_is_global" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 418: /* "opt_flag" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 419: /* "grant_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 420: /* "priv_type_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 421: /* "priv_type" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 422: /* "opt_privilege" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 423: /* "priv_level" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 424: /* "revoke_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 425: /* "opt_on_priv_level" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 426: /* "prepare_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 427: /* "stmt_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 428: /* "preparable_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 429: /* "variable_set_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 430: /* "var_and_val_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 431: /* "var_and_val" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 432: /* "to_or_eq" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 433: /* "argument" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 434: /* "execute_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 435: /* "opt_using_args" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 436: /* "argument_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 437: /* "deallocate_prepare_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 438: /* "deallocate_or_drop" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 439: /* "alter_table_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 440: /* "alter_column_actions" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 441: /* "alter_column_action" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 442: /* "opt_column" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 444: /* "alter_column_behavior" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 445: /* "alter_system_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 446: /* "opt_force" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 447: /* "alter_system_actions" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 448: /* "alter_system_action" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 449: /* "opt_comment" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 451: /* "server_type" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 452: /* "opt_cluster_or_address" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 453: /* "column_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 454: /* "relation_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 455: /* "function_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 456: /* "column_label" */

	{destroy_tree((yyvaluep->node));};

	break;

      default:
	break;
    }
}

/* Prevent warnings from -Wmissing-prototypes.  */
#ifdef YYPARSE_PARAM
#if defined __STDC__ || defined __cplusplus
int yyparse (void *YYPARSE_PARAM);
#else
int yyparse ();
#endif
#else /* ! YYPARSE_PARAM */
#if defined __STDC__ || defined __cplusplus
int yyparse (ParseResult* result);
#else
int yyparse ();
#endif
#endif /* ! YYPARSE_PARAM */





/*-------------------------.
| yyparse or yypush_parse.  |
`-------------------------*/

#ifdef YYPARSE_PARAM
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (void *YYPARSE_PARAM)
#else
int
yyparse (YYPARSE_PARAM)
    void *YYPARSE_PARAM;
#endif
#else /* ! YYPARSE_PARAM */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (ParseResult* result)
#else
int
yyparse (result)
    ParseResult* result;
#endif
#endif
{
/* The lookahead symbol.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;

/* Location data for the lookahead symbol.  */
YYLTYPE yylloc;

    /* Number of syntax errors so far.  */
    int yynerrs;

    int yystate;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus;

    /* The stacks and their tools:
       `yyss': related to states.
       `yyvs': related to semantic values.
       `yyls': related to locations.

       Refer to the stacks thru separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* The state stack.  */
    yytype_int16 yyssa[YYINITDEPTH];
    yytype_int16 *yyss;
    yytype_int16 *yyssp;

    /* The semantic value stack.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs;
    YYSTYPE *yyvsp;

    /* The location stack.  */
    YYLTYPE yylsa[YYINITDEPTH];
    YYLTYPE *yyls;
    YYLTYPE *yylsp;

    /* The locations where the error started and ended.  */
    YYLTYPE yyerror_range[2];

    YYSIZE_T yystacksize;

  int yyn;
  int yyresult;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;
  YYLTYPE yyloc;

#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYSIZE_T yymsg_alloc = sizeof yymsgbuf;
#endif

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N), yylsp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yytoken = 0;
  yyss = yyssa;
  yyvs = yyvsa;
  yyls = yylsa;
  yystacksize = YYINITDEPTH;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY; /* Cause a token to be read.  */

  /* Initialize stack pointers.
     Waste one element of value and location stack
     so that they stay on the same level as the state stack.
     The wasted elements are never initialized.  */
  yyssp = yyss;
  yyvsp = yyvs;
  yylsp = yyls;

#if YYLTYPE_IS_TRIVIAL
  /* Initialize the default location before parsing starts.  */
  yylloc.first_line   = yylloc.last_line   = 1;
  yylloc.first_column = yylloc.last_column = 1;
#endif

  goto yysetstate;

/*------------------------------------------------------------.
| yynewstate -- Push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
 yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;

 yysetstate:
  *yyssp = yystate;

  if (yyss + yystacksize - 1 <= yyssp)
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYSIZE_T yysize = yyssp - yyss + 1;

#ifdef yyoverflow
      {
	/* Give user a chance to reallocate the stack.  Use copies of
	   these so that the &'s don't force the real ones into
	   memory.  */
	YYSTYPE *yyvs1 = yyvs;
	yytype_int16 *yyss1 = yyss;
	YYLTYPE *yyls1 = yyls;

	/* Each stack pointer address is followed by the size of the
	   data in use in that stack, in bytes.  This used to be a
	   conditional around just the two extra args, but that might
	   be undefined if yyoverflow is a macro.  */
	yyoverflow (YY_("memory exhausted"),
		    &yyss1, yysize * sizeof (*yyssp),
		    &yyvs1, yysize * sizeof (*yyvsp),
		    &yyls1, yysize * sizeof (*yylsp),
		    &yystacksize);

	yyls = yyls1;
	yyss = yyss1;
	yyvs = yyvs1;
      }
#else /* no yyoverflow */
# ifndef YYSTACK_RELOCATE
      goto yyexhaustedlab;
# else
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
	goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
	yystacksize = YYMAXDEPTH;

      {
	yytype_int16 *yyss1 = yyss;
	union yyalloc *yyptr =
	  (union yyalloc *) YYSTACK_ALLOC (YYSTACK_BYTES (yystacksize));
	if (! yyptr)
	  goto yyexhaustedlab;
	YYSTACK_RELOCATE (yyss_alloc, yyss);
	YYSTACK_RELOCATE (yyvs_alloc, yyvs);
	YYSTACK_RELOCATE (yyls_alloc, yyls);
#  undef YYSTACK_RELOCATE
	if (yyss1 != yyssa)
	  YYSTACK_FREE (yyss1);
      }
# endif
#endif /* no yyoverflow */

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;
      yylsp = yyls + yysize - 1;

      YYDPRINTF ((stderr, "Stack size increased to %lu\n",
		  (unsigned long int) yystacksize));

      if (yyss + yystacksize - 1 <= yyssp)
	YYABORT;
    }

  YYDPRINTF ((stderr, "Entering state %d\n", yystate));

  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;

/*-----------.
| yybackup.  |
`-----------*/
yybackup:

  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yyn == YYPACT_NINF)
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid lookahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = YYLEX;
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yyn == 0 || yyn == YYTABLE_NINF)
	goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);

  /* Discard the shifted token.  */
  yychar = YYEMPTY;

  yystate = yyn;
  *++yyvsp = yylval;
  *++yylsp = yylloc;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- Do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     `$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];

  /* Default location.  */
  YYLLOC_DEFAULT (yyloc, (yylsp - yylen), yylen);
  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
        case 2:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_STMT_LIST, (yyvsp[(1) - (2)].node));
      result->result_tree_ = (yyval.node);
      YYACCEPT;
    ;}
    break;

  case 3:

    {
      if ((yyvsp[(3) - (3)].node) != NULL)
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      else
        (yyval.node) = (yyvsp[(1) - (3)].node);
    ;}
    break;

  case 4:

    {
      (yyval.node) = ((yyvsp[(1) - (1)].node) != NULL) ? (yyvsp[(1) - (1)].node) : NULL;
    ;}
    break;

  case 5:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 6:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 7:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 8:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 9:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 10:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 11:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 12:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 13:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 14:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 15:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 16:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 17:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 18:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 19:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 20:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 21:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 22:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 23:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 24:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 25:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 26:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 27:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 28:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 29:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 30:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 31:

    {(yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 32:

    {(yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 33:

    {(yyval.node)=(yyvsp[(1) - (1)].node);;}
    break;

  case 34:

    {(yyval.node)=(yyvsp[(1) - (1)].node);;}
    break;

  case 35:

    {(yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 36:

    {(yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 37:

    {(yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 38:

    { (yyval.node) = NULL; ;}
    break;

  case 39:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 40:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 41:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 42:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 43:

    {
      ParseNode *relation_node = NULL;
      malloc_non_terminal_node(relation_node, result->malloc_pool_, T_RELATION, 2, NULL, (yyvsp[(1) - (3)].node));
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_STAR);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NAME_FIELD, 2, relation_node, node);
      //add liumz, [column_alias_double_quotes]20151015:b
      (yyval.node)->is_column_label = 1;
      //add:e
    ;}
    break;

  case 44:

    {
      ParseNode *relation_node = NULL;
      malloc_non_terminal_node(relation_node, result->malloc_pool_, T_RELATION, 2, (yyvsp[(1) - (5)].node), (yyvsp[(3) - (5)].node));
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_STAR);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NAME_FIELD, 2, relation_node, node);
      //add liumz, [column_alias_double_quotes]20151015:b
      (yyval.node)->is_column_label = 1;
      //add:e
    ;}
    break;

  case 45:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      //add liumz, [column_alias_double_quotes]20151015:b
      (yyval.node)->is_column_label = 1;
      //add:e
    ;}
    break;

  case 46:

    {
      ParseNode *relation_node = NULL;
      malloc_non_terminal_node(relation_node, result->malloc_pool_, T_RELATION, 2, NULL, (yyvsp[(1) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NAME_FIELD, 2, relation_node, (yyvsp[(3) - (3)].node));
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(3) - (3)]).first_column, (yylsp[(3) - (3)]).last_column);
      //add liumz, [column_alias_double_quotes]20151015:b
      (yyval.node)->is_column_label = 1;
      //add:e
    ;}
    break;

  case 47:

    {
      ParseNode *relation_node = NULL;
      malloc_non_terminal_node(relation_node, result->malloc_pool_, T_RELATION, 2, (yyvsp[(1) - (5)].node), (yyvsp[(3) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NAME_FIELD, 2, relation_node, (yyvsp[(5) - (5)].node));
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(5) - (5)]).first_column, (yylsp[(5) - (5)]).last_column);
      //add liumz, [column_alias_double_quotes]20151015:b
      (yyval.node)->is_column_label = 1;
      //add:e
    ;}
    break;

  case 48:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 49:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 50:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 51:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 52:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 53:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 54:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 55:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 56:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 57:

    { (yyvsp[(3) - (3)].node)->type_ = T_SYSTEM_VARIABLE; (yyval.node) = (yyvsp[(3) - (3)].node); ;}
    break;

  case 58:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 59:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 60:

    { (yyval.node) = (yyvsp[(2) - (3)].node); ;}
    break;

  case 61:

    {
      ParseNode *node = NULL;
      malloc_non_terminal_node(node, result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(2) - (5)].node), (yyvsp[(4) - (5)].node));
      merge_nodes((yyval.node), result->malloc_pool_, T_EXPR_LIST, node);
    ;}
    break;

  case 62:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      /*
      yyerror(&@1, result, "CASE expression is not supported yet!");
      YYABORT;
      */
    ;}
    break;

  case 63:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 64:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 65:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 66:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_EXISTS, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 67:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 68:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 69:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POS, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 70:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NEG, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 71:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_ADD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 72:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MINUS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 73:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MUL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 74:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_DIV, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 75:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_REM, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 76:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POW, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 77:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MOD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 78:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 79:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POS, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 80:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NEG, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 81:

    {
     malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_ADD, 2, (yyvsp[(1) - (3)].node),(yyvsp[(3) - (3)].node));
    ;}
    break;

  case 82:

    {
          ParseNode *default_operand = NULL;
          ParseNode *multi_result = NULL;
          ParseNode *func_name = NULL;
          malloc_terminal_node(default_operand, result->malloc_pool_, T_INT);
          malloc_terminal_node(func_name, result->malloc_pool_, T_IDENT);
          func_name->type_ = T_DATE_ADD;
          default_operand->value_ = 1;
          switch((yyvsp[(4) - (4)].node)->type_)
          {
            case T_INTERVAL_YEARS:
                  func_name->str_value_ = "date_add_year";
                  func_name->value_ = strlen(func_name->str_value_);
                break;
            case T_INTERVAL_MONTHS:
                  func_name->str_value_ = "date_add_month";
                  func_name->value_ = strlen(func_name->str_value_);
                break;
            case T_INTERVAL_DAYS:
                  default_operand->value_ = 86400000000;
                  func_name->str_value_ = "date_add_day";
                  func_name->value_ = strlen(func_name->str_value_);
                break;
            case T_INTERVAL_HOURS:
                  default_operand->value_ = 3600000000;
                  func_name->str_value_ = "date_add_hour";
                  func_name->value_ = strlen(func_name->str_value_);
                break;
            case T_INTERVAL_MINUTES:
                  default_operand->value_ = 60000000;
                  func_name->str_value_ = "date_add_minute";
                  func_name->value_ = strlen(func_name->str_value_);
                break;
            case T_INTERVAL_SECONDS:
                  default_operand->value_ = 1000000;
                  func_name->str_value_ = "date_add_second";
                  func_name->value_ = strlen(func_name->str_value_);
                break;
            case T_INTERVAL_MICROSECONDS:
                  default_operand->value_ = 1;
                  func_name->str_value_ = "date_add_microsecond";
                  func_name->value_ = strlen(func_name->str_value_);
                break;
            default:
               yyerror(&(yylsp[(1) - (4)]),  result,"type is not supported!");
               YYABORT;
               break;
          }
          malloc_non_terminal_node(multi_result, result->malloc_pool_, T_OP_MUL, 2, (yyvsp[(3) - (4)].node), default_operand);
          malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 3, func_name, (yyvsp[(1) - (4)].node), multi_result);
    ;}
    break;

  case 83:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MINUS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 84:

    {
          ParseNode *default_operand = NULL;
          ParseNode *multi_result = NULL;
          ParseNode *func_name = NULL;
          malloc_terminal_node(default_operand, result->malloc_pool_, T_INT);
          malloc_terminal_node(func_name, result->malloc_pool_, T_IDENT);
          func_name->type_ = T_DATE_SUB;
          default_operand->value_ = 1;
          switch((yyvsp[(4) - (4)].node)->type_)
          {
            case T_INTERVAL_YEARS:
                  func_name->str_value_ = "date_sub_year";
                  func_name->value_ = strlen(func_name->str_value_);
                break;
            case T_INTERVAL_MONTHS:
                  func_name->str_value_ = "date_sub_month";
                  func_name->value_ = strlen(func_name->str_value_);
                break;
            case T_INTERVAL_DAYS:
                  default_operand->value_ = 86400000000;
                  func_name->str_value_ = "date_sub_day";
                  func_name->value_ = strlen(func_name->str_value_);
                break;
            case T_INTERVAL_HOURS:
                  default_operand->value_ = 3600000000;
                  func_name->str_value_ = "date_sub_hour";
                  func_name->value_ = strlen(func_name->str_value_);
                break;
            case T_INTERVAL_MINUTES:
                  default_operand->value_ = 60000000;
                  func_name->str_value_ = "date_sub_minute";
                  func_name->value_ = strlen(func_name->str_value_);
                break;
            case T_INTERVAL_SECONDS:
                  default_operand->value_ = 1000000;
                  func_name->str_value_ = "date_sub_second";
                  func_name->value_ = strlen(func_name->str_value_);
                break;
            case T_INTERVAL_MICROSECONDS:
                  default_operand->value_ = 1;
                  func_name->str_value_ = "date_sub_microsecond";
                  func_name->value_ = strlen(func_name->str_value_);
                break;
            default:
               yyerror(&(yylsp[(1) - (4)]),  result,"type is not supported!");
               YYABORT;
               break;
          }
          malloc_non_terminal_node(multi_result, result->malloc_pool_, T_OP_MUL, 2, (yyvsp[(3) - (4)].node), default_operand);
          malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 3, func_name, (yyvsp[(1) - (4)].node), multi_result);
    ;}
    break;

  case 85:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MUL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 86:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_DIV, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 87:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_REM, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 88:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POW, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 89:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MOD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 90:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 91:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LT, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 92:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_EQ, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 93:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_GE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 94:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_GT, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 95:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 96:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LIKE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 97:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT_LIKE, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node)); ;}
    break;

  case 98:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_AND, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 99:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_OR, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 100:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 101:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 102:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS_NOT, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 103:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 104:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS_NOT, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 105:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 106:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS_NOT, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 107:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_BTW, 3, (yyvsp[(1) - (5)].node), (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    ;}
    break;

  case 108:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT_BTW, 3, (yyvsp[(1) - (6)].node), (yyvsp[(4) - (6)].node), (yyvsp[(6) - (6)].node));
    ;}
    break;

  case 109:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IN, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 110:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT_IN, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 111:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_CNN, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 112:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_CNN, 2, (yyvsp[(3) - (6)].node), (yyvsp[(5) - (6)].node));
    ;}
    break;

  case 113:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 114:

    { merge_nodes((yyval.node), result->malloc_pool_, T_EXPR_LIST, (yyvsp[(2) - (3)].node)); ;}
    break;

  case 115:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_WHEN_LIST, (yyvsp[(3) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CASE, 3, (yyvsp[(2) - (5)].node), (yyval.node), (yyvsp[(4) - (5)].node));
    ;}
    break;

  case 116:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 117:

    { (yyval.node) = NULL; ;}
    break;

  case 118:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 119:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node)); ;}
    break;

  case 120:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_WHEN, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 121:

    { (yyval.node) = (yyvsp[(2) - (2)].node); ;}
    break;

  case 122:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_NULL); ;}
    break;

  case 123:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_DATE_ADD);(yyval.node)->str_value_ = "adddate";(yyval.node)->value_ = strlen((yyval.node)->str_value_);;}
    break;

  case 124:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_DATE_ADD);(yyval.node)->str_value_ = "adddate";(yyval.node)->value_ = strlen((yyval.node)->str_value_);;}
    break;

  case 125:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_DATE_SUB);(yyval.node)->str_value_ = "subdate";(yyval.node)->value_ = strlen((yyval.node)->str_value_);;}
    break;

  case 126:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_DATE_SUB);(yyval.node)->str_value_ = "subdate";(yyval.node)->value_ = strlen((yyval.node)->str_value_);;}
    break;

  case 127:

    { malloc_terminal_node((yyval.node), result->malloc_pool_,T_INTERVAL_DAYS); ;}
    break;

  case 128:

    { malloc_terminal_node((yyval.node), result->malloc_pool_,T_INTERVAL_HOURS); ;}
    break;

  case 129:

    { malloc_terminal_node((yyval.node), result->malloc_pool_,T_INTERVAL_MINUTES); ;}
    break;

  case 130:

    { malloc_terminal_node((yyval.node), result->malloc_pool_,T_INTERVAL_MONTHS);;}
    break;

  case 131:

    {malloc_terminal_node((yyval.node), result->malloc_pool_,T_INTERVAL_SECONDS); ;}
    break;

  case 132:

    { malloc_terminal_node((yyval.node), result->malloc_pool_,T_INTERVAL_MICROSECONDS); ;}
    break;

  case 133:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_INTERVAL_YEARS);;}
    break;

  case 134:

    {
      if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "count") != 0)
      {
        yyerror(&(yylsp[(1) - (4)]), result, "Only COUNT function can be with '*' parameter!");
        YYABORT;
      }
      else
      {
        ParseNode* node = NULL;
        malloc_terminal_node(node, result->malloc_pool_, T_STAR);
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_COUNT, 1, node);
      }
    ;}
    break;

  case 135:

    {
      if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "count") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_COUNT, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "sum") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SUM, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "max") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MAX, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "min") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MIN, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "avg") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_AVG, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else
      {
        yyerror(&(yylsp[(1) - (5)]), result, "Wrong system function with 'DISTINCT/ALL'!");
        YYABORT;
      }
    ;}
    break;

  case 136:

    {
        if (strcasecmp((yyvsp[(1) - (9)].node)->str_value_, "listagg") != 0)
        {
          yyerror(&(yylsp[(1) - (9)]), result, "Only listagg function can be with this form!");
          YYABORT;
        }
        else
        {
            ParseNode *param_list = NULL;
            merge_nodes(param_list, result->malloc_pool_, T_EXPR_LIST, (yyvsp[(3) - (9)].node));
            malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_LISTAGG, 3, (yyvsp[(1) - (9)].node), param_list, (yyvsp[(8) - (9)].node));
        }
    ;}
    break;

  case 137:

    {
      if (strcasecmp((yyvsp[(1) - (8)].node)->str_value_, "row_number") != 0 && strcasecmp((yyvsp[(1) - (8)].node)->str_value_, "rownumber") != 0)
      {
        yyerror(&(yylsp[(1) - (8)]), result, "Only row_number() function can be with this form!");
        YYABORT;
      }
      else
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_ROW_NUMBER, 2, (yyvsp[(6) - (8)].node), (yyvsp[(7) - (8)].node));
      }
    ;}
    break;

  case 138:

    {
      if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "count") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "COUNT function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_COUNT, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "sum") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "SUM function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SUM, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "max") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "MAX function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MAX, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "min") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "MIN function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MIN, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "avg") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "AVG function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_AVG, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      /*add gaojt [new_agg_fun] 20140103:b*/
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "listagg") == 0)
      {
          ParseNode *param_list = NULL;
          merge_nodes(param_list, result->malloc_pool_, T_EXPR_LIST, (yyvsp[(3) - (4)].node));
          malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_LISTAGG, 2, NULL, param_list);
      }
      /* add 20140103:b */
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "trim") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "TRIM function syntax error! TRIM don't take %d params", (yyvsp[(3) - (4)].node)->num_child_);
          YYABORT;
        }
        else
        {
          ParseNode* default_type = NULL;
          malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
          default_type->value_ = 0;
          ParseNode* default_operand = NULL;
          malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
          default_operand->str_value_ = " "; /* blank for default */
          default_operand->value_ = strlen(default_operand->str_value_);
          ParseNode *params = NULL;
          malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (4)].node));
          malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (4)].node), params);
        }
      }
      /*add Yukun coalesce->value nvl*/
      else  if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "value") == 0 || strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "nvl") == 0 )
      {
        char *str = "coalesce";
        (yyvsp[(1) - (4)].node)->str_value_ = parse_strdup(str, result->malloc_pool_);
        (yyvsp[(1) - (4)].node)->value_ = strlen((yyvsp[(1) - (4)].node)->str_value_);

        ParseNode *params = NULL;
        merge_nodes(params, result->malloc_pool_, T_EXPR_LIST, (yyvsp[(3) - (4)].node));
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (4)].node), params);

      }
      /*end Yukun*/
      else  /* system function */
      {
        ParseNode *params = NULL;
        merge_nodes(params, result->malloc_pool_, T_EXPR_LIST, (yyvsp[(3) - (4)].node));
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (4)].node), params);
      }
    ;}
    break;

  case 139:

    {
       ParseNode *params = NULL;
       ParseNode *func_name = NULL;
       merge_nodes(params, result->malloc_pool_, T_EXPR_LIST, (yyvsp[(3) - (4)].node));
       malloc_terminal_node(func_name, result->malloc_pool_, T_IDENT);
       func_name->str_value_ = "days";
       func_name->value_ = strlen(func_name->str_value_);
       malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, func_name, params);
     ;}
    break;

  case 140:

    {
          ParseNode *default_operand = NULL;
          ParseNode *multi_result = NULL;
          malloc_terminal_node(default_operand, result->malloc_pool_, T_INT);
          default_operand->value_ = 1;
          switch((yyvsp[(7) - (8)].node)->type_)
          {
            case T_INTERVAL_YEARS:
                if((yyvsp[(1) - (8)].node)->type_ == T_DATE_ADD)
                {
                  (yyvsp[(1) - (8)].node)->str_value_ = "date_add_year";
                  (yyvsp[(1) - (8)].node)->value_ = strlen((yyvsp[(1) - (8)].node)->str_value_);
                }
                else if((yyvsp[(1) - (8)].node)->type_ == T_DATE_SUB)
                {
                  (yyvsp[(1) - (8)].node)->str_value_ = "date_sub_year";
                  (yyvsp[(1) - (8)].node)->value_ = strlen((yyvsp[(1) - (8)].node)->str_value_);
                }
                else
                {
                  yyerror(&(yylsp[(1) - (8)]),  result,"type is not supported!");
                  YYABORT;
                }
                break;
            case T_INTERVAL_MONTHS:
                if((yyvsp[(1) - (8)].node)->type_ == T_DATE_ADD)
                {
                  (yyvsp[(1) - (8)].node)->str_value_ = "date_add_month";
                  (yyvsp[(1) - (8)].node)->value_ = strlen((yyvsp[(1) - (8)].node)->str_value_);
                }
                else if((yyvsp[(1) - (8)].node)->type_ == T_DATE_SUB)
                {
                  (yyvsp[(1) - (8)].node)->str_value_ = "date_sub_month";
                  (yyvsp[(1) - (8)].node)->value_ = strlen((yyvsp[(1) - (8)].node)->str_value_);
                }
                else
                {
                  yyerror(&(yylsp[(1) - (8)]),  result,"type is not supported!");
                  YYABORT;
                }
                break;
            case T_INTERVAL_DAYS:
                if((yyvsp[(1) - (8)].node)->type_ == T_DATE_ADD)
                {
                  default_operand->value_ = 86400000000;
                  (yyvsp[(1) - (8)].node)->str_value_ = "date_add_day";
                  (yyvsp[(1) - (8)].node)->value_ = strlen((yyvsp[(1) - (8)].node)->str_value_);
                }
                else if((yyvsp[(1) - (8)].node)->type_ == T_DATE_SUB)
                {
                  default_operand->value_ = 86400000000;
                  (yyvsp[(1) - (8)].node)->str_value_ = "date_sub_day";
                  (yyvsp[(1) - (8)].node)->value_ = strlen((yyvsp[(1) - (8)].node)->str_value_);
                }
                else
                {
                  yyerror(&(yylsp[(1) - (8)]),  result,"type is not supported!");
                  YYABORT;
                }
                break;
            case T_INTERVAL_HOURS:
                if((yyvsp[(1) - (8)].node)->type_ == T_DATE_ADD)
                {
                  default_operand->value_ = 3600000000;
                  (yyvsp[(1) - (8)].node)->str_value_ = "date_add_hour";
                  (yyvsp[(1) - (8)].node)->value_ = strlen((yyvsp[(1) - (8)].node)->str_value_);
                }
                else if((yyvsp[(1) - (8)].node)->type_ == T_DATE_SUB)
                {
                  default_operand->value_ = 3600000000;
                  (yyvsp[(1) - (8)].node)->str_value_ = "date_sub_hour";
                  (yyvsp[(1) - (8)].node)->value_ = strlen((yyvsp[(1) - (8)].node)->str_value_);
                }
                else
                {
                  yyerror(&(yylsp[(1) - (8)]),  result,"type is not supported!");
                  YYABORT;
                }
                break;
            case T_INTERVAL_MINUTES:
                if((yyvsp[(1) - (8)].node)->type_ == T_DATE_ADD)
                {
                  default_operand->value_ = 60000000;
                  (yyvsp[(1) - (8)].node)->str_value_ = "date_add_minute";
                  (yyvsp[(1) - (8)].node)->value_ = strlen((yyvsp[(1) - (8)].node)->str_value_);
                }
                else if((yyvsp[(1) - (8)].node)->type_ == T_DATE_SUB)
                {
                  default_operand->value_ = 60000000;
                  (yyvsp[(1) - (8)].node)->str_value_ = "date_sub_minute";
                  (yyvsp[(1) - (8)].node)->value_ = strlen((yyvsp[(1) - (8)].node)->str_value_);
                }
                else
                {
                  yyerror(&(yylsp[(1) - (8)]),  result,"type is not supported!");
                  YYABORT;
                }
                break;
            case T_INTERVAL_SECONDS:
                if((yyvsp[(1) - (8)].node)->type_ == T_DATE_ADD)
                {
                  default_operand->value_ = 1000000;
                  (yyvsp[(1) - (8)].node)->str_value_ = "date_add_second";
                  (yyvsp[(1) - (8)].node)->value_ = strlen((yyvsp[(1) - (8)].node)->str_value_);
                }
                else if((yyvsp[(1) - (8)].node)->type_ == T_DATE_SUB)
                {
                  default_operand->value_ = 1000000;
                  (yyvsp[(1) - (8)].node)->str_value_ = "date_sub_second";
                  (yyvsp[(1) - (8)].node)->value_ = strlen((yyvsp[(1) - (8)].node)->str_value_);
                }
                else
                {
                  yyerror(&(yylsp[(1) - (8)]),  result,"type is not supported!");
                  YYABORT;
                }
                break;
            case T_INTERVAL_MICROSECONDS:
                if((yyvsp[(1) - (8)].node)->type_ == T_DATE_ADD)
                {
                  default_operand->value_ = 1;
                  (yyvsp[(1) - (8)].node)->str_value_ = "date_add_microsecond";
                  (yyvsp[(1) - (8)].node)->value_ = strlen((yyvsp[(1) - (8)].node)->str_value_);
                }
                else if((yyvsp[(1) - (8)].node)->type_ == T_DATE_SUB)
                {
                  default_operand->value_ = 1;
                  (yyvsp[(1) - (8)].node)->str_value_ = "date_sub_microsecond";
                  (yyvsp[(1) - (8)].node)->value_ = strlen((yyvsp[(1) - (8)].node)->str_value_);
                }
                else
                {
                  yyerror(&(yylsp[(1) - (8)]),  result,"type is not supported!");
                  YYABORT;
                }
                break;
            default:
               yyerror(&(yylsp[(1) - (8)]),  result,"type is not supported!");
               YYABORT;
               break;
          }
          malloc_non_terminal_node(multi_result, result->malloc_pool_, T_OP_MUL, 2, (yyvsp[(6) - (8)].node), default_operand);
          malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 3, (yyvsp[(1) - (8)].node), (yyvsp[(3) - (8)].node), multi_result);
      ;}
    break;

  case 141:

    {
      if (strcasecmp((yyvsp[(1) - (6)].node)->str_value_, "cast") == 0)
      {
        /*modify fanqiushi DECIMAL OceanBase_BankCommV0.2 2014_6_16:b*/
        /*$5->value_ = $5->type_;
        $5->type_ = T_INT;*/
    if((yyvsp[(5) - (6)].node)->type_!=T_TYPE_DECIMAL)
    {
      (yyvsp[(5) - (6)].node)->value_ = (yyvsp[(5) - (6)].node)->type_;
      (yyvsp[(5) - (6)].node)->type_ = T_INT;
    }
    /*modify:e*/
        ParseNode *params = NULL;
        malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, (yyvsp[(3) - (6)].node), (yyvsp[(5) - (6)].node));
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (6)].node), params);
      }
      else
      {
        yyerror(&(yylsp[(1) - (6)]), result, "AS support cast function only!");
        YYABORT;
      }
    ;}
    break;

  case 142:

    {
        char *str = "cast";
        ParseNode *pname = NULL;
        malloc_terminal_node(pname, result->malloc_pool_, T_IDENT);
        pname->str_value_ = parse_strdup(str, result->malloc_pool_);
        pname->value_ = strlen(pname->str_value_);

        ParseNode *decnode = NULL;
        malloc_non_terminal_node(decnode, result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(5) - (8)].node), (yyvsp[(7) - (8)].node));
        merge_nodes(decnode, result->malloc_pool_, T_TYPE_DECIMAL, decnode);

        ParseNode *params = NULL;
        malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, (yyvsp[(3) - (8)].node), decnode);
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, pname, params);
    ;}
    break;

  case 143:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (4)].node), (yyvsp[(3) - (4)].node));
    ;}
    break;

  case 144:

    {
      if (strcasecmp((yyvsp[(1) - (3)].node)->str_value_, "now") == 0
      //del liuzy [datetime func] 20151027:b
//        || strcasecmp($1->str_value_, "current_time") == 0
//        || strcasecmp($1->str_value_, "current_timestamp") == 0
      //del 20151027:e
      )
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CUR_TIME, 1, (yyvsp[(1) - (3)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (3)].node)->str_value_, "strict_current_timestamp") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CUR_TIME_UPS, 1, (yyvsp[(1) - (3)].node));
      }
      //add liuzy [datetime func] 20150901:b
      /*Exp: add system function "current_time()"*/
      else if (strcasecmp((yyvsp[(1) - (3)].node)->str_value_, "current_time") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CUR_TIME_HMS, 1, (yyvsp[(1) - (3)].node));
      }
      //add 20150901:e
      else
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 1, (yyvsp[(1) - (3)].node));
      }
      //yyerror(&@1, result, "system/user-define function is not supported yet!");
      //YYABORT;
    ;}
    break;

  case 145:

    {
      ParseNode* node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_IDENT);
      char *str = "current_timestamp";
      node->str_value_ = parse_strdup(str, result->malloc_pool_);
      node->value_ = strlen(node->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CUR_TIME, 1, node);
    ;}
    break;

  case 146:

    {
      ParseNode* node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_IDENT);
      char *str = "current_timestamp";
      node->str_value_ = parse_strdup(str, result->malloc_pool_);
      node->value_ = strlen(node->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CUR_TIME, 1, node);
    ;}
    break;

  case 147:

    {
      ParseNode* node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_IDENT);
      char *str = "current_timestamp";
      node->str_value_ = parse_strdup(str, result->malloc_pool_);
      node->value_ = strlen(node->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CUR_TIME, 1, node);
    ;}
    break;

  case 148:

    {
      ParseNode* node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_IDENT);
      char *str = "current_date";
      node->str_value_ = parse_strdup(str, result->malloc_pool_);
      node->value_ = strlen(node->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CUR_DATE, 1, node);
    ;}
    break;

  case 149:

    {
      ParseNode* node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_IDENT);
      char *str = "current_date";
      node->str_value_ = parse_strdup(str, result->malloc_pool_);
      node->value_ = strlen(node->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CUR_DATE, 1, node);
    ;}
    break;

  case 150:

    {
      ParseNode* node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_IDENT);
      char *str = "current_date";
      node->str_value_ = parse_strdup(str, result->malloc_pool_);
      node->value_ = strlen(node->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CUR_DATE, 1, node);
    ;}
    break;

  case 151:

    {
      ParseNode *params = NULL;
      merge_nodes(params, result->malloc_pool_, T_EXPR_LIST, (yyvsp[(3) - (4)].node));
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_FUN_REPLACE);
      (yyval.node)->str_value_ = "replace";
      (yyval.node)->value_ = strlen((yyval.node)->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyval.node), params);
    ;}
    break;

  case 152:

    {
      ParseNode* func = NULL;
      malloc_terminal_node(func, result->malloc_pool_, T_IDENT);
      char *str = "cast";
      func->str_value_ = parse_strdup(str, result->malloc_pool_);
      func->value_ = strlen(func->str_value_);
      ParseNode* data_type = NULL;
      malloc_terminal_node(data_type, result->malloc_pool_, T_INT);
      data_type->value_ = T_TYPE_DATE;
      ParseNode *params = NULL;
      malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, (yyvsp[(3) - (4)].node), data_type);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, func, params);
    ;}
    break;

  case 153:

    {
      ParseNode* func = NULL;
      malloc_terminal_node(func, result->malloc_pool_, T_IDENT);
      char *str = "cast";
      func->str_value_ = parse_strdup(str, result->malloc_pool_);
      func->value_ = strlen(func->str_value_);
      ParseNode* data_type = NULL;
      malloc_terminal_node(data_type, result->malloc_pool_, T_INT);
      data_type->value_ = T_TYPE_TIME;
      ParseNode *params = NULL;
      malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, (yyvsp[(3) - (4)].node), data_type);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, func, params);
    ;}
    break;

  case 154:

    {
      (yyval.node) = (yyvsp[(1) - (4)].node);
      (yyval.node)->children_[0] = (yyvsp[(3) - (4)].node);
    ;}
    break;

  case 155:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ROW_COUNT, 1, NULL);
    ;}
    break;

  case 160:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_ALL);
    ;}
    break;

  case 161:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_DISTINCT);
    ;}
    break;

  case 162:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_EXPR, 1, (yyvsp[(3) - (3)].node));
      (yyvsp[(3) - (3)].node)->value_ = 0;
    ;}
    break;

  case 163:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_EXPR, 1, (yyvsp[(3) - (3)].node));
      (yyvsp[(3) - (3)].node)->value_ = 1;
    ;}
    break;

  case 164:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_DELETE, 4, (yyvsp[(4) - (6)].node), (yyvsp[(5) - (6)].node), (yyvsp[(6) - (6)].node), (yyvsp[(2) - (6)].node));
    ;}
    break;

  case 165:

    {
      ParseNode* assign_list = NULL;
      merge_nodes(assign_list, result->malloc_pool_, T_ASSIGN_LIST, (yyvsp[(5) - (7)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_UPDATE, 5, (yyvsp[(3) - (7)].node), assign_list, (yyvsp[(6) - (7)].node), (yyvsp[(7) - (7)].node), (yyvsp[(2) - (7)].node));
    ;}
    break;

  case 166:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 167:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 168:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ASSIGN_ITEM, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 169:

    {
      ParseNode *table_elements = NULL;
      ParseNode *table_options = NULL;
      merge_nodes(table_elements, result->malloc_pool_, T_TABLE_ELEMENT_LIST, (yyvsp[(6) - (8)].node));
      merge_nodes(table_options, result->malloc_pool_, T_TABLE_OPTION_LIST, (yyvsp[(8) - (8)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CREATE_TABLE, 4,
              (yyvsp[(3) - (8)].node),                   /* if not exists */
              (yyvsp[(4) - (8)].node),                   /* table name */
              table_elements,       /* columns or primary key */
              table_options         /* table option(s) */
              );
    ;}
    break;

  case 170:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_IF_NOT_EXISTS); ;}
    break;

  case 171:

    { (yyval.node) = NULL; ;}
    break;

  case 172:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 173:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 174:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 175:

    {
      ParseNode* col_list= NULL;
      merge_nodes(col_list, result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(4) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PRIMARY_KEY, 1, col_list);
    ;}
    break;

  case 176:

    {
      ParseNode *attributes = NULL;
      merge_nodes(attributes, result->malloc_pool_, T_COLUMN_ATTRIBUTES, (yyvsp[(3) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_DEFINITION, 3, (yyvsp[(1) - (3)].node), (yyvsp[(2) - (3)].node), attributes);
    ;}
    break;

  case 177:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER ); ;}
    break;

  case 178:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER); ;}
    break;

  case 179:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER); ;}
    break;

  case 180:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER); ;}
    break;

  case 181:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_BIG_INTEGER); ;}
    break;

  case 182:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL);
      else
        merge_nodes((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL, (yyvsp[(2) - (2)].node));
      /*yyerror(&@1, result, "DECIMAL type is not supported");
      YYABORT;*/
    ;}
    break;

  case 183:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL);
      else
        merge_nodes((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL, (yyvsp[(2) - (2)].node));
      yyerror(&(yylsp[(1) - (2)]), result, "NUMERIC type is not supported");
      YYABORT;
    ;}
    break;

  case 184:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_BOOLEAN ); ;}
    break;

  case 185:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_FLOAT, 1, (yyvsp[(2) - (2)].node)); ;}
    break;

  case 186:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DOUBLE); ;}
    break;

  case 187:

    {
      (void)((yyvsp[(2) - (2)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DOUBLE);
    ;}
    break;

  case 188:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIMESTAMP);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIMESTAMP, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 189:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIMESTAMP); ;}
    break;

  case 190:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 191:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 192:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 193:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 194:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CREATETIME); ;}
    break;

  case 195:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_MODIFYTIME); ;}
    break;

  case 196:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DATE);
/*
 del peiouya [DATE_TIME] 20150906:b
      yyerror(&@1, result, "DATE type is not supported");
      YYABORT;
 del 20150906:e
*/
    ;}
    break;

  case 197:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIME);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIME, 1, (yyvsp[(2) - (2)].node));
 /*
 del peiouya [DATE_TIME] 20150906:b
        yyerror(&@1, result, "TIME type is not supported");
        YYABORT;
 del 20150906:e
 */
    ;}
    break;

  case 198:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(2) - (5)].node), (yyvsp[(4) - (5)].node)); ;}
    break;

  case 199:

    { (yyval.node) = (yyvsp[(2) - (3)].node); ;}
    break;

  case 200:

    { (yyval.node) = NULL; ;}
    break;

  case 201:

    { (yyval.node) = (yyvsp[(2) - (3)].node); ;}
    break;

  case 202:

    { (yyval.node) = NULL; ;}
    break;

  case 203:

    { (yyval.node) = NULL; ;}
    break;

  case 204:

    { (yyval.node) = NULL; ;}
    break;

  case 205:

    { (yyval.node) = (yyvsp[(2) - (3)].node); ;}
    break;

  case 206:

    { (yyval.node) = NULL; ;}
    break;

  case 207:

    { (yyval.node) = (yyvsp[(2) - (3)].node); ;}
    break;

  case 208:

    { (yyval.node) = NULL; ;}
    break;

  case 209:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node)); ;}
    break;

  case 210:

    { (yyval.node) = NULL; ;}
    break;

  case 211:

    {
      (void)((yyvsp[(2) - (2)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NOT_NULL);
    ;}
    break;

  case 212:

    {
      (void)((yyvsp[(1) - (1)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NULL);
    ;}
    break;

  case 213:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_DEFAULT, 1, (yyvsp[(2) - (2)].node)); ;}
    break;

  case 214:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_AUTO_INCREMENT); ;}
    break;

  case 215:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_PRIMARY_KEY); ;}
    break;

  case 216:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 217:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 218:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 219:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_INFO, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 220:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPIRE_INFO, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 221:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLET_MAX_SIZE, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 222:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLET_BLOCK_SIZE, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 223:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLET_ID, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 224:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_REPLICA_NUM, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 225:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COMPRESS_METHOD, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 226:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_USE_BLOOM_FILTER, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 227:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSISTENT_MODE);
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 228:

    {
      (void)((yyvsp[(2) - (3)].node)); /*  make bison mute*/
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COMMENT, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 229:

    { (yyval.node) = NULL; ;}
    break;

  case 230:

    { (yyval.node) = NULL; ;}
    break;

  case 231:

    {


		malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CREATE_INDEX, 4,
															(yyvsp[(5) - (7)].node), /*table name*/
															(yyvsp[(6) - (7)].node), /*column name*/
															(yyvsp[(3) - (7)].node),
															(yyvsp[(7) - (7)].node) /*storing attr*/
														);
	;}
    break;

  case 232:

    {
			(yyval.node)=(yyvsp[(1) - (1)].node);
		;}
    break;

  case 233:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(2) - (3)].node));
    ;}
    break;

  case 234:

    {
      (yyval.node)=(yyvsp[(2) - (2)].node);
    ;}
    break;

  case 235:

    {
      (yyval.node)=NULL;
    ;}
    break;

  case 236:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(2) - (3)].node));
    ;}
    break;

  case 237:

    {
		ParseNode *index =NULL;
		merge_nodes(index, result->malloc_pool_, T_TABLE_LIST, (yyvsp[(3) - (5)].node));
		malloc_non_terminal_node((yyval.node),result->malloc_pool_,T_DROP_INDEX,2,(yyvsp[(5) - (5)].node),index);
	;}
    break;

  case 238:

    {
		(yyval.node) = (yyvsp[(1) - (1)].node);
	;}
    break;

  case 239:

    {
			malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
		;}
    break;

  case 240:

    {
			(yyval.node) = (yyvsp[(1) - (1)].node);
		;}
    break;

  case 241:

    {
      ParseNode *tables = NULL;
      merge_nodes(tables, result->malloc_pool_, T_TABLE_LIST, (yyvsp[(4) - (4)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_DROP_TABLE, 2, (yyvsp[(3) - (4)].node), tables);
    ;}
    break;

  case 242:

    { (yyval.node) = NULL; ;}
    break;

  case 243:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_IF_EXISTS); ;}
    break;

  case 244:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 245:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 246:

    {
      ParseNode *tables = NULL;
      merge_nodes(tables, result->malloc_pool_, T_TABLE_LIST, (yyvsp[(4) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TRUNCATE_TABLE, 3, (yyvsp[(3) - (5)].node), tables, (yyvsp[(5) - (5)].node));
    ;}
    break;

  case 247:

    {
      ParseNode* val_list = NULL;
      merge_nodes(val_list, result->malloc_pool_, T_VALUE_LIST, (yyvsp[(6) - (7)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_INSERT, 6,
                              (yyvsp[(3) - (7)].node),           /* target relation */
                              (yyvsp[(4) - (7)].node),           /* column list */
                              val_list,     /* value list */
                              NULL,         /* value from sub-query */
                              (yyvsp[(1) - (7)].node),           /* is replacement */
                              (yyvsp[(7) - (7)].node)            /* when expression */
                              );
    ;}
    break;

  case 248:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_INSERT, 6,
                              (yyvsp[(3) - (4)].node),           /* target relation */
                              NULL,         /* column list */
                              NULL,         /* value list */
                              (yyvsp[(4) - (4)].node),           /* value from sub-query */
                              (yyvsp[(1) - (4)].node),           /* is replacement */
                              NULL          /* when expression */
                              );
    ;}
    break;

  case 249:

    {
      /* if opt_when is really needed, use select_with_parens instead */
      ParseNode* col_list = NULL;
      merge_nodes(col_list, result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(5) - (7)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_INSERT, 6,
                              (yyvsp[(3) - (7)].node),           /* target relation */
                              col_list,     /* column list */
                              NULL,         /* value list */
                              (yyvsp[(7) - (7)].node),           /* value from sub-query */
                              (yyvsp[(1) - (7)].node),           /* is replacement */
                              NULL          /* when expression */
                              );
    ;}
    break;

  case 250:

    { (yyval.node) = NULL; ;}
    break;

  case 251:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    ;}
    break;

  case 252:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 253:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    ;}
    break;

  case 254:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(2) - (3)].node));
    ;}
    break;

  case 255:

    { (yyval.node) = NULL; ;}
    break;

  case 256:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 257:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 258:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_VALUE_VECTOR, (yyvsp[(2) - (3)].node));
    ;}
    break;

  case 259:

    {
    merge_nodes((yyvsp[(4) - (5)].node), result->malloc_pool_, T_VALUE_VECTOR, (yyvsp[(4) - (5)].node));
    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (5)].node), (yyvsp[(4) - (5)].node));
  ;}
    break;

  case 260:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 261:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 262:

    {
      (yyval.node) = (yyvsp[(1) - (2)].node);
      (yyval.node)->children_[14] = (yyvsp[(2) - (2)].node);
      if ((yyval.node)->children_[12] == NULL && (yyvsp[(2) - (2)].node) != NULL)
      {
        malloc_terminal_node((yyval.node)->children_[12], result->malloc_pool_, T_BOOL);
        (yyval.node)->children_[12]->value_ = 1;
      }
    ;}
    break;

  case 263:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 264:

    { (yyval.node) = (yyvsp[(2) - (3)].node); ;}
    break;

  case 265:

    { (yyval.node) = (yyvsp[(2) - (3)].node); ;}
    break;

  case 266:

    {
      (yyval.node)= (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 267:

    {
      (yyval.node) = (yyvsp[(1) - (2)].node);
      (yyval.node)->children_[12] = (yyvsp[(2) - (2)].node);
    ;}
    break;

  case 268:

    {
      /* use the new order by to replace old one */
      ParseNode* select = (ParseNode*)(yyvsp[(1) - (3)].node);
      if (select->children_[10])
        destroy_tree(select->children_[10]);
      select->children_[10] = (yyvsp[(2) - (3)].node);
      select->children_[12] = (yyvsp[(3) - (3)].node);
      (yyval.node) = select;
    ;}
    break;

  case 269:

    {
      /* use the new order by to replace old one */
      ParseNode* select = (ParseNode*)(yyvsp[(1) - (4)].node);
      if ((yyvsp[(2) - (4)].node))
      {
        if (select->children_[10])
          destroy_tree(select->children_[10]);
        select->children_[10] = (yyvsp[(2) - (4)].node);
      }

      /* set limit value */
      if (select->children_[11])
        destroy_tree(select->children_[11]);
      select->children_[11] = (yyvsp[(3) - (4)].node);
      select->children_[12] = (yyvsp[(4) - (4)].node);
      (yyval.node) = select;
    ;}
    break;

  case 270:

    {
      ParseNode* project_list = NULL;
      merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, (yyvsp[(4) - (5)].node));
      /*mod tianz [EXPORT_TOOL] 20141120:b*/
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 16,
                              (yyvsp[(3) - (5)].node),             /* 1. distinct */
                              project_list,   /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              (yyvsp[(5) - (5)].node),             /* 12. limit */
                              NULL,           /* 13. for update */
                              (yyvsp[(2) - (5)].node),             /* 14 hints */
                              NULL            /* 15 when clause */
                              ,NULL
                              );
     /*mod 20141120:e*/
    ;}
    break;

  case 271:

    {
      ParseNode* project_list = NULL;
      merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, (yyvsp[(4) - (8)].node));
      /*mod tianz [EXPORT_TOOL] 20141120:b*/
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 16,
                              (yyvsp[(3) - (8)].node),             /* 1. distinct */
                              project_list,   /* 2. select clause */
                              NULL,           /* 3. from clause */
                              (yyvsp[(7) - (8)].node),             /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              (yyvsp[(8) - (8)].node),             /* 12. limit */
                              NULL,           /* 13. for update */
                              (yyvsp[(2) - (8)].node),             /* 14 hints */
                              NULL            /* 15 when clause */
                              ,NULL
                              );
     /*mod 20141120:e*/
    ;}
    break;

  case 272:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 273:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 274:

    {
      ParseNode* project_list = NULL;
      ParseNode* from_list = NULL;
      merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, (yyvsp[(4) - (10)].node));
      merge_nodes(from_list, result->malloc_pool_, T_FROM_LIST, (yyvsp[(6) - (10)].node));
      /*mod tianz [EXPORT_TOOL] 20141120:b*/
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 16,
                              (yyvsp[(3) - (10)].node),             /* 1. distinct */
                              project_list,   /* 2. select clause */
                              from_list,      /* 3. from clause */
                              (yyvsp[(7) - (10)].node),             /* 4. where */
                              (yyvsp[(8) - (10)].node),             /* 5. group by */
                              (yyvsp[(9) - (10)].node),             /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              (yyvsp[(2) - (10)].node),             /* 14 hints */
                              NULL            /* 15 when clause */
                              ,(yyvsp[(10) - (10)].node)
                              );
    /*mod 20141120:e*/
    ;}
    break;

  case 275:

    {
      ParseNode* set_op = NULL;
      malloc_terminal_node(set_op, result->malloc_pool_, T_SET_UNION);
      /*mod tianz [EXPORT_TOOL] 20141120:b*/
            malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 16,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              (yyvsp[(3) - (4)].node),             /* 8. all specified? */
                              (yyvsp[(1) - (4)].node),             /* 9. former select stmt */
                              (yyvsp[(4) - (4)].node),             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              NULL,           /* 14 hints */
                              NULL            /* 15 when clause */
                              ,NULL
                              );
     /*mod 20141120:e*/
    ;}
    break;

  case 276:

    {
      ParseNode* set_op = NULL;
      malloc_terminal_node(set_op, result->malloc_pool_, T_SET_INTERSECT);
      /*mod tianz [EXPORT_TOOL] 20141120:b*/
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 16,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              (yyvsp[(3) - (4)].node),             /* 8. all specified? */
                              (yyvsp[(1) - (4)].node),             /* 9. former select stmt */
                              (yyvsp[(4) - (4)].node),             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              NULL,           /* 14 hints */
                              NULL            /* 15 when clause */
                              ,NULL
                              );
    /*mod 20141120:e*/
    ;}
    break;

  case 277:

    {
      ParseNode* set_op = NULL;
      malloc_terminal_node(set_op, result->malloc_pool_, T_SET_EXCEPT);
      /*mod tianz [EXPORT_TOOL] 20141120:b*/
            malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 16,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              (yyvsp[(3) - (4)].node),             /* 8. all specified? */
                              (yyvsp[(1) - (4)].node),             /* 9. former select stmt */
                              (yyvsp[(4) - (4)].node),             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              NULL,           /* 14 hints */
                              NULL            /* 15 when clause */
                              ,NULL
                              );
      /*mod 20141120:e*/
    ;}
    break;

  case 278:

    {(yyval.node) = NULL;;}
    break;

  case 279:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    ;}
    break;

  case 280:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_WHERE_CLAUSE, 2, (yyvsp[(3) - (3)].node), (yyvsp[(2) - (3)].node));
    ;}
    break;

  case 281:

    {(yyval.node) = NULL;;}
    break;

  case 282:

    {
    merge_nodes((yyvsp[(4) - (10)].node), result->malloc_pool_, T_VALUE_VECTOR, (yyvsp[(4) - (10)].node));
    merge_nodes((yyvsp[(8) - (10)].node), result->malloc_pool_, T_VALUE_VECTOR, (yyvsp[(8) - (10)].node));
    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_RANGE, 2, (yyvsp[(4) - (10)].node), (yyvsp[(8) - (10)].node));

  ;}
    break;

  case 283:

    {
    merge_nodes((yyvsp[(5) - (7)].node), result->malloc_pool_, T_VALUE_VECTOR, (yyvsp[(5) - (7)].node));
    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_RANGE, 2, NULL, (yyvsp[(5) - (7)].node));
  ;}
    break;

  case 284:

    {
    merge_nodes((yyvsp[(4) - (7)].node), result->malloc_pool_, T_VALUE_VECTOR, (yyvsp[(4) - (7)].node));
    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_RANGE, 2, (yyvsp[(4) - (7)].node), NULL);
  ;}
    break;

  case 285:

    {
    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_RANGE, 2, NULL, NULL);
  ;}
    break;

  case 286:

    {
      if ((yyvsp[(2) - (4)].node)->type_ == T_QUESTIONMARK && (yyvsp[(4) - (4)].node)->type_ == T_QUESTIONMARK)
      {
        (yyvsp[(4) - (4)].node)->value_++;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 287:

    {
      if ((yyvsp[(2) - (4)].node)->type_ == T_QUESTIONMARK && (yyvsp[(4) - (4)].node)->type_ == T_QUESTIONMARK)
      {
        (yyvsp[(4) - (4)].node)->value_++;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(4) - (4)].node), (yyvsp[(2) - (4)].node));
    ;}
    break;

  case 288:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(2) - (2)].node), NULL);
    ;}
    break;

  case 289:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, NULL, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 290:

    {
      if ((yyvsp[(2) - (4)].node)->type_ == T_QUESTIONMARK && (yyvsp[(4) - (4)].node)->type_ == T_QUESTIONMARK)
      {
        (yyvsp[(4) - (4)].node)->value_++;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(4) - (4)].node), (yyvsp[(2) - (4)].node));
    ;}
    break;

  case 291:

    {
      ParseNode *default_limit_value = NULL;
      malloc_terminal_node(default_limit_value, result->malloc_pool_, T_INT);
      default_limit_value->value_ = 1;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, default_limit_value, NULL);
    ;}
    break;

  case 292:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(3) - (5)].node), NULL);
    ;}
    break;

  case 293:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 294:

    {
      if ((yyvsp[(2) - (3)].node))
      {
        merge_nodes((yyval.node), result->malloc_pool_, T_HINT_OPTION_LIST, (yyvsp[(2) - (3)].node));
      }
      else
      {
        (yyval.node) = NULL;
      }
    ;}
    break;

  case 295:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 296:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 297:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 298:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 299:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 300:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_READ_STATIC);
    ;}
    break;

  case 301:

    {
    malloc_terminal_node((yyval.node), result->malloc_pool_, T_NOT_USE_INDEX);
  ;}
    break;

  case 302:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_HOTSPOT);
    ;}
    break;

  case 303:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_READ_CONSISTENCY);
      (yyval.node)->value_ = (yyvsp[(3) - (4)].ival);
    ;}
    break;

  case 304:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_USE_INDEX, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
    ;}
    break;

  case 305:

    {
    (void)((yyvsp[(1) - (4)].node));
    (void)((yyvsp[(3) - (4)].node));
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_UNKOWN_HINT);
    ;}
    break;

  case 306:

    {
      (yyval.node) = (yyvsp[(2) - (3)].node);
    ;}
    break;

  case 307:

    {
      merge_nodes((yyval.node), result->malloc_pool_,T_JOIN_LIST,(yyvsp[(3) - (4)].node));
    ;}
    break;

  case 308:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_I_MUTLI_BATCH);
    ;}
    break;

  case 309:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_UD_MUTLI_BATCH);
    ;}
    break;

  case 310:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_UD_ALL_ROWKEY);
    ;}
    break;

  case 311:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_UD_NOT_PARALLAL);
    ;}
    break;

  case 312:

    {
  malloc_terminal_node((yyval.node), result->malloc_pool_, T_CHANGE_VALUE_SIZE);
  ;}
    break;

  case 313:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 314:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 315:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BLOOMFILTER_JOIN);

    ;}
    break;

  case 316:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_MERGE_JOIN);
    ;}
    break;

  case 317:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_SEMI_JOIN);
    ;}
    break;

  case 318:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_SEMI_BTW_JOIN);
    ;}
    break;

  case 319:

    {
      (yyval.node) = (yyvsp[(1) - (2)].node);
    ;}
    break;

  case 320:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 321:

    {
    (yyval.ival) = 3;
  ;}
    break;

  case 322:

    {
    (yyval.ival) = 4;
  ;}
    break;

  case 323:

    {
    (yyval.ival) = 1;
  ;}
    break;

  case 324:

    {
    (yyval.ival) = 2;
  ;}
    break;

  case 325:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 326:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 327:

    { (yyval.node) = NULL; ;}
    break;

  case 328:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 329:

    { (yyval.node) = NULL; ;}
    break;

  case 330:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 331:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 0;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 332:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 0;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 333:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 1;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 334:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 2;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 335:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 0;
      ParseNode *default_operand = NULL;
      malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
      default_operand->str_value_ = " "; /* blank for default */
      default_operand->value_ = strlen(default_operand->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 336:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 1;
      ParseNode *default_operand = NULL;
      malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
      default_operand->str_value_ = " "; /* blank for default */
      default_operand->value_ = strlen(default_operand->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 337:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 2;
      ParseNode *default_operand = NULL;
      malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
      default_operand->str_value_ = " "; /* blank for default */
      default_operand->value_ = strlen(default_operand->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 338:

    { (yyval.node) = NULL; ;}
    break;

  case 339:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_EXPR_LIST, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 340:

    { (yyval.node) = NULL; ;}
    break;

  case 341:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_EXPR_LIST, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 342:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 343:

    { (yyval.node) = NULL; ;}
    break;

  case 344:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_SORT_LIST, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 345:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 346:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 347:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SORT_KEY, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 348:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SORT_ASC); ;}
    break;

  case 349:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SORT_ASC); ;}
    break;

  case 350:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SORT_DESC); ;}
    break;

  case 351:

    { (yyval.node) = 0; ;}
    break;

  case 352:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    ;}
    break;

  case 353:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 354:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_ALL);
    ;}
    break;

  case 355:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_DISTINCT);
    ;}
    break;

  case 356:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, (yyvsp[(1) - (1)].node));
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (1)]).first_column, (yylsp[(1) - (1)]).last_column);
      //add liumz, [column_alias_double_quotes]20151015:b
      if (strncasecmp((yyval.node)->str_value_, "\"", 1) == 0 && (yyvsp[(1) - (1)].node)->is_column_label)
      {
        dup_expr_string_no_quotes((yyval.node)->str_value_, result, (yylsp[(1) - (1)]).first_column, (yylsp[(1) - (1)]).last_column);
      }
      //mod:e
    ;}
    break;

  case 357:

    {
      ParseNode* alias_node = NULL;
      malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, alias_node);
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (2)]).first_column, (yylsp[(1) - (2)]).last_column);
      dup_expr_string(alias_node->str_value_, result, (yylsp[(2) - (2)]).first_column, (yylsp[(2) - (2)]).last_column);
      //add liumz, [column_alias_double_quotes]20151015:b
      if (strncasecmp(alias_node->str_value_, "\"", 1) == 0)
      {
        dup_expr_string_no_quotes(alias_node->str_value_, result, (yylsp[(2) - (2)]).first_column, (yylsp[(2) - (2)]).last_column);
      }
      //mod:e
    ;}
    break;

  case 358:

    {
      ParseNode* alias_node = NULL;
      malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, alias_node);
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (3)]).first_column, (yylsp[(1) - (3)]).last_column);
      dup_expr_string(alias_node->str_value_, result, (yylsp[(3) - (3)]).first_column, (yylsp[(3) - (3)]).last_column);
      //add liumz, [column_alias_double_quotes]20151015:b
      if (strncasecmp(alias_node->str_value_, "\"", 1) == 0)
      {
        dup_expr_string_no_quotes(alias_node->str_value_, result, (yylsp[(3) - (3)]).first_column, (yylsp[(3) - (3)]).last_column);
      }
      //mod:e
    ;}
    break;

  case 359:

    {
      ParseNode* star_node = NULL;
      malloc_terminal_node(star_node, result->malloc_pool_, T_STAR);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, star_node);
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (1)]).first_column, (yylsp[(1) - (1)]).last_column);
    ;}
    break;

  case 360:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 361:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 362:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 363:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 364:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 365:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 366:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 367:

    {
      ParseNode* node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_IDENT);
      char str[64] = {'\0'};
      random_uuid(str);
      node->str_value_ = parse_strdup(str, result->malloc_pool_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (1)].node), node);
    ;}
    break;

  case 368:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 369:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(2) - (5)].node), (yyvsp[(5) - (5)].node));
      yyerror(&(yylsp[(1) - (5)]), result, "qualied joined table can not be aliased!");
      YYABORT;
    ;}
    break;

  case 370:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 371:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 372:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 373:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_RELATION, 2, NULL, (yyvsp[(1) - (1)].node));
    ;}
    break;

  case 374:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_RELATION, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 375:

    {
      (yyval.node) = (yyvsp[(2) - (3)].node);
    ;}
    break;

  case 376:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_JOINED_TABLE, 4, (yyvsp[(2) - (6)].node), (yyvsp[(1) - (6)].node), (yyvsp[(4) - (6)].node), (yyvsp[(6) - (6)].node));
    ;}
    break;

  case 377:

    {
      ParseNode* node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_JOIN_INNER);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_JOINED_TABLE, 4, node, (yyvsp[(1) - (5)].node), (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    ;}
    break;

  case 378:

    {
      /* make bison mute */
      (void)((yyvsp[(2) - (2)].node));
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_FULL);
    ;}
    break;

  case 379:

    {
      /* make bison mute */
      (void)((yyvsp[(2) - (2)].node));
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_LEFT);
    ;}
    break;

  case 380:

    {
      /* make bison mute */
      (void)((yyvsp[(2) - (2)].node));
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_RIGHT);
    ;}
    break;

  case 381:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_INNER);
    ;}
    break;

  case 382:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_SEMI);
    ;}
    break;

  case 383:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_SEMI_LEFT);
    ;}
    break;

  case 384:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_SEMI_RIGHT);
    ;}
    break;

  case 385:

    { (yyval.node) = NULL; ;}
    break;

  case 386:

    { (yyval.node) = NULL; ;}
    break;

  case 387:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPLAIN, 1, (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = ((yyvsp[(2) - (3)].node) ? 1 : 0); /* positive: verbose */
    ;}
    break;

  case 388:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 389:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 390:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 391:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 392:

    { (yyval.node) = (ParseNode*)1; ;}
    break;

  case 393:

    { (yyval.node) = NULL; ;}
    break;

  case 394:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_TABLES, 1, (yyvsp[(3) - (3)].node)); ;}
    break;

  case 395:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_INDEX, 2, (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node)); ;}
    break;

  case 396:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node)); ;}
    break;

  case 397:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node)); ;}
    break;

  case 398:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_TABLE_STATUS, 1, (yyvsp[(4) - (4)].node)); ;}
    break;

  case 399:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_SERVER_STATUS, 1, (yyvsp[(4) - (4)].node)); ;}
    break;

  case 400:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_VARIABLES, 1, (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = (yyvsp[(2) - (4)].ival);
    ;}
    break;

  case 401:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_SCHEMA); ;}
    break;

  case 402:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_CREATE_TABLE, 1, (yyvsp[(4) - (4)].node)); ;}
    break;

  case 403:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 404:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 405:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_WARNINGS, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 406:

    {
      if ((yyvsp[(2) - (3)].node)->type_ != T_FUN_COUNT)
      {
        yyerror(&(yylsp[(1) - (3)]), result, "Only COUNT(*) function is supported in SHOW WARNINGS statement!");
        YYABORT;
      }
      else
      {
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_WARNINGS);
        (yyval.node)->value_ = 1;
      }
    ;}
    break;

  case 407:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_GRANTS, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 408:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_PARAMETERS, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 409:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_PROCESSLIST);
      (yyval.node)->value_ = (yyvsp[(2) - (3)].ival);
    ;}
    break;

  case 410:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_DATABASES);
    ;}
    break;

  case 411:

    {
    malloc_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_CURRENT_DATABASE);
  ;}
    break;

  case 412:

    {
    malloc_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_SYSTEM_TABLES);
  ;}
    break;

  case 413:

    {
      if ((yyvsp[(2) - (4)].node)->value_ < 0 || (yyvsp[(4) - (4)].node)->value_ < 0)
      {
        yyerror(&(yylsp[(1) - (4)]), result, "OFFSET/COUNT must not be less than 0!");
        YYABORT;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_LIMIT, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 414:

    {
      if ((yyvsp[(2) - (2)].node)->value_ < 0)
      {
        yyerror(&(yylsp[(1) - (2)]), result, "COUNT must not be less than 0!");
        YYABORT;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_LIMIT, 2, NULL, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 415:

    { (yyval.node) = NULL; ;}
    break;

  case 416:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 417:

    { (yyval.node) = NULL; ;}
    break;

  case 418:

    { (yyval.node) = NULL; ;}
    break;

  case 419:

    { (yyval.ival) = 1; ;}
    break;

  case 420:

    { (yyval.ival) = 0; ;}
    break;

  case 421:

    { (yyval.ival) = 0; ;}
    break;

  case 422:

    { (yyval.node) = NULL; ;}
    break;

  case 423:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LIKE, 1, (yyvsp[(2) - (2)].node)); ;}
    break;

  case 424:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_WHERE_CLAUSE, 1, (yyvsp[(2) - (2)].node)); ;}
    break;

  case 425:

    { (yyval.node) = NULL; ;}
    break;

  case 426:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LIKE, 1, (yyvsp[(1) - (1)].node)); ;}
    break;

  case 427:

    { (yyval.ival) = 0; ;}
    break;

  case 428:

    { (yyval.ival) = 1; ;}
    break;

  case 429:

    {
        merge_nodes((yyval.node), result->malloc_pool_, T_CREATE_USER, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 430:

    {
        (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 431:

    {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 432:

    {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CREATE_USER_SPEC, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 433:

    {
        (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 434:

    {
        (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 435:

    {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CREATE_DATABASE, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 436:

    {
        (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 437:

    {
    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_USE_DATABASE, 1, (yyvsp[(3) - (3)].node));
  ;}
    break;

  case 438:

    {
		(yyval.node) = (yyvsp[(1) - (1)].node);
	;}
    break;

  case 439:

    {
    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_DROP_DATABASE, 1, (yyvsp[(3) - (3)].node));
  ;}
    break;

  case 440:

    {
		(yyval.node) = (yyvsp[(1) - (1)].node);
	;}
    break;

  case 441:

    {
        merge_nodes((yyval.node), result->malloc_pool_, T_DROP_USER, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 442:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 443:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 444:

    {
      ParseNode* sequence_clause_list = NULL;
      merge_nodes(sequence_clause_list, result->malloc_pool_, T_SEQUENCE_LABEL_LIST, (yyvsp[(5) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_CREATE, 3,
                               (yyvsp[(2) - (5)].node), /*opt_or_replace*/
                               (yyvsp[(4) - (5)].node), /*sequence_name*/
                               sequence_clause_list); /*sequence_label_list*/
    ;}
    break;

  case 445:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_CREATE, 3,
                               (yyvsp[(2) - (4)].node), /*opt_or_replace*/
                               (yyvsp[(4) - (4)].node), /*sequence_name*/
                               NULL);/*sequence_label_list*/
    ;}
    break;

  case 446:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 447:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 448:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 449:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 450:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 451:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 452:

    {
      (yyval.node) =(yyvsp[(1) - (1)].node);
    ;}
    break;

  case 453:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 454:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 455:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 456:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 457:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_REPLACE);
    ;}
    break;

  case 458:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 459:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 460:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    ;}
    break;

  case 461:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_START_WITH, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 462:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_INCREMENT_BY, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 463:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_MINVALUE, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 464:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_MINVALUE);
    ;}
    break;

  case 465:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_MAXVALUE, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 466:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_MAXVALUE);
    ;}
    break;

  case 467:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_CYCLE);
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 468:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_CYCLE);
      (yyval.node)->value_ = 0;
    ;}
    break;

  case 469:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_CACHE, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 470:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_CACHE);
    ;}
    break;

  case 471:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_ORDER);
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 472:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_ORDER);
      (yyval.node)->value_ = 0;
    ;}
    break;

  case 473:

    {
       malloc_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_QUICK);
       (yyval.node)->value_ = 1;
    ;}
    break;

  case 474:

    {
       malloc_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_QUICK);
       (yyval.node)->value_ = 0;
    ;}
    break;

  case 475:

    {
       malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_DROP, 1, (yyvsp[(3) - (3)].node));
     ;}
    break;

  case 476:

    {
      ParseNode* sequence_clause_list = NULL;
      merge_nodes(sequence_clause_list, result->malloc_pool_, T_SEQUENCE_LABEL_LIST, (yyvsp[(4) - (4)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_ALTER, 2,
                               (yyvsp[(3) - (4)].node), /*sequence_name*/
                               sequence_clause_list); /*sequence_label_list_alter*/
    ;}
    break;

  case 477:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 478:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 479:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 480:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 481:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 482:

    {
      (yyval.node) =(yyvsp[(1) - (1)].node);
    ;}
    break;

  case 483:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 484:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 485:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 486:

    {
    (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 487:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_RESTART_WITH);
    ;}
    break;

  case 488:

    {
    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SEQUENCE_RESTART_WITH, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 489:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SET_PASSWORD, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    ;}
    break;

  case 490:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SET_PASSWORD, 2, (yyvsp[(3) - (6)].node), (yyvsp[(6) - (6)].node));
    ;}
    break;

  case 491:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    ;}
    break;

  case 492:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 493:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_RENAME_USER, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 494:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_RENAME_INFO, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 495:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 496:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 497:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LOCK_USER, 2, (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 498:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 499:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    ;}
    break;

  case 500:

    {
      (void)(yyval.node);
    ;}
    break;

  case 501:

    {
      (void)(yyval.node);
    ;}
    break;

  case 502:

    {
      (yyval.ival) = 1;
    ;}
    break;

  case 503:

    {
      (yyval.ival) = 0;
    ;}
    break;

  case 504:

    {
      (void)(yyvsp[(2) - (2)].node);
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BEGIN);
      (yyval.node)->value_ = 0;
    ;}
    break;

  case 505:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BEGIN);
      (yyval.node)->value_ = (yyvsp[(3) - (3)].ival);
    ;}
    break;

  case 506:

    {
      (void)(yyvsp[(2) - (2)].node);
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_COMMIT);
    ;}
    break;

  case 507:

    {
      (void)(yyvsp[(2) - (2)].node);
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_ROLLBACK);
    ;}
    break;

  case 508:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_KILL, 3, (yyvsp[(2) - (4)].node), (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 509:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    ;}
    break;

  case 510:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 511:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    ;}
    break;

  case 512:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 513:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    ;}
    break;

  case 514:

    {
      ParseNode *privileges_node = NULL;
      ParseNode *users_node = NULL;
      merge_nodes(privileges_node, result->malloc_pool_, T_PRIVILEGES, (yyvsp[(2) - (6)].node));
      merge_nodes(users_node, result->malloc_pool_, T_USERS, (yyvsp[(6) - (6)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_GRANT,
                                 3, privileges_node, (yyvsp[(4) - (6)].node), users_node);
    ;}
    break;

  case 515:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 516:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 517:

    {
      (void)(yyvsp[(2) - (2)].node);                 /* useless */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_ALL;
    ;}
    break;

  case 518:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_ALTER;
    ;}
    break;

  case 519:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_CREATE;
    ;}
    break;

  case 520:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_CREATE_USER;
    ;}
    break;

  case 521:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_DELETE;
    ;}
    break;

  case 522:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_DROP;
    ;}
    break;

  case 523:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_DROP;
    ;}
    break;

  case 524:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_GRANT_OPTION;
    ;}
    break;

  case 525:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_INSERT;
    ;}
    break;

  case 526:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_UPDATE;
    ;}
    break;

  case 527:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_SELECT;
    ;}
    break;

  case 528:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_REPLACE;
    ;}
    break;

  case 529:

    {
      (void)(yyval.node);
    ;}
    break;

  case 530:

    {
      (void)(yyval.node);
    ;}
    break;

  case 531:

    {
      /* means global priv_level */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_LEVEL);
    ;}
    break;

  case 532:

    {
      /* still means global priv_level */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_LEVEL);
    ;}
    break;

  case 533:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_STAR);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_LEVEL, 2, (yyvsp[(1) - (3)].node), node);
    ;}
    break;

  case 534:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_LEVEL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 535:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_LEVEL, 2, NULL, (yyvsp[(1) - (1)].node));
    ;}
    break;

  case 536:

    {
      ParseNode *privileges_node = NULL;
      ParseNode *priv_level = NULL;
      merge_nodes(privileges_node, result->malloc_pool_, T_PRIVILEGES, (yyvsp[(2) - (5)].node));
      if ((yyvsp[(3) - (5)].node) == NULL)
      {
        /* check privileges: should have and only have ALL PRIVILEGES, GRANT OPTION */
        int check_ok = 0;
        if (2 == privileges_node->num_child_)
        {
          assert(privileges_node->children_[0]->num_child_ == 0);
          assert(privileges_node->children_[0]->type_ == T_PRIV_TYPE);
          assert(privileges_node->children_[1]->num_child_ == 0);
          assert(privileges_node->children_[1]->type_ == T_PRIV_TYPE);
          if ((privileges_node->children_[0]->value_ == OB_PRIV_ALL
               && privileges_node->children_[1]->value_ == OB_PRIV_GRANT_OPTION)
              || (privileges_node->children_[1]->value_ == OB_PRIV_ALL
                  && privileges_node->children_[0]->value_ == OB_PRIV_GRANT_OPTION))
          {
            check_ok = 1;
          }
        }
        if (!check_ok)
        {
          yyerror(&(yylsp[(1) - (5)]), result, "support only ALL PRIVILEGES, GRANT OPTION");
          YYABORT;
        }
      }
      else
      {
        priv_level = (yyvsp[(3) - (5)].node);
      }
      ParseNode *users_node = NULL;
      merge_nodes(users_node, result->malloc_pool_, T_USERS, (yyvsp[(5) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_REVOKE,
                                 3, privileges_node, priv_level, users_node);
    ;}
    break;

  case 537:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    ;}
    break;

  case 538:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 539:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PREPARE, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 540:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 541:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 542:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 543:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 544:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 545:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_VARIABLE_SET, (yyvsp[(2) - (2)].node));;
      (yyval.node)->value_ = 2;
    ;}
    break;

  case 546:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 547:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 548:

    {
      (void)((yyvsp[(2) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = 2;
    ;}
    break;

  case 549:

    {
      (void)((yyvsp[(2) - (3)].node));
      (yyvsp[(1) - (3)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = 2;
    ;}
    break;

  case 550:

    {
      (void)((yyvsp[(3) - (4)].node));
      (yyvsp[(2) - (4)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 551:

    {
      (void)((yyvsp[(3) - (4)].node));
      (yyvsp[(2) - (4)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = 2;
    ;}
    break;

  case 552:

    {
      (void)((yyvsp[(4) - (5)].node));
      (yyvsp[(3) - (5)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 553:

    {
      (void)((yyvsp[(4) - (5)].node));
      (yyvsp[(3) - (5)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
      (yyval.node)->value_ = 2;
    ;}
    break;

  case 554:

    {
      (void)((yyvsp[(2) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = 2;
    ;}
    break;

  case 555:

    { (yyval.node) = NULL; ;}
    break;

  case 556:

    { (yyval.node) = NULL; ;}
    break;

  case 557:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 558:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXECUTE, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 559:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_ARGUMENT_LIST, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 560:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 561:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 562:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 563:

    {
      (void)((yyvsp[(1) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_DEALLOCATE, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 564:

    { (yyval.node) = NULL; ;}
    break;

  case 565:

    { (yyval.node) = NULL; ;}
    break;

  case 566:

    {
      ParseNode *alter_actions = NULL;
      merge_nodes(alter_actions, result->malloc_pool_, T_ALTER_ACTION_LIST, (yyvsp[(4) - (4)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_TABLE, 2, (yyvsp[(3) - (4)].node), alter_actions);
    ;}
    break;

  case 567:

    {
      /*yyerror(&@1, result, "Table rename is not supported now");
      YYABORT;*/  //add liuj [Alter_Rename] [JHOBv0.1] 20150104
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLE_RENAME, 1, (yyvsp[(6) - (6)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_ACTION_LIST, 1, (yyval.node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_TABLE, 2, (yyvsp[(3) - (6)].node), (yyval.node));
    ;}
    break;

  case 568:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLE_RENAME, 1, (yyvsp[(5) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_ACTION_LIST, 1, (yyval.node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_TABLE, 2, (yyvsp[(3) - (5)].node), (yyval.node));
   ;}
    break;

  case 569:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 570:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 571:

    {
      (void)((yyvsp[(2) - (3)].node)); /* make bison mute */
      (yyval.node) = (yyvsp[(3) - (3)].node); /* T_COLUMN_DEFINITION */
    ;}
    break;

  case 572:

    {
      (void)((yyvsp[(2) - (4)].node)); /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_DROP, 1, (yyvsp[(3) - (4)].node));
      (yyval.node)->value_ = (yyvsp[(4) - (4)].ival);
    ;}
    break;

  case 573:

    {
      (void)((yyvsp[(2) - (4)].node)); /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_ALTER, 2, (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 574:

    {
      (void)((yyvsp[(2) - (5)].node)); /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_RENAME, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    ;}
    break;

  case 575:

    { (yyval.node) = NULL; ;}
    break;

  case 576:

    { (yyval.node) = NULL; ;}
    break;

  case 577:

    { (yyval.ival) = 2; ;}
    break;

  case 578:

    { (yyval.ival) = 1; ;}
    break;

  case 579:

    { (yyval.ival) = 0; ;}
    break;

  case 580:

    {
      (void)((yyvsp[(3) - (3)].node)); /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NOT_NULL);
    ;}
    break;

  case 581:

    {
      (void)((yyvsp[(3) - (3)].node)); /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NULL);
    ;}
    break;

  case 582:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_DEFAULT, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 583:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_NULL);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_DEFAULT, 1, (yyval.node));
    ;}
    break;

  case 584:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_SYTEM_ACTION_LIST, (yyvsp[(4) - (4)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_SYSTEM, 1, (yyval.node));
    ;}
    break;

  case 585:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_SET_MASTER_SLAVE);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CHANGE_OBI, 3, node, (yyvsp[(7) - (7)].node), (yyvsp[(3) - (7)].node));
    ;}
    break;

  case 586:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_SET_MASTER_SLAVE);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CHANGE_OBI, 3, node, (yyvsp[(7) - (7)].node), (yyvsp[(3) - (7)].node));
    ;}
    break;

  case 587:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_SET_SLAVE);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CHANGE_OBI, 2, node, (yyvsp[(6) - (6)].node));
    ;}
    break;

  case 588:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 589:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_FORCE);
    ;}
    break;

  case 590:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 591:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 592:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SYSTEM_ACTION, 5,
                               (yyvsp[(1) - (9)].node),    /* param_name */
                               (yyvsp[(3) - (9)].node),    /* param_value */
                               (yyvsp[(4) - (9)].node),    /* comment */
                               (yyvsp[(8) - (9)].node),    /* server type */
                               (yyvsp[(9) - (9)].node)     /* cluster or IP/port */
                               );
      (yyval.node)->value_ = (yyvsp[(5) - (9)].ival);                /* scope */
    ;}
    break;

  case 593:

    { (yyval.node) = (yyvsp[(2) - (2)].node); ;}
    break;

  case 594:

    { (yyval.node) = NULL; ;}
    break;

  case 595:

    { (yyval.ival) = 0; ;}
    break;

  case 596:

    { (yyval.ival) = 1; ;}
    break;

  case 597:

    { (yyval.ival) = 2; ;}
    break;

  case 598:

    { (yyval.ival) = 2; ;}
    break;

  case 599:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 600:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 4;
    ;}
    break;

  case 601:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 2;
    ;}
    break;

  case 602:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 3;
    ;}
    break;

  case 603:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CLUSTER, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 604:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SERVER_ADDRESS, 2, (yyvsp[(3) - (6)].node), (yyvsp[(6) - (6)].node));
    ;}
    break;

  case 605:

    { (yyval.node) = NULL; ;}
    break;

  case 606:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 607:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_IDENT);
      (yyval.node)->str_value_ = parse_strdup((yyvsp[(1) - (1)].non_reserved_keyword)->keyword_name, result->malloc_pool_);
      if ((yyval.node)->str_value_ == NULL)
      {
        yyerror(NULL, result, "No more space for string duplicate");
        YYABORT;
      }
      else
      {
        (yyval.node)->value_ = strlen((yyval.node)->str_value_);
      }
    ;}
    break;

  case 608:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 609:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_IDENT);
      (yyval.node)->str_value_ = parse_strdup((yyvsp[(1) - (1)].non_reserved_keyword)->keyword_name, result->malloc_pool_);
      if ((yyval.node)->str_value_ == NULL)
      {
        yyerror(NULL, result, "No more space for string duplicate");
        YYABORT;
      }
      else
      {
        (yyval.node)->value_ = strlen((yyval.node)->str_value_);
      }
    ;}
    break;

  case 611:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 612:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_IDENT);
      (yyval.node)->str_value_ = parse_strdup((yyvsp[(1) - (1)].non_reserved_keyword)->keyword_name, result->malloc_pool_);
      if ((yyval.node)->str_value_ == NULL)
      {
        yyerror(NULL, result, "No more space for string duplicate");
        YYABORT;
      }
    ;}
    break;



      default: break;
    }
  YY_SYMBOL_PRINT ("-> $$ =", yyr1[yyn], &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);

  *++yyvsp = yyval;
  *++yylsp = yyloc;

  /* Now `shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */

  yyn = yyr1[yyn];

  yystate = yypgoto[yyn - YYNTOKENS] + *yyssp;
  if (0 <= yystate && yystate <= YYLAST && yycheck[yystate] == *yyssp)
    yystate = yytable[yystate];
  else
    yystate = yydefgoto[yyn - YYNTOKENS];

  goto yynewstate;


/*------------------------------------.
| yyerrlab -- here on detecting error |
`------------------------------------*/
yyerrlab:
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
#if ! YYERROR_VERBOSE
      yyerror (&yylloc, result, YY_("syntax error"));
#else
      {
	YYSIZE_T yysize = yysyntax_error (0, yystate, yychar);
	if (yymsg_alloc < yysize && yymsg_alloc < YYSTACK_ALLOC_MAXIMUM)
	  {
	    YYSIZE_T yyalloc = 2 * yysize;
	    if (! (yysize <= yyalloc && yyalloc <= YYSTACK_ALLOC_MAXIMUM))
	      yyalloc = YYSTACK_ALLOC_MAXIMUM;
	    if (yymsg != yymsgbuf)
	      YYSTACK_FREE (yymsg);
	    yymsg = (char *) YYSTACK_ALLOC (yyalloc);
	    if (yymsg)
	      yymsg_alloc = yyalloc;
	    else
	      {
		yymsg = yymsgbuf;
		yymsg_alloc = sizeof yymsgbuf;
	      }
	  }

	if (0 < yysize && yysize <= yymsg_alloc)
	  {
	    (void) yysyntax_error (yymsg, yystate, yychar);
	    yyerror (&yylloc, result, yymsg);
	  }
	else
	  {
	    yyerror (&yylloc, result, YY_("syntax error"));
	    if (yysize != 0)
	      goto yyexhaustedlab;
	  }
      }
#endif
    }

  yyerror_range[0] = yylloc;

  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
	 error, discard it.  */

      if (yychar <= YYEOF)
	{
	  /* Return failure if at end of input.  */
	  if (yychar == YYEOF)
	    YYABORT;
	}
      else
	{
	  yydestruct ("Error: discarding",
		      yytoken, &yylval, &yylloc, result);
	  yychar = YYEMPTY;
	}
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:

  /* Pacify compilers like GCC when the user code never invokes
     YYERROR and the label yyerrorlab therefore never appears in user
     code.  */
  if (/*CONSTCOND*/ 0)
     goto yyerrorlab;

  yyerror_range[0] = yylsp[1-yylen];
  /* Do not reclaim the symbols of the rule which action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;	/* Each real token shifted decrements this.  */

  for (;;)
    {
      yyn = yypact[yystate];
      if (yyn != YYPACT_NINF)
	{
	  yyn += YYTERROR;
	  if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYTERROR)
	    {
	      yyn = yytable[yyn];
	      if (0 < yyn)
		break;
	    }
	}

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
	YYABORT;

      yyerror_range[0] = *yylsp;
      yydestruct ("Error: popping",
		  yystos[yystate], yyvsp, yylsp, result);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  *++yyvsp = yylval;

  yyerror_range[1] = yylloc;
  /* Using YYLLOC is tempting, but would change the location of
     the lookahead.  YYLOC is available though.  */
  YYLLOC_DEFAULT (yyloc, (yyerror_range - 1), 2);
  *++yylsp = yyloc;

  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", yystos[yyn], yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;

/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;

#if !defined(yyoverflow) || YYERROR_VERBOSE
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (&yylloc, result, YY_("memory exhausted"));
  yyresult = 2;
  /* Fall through.  */
#endif

yyreturn:
  if (yychar != YYEMPTY)
     yydestruct ("Cleanup: discarding lookahead",
		 yytoken, &yylval, &yylloc, result);
  /* Do not reclaim the symbols of the rule which action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
		  yystos[*yyssp], yyvsp, yylsp, result);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
#if YYERROR_VERBOSE
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
#endif
  /* Make sure YYID is used.  */
  return YYID (yyresult);
}





void yyerror(YYLTYPE* yylloc, ParseResult* p, char* s, ...)
{
  if (p != NULL)
  {
    p->result_tree_ = 0;
    va_list ap;
    va_start(ap, s);
    vsnprintf(p->error_msg_, MAX_ERROR_MSG, s, ap);
    if (yylloc != NULL)
    {
      if (p->input_sql_[yylloc->first_column - 1] != '\'')
        p->start_col_ = yylloc->first_column;
      p->end_col_ = yylloc->last_column;
      p->line_ = yylloc->first_line;
    }
  }
}

int parse_init(ParseResult* p)
{
  int ret = 0;  // can not include C++ file "ob_define.h"
  if (!p || !p->malloc_pool_)
  {
    ret = -1;
    if (p)
    {
      snprintf(p->error_msg_, MAX_ERROR_MSG, "malloc_pool_ must be set");
    }
  }
  if (ret == 0)
  {
    ret = yylex_init_extra(p, &(p->yyscan_info_));
  }
  return ret;
}

int parse_terminate(ParseResult* p)
{
  return yylex_destroy(p->yyscan_info_);
}

int parse_sql(ParseResult* p, const char* buf, size_t len)
{
  int ret = -1;
  p->result_tree_ = 0;
  p->error_msg_[0] = 0;
  p->input_sql_ = buf;
  p->input_sql_len_ = len;
  p->start_col_ = 1;
  p->end_col_ = 1;
  p->line_ = 1;
  p->yycolumn_ = 1;
  p->yylineno_ = 1;
  p->tmp_literal_ = NULL;

  if (buf == NULL || len <= 0)
  {
    snprintf(p->error_msg_, MAX_ERROR_MSG, "Input SQL can not be empty");
    return ret;
  }

  while(len > 0 && isspace(buf[len - 1]))
    --len;

  if (len <= 0)
  {
    snprintf(p->error_msg_, MAX_ERROR_MSG, "Input SQL can not be while space only");
    return ret;
  }

  YY_BUFFER_STATE bp;

  //bp = yy_scan_string(buf, p->yyscan_info_);
  bp = yy_scan_bytes(buf, len, p->yyscan_info_);
  yy_switch_to_buffer(bp, p->yyscan_info_);
  ret = yyparse(p);
  yy_delete_buffer(bp, p->yyscan_info_);
  return ret;
}

