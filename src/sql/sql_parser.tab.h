
/* A Bison parser, made by GNU Bison 2.4.1.  */

/* Skeleton interface for Bison's Yacc-like parsers in C
   
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



