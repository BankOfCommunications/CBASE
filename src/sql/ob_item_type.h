#ifndef OCEANBASE_SQL_OB_ITEM_TYPE_H_
#define OCEANBASE_SQL_OB_ITEM_TYPE_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef enum ObItemType
{
  T_INVALID = 0,  // Min tag

  /* Literal data type tags */
  T_INT,
  //add lijianqiang [INT_32] 20151008:b
  T_INT32,
  //add 20151008:e
  T_STRING,
  T_BINARY,
  T_DATE,     // WE may need time and timestamp here
  T_FLOAT,
  T_DOUBLE,
  T_DECIMAL,
  T_BOOL,
  T_NULL,
  T_QUESTIONMARK,
  T_UNKNOWN,
  //add peiouya [DATE_TIME] 20150912:b
  T_DATE_NEW,
  T_TIME,
  //add 20120912:e

  /* Reference type tags*/
  T_REF_COLUMN,
  T_REF_EXPR,
  T_REF_QUERY,

  T_HINT,     // Hint message from rowkey
  T_IDENT,
  T_STAR,
  T_SYSTEM_VARIABLE,
  T_TEMP_VARIABLE,

  /* Data type tags */
  T_TYPE_INTEGER,
  //add lijianqiang [INT_32] 20151008:b
  T_TYPE_BIG_INTEGER,
  //add 20151008:e
  T_TYPE_FLOAT,
  T_TYPE_DOUBLE,
  T_TYPE_DECIMAL,
  T_TYPE_BOOLEAN,
  T_TYPE_DATE,
  T_TYPE_TIME,
  T_TYPE_DATETIME,
  T_TYPE_TIMESTAMP,
  T_TYPE_CHARACTER,
  T_TYPE_VARCHAR,
  T_TYPE_CREATETIME,
  T_TYPE_MODIFYTIME,
  /*add wuna [datetime func] 20150908:b*/
  T_DATE_ADD,
  T_DATE_SUB,
  T_INTERVAL_DAYS,
  T_INTERVAL_HOURS,
  T_INTERVAL_MINUTES,
  T_INTERVAL_MONTHS,
  T_INTERVAL_SECONDS,
  T_INTERVAL_MICROSECONDS,
  T_INTERVAL_YEARS,
  /*add 20150908:e*/
  T_FUN_REPLACE, //add lijianqiang [replace_function] 20151102

  // @note !! the order of the following tags between T_MIN_OP and T_MAX_OP SHOULD NOT be changed
  /* Operator tags */
  T_MIN_OP = 100,
  /* 1. arithmetic operators */
  T_OP_NEG,   // negative
  T_OP_POS,   // positive
  T_OP_ADD,
  T_OP_MINUS,
  T_OP_MUL,
  T_OP_DIV,
  T_OP_POW,
  T_OP_REM,   // remainder
  T_OP_MOD,
  T_OP_EQ,      /* 2. Bool operators */
  T_OP_LE,
  T_OP_LT,
  T_OP_GE,
  T_OP_GT,
  T_OP_NE,
  T_OP_IS,
  T_OP_IS_NOT,
  T_OP_BTW,
  T_OP_NOT_BTW,
  T_OP_LIKE,
  T_OP_NOT_LIKE,
  T_OP_NOT,
  T_OP_AND,
  T_OP_OR,
  T_OP_IN,
  T_OP_NOT_IN,
  T_OP_ARG_CASE,
  T_OP_CASE,
  T_OP_ROW,
  T_OP_EXISTS,

  T_OP_CNN,  /* 3. String operators */

  T_FUN_SYS,                    // system functions, CHAR_LENGTH, ROUND, etc.
  T_OP_LEFT_PARAM_END,
  T_OP_CAST_THIN,  //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608
  //add wanglei [case when fix] 20160325:b
  T_CASE_BEGIN,
  T_CASE_WHEN,
  T_CASE_THEN,
  T_CASE_ELSE,
  T_CASE_BREAK,
  T_CASE_END,
  //add wanglei:e
  T_MAX_OP,

  T_CUR_TIME,
  T_CUR_DATE, //add wuna [datetime func] 20150902
  T_CUR_TIME_UPS,
  T_CUR_TIME_OP,
  T_CUR_TIME_HMS,//add liuzy [datetime func] 20150902 /*Exp: only get time from current timestamp*/

  T_ROW_COUNT,
  T_FUN_SYS_CAST,               // special system function : CAST(val AS type)

  /* 4. name filed specificator */
  T_OP_NAME_FIELD,

  // @note !! the order of the following tags between T_FUN_MAX and T_FUN_AVG SHOULD NOT be changed
  /* Function tags */
  T_FUN_MAX,
  T_FUN_MIN,
  T_FUN_SUM,
  T_FUN_COUNT,
  T_FUN_LISTAGG,//add gaojt [ListAgg][JHOBv0.1]20150104
  T_FUN_ROW_NUMBER,//add liumz [ROW_NUMBER]20150822
  T_FUN_AVG,

  /* parse tree node tags */
  T_DELETE,
  T_SELECT,
  T_UPDATE,
  T_INSERT,
  T_INSERT_MULTI_BATCH,//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150507
  T_EXPLAIN,
  T_LINK_NODE,
  T_ASSIGN_LIST,
  T_ASSIGN_ITEM,
  T_STMT_LIST,
  T_EXPR_LIST,
  T_WHEN_LIST,
  T_PROJECT_LIST,
  T_PROJECT_ITEM,
  T_FROM_LIST,
  T_SET_UNION,
  T_SET_INTERSECT,
  T_SET_EXCEPT,
  T_WHERE_CLAUSE,
  T_LIMIT_CLAUSE,
  T_SORT_LIST,
  T_SORT_KEY,
  T_SORT_ASC,
  T_SORT_DESC,
  T_ALL,
  T_DISTINCT,
  T_RELATION,//add liumz, [multi_database.sql]:20150613 172
  T_ALIAS,
  T_PROJECT_STRING,
  T_COLUMN_LIST,
  T_VALUE_LIST,
  T_VALUE_VECTOR,
  T_JOINED_TABLE,
  T_JOIN_INNER,
    T_JOIN_SEMI,
    T_JOIN_SEMI_LEFT,
    T_JOIN_SEMI_RIGHT,
  T_JOIN_FULL,
  T_JOIN_LEFT,
  T_JOIN_RIGHT,
  T_CASE,
  T_WHEN,

  T_CREATE_TABLE,
  T_TABLE_ELEMENT_LIST,
  T_TABLE_OPTION_LIST,
  T_PRIMARY_KEY,
  T_COLUMN_DEFINITION,
  T_COLUMN_ATTRIBUTES,
  T_CONSTR_NOT_NULL,
  T_CONSTR_NULL,
  T_CONSTR_DEFAULT,
  T_CONSTR_AUTO_INCREMENT,
  T_CONSTR_PRIMARY_KEY,
  T_IF_NOT_EXISTS,
  T_IF_EXISTS,
  T_JOIN_INFO,
  T_EXPIRE_INFO,
  T_TABLET_MAX_SIZE,
  T_TABLET_BLOCK_SIZE,
  T_TABLET_ID,
  T_REPLICA_NUM,
  T_COMPRESS_METHOD,
  T_COMMENT,
  T_USE_BLOOM_FILTER,
  T_CONSISTENT_MODE,
  T_DROP_TABLE,
  T_TABLE_LIST,

  T_ALTER_TABLE,
  T_ALTER_ACTION_LIST,
  T_TABLE_RENAME,
  T_COLUMN_DROP,
  T_COLUMN_ALTER,
  T_COLUMN_RENAME,
  //add zhaoqiong [Truncate Table] 20151204:b
  T_TRUNCATE_TABLE,
  //add:e

  T_ALTER_SYSTEM,
  T_CHANGE_OBI,
  T_FORCE,
  T_SET_MASTER,
  T_SET_SLAVE,
  T_SET_MASTER_SLAVE,
  T_SYTEM_ACTION_LIST,
  T_SYSTEM_ACTION,
  T_CLUSTER,
  T_SERVER_ADDRESS,

  T_SHOW_TABLES,
  T_SHOW_SYSTEM_TABLES,// add zhangcd [multi_database.show_tables] 20150616
  //add liumengzhan_show_index [20141208]
  T_SHOW_INDEX,
  //add:e
  T_SHOW_VARIABLES,
  T_SHOW_COLUMNS,
  T_SHOW_SCHEMA,
  T_SHOW_CREATE_TABLE,
  T_SHOW_TABLE_STATUS,
  T_SHOW_PARAMETERS,
  T_SHOW_DATABASES, //add dolphin [show database] 20150604
  T_SHOW_CURRENT_DATABASE,// add zhangcd [multi_database.show_databases] 20150617
  T_SHOW_SERVER_STATUS,
  T_SHOW_WARNINGS,
  T_SHOW_GRANTS,
  T_SHOW_PROCESSLIST,
  T_SHOW_LIMIT,


  T_CREATE_USER,
  T_CREATE_USER_SPEC,
  T_DROP_USER,
  //add wenghaixing [database manage]20150608
  T_CREATE_DATABASE,
  T_DROP_DATABASE,
  T_USE_DATABASE,
  //add e
  //add lijianqiang [sequence] 20150324b:
  T_SEQUENCE,
  T_SEQUENCE_ASSIST,
  T_SEQUENCE_CREATE,
  T_SEQUENCE_REPLACE,
  T_SEQUENCE_LABEL_LIST,
  T_SEQUENCE_START_WITH,
  T_SEQUENCE_INCREMENT_BY,
  T_SEQUENCE_MINVALUE,
  T_SEQUENCE_MAXVALUE,
  T_SEQUENCE_CYCLE,
  T_SEQUENCE_CACHE,
  T_SEQUENCE_ORDER,
  T_SEQUENCE_DROP,
  T_SEQUENCE_EXPR,
  T_SEQUENCE_ALTER, /*add liuzy [sequence] 20150429*/
  T_SEQUENCE_RESTART_WITH,/*add liuzy [sequence] 20150429*/
  T_SEQUENCE_QUICK,/*add liuzy [sequence] 20150623*/
  //add 20150324:e
  T_SET_PASSWORD,
  T_RENAME_USER,
  T_RENAME_INFO,
  T_LOCK_USER,
  T_GRANT,
  T_PRIVILEGES,
  T_PRIV_LEVEL,
  T_PRIV_TYPE,
  T_USERS,
  T_REVOKE,
  T_BEGIN,
  T_COMMIT,
  T_PREPARE,
  T_DEALLOCATE,
  T_EXECUTE,
  T_ARGUMENT_LIST,
  T_VARIABLE_SET,
  T_VAR_VAL,
  T_ROLLBACK,

  T_HINT_OPTION_LIST,
  T_READ_STATIC,
  T_HOTSPOT,
  T_READ_CONSISTENCY,
  T_USE_INDEX,// add by zcd 20141216
  T_UNKOWN_HINT,// add by zcd 20150601
  T_RANGE,//add tianz [EXPORT_TOOL] 20141120
  T_NOT_USE_INDEX,//addzhuyanchao secondary index 20150708

  //add wenghaixing [secondary index] 20141024
  T_CREATE_INDEX,
  T_INDEX_NAME,
  //add e
  //add wenghaixing [secondary index drop index]20141222
  T_DROP_INDEX,
  T_JOIN_LIST,//add hushuang [bloomfilter_join] 20150422
  T_BLOOMFILTER_JOIN,  //add hushuang [bloomfilter_join] 20150422
  T_MERGE_JOIN,  //add hushuang [bloomfilter_join] 20150422
  //add e
 //add wanglei:b
  T_SEMI_JOIN,
  T_SEMI_BTW_JOIN,
  T_I_MUTLI_BATCH,//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20151213
  T_UD_MUTLI_BATCH, //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160302
  T_UD_ALL_ROWKEY, //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160418
  T_UD_NOT_PARALLAL, //add gaojt [Delete_Update_Function_isolation] [JHOBv0.1] 20160612
  T_CHANGE_VALUE_SIZE,//add maosy [Delete_Update_Function] [JHOBv0.1] 20161103
  T_KILL,

  T_MAX,

} ObItemType;

#ifdef __cplusplus
}
#endif

#endif //OCEANBASE_SQL_OB_ITEM_TYPE_H_
