#include "ob_item_type.h"
const char* get_type_name(int type)
{
	switch(type){
	case T_INT : return "T_INT";
	case T_STRING : return "T_STRING";
	case T_BINARY : return "T_BINARY";
	case T_DATE : return "T_DATE";     // WE may need time and timestamp here
	case T_FLOAT : return "T_FLOAT";
	case T_DOUBLE : return "T_DOUBLE";
	case T_DECIMAL : return "T_DECIMAL";
	case T_BOOL : return "T_BOOL";
	case T_NULL : return "T_NULL";
	case T_QUESTIONMARK : return "T_QUESTIONMARK";
	case T_UNKNOWN : return "T_UNKNOWN";
	case T_DATE_NEW : return "T_DATE_NEW";
	case T_TIME : return "T_TIME";
	case T_REF_COLUMN : return "T_REF_COLUMN";
	case T_REF_EXPR : return "T_REF_EXPR";
	case T_REF_QUERY : return "T_REF_QUERY";
	case T_HINT : return "T_HINT";     // Hint message from rowkey
	case T_IDENT : return "T_IDENT";
	case T_STAR : return "T_STAR";
	case T_SYSTEM_VARIABLE : return "T_SYSTEM_VARIABLE";
	case T_TEMP_VARIABLE : return "T_TEMP_VARIABLE";
	case T_TYPE_INTEGER : return "T_TYPE_INTEGER";
	case T_TYPE_BIG_INTEGER : return "T_TYPE_BIG_INTEGER";
	case T_TYPE_FLOAT : return "T_TYPE_FLOAT";
	case T_TYPE_DOUBLE : return "T_TYPE_DOUBLE";
	case T_TYPE_DECIMAL : return "T_TYPE_DECIMAL";
	case T_TYPE_BOOLEAN : return "T_TYPE_BOOLEAN";
	case T_TYPE_DATE : return "T_TYPE_DATE";
	case T_TYPE_TIME : return "T_TYPE_TIME";
	case T_TYPE_DATETIME : return "T_TYPE_DATETIME";
	case T_TYPE_TIMESTAMP : return "T_TYPE_TIMESTAMP";
	case T_TYPE_CHARACTER : return "T_TYPE_CHARACTER";
	case T_TYPE_VARCHAR : return "T_TYPE_VARCHAR";
	case T_TYPE_CREATETIME : return "T_TYPE_CREATETIME";
	case T_TYPE_MODIFYTIME : return "T_TYPE_MODIFYTIME";
	case T_DATE_ADD : return "T_DATE_ADD";
	case T_DATE_SUB : return "T_DATE_SUB";
	case T_INTERVAL_DAYS : return "T_INTERVAL_DAYS";
	case T_INTERVAL_HOURS : return "T_INTERVAL_HOURS";
	case T_INTERVAL_MINUTES : return "T_INTERVAL_MINUTES";
	case T_INTERVAL_MONTHS : return "T_INTERVAL_MONTHS";
	case T_INTERVAL_SECONDS : return "T_INTERVAL_SECONDS";
	case T_INTERVAL_MICROSECONDS : return "T_INTERVAL_MICROSECONDS";
	case T_INTERVAL_YEARS : return "T_INTERVAL_YEARS";
	case T_FUN_REPLACE : return "T_FUN_REPLACE"; //add lijianqiang [replace_function] 20151102
	case T_OP_NEG : return "T_OP_NEG";   // negative
	case T_OP_POS : return "T_OP_POS";   // positive
	case T_OP_ADD : return "T_OP_ADD";
	case T_OP_MINUS : return "T_OP_MINUS";
	case T_OP_MUL : return "T_OP_MUL";
	case T_OP_DIV : return "T_OP_DIV";
	case T_OP_POW : return "T_OP_POW";
	case T_OP_REM : return "T_OP_REM";   // remainder
	case T_OP_MOD : return "T_OP_MOD";
	case T_OP_EQ : return "T_OP_EQ";      /* 2. Bool operators */
	case T_OP_LE : return "T_OP_LE";
	case T_OP_LT : return "T_OP_LT";
	case T_OP_GE : return "T_OP_GE";
	case T_OP_GT : return "T_OP_GT";
	case T_OP_NE : return "T_OP_NE";
	case T_OP_IS : return "T_OP_IS";
	case T_OP_IS_NOT : return "T_OP_IS_NOT";
	case T_OP_BTW : return "T_OP_BTW";
	case T_OP_NOT_BTW : return "T_OP_NOT_BTW";
	case T_OP_LIKE : return "T_OP_LIKE";
	case T_OP_NOT_LIKE : return "T_OP_NOT_LIKE";
	case T_OP_NOT : return "T_OP_NOT";
	case T_OP_AND : return "T_OP_AND";
	case T_OP_OR : return "T_OP_OR";
	case T_OP_IN : return "T_OP_IN";
	case T_OP_NOT_IN : return "T_OP_NOT_IN";
	case T_OP_ARG_CASE : return "T_OP_ARG_CASE";
	case T_OP_CASE : return "T_OP_CASE";
	case T_OP_ROW : return "T_OP_ROW";
	case T_OP_EXISTS : return "T_OP_EXISTS";
	case T_OP_CNN : return "T_OP_CNN";  /* 3. String operators */
	case T_FUN_SYS : return "T_FUN_SYS";                    // system functions, CHAR_LENGTH, ROUND, etc.
	case T_OP_LEFT_PARAM_END : return "T_OP_LEFT_PARAM_END";
	case T_OP_CAST_THIN : return "T_OP_CAST_THIN";  //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608
	case T_CASE_BEGIN : return "T_CASE_BEGIN";
	case T_CASE_WHEN : return "T_CASE_WHEN";
	case T_CASE_THEN : return "T_CASE_THEN";
	case T_CASE_ELSE : return "T_CASE_ELSE";
	case T_CASE_BREAK : return "T_CASE_BREAK";
	case T_CASE_END : return "T_CASE_END";
	case T_MAX_OP : return "T_MAX_OP";
	case T_CUR_TIME : return "T_CUR_TIME";
	case T_CUR_DATE : return "T_CUR_DATE"; //add wuna [datetime func] 20150902
	case T_CUR_TIME_UPS : return "T_CUR_TIME_UPS";
	case T_CUR_TIME_OP : return "T_CUR_TIME_OP";
	case T_CUR_TIME_HMS : return "T_CUR_TIME_HMS";//add liuzy [datetime func] 20150902 /*Exp: only get time from current timestamp*/
	case T_ROW_COUNT : return "T_ROW_COUNT";
	case T_FUN_SYS_CAST : return "T_FUN_SYS_CAST";               // special system function : CAST(val AS type)
	case T_OP_NAME_FIELD : return "T_OP_NAME_FIELD";
	case T_FUN_MAX : return "T_FUN_MAX";
	case T_FUN_MIN : return "T_FUN_MIN";
	case T_FUN_SUM : return "T_FUN_SUM";
	case T_FUN_COUNT : return "T_FUN_COUNT";
	case T_FUN_LISTAGG : return "T_FUN_LISTAGG";//add gaojt [ListAgg][JHOBv0.1]20150104
	case T_FUN_ROW_NUMBER : return "T_FUN_ROW_NUMBER";//add liumz [ROW_NUMBER]20150822
	case T_FUN_AVG : return "T_FUN_AVG";
	case T_DELETE : return "T_DELETE";
	case T_SELECT : return "T_SELECT";
	case T_UPDATE : return "T_UPDATE";
	case T_INSERT : return "T_INSERT";
	case T_INSERT_MULTI_BATCH : return "T_INSERT_MULTI_BATCH";//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150507
	case T_EXPLAIN : return "T_EXPLAIN";
	case T_LINK_NODE : return "T_LINK_NODE";
	case T_ASSIGN_LIST : return "T_ASSIGN_LIST";
	case T_ASSIGN_ITEM : return "T_ASSIGN_ITEM";
	case T_STMT_LIST : return "T_STMT_LIST";
	case T_EXPR_LIST : return "T_EXPR_LIST";
	case T_WHEN_LIST : return "T_WHEN_LIST";
	case T_PROJECT_LIST : return "T_PROJECT_LIST";
	case T_PROJECT_ITEM : return "T_PROJECT_ITEM";
	case T_FROM_LIST : return "T_FROM_LIST";
	case T_SET_UNION : return "T_SET_UNION";
	case T_SET_INTERSECT : return "T_SET_INTERSECT";
	case T_SET_EXCEPT : return "T_SET_EXCEPT";
	case T_WHERE_CLAUSE : return "T_WHERE_CLAUSE";
	case T_LIMIT_CLAUSE : return "T_LIMIT_CLAUSE";
	case T_SORT_LIST : return "T_SORT_LIST";
	case T_SORT_KEY : return "T_SORT_KEY";
	case T_SORT_ASC : return "T_SORT_ASC";
	case T_SORT_DESC : return "T_SORT_DESC";
	case T_ALL : return "T_ALL";
	case T_DISTINCT : return "T_DISTINCT";
	case T_RELATION : return "T_RELATION";//add liumz, [multi_database.sql]:20150613 172
	case T_ALIAS : return "T_ALIAS";
	case T_PROJECT_STRING : return "T_PROJECT_STRING";
	case T_COLUMN_LIST : return "T_COLUMN_LIST";
	case T_VALUE_LIST : return "T_VALUE_LIST";
	case T_VALUE_VECTOR : return "T_VALUE_VECTOR";
	case T_JOINED_TABLE : return "T_JOINED_TABLE";
	case T_JOIN_INNER : return "T_JOIN_INNER";
	case T_JOIN_SEMI : return "T_JOIN_SEMI";
	case T_JOIN_SEMI_LEFT : return "T_JOIN_SEMI_LEFT";
	case T_JOIN_SEMI_RIGHT : return "T_JOIN_SEMI_RIGHT";
	case T_JOIN_FULL : return "T_JOIN_FULL";
	case T_JOIN_LEFT : return "T_JOIN_LEFT";
	case T_JOIN_RIGHT : return "T_JOIN_RIGHT";
	case T_CASE : return "T_CASE";
	case T_WHEN : return "T_WHEN";
	case T_CREATE_TABLE : return "T_CREATE_TABLE";
	case T_TABLE_ELEMENT_LIST : return "T_TABLE_ELEMENT_LIST";
	case T_TABLE_OPTION_LIST : return "T_TABLE_OPTION_LIST";
	case T_PRIMARY_KEY : return "T_PRIMARY_KEY";
	case T_COLUMN_DEFINITION : return "T_COLUMN_DEFINITION";
	case T_COLUMN_ATTRIBUTES : return "T_COLUMN_ATTRIBUTES";
	case T_CONSTR_NOT_NULL : return "T_CONSTR_NOT_NULL";
	case T_CONSTR_NULL : return "T_CONSTR_NULL";
	case T_CONSTR_DEFAULT : return "T_CONSTR_DEFAULT";
	case T_CONSTR_AUTO_INCREMENT : return "T_CONSTR_AUTO_INCREMENT";
	case T_CONSTR_PRIMARY_KEY : return "T_CONSTR_PRIMARY_KEY";
	case T_IF_NOT_EXISTS : return "T_IF_NOT_EXISTS";
	case T_IF_EXISTS : return "T_IF_EXISTS";
	case T_JOIN_INFO : return "T_JOIN_INFO";
	case T_EXPIRE_INFO : return "T_EXPIRE_INFO";
	case T_TABLET_MAX_SIZE : return "T_TABLET_MAX_SIZE";
	case T_TABLET_BLOCK_SIZE : return "T_TABLET_BLOCK_SIZE";
	case T_TABLET_ID : return "T_TABLET_ID";
	case T_REPLICA_NUM : return "T_REPLICA_NUM";
	case T_COMPRESS_METHOD : return "T_COMPRESS_METHOD";
	case T_COMMENT : return "T_COMMENT";
	case T_USE_BLOOM_FILTER : return "T_USE_BLOOM_FILTER";
	case T_CONSISTENT_MODE : return "T_CONSISTENT_MODE";
	case T_DROP_TABLE : return "T_DROP_TABLE";
	case T_TABLE_LIST : return "T_TABLE_LIST";
	case T_ALTER_TABLE : return "T_ALTER_TABLE";
	case T_ALTER_ACTION_LIST : return "T_ALTER_ACTION_LIST";
	case T_TABLE_RENAME : return "T_TABLE_RENAME";
	case T_COLUMN_DROP : return "T_COLUMN_DROP";
	case T_COLUMN_ALTER : return "T_COLUMN_ALTER";
	case T_COLUMN_RENAME : return "T_COLUMN_RENAME";
	case T_TRUNCATE_TABLE : return "T_TRUNCATE_TABLE";
	case T_ALTER_SYSTEM : return "T_ALTER_SYSTEM";
	case T_CHANGE_OBI : return "T_CHANGE_OBI";
	case T_FORCE : return "T_FORCE";
	case T_SET_MASTER : return "T_SET_MASTER";
	case T_SET_SLAVE : return "T_SET_SLAVE";
	case T_SET_MASTER_SLAVE : return "T_SET_MASTER_SLAVE";
	case T_SYTEM_ACTION_LIST : return "T_SYTEM_ACTION_LIST";
	case T_SYSTEM_ACTION : return "T_SYSTEM_ACTION";
	case T_CLUSTER : return "T_CLUSTER";
	case T_SERVER_ADDRESS : return "T_SERVER_ADDRESS";
	case T_SHOW_TABLES : return "T_SHOW_TABLES";
	case T_SHOW_SYSTEM_TABLES : return "T_SHOW_SYSTEM_TABLES";// add zhangcd [multi_database.show_tables] 20150616
	case T_SHOW_INDEX : return "T_SHOW_INDEX";
	case T_SHOW_VARIABLES : return "T_SHOW_VARIABLES";
	case T_SHOW_COLUMNS : return "T_SHOW_COLUMNS";
	case T_SHOW_SCHEMA : return "T_SHOW_SCHEMA";
	case T_SHOW_CREATE_TABLE : return "T_SHOW_CREATE_TABLE";
	case T_SHOW_TABLE_STATUS : return "T_SHOW_TABLE_STATUS";
	case T_SHOW_PARAMETERS : return "T_SHOW_PARAMETERS";
	case T_SHOW_DATABASES : return "T_SHOW_DATABASES"; //add dolphin [show database] 20150604
	case T_SHOW_CURRENT_DATABASE : return "T_SHOW_CURRENT_DATABASE";// add zhangcd [multi_database.show_databases] 20150617
	case T_SHOW_SERVER_STATUS : return "T_SHOW_SERVER_STATUS";
	case T_SHOW_WARNINGS : return "T_SHOW_WARNINGS";
	case T_SHOW_GRANTS : return "T_SHOW_GRANTS";
	case T_SHOW_PROCESSLIST : return "T_SHOW_PROCESSLIST";
	case T_SHOW_LIMIT : return "T_SHOW_LIMIT";
	case T_CREATE_USER : return "T_CREATE_USER";
	case T_CREATE_USER_SPEC : return "T_CREATE_USER_SPEC";
	case T_DROP_USER : return "T_DROP_USER";
	case T_CREATE_DATABASE : return "T_CREATE_DATABASE";
	case T_DROP_DATABASE : return "T_DROP_DATABASE";
	case T_USE_DATABASE : return "T_USE_DATABASE";
	case T_SEQUENCE : return "T_SEQUENCE";
	case T_SEQUENCE_ASSIST : return "T_SEQUENCE_ASSIST";
	case T_SEQUENCE_CREATE : return "T_SEQUENCE_CREATE";
	case T_SEQUENCE_REPLACE : return "T_SEQUENCE_REPLACE";
	case T_SEQUENCE_LABEL_LIST : return "T_SEQUENCE_LABEL_LIST";
	case T_SEQUENCE_START_WITH : return "T_SEQUENCE_START_WITH";
	case T_SEQUENCE_INCREMENT_BY : return "T_SEQUENCE_INCREMENT_BY";
	case T_SEQUENCE_MINVALUE : return "T_SEQUENCE_MINVALUE";
	case T_SEQUENCE_MAXVALUE : return "T_SEQUENCE_MAXVALUE";
	case T_SEQUENCE_CYCLE : return "T_SEQUENCE_CYCLE";
	case T_SEQUENCE_CACHE : return "T_SEQUENCE_CACHE";
	case T_SEQUENCE_ORDER : return "T_SEQUENCE_ORDER";
	case T_SEQUENCE_DROP : return "T_SEQUENCE_DROP";
	case T_SEQUENCE_EXPR : return "T_SEQUENCE_EXPR";
	case T_SEQUENCE_ALTER : return "T_SEQUENCE_ALTER"; /*add liuzy [sequence] 20150429*/
	case T_SEQUENCE_RESTART_WITH : return "T_SEQUENCE_RESTART_WITH";/*add liuzy [sequence] 20150429*/
	case T_SEQUENCE_QUICK : return "T_SEQUENCE_QUICK";/*add liuzy [sequence] 20150623*/
	case T_SET_PASSWORD : return "T_SET_PASSWORD";
	case T_RENAME_USER : return "T_RENAME_USER";
	case T_RENAME_INFO : return "T_RENAME_INFO";
	case T_LOCK_USER : return "T_LOCK_USER";
	case T_GRANT : return "T_GRANT";
	case T_PRIVILEGES : return "T_PRIVILEGES";
	case T_PRIV_LEVEL : return "T_PRIV_LEVEL";
	case T_PRIV_TYPE : return "T_PRIV_TYPE";
	case T_USERS : return "T_USERS";
	case T_REVOKE : return "T_REVOKE";
	case T_BEGIN : return "T_BEGIN";
	case T_COMMIT : return "T_COMMIT";
	case T_PREPARE : return "T_PREPARE";
	case T_DEALLOCATE : return "T_DEALLOCATE";
	case T_EXECUTE : return "T_EXECUTE";
	case T_ARGUMENT_LIST : return "T_ARGUMENT_LIST";
	case T_VARIABLE_SET : return "T_VARIABLE_SET";
	case T_VAR_VAL : return "T_VAR_VAL";
	case T_ROLLBACK : return "T_ROLLBACK";
	case T_HINT_OPTION_LIST : return "T_HINT_OPTION_LIST";
	case T_READ_STATIC : return "T_READ_STATIC";
	case T_HOTSPOT : return "T_HOTSPOT";
	case T_READ_CONSISTENCY : return "T_READ_CONSISTENCY";
	case T_USE_INDEX : return "T_USE_INDEX";// add by zcd 20141216
	case T_UNKOWN_HINT : return "T_UNKOWN_HINT";// add by zcd 20150601
	case T_RANGE : return "T_RANGE";//add tianz [EXPORT_TOOL] 20141120
	case T_NOT_USE_INDEX : return "T_NOT_USE_INDEX";//addzhuyanchao secondary index 20150708
	case T_CREATE_INDEX : return "T_CREATE_INDEX";
	case T_INDEX_NAME : return "T_INDEX_NAME";
	case T_DROP_INDEX : return "T_DROP_INDEX";
	case T_JOIN_LIST : return "T_JOIN_LIST";//add hushuang [bloomfilter_join] 20150422
	case T_BLOOMFILTER_JOIN : return "T_BLOOMFILTER_JOIN";  //add hushuang [bloomfilter_join] 20150422
	case T_MERGE_JOIN : return "T_MERGE_JOIN";  //add hushuang [bloomfilter_join] 20150422
	case T_SEMI_JOIN : return "T_SEMI_JOIN";
	case T_SEMI_BTW_JOIN : return "T_SEMI_BTW_JOIN";
	case T_I_MUTLI_BATCH : return "T_I_MUTLI_BATCH";//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20151213
	case T_UD_MUTLI_BATCH : return "T_UD_MUTLI_BATCH"; //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160302
	case T_UD_ALL_ROWKEY : return "T_UD_ALL_ROWKEY"; //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160418
	case T_UD_NOT_PARALLAL : return "T_UD_NOT_PARALLAL"; //add gaojt [Delete_Update_Function_isolation] [JHOBv0.1] 20160612
	case T_CHANGE_VALUE_SIZE : return "T_CHANGE_VALUE_SIZE";//add maosy [Delete_Update_Function] [JHOBv0.1] 20161103
	case T_KILL : return "T_KILL";
	case T_MAX : return "T_MAX";
	default:return "Unknown";
	}
}
