/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_errno.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_errno.h"
#include "ob_define.h"
#include "tbsys.h"
using namespace oceanbase::common;
static const char* STR_ERROR[OB_MAX_ERROR_CODE];
#define ADD_ERROR_STR(err, str) STR_ERROR[-err] = str

// @todo yzf:refector later
static struct ObStrErrorInit
{
  ObStrErrorInit()
  {
    memset(STR_ERROR, 0, sizeof(STR_ERROR));
    ADD_ERROR_STR(OB_SUCCESS, "Success");
    ADD_ERROR_STR(OB_OBJ_TYPE_ERROR, "Object type error");
    ADD_ERROR_STR(OB_INVALID_ARGUMENT, "Invalid argument");
    ADD_ERROR_STR(OB_ARRAY_OUT_OF_RANGE, "Array index out of range");
    ADD_ERROR_STR(OB_SERVER_LISTEN_ERROR, "Failed to listen to the port");
    ADD_ERROR_STR(OB_INIT_TWICE, "The object is initialized twice");
    ADD_ERROR_STR(OB_NOT_INIT, "The object is not initialized");
    ADD_ERROR_STR(OB_NOT_SUPPORTED, "Not supported feature or function");
    ADD_ERROR_STR(OB_ITER_END, "End of iteration");
    ADD_ERROR_STR(OB_IO_ERROR, "IO error");
    ADD_ERROR_STR(OB_ERROR_FUNC_VERSION, "Wrong RPC command version");
    ADD_ERROR_STR(OB_PACKET_NOT_SENT, "Can not send packet");
    ADD_ERROR_STR(OB_RESPONSE_TIME_OUT, "Timeout");
    ADD_ERROR_STR(OB_ALLOCATE_MEMORY_FAILED, "No memory");
    ADD_ERROR_STR(OB_MEM_OVERFLOW, "Memory overflow");
    ADD_ERROR_STR(OB_ERR_SYS, "System error");
    ADD_ERROR_STR(OB_ERR_UNEXPECTED, "Ooooooooooooops");
    ADD_ERROR_STR(OB_ENTRY_EXIST, "Entry already exist");
    ADD_ERROR_STR(OB_ENTRY_NOT_EXIST, "Entry not exist");
    ADD_ERROR_STR(OB_SIZE_OVERFLOW, "Size overflow");
    ADD_ERROR_STR(OB_REF_NUM_NOT_ZERO, "Reference count is not zero");
    ADD_ERROR_STR(OB_CONFLICT_VALUE, "Conflict value");
    ADD_ERROR_STR(OB_ITEM_NOT_SETTED, "Item not set");
    ADD_ERROR_STR(OB_EAGAIN, "Try again");
    ADD_ERROR_STR(OB_BUF_NOT_ENOUGH, "Buffer not enough");
    ADD_ERROR_STR(OB_PARTIAL_FAILED, "Partial failed");
    ADD_ERROR_STR(OB_READ_NOTHING, "Nothing to read");
    ADD_ERROR_STR(OB_FILE_NOT_EXIST, "File not exist");
    ADD_ERROR_STR(OB_DISCONTINUOUS_LOG, "Log entry not continuous");
    ADD_ERROR_STR(OB_SCHEMA_ERROR, "Schema error");
    ADD_ERROR_STR(OB_DATA_NOT_SERVE, "Required data not served by the ChunkServer");
    ADD_ERROR_STR(OB_UNKNOWN_OBJ, "Unknown object");
    ADD_ERROR_STR(OB_NO_MONITOR_DATA, "No monitor data");
    ADD_ERROR_STR(OB_SERIALIZE_ERROR, "Serialize error");
    ADD_ERROR_STR(OB_DESERIALIZE_ERROR, "Deserialize error");
    ADD_ERROR_STR(OB_AIO_TIMEOUT, "Asynchronous IO error");
    ADD_ERROR_STR(OB_NEED_RETRY, "Need retry"); // need retry
    ADD_ERROR_STR(OB_TOO_MANY_SSTABLE, "Too many sstable");
    ADD_ERROR_STR(OB_NOT_MASTER, "The UpdateServer or cluster is not the master"); // !!! don't modify this value, OB_NOT_MASTER = -38
    ADD_ERROR_STR(OB_TOKEN_EXPIRED, "Expired token");
    ADD_ERROR_STR(OB_ENCRYPT_FAILED, "Encrypt error");
    ADD_ERROR_STR(OB_DECRYPT_FAILED, "Decrypt error");
    ADD_ERROR_STR(OB_USER_NOT_EXIST, "User not exist");
    ADD_ERROR_STR(OB_PASSWORD_WRONG, "Incorrect password");
    ADD_ERROR_STR(OB_SKEY_VERSION_WRONG, "Wrong skey version");
    ADD_ERROR_STR(OB_NOT_A_TOKEN, "Not a token");
    ADD_ERROR_STR(OB_NO_PERMISSION, "No permission");
    ADD_ERROR_STR(OB_COND_CHECK_FAIL, "Cond check error");
    ADD_ERROR_STR(OB_NOT_REGISTERED, "Not registered");
    ADD_ERROR_STR(OB_PROCESS_TIMEOUT, "Process timeout");
    ADD_ERROR_STR(OB_NOT_THE_OBJECT, "Not the object");
    ADD_ERROR_STR(OB_ALREADY_REGISTERED, "Already registered");
    ADD_ERROR_STR(OB_LAST_LOG_RUINNED, "Corrupted log entry");
    ADD_ERROR_STR(OB_NO_CS_SELECTED, "No ChunkServer selected");
    ADD_ERROR_STR(OB_NO_TABLETS_CREATED, "No tablets created");
    ADD_ERROR_STR(OB_INVALID_ERROR, "Invalid entry");
    ADD_ERROR_STR(OB_CONN_ERROR, "Connection error");
    ADD_ERROR_STR(OB_DECIMAL_OVERFLOW_WARN, "Decimal overflow warning");
    ADD_ERROR_STR(OB_DECIMAL_UNLEGAL_ERROR, "Decimal overflow error");
    ADD_ERROR_STR(OB_OBJ_DIVIDE_BY_ZERO, "Devided by zero");
    ADD_ERROR_STR(OB_OBJ_DIVIDE_ERROR, "Devide error");
    ADD_ERROR_STR(OB_NOT_A_DECIMAL, "Not a decimal");
    ADD_ERROR_STR(OB_DECIMAL_PRECISION_NOT_EQUAL, "Decimal precision error");
    ADD_ERROR_STR(OB_EMPTY_RANGE, "Empty range"); // get emptry range
    ADD_ERROR_STR(OB_SESSION_KILLED, "Session killed");
    ADD_ERROR_STR(OB_LOG_NOT_SYNC, "Log not sync");
    ADD_ERROR_STR(OB_DIR_NOT_EXIST, "Directory not exist");
    ADD_ERROR_STR(OB_NET_SESSION_END, "RPC session finished");
    ADD_ERROR_STR(OB_INVALID_LOG, "Invalid log");
    ADD_ERROR_STR(OB_FOR_PADDING, "For padding");
    ADD_ERROR_STR(OB_INVALID_DATA, "Invalid data");
    ADD_ERROR_STR(OB_ALREADY_DONE, "Already done");
    ADD_ERROR_STR(OB_CANCELED, "Operation canceled");
    ADD_ERROR_STR(OB_LOG_SRC_CHANGED, "Log source changed");
    ADD_ERROR_STR(OB_LOG_NOT_ALIGN, "Log not aligned");
    ADD_ERROR_STR(OB_LOG_MISSING, "Log entry missed");
    ADD_ERROR_STR(OB_NEED_WAIT, "Need wait");
    ADD_ERROR_STR(OB_NOT_IMPLEMENT, "Not implemented feature");
    ADD_ERROR_STR(OB_DIVISION_BY_ZERO, "Divided by zero");
    ADD_ERROR_STR(OB_VALUE_OUT_OF_RANGE, "Value out of range");
    ADD_ERROR_STR(OB_EXCEED_MEM_LIMIT, "exceed memory limit");
    ADD_ERROR_STR(OB_RESULT_UNKNOWN, "Unknown result");
    ADD_ERROR_STR(OB_ERR_DATA_FORMAT, "Data format error");
    ADD_ERROR_STR(OB_MAYBE_LOOP, "Mayby loop");
    ADD_ERROR_STR(OB_NO_RESULT, "No result");
    ADD_ERROR_STR(OB_QUEUE_OVERFLOW, "Queue overflow");
    ADD_ERROR_STR(OB_START_LOG_CURSOR_INVALID, "Invalid log cursor");
    ADD_ERROR_STR(OB_LOCK_NOT_MATCH, "Lock not match");
    ADD_ERROR_STR(OB_DEAD_LOCK, "Deadlock");
    ADD_ERROR_STR(OB_PARTIAL_LOG, "Incomplete log entry");
    ADD_ERROR_STR(OB_CHECKSUM_ERROR, "Data checksum error");
    ADD_ERROR_STR(OB_INIT_FAIL, "Initialize error");
    ADD_ERROR_STR(OB_ASYNC_CLIENT_WAITING_RESPONSE, "Asynchronous client failed to get response");
    ADD_ERROR_STR(OB_STATE_NOT_MATCH, "State not match");
    ADD_ERROR_STR(OB_READ_ZERO_LOG, "Read zero log");
    ADD_ERROR_STR(OB_SWITCH_LOG_NOT_MATCH, "Switch log not match");
    ADD_ERROR_STR(OB_LOG_NOT_START, "Log not start");
    ADD_ERROR_STR(OB_IN_FATAL_STATE, "In FATAL state");
    ADD_ERROR_STR(OB_IN_STOP_STATE, "In STOP state");
    ADD_ERROR_STR(OB_UPS_MASTER_EXISTS, "Master UpdateServer already exists");
    ADD_ERROR_STR(OB_LOG_NOT_CLEAR, "Log not clear");
    ADD_ERROR_STR(OB_FILE_ALREADY_EXIST, "File already exist");
    ADD_ERROR_STR(OB_UNKNOWN_PACKET, "Unknown packet");
    ADD_ERROR_STR(OB_TRANS_ROLLBACKED, "Transaction rollbacked");
    ADD_ERROR_STR(OB_LOG_TOO_LARGE, "Log too large");

    ADD_ERROR_STR(OB_RPC_SEND_ERROR, "PRC send error");
    ADD_ERROR_STR(OB_RPC_POST_ERROR, "PRC post error");
    ADD_ERROR_STR(OB_LIBEASY_ERROR, "Libeasy error");
    ADD_ERROR_STR(OB_CONNECT_ERROR, "Connect error");
    ADD_ERROR_STR(OB_NOT_FREE, "Not free");
    ADD_ERROR_STR(OB_INIT_SQL_CONTEXT_ERROR, "Init SQL context error");
    ADD_ERROR_STR(OB_SKIP_INVALID_ROW, "Skip invalid row");

    ADD_ERROR_STR(OB_SYS_CONFIG_TABLE_ERROR, "SYS_CONFIG table error");
    ADD_ERROR_STR(OB_READ_CONFIG_ERROR, "Read config error");

    ADD_ERROR_STR(OB_TRANS_NOT_MATCH, "Transaction not match");
    ADD_ERROR_STR(OB_TRANS_IS_READONLY, "Transaction is readonly");
    ADD_ERROR_STR(OB_ROW_MODIFIED, "Row modified");
    ADD_ERROR_STR(OB_VERSION_NOT_MATCH, "Version not match");
    ADD_ERROR_STR(OB_BAD_ADDRESS, "Bad address");
    ADD_ERROR_STR(OB_DUPLICATE_COLUMN, "Duplicated column");
    ADD_ERROR_STR(OB_ENQUEUE_FAILED, "Enqueue error");
    ADD_ERROR_STR(OB_INVALID_CONFIG, "Invalid config");
    ADD_ERROR_STR(OB_WAITING_COMMIT, "Waiting commit error");
    ADD_ERROR_STR(OB_STMT_EXPIRED, "Expired statement");

    //add zhaoqiong [Schema Manager] 20150514:b
    ADD_ERROR_STR(OB_SCHEMA_SYNC_ERROR,"Sync schema error");
    //add:e
    //add dolphin[coalesce return type]@20151201:b
    ADD_ERROR_STR(OB_VARCHAR_DECIMAL_INVALID,"VARCHAR cast to DECIMAL error");

    ADD_ERROR_STR(OB_TABLE_UPDATE_LOCKED, "table is locked"); //add zhaoqiong [Truncate Table]:20160318
    //add gaojt [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
    ADD_ERROR_STR(OB_UD_PARALLAL_DATA_NOT_SAFE, "batch update/delete data is not safe");
    //add gaojt 20160531:e
    //error code for chunk server -1001 ---- -2000
    ADD_ERROR_STR(OB_CS_CACHE_NOT_HIT, "Cache not hit");   // 缓存没有命中
    ADD_ERROR_STR(OB_CS_TIMEOUT, "ChunkServer timeout");         // 超时
    ADD_ERROR_STR(OB_CS_TABLET_NOT_EXIST, "Tablet not exist on Chunkserver"); // tablet not exist
    ADD_ERROR_STR(OB_CS_EMPTY_TABLET, "Tablet is empty");     // tablet has no data.
    ADD_ERROR_STR(OB_CS_EAGAIN, "ChunkServer try again");           //重试

    ADD_ERROR_STR(OB_GET_NEXT_COLUMN, "Get next column");
    ADD_ERROR_STR(OB_GET_NEXT_ROW, "To get next row"); // for internal use, scan next row.
    ADD_ERROR_STR(OB_INVALID_ROW_KEY, "Invalid row key");//不合法的rowKey
    ADD_ERROR_STR(OB_SEARCH_MODE_NOT_IMPLEMENT, "Search mode not implemented"); // search mode not implement, internal error
    ADD_ERROR_STR(OB_INVALID_BLOCK_INDEX, "Invalid block index"); // illegal block index data, internal error
    ADD_ERROR_STR(OB_INVALID_BLOCK_DATA, "Invalid block data");  // illegal block data, internal error
    ADD_ERROR_STR(OB_SEARCH_NOT_FOUND, "Value not found");    // value not found? for cs_get
    ADD_ERROR_STR(OB_BEYOND_THE_RANGE, "Key out of range");    // search key or range not in current tablet
    ADD_ERROR_STR(OB_CS_COMPLETE_TRAVERSAL, "ChunkServer complete tranverse block cache"); //complete traverse block cache
    ADD_ERROR_STR(OB_END_OF_ROW, "End of row");
    ADD_ERROR_STR(OB_CS_MERGE_ERROR, "ChunkServer merge error");
    ADD_ERROR_STR(OB_CS_SCHEMA_INCOMPATIBLE, "Incomplete schema");
    ADD_ERROR_STR(OB_CS_SERVICE_NOT_STARTED, "ChunkServer service unavailable");
    ADD_ERROR_STR(OB_CS_LEASE_EXPIRED, "Expired lease");
    ADD_ERROR_STR(OB_CS_MERGE_HAS_TIMEOUT, "Merge timeout on ChunkServer");
    ADD_ERROR_STR(OB_CS_TABLE_HAS_DELETED, "Table deleted on ChunkServer");
    ADD_ERROR_STR(OB_CS_MERGE_CANCELED, "Merge canceled ChunkServer");
    ADD_ERROR_STR(OB_CS_COMPRESS_LIB_ERROR, "ChunkServer failed to get compress library");
    ADD_ERROR_STR(OB_CS_OUTOF_DISK_SPACE, "ChunkServer out of disk space");
    ADD_ERROR_STR(OB_CS_MIGRATE_IN_EXIST, "Migrate-in task already exist");
    ADD_ERROR_STR(OB_AIO_BUF_END_BLOCK, "AIO  buffer end block");
    ADD_ERROR_STR(OB_AIO_EOF, "AIO EOF");
    ADD_ERROR_STR(OB_AIO_BUSY, "AIO busy");
    ADD_ERROR_STR(OB_WRONG_SSTABLE_DATA, "Wrong sstable data");
    ADD_ERROR_STR(OB_COLUMN_GROUP_NOT_FOUND, "Column group not found");
    ADD_ERROR_STR(OB_NO_IMPORT_SSTABLE, "No import sstable");
    ADD_ERROR_STR(OB_IMPORT_SSTABLE_NOT_EXIST, "Imported sstable not exist");

    //error code for update server -2001 ---- -3000
    ADD_ERROR_STR(OB_UPS_TRANS_RUNNING, "UpdateServer transaction in progress");     // 事务正在执行
    ADD_ERROR_STR(OB_FREEZE_MEMTABLE_TWICE, "Memtable frozen twice"); // memtable has been frozen
    ADD_ERROR_STR(OB_DROP_MEMTABLE_TWICE, "Memtable drop twice");   // memtable has been dropped
    ADD_ERROR_STR(OB_INVALID_START_VERSION, "Invalid memtable start version"); // memtable start version invalid
    ADD_ERROR_STR(OB_UPS_NOT_EXIST, "Not exist for UpdateServer");         // not exist
    ADD_ERROR_STR(OB_UPS_ACQUIRE_TABLE_FAIL, "Acquire table error on UpdateServer");// acquire table via version fail
    ADD_ERROR_STR(OB_UPS_INVALID_MAJOR_VERSION, "Invalid major version");
    ADD_ERROR_STR(OB_UPS_TABLE_NOT_FROZEN, "Table not frozen");
    ADD_ERROR_STR(OB_UPS_CHANGE_MASTER_TIMEOUT, "UpdateServer change master timeout");
    ADD_ERROR_STR(OB_FORCE_TIME_OUT, "Force timeout");
    ADD_ERROR_STR(OB_BEGIN_TRANS_LOCKED, "Begin transaction locked");

    //error code for root server -3001 ---- -4000
    ADD_ERROR_STR(OB_ERROR_TIME_STAMP, "Wrong timestamp");
    ADD_ERROR_STR(OB_ERROR_INTRESECT, "Tablet range intersect");
    ADD_ERROR_STR(OB_ERROR_OUT_OF_RANGE, "Out of range");
    ADD_ERROR_STR(OB_RS_STATUS_INIT, "RootServer status init");
    ADD_ERROR_STR(OB_IMPORT_NOT_IN_SERVER, "Import not in server");
    ADD_ERROR_STR(OB_FIND_OUT_OF_RANGE, "Search out of range");
    ADD_ERROR_STR(OB_CONVERT_ERROR, "Convert error");
    ADD_ERROR_STR(OB_MS_ITER_END, "MS end of iteration");
    ADD_ERROR_STR(OB_MS_NOT_EXIST, "MS not exist");
    ADD_ERROR_STR(OB_BYPASS_TIMEOUT, "Bypass timeout");
    ADD_ERROR_STR(OB_BYPASS_DELETE_DONE, "Bypass delete done");
    ADD_ERROR_STR(OB_RS_STATE_NOT_ALLOW, "RootServer state error");
    ADD_ERROR_STR(OB_BYPASS_NEED_REPORT, "Bypass need report");
    ADD_ERROR_STR(OB_ROOT_TABLE_CHANGED, "Table changed");
    ADD_ERROR_STR(OB_ROOT_REPLICATE_NO_DEST_CS_FOUND, "No destination ChunkServer");
    ADD_ERROR_STR(OB_ROOT_TABLE_RANGE_NOT_EXIST, "Tablet range not exist");
    ADD_ERROR_STR(OB_ROOT_MIGRATE_CONCURRENCY_FULL, "Migrate concurrency full");
    ADD_ERROR_STR(OB_ROOT_MIGRATE_INFO_NOT_FOUND, "Migrate info not found");
    ADD_ERROR_STR(OB_NOT_DATA_LOAD_TABLE, "No data to load");
    ADD_ERROR_STR(OB_DATA_LOAD_TABLE_DUPLICATED, "Duplicated table data to load");
    ADD_ERROR_STR(OB_ROOT_TABLE_ID_EXIST, "Table ID exist");
    ADD_ERROR_STR(OB_DATA_SOURCE_NOT_EXIST, "Data source not exist");
    ADD_ERROR_STR(OB_DATA_SOURCE_TABLE_NOT_EXIST, "Data source table not exist");
    ADD_ERROR_STR(OB_DATA_SOURCE_RANGE_NOT_EXIST, "Data source range not exist");
    ADD_ERROR_STR(OB_DATA_SOURCE_DATA_NOT_EXIST, "Data source data not exist");
    ADD_ERROR_STR(OB_DATA_SOURCE_SYS_ERROR, "Data source sys error");
    ADD_ERROR_STR(OB_DATA_SOURCE_TIMEOUT, "Data source timeout");
    ADD_ERROR_STR(OB_DATA_SOURCE_CONCURRENCY_FULL, "Data source concurrency full");
    ADD_ERROR_STR(OB_DATA_SOURCE_WRONG_URI_FORMAT, "Data source wrong URI format");
    ADD_ERROR_STR(OB_SSTABLE_VERSION_UNEQUAL, "SSTable version not equal");
    ADD_ERROR_STR(OB_CLUSTER_ALREADY_MASTER, "Target cluster is already master");
    ADD_ERROR_STR(OB_IP_PORT_IS_NOT_SLAVE_CLUSTER, "Target cluster is not slave");
    ADD_ERROR_STR(OB_CLUSTER_IS_NOT_SLAVE, "Cluster is not slave");
    ADD_ERROR_STR(OB_CLUSTER_IS_NOT_MASTER, "Cluster is not master");
    ADD_ERROR_STR(OB_CONFIG_NOT_SYNC, "Config not sync");
    ADD_ERROR_STR(OB_IP_PORT_IS_NOT_CLUSTER, "Target cluster not exist");
    ADD_ERROR_STR(OB_MASTER_CLUSTER_NOT_EXIST, "Master cluster not exist");
    ADD_ERROR_STR(OB_GET_CLUSTER_MASTER_UPS_FAILED, "Fetch master cluster ups list fail");
    ADD_ERROR_STR(OB_MULTIPLE_MASTER_CLUSTERS_EXIST, "Multiple master clusters exist");
    ADD_ERROR_STR(OB_MASTER_CLUSTER_ALREADY_EXISTS, "Master cluster already exist");
    ADD_ERROR_STR(OB_BROADCAST_MASTER_CLUSTER_ADDR_FAIL, "Broadcast master cluster address fail");
	//add pangtianze [Paxos] 20170523:b
    ADD_ERROR_STR(OB_UPS_TIMEOUT, "Master ups timeout, maybe ups master is busy or changing");
    //add:e

    //error code for merge server -4000 ---- -5000
    ADD_ERROR_STR(OB_INNER_STAT_ERROR, "Inner state error");     // inner stat check error
    ADD_ERROR_STR(OB_OLD_SCHEMA_VERSION, "Schema version too old");   // find old schema version
    ADD_ERROR_STR(OB_INPUT_PARAM_ERROR, "Input parameter error");    // check input param error
    ADD_ERROR_STR(OB_NO_EMPTY_ENTRY, "No empty entry");       // not find empty entry
    ADD_ERROR_STR(OB_RELEASE_SCHEMA_ERROR, "Release schema error"); // release schema error
    ADD_ERROR_STR(OB_ITEM_COUNT_ERROR, "Item count error");     // fullfill item count error
    ADD_ERROR_STR(OB_OP_NOT_ALLOW, "Operation not allowed now");         // fetch new schema not allowed
    ADD_ERROR_STR(OB_CHUNK_SERVER_ERROR, "ChunkServer error");   // chunk server cached is error
    ADD_ERROR_STR(OB_NO_NEW_SCHEMA, "No new schema");        // no new schema when parse error
    ADD_ERROR_STR(OB_MS_SUB_REQ_TOO_MANY, "Too many scan request"); // too many sub scan request
    ADD_ERROR_STR(OB_TOO_MANY_BLOOM_FILTER_TASK, "too many bloomfilter task");

    // SQL specific error code, -5000 ~ -6000
    ADD_ERROR_STR(OB_ERR_PARSER_INIT, "Failed to init SQL parser");
    ADD_ERROR_STR(OB_ERR_PARSE_SQL, "Parse error");
    ADD_ERROR_STR(OB_ERR_RESOLVE_SQL, "Resolve error");
    ADD_ERROR_STR(OB_ERR_GEN_PLAN, "Generate plan error");
    ADD_ERROR_STR(OB_ERR_UNKNOWN_SYS_FUNC, "Unknown system function");
    ADD_ERROR_STR(OB_ERR_PARSER_MALLOC_FAILED, "Parser malloc error");
    ADD_ERROR_STR(OB_ERR_PARSER_SYNTAX, "Syntax error");
    ADD_ERROR_STR(OB_ERR_COLUMN_SIZE, "Wrong number of columns");
    ADD_ERROR_STR(OB_ERR_COLUMN_DUPLICATE, "Duplicated column");
    ADD_ERROR_STR(OB_ERR_COLUMN_UNKNOWN, "Unknown column");
    ADD_ERROR_STR(OB_ERR_OPERATOR_UNKNOWN, "Unknown operator");
    ADD_ERROR_STR(OB_ERR_STAR_DUPLICATE, "Duplicated star");
    ADD_ERROR_STR(OB_ERR_ILLEGAL_ID, "Illegal ID");
    ADD_ERROR_STR(OB_ERR_WRONG_POS, "Invalid position");
    ADD_ERROR_STR(OB_ERR_ILLEGAL_VALUE, "Illegal value");
    ADD_ERROR_STR(OB_ERR_COLUMN_AMBIGOUS, "Ambigous column");
    ADD_ERROR_STR(OB_ERR_LOGICAL_PLAN_FAILD, "Generate logical plan error");
    ADD_ERROR_STR(OB_ERR_SCHEMA_UNSET, "Schema not set");
    ADD_ERROR_STR(OB_ERR_ILLEGAL_NAME, "Illegal name");
    ADD_ERROR_STR(OB_ERR_TABLE_UNKNOWN, "Unknown table");
    ADD_ERROR_STR(OB_ERR_TABLE_DUPLICATE, "Duplicated table");
    ADD_ERROR_STR(OB_ERR_NAME_TRUNCATE, "Name truncated");
    ADD_ERROR_STR(OB_ERR_EXPR_UNKNOWN, "Unknown expression");
    ADD_ERROR_STR(OB_ERR_ILLEGAL_TYPE, "Illegal type");
    ADD_ERROR_STR(OB_ERR_PRIMARY_KEY_DUPLICATE, "Duplicated primary key");
    ADD_ERROR_STR(OB_ERR_ALREADY_EXISTS, "Already exist");
    ADD_ERROR_STR(OB_ERR_CREATETIME_DUPLICATE, "Duplicated createtime");
    ADD_ERROR_STR(OB_ERR_MODIFYTIME_DUPLICATE, "Duplicated modifytime");
    ADD_ERROR_STR(OB_ERR_ILLEGAL_INDEX, "Illegal index");
    ADD_ERROR_STR(OB_ERR_INVALID_SCHEMA, "Invalid schema");
    ADD_ERROR_STR(OB_ERR_INSERT_NULL_ROWKEY, "Insert null rowkey"); // SQLSTATE '23000'
    ADD_ERROR_STR(OB_ERR_COLUMN_NOT_FOUND, "Column not found");   // SQLSTATE '42S22'
    ADD_ERROR_STR(OB_ERR_DELETE_NULL_ROWKEY, "Delete null rowkey");
    ADD_ERROR_STR(OB_ERR_INSERT_INNER_JOIN_COLUMN, "Insert inner join column error");
    ADD_ERROR_STR(OB_ERR_USER_EMPTY, "No user");
    ADD_ERROR_STR(OB_ERR_USER_NOT_EXIST, "User not exist");
    //add liumz, [multi_database.priv_management]:20150608:b
    ADD_ERROR_STR(OB_ERR_SESSION_UNSET, "Session not set");
    //add:e
    //add wenghaixing [database manage]20150610
    ADD_ERROR_STR(OB_ERR_DATABASE_NOT_EXSIT, "Database not exist");
    ADD_ERROR_STR(OB_ERR_NO_ACCESS_TO_DATABASE, "Have no access to database");
    ADD_ERROR_STR(OB_ERR_STILL_HAS_TABLE_IN_DATABASE,"Still has table in database...");
    ADD_ERROR_STR(OB_ERR_DROP_DEFAULT_DB,"Default db can not be dropped!!!");
    //add e
    //add dragon [varchar limit] 2016-8-15 16:18:13
    ADD_ERROR_STR(OB_ERR_STMT_COL_NOT_FOUND, "Cannot find column in the statement structure");
    ADD_ERROR_STR(OB_ERR_VARCHAR_TOO_LONG, "Varchar is too long");
    ADD_ERROR_STR(OB_ERR_NULL_POINTER, "Use nullptr");
    //add e
    /*add by wanglei [semi join] 20151106*/
    ADD_ERROR_STR(OB_ERR_CAN_NOT_USE_SEMI_JOIN,"can't use semi join,change your join type,retry again!");
    ADD_ERROR_STR(OB_ERR_HAS_NO_EQUI_COND,"can not use semi join,check your on expression!");
    ADD_ERROR_STR(OB_ERR_INDEX_NUM_IS_ZERO,"filter is null, empty set!");
    ADD_ERROR_STR(OB_ERR_POINTER_IS_NULL,"semi join:null pointer!");
    ADD_ERROR_STR(OB_ERR_FUNC_DEV,"semi join:function is on the way!");
    //add:e
    ADD_ERROR_STR(OB_ERR_NO_PRIVILEGE, "No privilege");
    ADD_ERROR_STR(OB_ERR_NO_AVAILABLE_PRIVILEGE_ENTRY, "No privilege entry");
    ADD_ERROR_STR(OB_ERR_WRONG_PASSWORD, "Incorrect password");
    ADD_ERROR_STR(OB_ERR_USER_IS_LOCKED, "User locked");
    ADD_ERROR_STR(OB_ERR_UPDATE_ROWKEY_COLUMN, "Can not update rowkey column");
    ADD_ERROR_STR(OB_ERR_UPDATE_JOIN_COLUMN, "Can not update join column");
    ADD_ERROR_STR(OB_ERR_INVALID_COLUMN_NUM, "Invalid column number"); // SQLSTATE 'S1002'
    ADD_ERROR_STR(OB_ERR_PREPARE_STMT_UNKNOWN, "Unknown prepared statement"); // SQLSTATE 'HY007'
    ADD_ERROR_STR(OB_ERR_VARIABLE_UNKNOWN, "Unknown variable");
    ADD_ERROR_STR(OB_ERR_SESSION_INIT, "Init session error");
    ADD_ERROR_STR(OB_ERR_OLDER_PRIVILEGE_VERSION, "Older privilege version");
    ADD_ERROR_STR(OB_ERR_LACK_OF_ROWKEY_COL, "No rowkey column specified");
    ADD_ERROR_STR(OB_ERR_EXCLUSIVE_LOCK_CONFLICT, "Exclusive lock conflict");
    ADD_ERROR_STR(OB_ERR_SHARED_LOCK_CONFLICT, "Shared lock conflict");
    ADD_ERROR_STR(OB_ERR_USER_EXIST, "User exists");
    ADD_ERROR_STR(OB_ERR_PASSWORD_EMPTY, "Empty password");
    ADD_ERROR_STR(OB_ERR_GRANT_PRIVILEGES_TO_CREATE_TABLE, "Failed to grant privelege");
    ADD_ERROR_STR(OB_ERR_WRONG_DYNAMIC_PARAM, "Wrong dynamic parameters"); // SQLSTATE '07001'
    ADD_ERROR_STR(OB_ERR_PARAM_SIZE, "Wrong number of parameters");
    ADD_ERROR_STR(OB_ERR_FUNCTION_UNKNOWN, "Unknown function");
    ADD_ERROR_STR(OB_ERR_CREAT_MODIFY_TIME_COLUMN, "CreateTime or ModifyTime column cannot be modified");
    ADD_ERROR_STR(OB_ERR_MODIFY_PRIMARY_KEY, "Primary key cannot be modified");

    //add qianzm [set_operation] 20151222 :b
    ADD_ERROR_STR(OB_ERR_COLUMN_TYPE_NOT_COMPATIBLE, "The data types of some columns are not compatible");
    //add :e

    //add peioy [Alter_Rename] [JHOBv0.1] 20150210:b
    ADD_ERROR_STR(OB_ERR_TABLE_NAME_LENGTH, "New table name is too long");
    //add 20150210:e

    ADD_ERROR_STR(OB_ERR_PARAM_DUPLICATE, "Duplicated parameters");
    ADD_ERROR_STR(OB_ERR_TOO_MANY_SESSIONS, "Too many sessions");
    ADD_ERROR_STR(OB_ERR_TRANS_ALREADY_STARTED, "Transaction already started");
    ADD_ERROR_STR(OB_ERR_TOO_MANY_PS, "Too many prepared statements");
    ADD_ERROR_STR(OB_ERR_NOT_IN_TRANS, "Not in transaction");
    ADD_ERROR_STR(OB_ERR_HINT_UNKNOWN, "Unknown hint");
    ADD_ERROR_STR(OB_ERR_WHEN_UNSATISFIED, "When condition not satisfied");
    ADD_ERROR_STR(OB_ERR_QUERY_INTERRUPTED, "Query interrupted");
    ADD_ERROR_STR(OB_ERR_SESSION_INTERRUPTED, "Session interrupted");
    ADD_ERROR_STR(OB_ERR_UNKNOWN_SESSION_ID, "Unknown session ID");
    ADD_ERROR_STR(OB_ERR_PROTOCOL_NOT_RECOGNIZE, "Incorrect protocol");
    ADD_ERROR_STR(OB_ERR_WRITE_AUTH_ERROR, "Write auth packet error");

    ADD_ERROR_STR(OB_ERR_PS_TOO_MANY_PARAM, "Prepared statement contains too many placeholders");
    ADD_ERROR_STR(OB_ERR_READ_ONLY, "The server is read only now");
    //add lbzhong [Update rowkey] 20160419:b
    ADD_ERROR_STR(OB_ERR_PRIMARY_KEY_CONFLICT, "Update primary key conflict, please try again");
    //add:e
  //add peiouya [NotNULL_check] [JHOBv0.1] 20140306:b
  ADD_ERROR_STR(OB_ERR_VARIABLE_NULL, "The bind parameter variable can not be null");
  //add  20140306:e
  //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140606:b
    ADD_ERROR_STR(OB_ERR_SUB_QUERY_OVERFLOW, "too many sub_query,not support well now");
    ADD_ERROR_STR(OB_ERR_SUB_QUERY_RESULTSET_EXCESSIVE, "Subquery returns more than 1 row");
    //add 20140606:e
    //add liuxiao[secondary index]20150609
    ADD_ERROR_STR(OB_INDEX_NOT_EXIST, "no index to drop!");
    //add e
    ADD_ERROR_STR(OB_NOT_ENOUGH_SLAVE, "No enough UPS slaves in cluster");//add by wangdonghui [paxos ups_replication] 20160929 :b:e
    ADD_ERROR_STR(OB_HAVNT_DONE, "Processing time more than wait time, please check the answer after");//add by wangdonghui [paxos ups_replication] 20160929 :b:e
    // Fatal errors and the client should close the connection, -8000 ~ -8999
    ADD_ERROR_STR(OB_ERR_SERVER_IN_INIT, "Server is initializing");
    //add peiouya [DATE_TIME] 20150914:b
    ADD_ERROR_STR(OB_VALUE_OUT_OF_DATES_RANGE, "result value is not within the valid range of dates.");
    ADD_ERROR_STR(OB_ERR_TIMESTAMP_TYPE_FORMAT, "Format of a TIMESTAMP value is incorrect");//add liuzy [datetime bug] 20150928
    ADD_ERROR_STR(OB_ERR_DATE_TYPE_FORMAT, "Format of a DATE value is incorrect");          //add liuzy [datetime bug] 20150928
    ADD_ERROR_STR(OB_ERR_TIME_TYPE_FORMAT, "Format of a TIME value is incorrect");          //add liuzy [datetime bug] 20150928
    ADD_ERROR_STR(OB_ERR_VALUE_OF_DATE, "Value of DATE is out of the range");               //add liuzy [datetime bug] 20150928
    ADD_ERROR_STR(OB_ERR_VALUE_OF_TIME, "Value of TIME is out of the range");               //add liuzy [datetime bug] 20150928
    //add 20150914:e
    // add by maosy [Delete_Update_Function_for_snpshot]  20161212
    ADD_ERROR_STR(OB_ERR_BATCH_EMPTY_ROW, "batch delete or update is empty row");
    // add by maosy e
  }
} local_init;

namespace oceanbase
{
  namespace common
  {
    const char* ob_strerror(int err)
    {
      const char* ret = "Unknown error";
      if (OB_LIKELY(0 >= err && err > -OB_MAX_ERROR_CODE))
      {
        ret = STR_ERROR[-err];
        if (OB_UNLIKELY(NULL == ret))
        {
          ret = "Unknown Error";
        }
      }
      return ret;
    }
  } // end namespace common
} // end namespace oceanbase
