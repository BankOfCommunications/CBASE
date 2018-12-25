/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_extra_tables_schema.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *   zhidong sun <xielun.szd@alipay.com>
 *
 */

#include "ob_schema_macro_define.h"
#include "ob_extra_tables_schema.h"
#include "common/roottable/ob_first_tablet_entry_schema.h"
#include "sql/ob_sequence_define.h"//add lijianqiang [sequence]20150608
using namespace oceanbase::common;

////////////////////////////////////////////////////////////////////////////
//                        OCEANBASE CORE TABLES                           //
////////////////////////////////////////////////////////////////////////////
int ObExtraTablesSchema::first_tablet_entry_schema(TableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  strcpy(table_schema.table_name_, FIRST_TABLET_TABLE_NAME);
  table_schema.table_id_ = OB_FIRST_TABLET_ENTRY_TID;
  table_schema.table_type_ = TableSchema::NORMAL;
  table_schema.load_type_ = TableSchema::DISK;
  table_schema.table_def_type_ = TableSchema::INTERNAL;
  //modify dolphin [datbase manager]@20150609:b
  //table_schema.rowkey_column_num_ = 1;
  table_schema.rowkey_column_num_ = 2;
  //modify:e
  //add zhaoqiong[roottable tablet management]20150302:b
  //table_schema.replica_num_ = OB_SAFE_COPY_COUNT;
  table_schema.replica_num_ = OB_MAX_COPY_COUNT;
  // add e
  // @TODO
  //modify dolphin [database manager]@20150609
  //table_schema.max_used_column_id_ = first_tablet_entry_cid::SCHEMA_VERSION_ID;
  table_schema.max_used_column_id_ = first_tablet_entry_cid::DBNAME;
  //modify:e
  table_schema.create_mem_version_ = 1;
  table_schema.max_rowkey_length_ = OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH;//add dolphin [database manager]@20150609
  strncpy(table_schema.compress_func_name_, OB_DEFAULT_COMPRESS_FUNC_NAME, OB_MAX_TABLE_NAME_LENGTH);
  table_schema.is_use_bloomfilter_ = false;
  table_schema.is_pure_update_table_ = false;
  table_schema.rowkey_split_ = OB_MAX_TABLE_NAME_LENGTH;
  table_schema.merge_write_sstable_version_ = TableSchema::DEFAULT_SSTABLE_VERSION;
  table_schema.create_time_column_id_ = OB_CREATE_TIME_COLUMN_ID;
  table_schema.modify_time_column_id_ = OB_MODIFY_TIME_COLUMN_ID;

  ADD_COLUMN_SCHEMA("table_name", //column_name
      first_tablet_entry_cid::TNAME, //column_id
      1, //rowkey_id
      ObVarcharType,  //column_type
      OB_MAX_TABLE_NAME_LENGTH, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("create_time_column_id", //column_name
      first_tablet_entry_cid::CREATE_TIME_COLUMN_ID, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("modify_time_column_id", //column_name
      first_tablet_entry_cid::MODIFY_TIME_COLUMN_ID, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("table_id", //column_name
      first_tablet_entry_cid::TID, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("table_type", //column_name
      first_tablet_entry_cid::TABLE_TYPE, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("load_type", //column_name
      first_tablet_entry_cid::LOAD_TYPE, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("table_def_type", //column_name
      first_tablet_entry_cid::TABLE_DEF_TYPE, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("rowkey_column_num", //column_name
      first_tablet_entry_cid::ROWKEY_COLUMN_NUM, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("column_num", //column_name
      first_tablet_entry_cid::COLUMN_NUM, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("max_used_column_id", //column_name
      first_tablet_entry_cid::MAX_USED_COLUMN_ID, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("replica_num", //column_name
      first_tablet_entry_cid::REPLICA_NUM, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("create_mem_version", //column_name
      first_tablet_entry_cid::CREATE_MEM_VERSION, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("tablet_max_size", //column_name
      first_tablet_entry_cid::TABLET_MAX_SIZE, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("max_rowkey_length", //column_name
      first_tablet_entry_cid::MAX_ROWKEY_LENGTH_ID, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("compress_func_name", //column_name
      first_tablet_entry_cid::COMPRESS_FUNC_NAME_ID, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      OB_MAX_TABLE_NAME_LENGTH, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("is_use_bloomfilter", //column_name
      first_tablet_entry_cid::USE_BLOOMFILTER_ID, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  /*这个column的实际含义和名字不符，为了兼容，取这个名字，实际含义是consistency level*/
  ADD_COLUMN_SCHEMA("is_read_static", //column_name
      first_tablet_entry_cid::READ_STATIC_ID, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("merge_write_sstable_version", //column_name
      first_tablet_entry_cid::MERGE_WRITE_SSTABLE_VERSION, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("is_pure_update_table", //column_name
      first_tablet_entry_cid::PURE_UPDATE_TABLE_ID, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("rowkey_split", //column_name
      first_tablet_entry_cid::ROWKEY_SPLIT_ID, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      true); //is nullable
  ADD_COLUMN_SCHEMA("expire_condition", //column_name
      first_tablet_entry_cid::EXPIRE_CONDITION_ID, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      OB_MAX_EXPIRE_CONDITION_LENGTH, //column length
      true); //is nullable
  /*
   * 暂时去掉这个列，以和0.4.1的schema兼容
  ADD_COLUMN_SCHEMA("comment_str",
      first_tablet_entry_cid::COMMENT_STR_ID, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      OB_MAX_TABLE_COMMENT_LENGTH, //column length
      true); //is nullable
      */
  ADD_COLUMN_SCHEMA("schema_version",
      first_tablet_entry_cid::SCHEMA_VERSION_ID, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      true); //is nullable
  ADD_COLUMN_SCHEMA("tablet_block_size", //column_name
      first_tablet_entry_cid::SSTABLE_BLOCK_SIZE_ID, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  //add dolphin [database manager]@20150609
  ADD_COLUMN_SCHEMA("db_name", //column_name
      first_tablet_entry_cid::DBNAME, //column_id
      2, //rowkey_id
      ObVarcharType,  //column_type
      OB_MAX_DATBASE_NAME_LENGTH, //column length
      true); //is nullable
  //add:e
  ADD_COLUMN_SCHEMA("gm_create", //column_name
      OB_CREATE_TIME_COLUMN_ID, //column_id
      0, //rowkey_id
      ObCreateTimeType,  //column_type
      sizeof(ObCreateTime), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("gm_modify", //column_name
      OB_MODIFY_TIME_COLUMN_ID, //column_id
      0, //rowkey_id
      ObModifyTimeType,  //column_type
      sizeof(ObModifyTime), //column length
      false); //is nullable
 // add wenghaixing [secondary index ] 20141105
  /*
   * 修改了schema和内部表的映射方案，不添加内部表，而是在这张schema表里添加两个字段
   **/
  ADD_COLUMN_SCHEMA("data_table_id",
                    first_tablet_entry_cid::TABLE_ID_COLUMN_ID,
                    0,
                    ObIntType,
                    sizeof(int64_t),
                    false
                    );
  ADD_COLUMN_SCHEMA("index_status",
                    first_tablet_entry_cid::INDEX_STATUS_ID,
                    0,
                    ObIntType,
                    sizeof(int64_t),
                    false
                    );
  //add e
  return ret;
}

int ObExtraTablesSchema::all_all_column_schema(TableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  strcpy(table_schema.table_name_, OB_ALL_COLUMN_TABLE_NAME);
  table_schema.table_id_ = OB_ALL_ALL_COLUMN_TID;
  table_schema.table_type_ = TableSchema::NORMAL;
  table_schema.load_type_ = TableSchema::DISK;
  table_schema.table_def_type_ = TableSchema::INTERNAL;
  table_schema.rowkey_column_num_ = 2;
  //add zhaoqiong[roottable tablet management]20150302:b
  //table_schema.replica_num_ = OB_SAFE_COPY_COUNT;
  table_schema.replica_num_ = OB_MAX_COPY_COUNT;
  // add e
  table_schema.max_used_column_id_ = OB_ALL_JOIN_INFO_MAX_COLUMN_ID;
  table_schema.create_mem_version_ = 1;
  table_schema.max_rowkey_length_ = OB_MAX_COLUMN_NAME_LENGTH + sizeof(int64_t);

  strcpy(table_schema.compress_func_name_, OB_DEFAULT_COMPRESS_FUNC_NAME);
  table_schema.is_use_bloomfilter_ = false;
  table_schema.is_pure_update_table_ = false;
  table_schema.rowkey_split_ = 0;
  table_schema.merge_write_sstable_version_ = TableSchema::DEFAULT_SSTABLE_VERSION;
  table_schema.create_time_column_id_ = OB_CREATE_TIME_COLUMN_ID;
  table_schema.modify_time_column_id_ = OB_MODIFY_TIME_COLUMN_ID;

  int column_id = OB_APP_MIN_COLUMN_ID;
  ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id ++, //column_id
      1, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("column_name", //column_name
      column_id ++, //column_id
      2, //rowkey_id
      ObVarcharType,  //column_type
      OB_MAX_COLUMN_NAME_LENGTH, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("table_name", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      OB_MAX_TABLE_NAME_LENGTH, //column length
      true); //is nullable
  ADD_COLUMN_SCHEMA("column_id", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("column_group_id", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("rowkey_id", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("length_in_rowkey", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("order_in_rowkey", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int32_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("join_table_id", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("join_column_id", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("data_type", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("data_length", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("data_precision", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("data_scale", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("nullable", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("gm_create", //column_name
      OB_CREATE_TIME_COLUMN_ID, //column_id
      0, //rowkey_id
      ObCreateTimeType,  //column_type
      sizeof(ObCreateTime), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("gm_modify", //column_name
      OB_MODIFY_TIME_COLUMN_ID, //column_id
      0, //rowkey_id
      ObModifyTimeType,  //column_type
      sizeof(ObModifyTime), //column length
      false); //is nullable
  return ret;
}

int ObExtraTablesSchema::all_join_info_schema(TableSchema& table_schema)
{
  int ret = OB_SUCCESS;

  strcpy(table_schema.table_name_, OB_ALL_JOININFO_TABLE_NAME);
  table_schema.table_id_ = OB_ALL_JOIN_INFO_TID;
  table_schema.table_type_ = TableSchema::NORMAL;
  table_schema.load_type_ = TableSchema::DISK;
  table_schema.table_def_type_ = TableSchema::INTERNAL;
  table_schema.rowkey_column_num_ = 4;
  //add zhaoqiong[roottable tablet management]20150302:b
  //table_schema.replica_num_ = OB_SAFE_COPY_COUNT;
  table_schema.replica_num_ = OB_MAX_COPY_COUNT;
  // add e
  table_schema.max_used_column_id_ = OB_ALL_ALL_COLUMN_MAX_COLUMN_ID;
  table_schema.create_mem_version_ = 1;
  table_schema.max_rowkey_length_ = 4 * sizeof(int64_t);

  strcpy(table_schema.compress_func_name_, OB_DEFAULT_COMPRESS_FUNC_NAME);
  table_schema.is_use_bloomfilter_ = false;
  table_schema.is_pure_update_table_ = false;
  table_schema.rowkey_split_ = 0;
  table_schema.merge_write_sstable_version_ = TableSchema::DEFAULT_SSTABLE_VERSION;
  table_schema.create_time_column_id_ = OB_CREATE_TIME_COLUMN_ID;
  table_schema.modify_time_column_id_ = OB_MODIFY_TIME_COLUMN_ID;

  int column_id = OB_APP_MIN_COLUMN_ID;
  ADD_COLUMN_SCHEMA("left_table_id", //column_name
      column_id ++, //column_id
      1, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("left_column_id", //column_name
      column_id ++, //column_id
      2, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("right_table_id", //column_name
      column_id ++, //column_id
      3, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("right_column_id", //column_name
      column_id ++, //column_id
      4, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  // WARNING: names belowing must be nullable
  ADD_COLUMN_SCHEMA("left_table_name", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      OB_MAX_TABLE_NAME_LENGTH, //column length
      true); //is nullable
  ADD_COLUMN_SCHEMA("left_column_name", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      OB_MAX_TABLE_NAME_LENGTH, //column length
      true); //is nullable
  ADD_COLUMN_SCHEMA("right_table_name", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      OB_MAX_TABLE_NAME_LENGTH, //column length
      true); //is nullable
  ADD_COLUMN_SCHEMA("right_column_name", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      OB_MAX_TABLE_NAME_LENGTH, //column length
      true); //is nullable
  ADD_COLUMN_SCHEMA("gm_create", //column_name
      OB_CREATE_TIME_COLUMN_ID, //column_id
      0, //rowkey_id
      ObCreateTimeType,  //column_type
      sizeof(ObCreateTime), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("gm_modify", //column_name
      OB_MODIFY_TIME_COLUMN_ID, //column_id
      0, //rowkey_id
      ObModifyTimeType,  //column_type
      sizeof(ObModifyTime), //column length
      false); //is nullable
  return ret;
}

//add zhaoqiong [Schema Manager] 20150327:b
int ObExtraTablesSchema::all_ddl_operation(TableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_ALL_DDL_OPERATION_NAME);
  table_schema.table_id_ = OB_DDL_OPERATION_TID;
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + 2;
  table_schema.rowkey_column_num_ = 1;
  table_schema.max_rowkey_length_ = sizeof(int64_t);

  int column_id = OB_APP_MIN_COLUMN_ID;
  ADD_COLUMN_SCHEMA("schema_version", //column_name
      column_id ++, //column_id
      1, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("operation_type", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("gm_create", //column_name
      OB_CREATE_TIME_COLUMN_ID,//column_id
      0, //rowkey_id
      ObCreateTimeType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("gm_modify", //column_name
      OB_MODIFY_TIME_COLUMN_ID, //column_id
      0, //rowkey_id
      ObModifyTimeType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  return ret;
}
//add:e
////////////////////////////////////////////////////////////////////////////
//                        OCEANBASE SYSTEM TABLES                         //
////////////////////////////////////////////////////////////////////////////
int ObExtraTablesSchema::all_cluster_schema(TableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_ALL_CLUSTER);
  table_schema.table_id_ = OB_ALL_CLUSTER_TID;
  table_schema.rowkey_column_num_ = 1;
  table_schema.max_rowkey_length_ = sizeof(int64_t);
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + 8;

  table_schema.create_time_column_id_ = OB_CREATE_TIME_COLUMN_ID;
  table_schema.modify_time_column_id_ = OB_MODIFY_TIME_COLUMN_ID;

  int column_id = OB_APP_MIN_COLUMN_ID;
  ADD_COLUMN_SCHEMA("cluster_id",
      column_id++,
      1,
      ObIntType,
      sizeof (int64_t),
      false);
  ADD_COLUMN_SCHEMA("cluster_vip",
      column_id++,
      0,
      ObVarcharType,
      SERVER_IP_LENGTH,
      false);
  ADD_COLUMN_SCHEMA("cluster_port",
      column_id++,
      0,
      ObIntType,
      sizeof (int64_t),
      false);
  ADD_COLUMN_SCHEMA("cluster_role",
      column_id++,
      0,
      ObIntType,
      sizeof (int64_t),
      false);
  ADD_COLUMN_SCHEMA("cluster_name",
      column_id++,
      0,
      ObVarcharType,
      OB_MAX_CLUSTER_NAME,
      false);
  ADD_COLUMN_SCHEMA("cluster_info",
      column_id++,
      0,
      ObVarcharType,
      OB_MAX_CLUSTER_INFO,
      false);
  ADD_COLUMN_SCHEMA("cluster_flow_percent",
      column_id++,
      0,
      ObIntType,
      sizeof (int64_t),
      false);
  ADD_COLUMN_SCHEMA("read_strategy",
      column_id++,
      0,
      ObIntType,
      sizeof (int64_t),
      false);
  ADD_COLUMN_SCHEMA("rootserver_port",
      column_id++,
      0,
      ObIntType,
      sizeof(int64_t),
      false);

  ADD_COLUMN_SCHEMA("gm_create", //column_name
      OB_CREATE_TIME_COLUMN_ID,//column_id
      0, //rowkey_id
      ObCreateTimeType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("gm_modify", //column_name
      OB_MODIFY_TIME_COLUMN_ID, //column_id
      0, //rowkey_id
      ObModifyTimeType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  return ret;
}

int ObExtraTablesSchema::all_client_schema(TableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_ALL_CLIENT);
  table_schema.table_id_ = OB_ALL_CLIENT_TID;
  table_schema.rowkey_column_num_ = 2;
  table_schema.max_rowkey_length_ = TEMP_ROWKEY_LENGTH;
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + 7;

  int column_id = OB_APP_MIN_COLUMN_ID;
  table_schema.create_time_column_id_ = OB_CREATE_TIME_COLUMN_ID;
  table_schema.modify_time_column_id_ = OB_MODIFY_TIME_COLUMN_ID;

  ADD_COLUMN_SCHEMA("client_ip",
      column_id++,
      1,
      ObVarcharType,
      SERVER_IP_LENGTH,
      false);
  ADD_COLUMN_SCHEMA("version",
      column_id++,
      2,
      ObVarcharType,
      SERVER_TYPE_LENGTH,
      false);
  ADD_COLUMN_SCHEMA("status",
      column_id++,
      0,
      ObVarcharType,
      SERVER_IP_LENGTH,
      false);
  ADD_COLUMN_SCHEMA("extra1",
      column_id++,
      0,
      ObVarcharType,
      SERVER_IP_LENGTH,
      false);
  ADD_COLUMN_SCHEMA("extra2",
      column_id++,
      0,
      ObIntType,
      sizeof (int64_t),
      false);
  ADD_COLUMN_SCHEMA("gm_create",
      OB_CREATE_TIME_COLUMN_ID,
      0,
      ObCreateTimeType,
      sizeof(int64_t),
      false);
  ADD_COLUMN_SCHEMA("gm_modify",
      OB_MODIFY_TIME_COLUMN_ID,
      0,
      ObModifyTimeType,
      sizeof(int64_t),
      false);
  return ret;
}

//add zhaoqiong [Truncate Table]:20160318:b
int ObExtraTablesSchema::all_truncate_op_info(TableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_TRUNCATE_OP_TABLE_NAME);
  table_schema.table_id_ = OB_ALL_TRUNCATE_OP_TID;
  table_schema.rowkey_column_num_ = 2;
  table_schema.max_rowkey_length_ = TEMP_ROWKEY_LENGTH;
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + 7;

  int column_id = OB_APP_MIN_COLUMN_ID;

  ADD_COLUMN_SCHEMA("rs_trun_time",
      column_id++,
      1,
      ObPreciseDateTimeType,
      sizeof(int64_t),
      false);
  ADD_COLUMN_SCHEMA("table_id",
      column_id++,
      2,
      ObIntType,
      sizeof(int64_t),
      false);
  ADD_COLUMN_SCHEMA("table_name",
      column_id++,
      0,
      ObVarcharType,
      OB_MAX_TABLE_NAME_LENGTH+OB_MAX_DATBASE_NAME_LENGTH+1,
      false);
  ADD_COLUMN_SCHEMA("user_name",
      column_id++,
      0,
      ObVarcharType,
      OB_MAX_USERNAME_LENGTH,
      false);
  ADD_COLUMN_SCHEMA("info",
      column_id++,
      0,
      ObVarcharType,
      2 * OB_MAX_TABLE_NAME_LENGTH,
      false);
  return ret;
}
//add:e

//add lijianqiang [sequence] 20150324b:
int ObExtraTablesSchema::all_sequence_schema(TableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_ALL_SEQUENCE_TABLE_NAME);
  table_schema.table_id_ = OB_ALL_SEQUENCE_TID;

  table_schema.rowkey_column_num_ = 1;
  table_schema.max_rowkey_length_ = sizeof(int64_t);
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + (END_ID - NAME_ID);//13

  table_schema.create_time_column_id_ = OB_CREATE_TIME_COLUMN_ID;
  table_schema.modify_time_column_id_ = OB_MODIFY_TIME_COLUMN_ID;

//  int column_id = OB_APP_MIN_COLUMN_ID;
  ADD_COLUMN_SCHEMA(OB_SEQUENCE_NAME,//column_name
      NAME_ID,//column_id 16
      1,//rowkey_id
      ObVarcharType,//column_type
      OB_MAX_COLUMN_NAME_LENGTH,//column length 256
      false);//is nullable
  ADD_COLUMN_SCHEMA(OB_SEQUENCE_DATA_TYPE, //column_name
      DATA_TYPE_ID,//column_id 17
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA(OB_SEQUENCE_CURRENT_VALUE, //column_name
      CURRENT_VALUE_ID,//column_id 18
      0, //rowkey_id
      ObDecimalType,  //column_type
      OB_MAX_COLUMN_NAME_LENGTH, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA(OB_SEQUENCE_INCREMENT_BY, //column_name
      INC_BY_ID,//column_id 19
      0, //rowkey_id
      ObDecimalType,  //column_type
      OB_MAX_COLUMN_NAME_LENGTH, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA(OB_SEQUENCE_MIN_VALUE, //column_name
      MIN_VALUE_ID,//column_id 20
      0, //rowkey_id
      ObDecimalType,  //column_type
      OB_MAX_COLUMN_NAME_LENGTH, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA(OB_SEQUENCE_MAX_VALUE, //column_name
      MAX_VALUE_ID,//column_id 21
      0, //rowkey_id
      ObDecimalType,  //column_type
      OB_MAX_COLUMN_NAME_LENGTH, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA(OB_SEQUENCE_IS_CYCLE, //column_name
      CYCLE_ID,//column_id 22
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA(OB_SEQUENCE_CACHE_NUM, //column_name
      CACHE_NUM_ID,//column_id 23
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA(OB_SEQUENCE_IS_ORDER, //column_name
      ORDER_ID,//column_id 24
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA(OB_SEQUENCE_IS_VALID, //column_name
      VALID_ID,//column_id 25
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA(OB_SEQUENCE_CONST_START_WITH, //column_name
      CONST_START_WITH_ID,//column_id 26
      0, //rowkey_id
      ObDecimalType,  //column_type
      OB_MAX_COLUMN_NAME_LENGTH, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA(OB_SEQUENCE_CAN_USE_PREVVAL, //column_name
      CAN_USE_PREVVAL_ID,//column_id 27
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA(OB_SEQUENCE_USE_QUICK_PATH, //column_name
      USE_QUICK_PATH_ID,//column_id 28
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable

  return ret;
}
//add 20150324:e

int ObExtraTablesSchema::all_server_schema(TableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_ALL_SERVER);
  table_schema.table_id_ = OB_ALL_SERVER_TID;
  table_schema.rowkey_column_num_ = 4;
  table_schema.max_rowkey_length_ = TEMP_ROWKEY_LENGTH;
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + 7;

  int column_id = OB_APP_MIN_COLUMN_ID;
  table_schema.create_time_column_id_ = OB_CREATE_TIME_COLUMN_ID;
  table_schema.modify_time_column_id_ = OB_MODIFY_TIME_COLUMN_ID;

  ADD_COLUMN_SCHEMA("cluster_id",
      column_id++,
      1,
      ObIntType,
      sizeof (int64_t),
      false);
  ADD_COLUMN_SCHEMA("svr_type",
      column_id++,
      2,
      ObVarcharType,
      SERVER_TYPE_LENGTH,
      false);
  ADD_COLUMN_SCHEMA("svr_ip",
      column_id++,
      3,
      ObVarcharType,
      SERVER_IP_LENGTH,
      false);
  ADD_COLUMN_SCHEMA("svr_port",
      column_id++,
      4,
      ObIntType,
      sizeof (int64_t),
      false);
  ADD_COLUMN_SCHEMA("inner_port",
      column_id++,
      0,
      ObIntType,
      sizeof (int64_t),
      false);
  ADD_COLUMN_SCHEMA("svr_role",
      column_id++,
      0,
      ObIntType,
      sizeof (int64_t),
      false);
  ADD_COLUMN_SCHEMA("svr_version",
      column_id++,
      0,
      ObVarcharType,
      OB_SERVER_VERSION_LENGTH,
      false);
  ADD_COLUMN_SCHEMA("gm_create", //column_name
      OB_CREATE_TIME_COLUMN_ID,//column_id
      0, //rowkey_id
      ObCreateTimeType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("gm_modify", //column_name
      OB_MODIFY_TIME_COLUMN_ID, //column_id
      0, //rowkey_id
      ObModifyTimeType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  return ret;
}

////////////////////////////////////////////////////////////////////////////
//                    OCEANBASE USER PRIVILEGE TABLES                     //
////////////////////////////////////////////////////////////////////////////
int ObExtraTablesSchema::all_user_schema(TableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  int column_id = OB_APP_MIN_COLUMN_ID;
  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_ALL_USER_TABLE_NAME);
  table_schema.table_id_ = OB_USERS_TID;
  table_schema.rowkey_column_num_ = 1;
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID+16;
  table_schema.max_rowkey_length_ = sizeof(int64_t);
  table_schema.rowkey_split_ = 0;

  ADD_COLUMN_SCHEMA("user_name", //column_name
      column_id ++, //column_id
      1, //rowkey_id
      ObVarcharType,  //column_type
      1024, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("user_id", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("pass_word", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      1024, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("info", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      1024, //column length
      true); //is nullable
  ADD_COLUMN_SCHEMA("priv_all", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_alter", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_create", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_create_user", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_delete", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_drop", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_grant_option", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_insert", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_update", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_select", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_replace", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("is_locked", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("gm_create", //column_name
      OB_CREATE_TIME_COLUMN_ID,//column_id
      0, //rowkey_id
      ObCreateTimeType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("gm_modify", //column_name
      OB_MODIFY_TIME_COLUMN_ID, //column_id
      0, //rowkey_id
      ObModifyTimeType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  return ret;
}

int ObExtraTablesSchema::all_table_privilege_schema(TableSchema &table_schema)
{
  // hard code
  int ret = OB_SUCCESS;

  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_ALL_TABLE_PRIVILEGE_TABLE_NAME);
  table_schema.table_id_ = OB_TABLE_PRIVILEGES_TID;
  table_schema.rowkey_column_num_ = 3;//modify dolphin [database manager]@20150609
  //modify dolphin [database manager]@20150609
  //table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID+13;
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID+14;
  //modify:e
  table_schema.max_rowkey_length_ = sizeof(int64_t);
  table_schema.rowkey_split_ = 0;
  int column_id = OB_APP_MIN_COLUMN_ID;

  ADD_COLUMN_SCHEMA("user_id", //column_name
      column_id ++, //column_id
      1, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  //modify dolphin [database manager]@20150609
  /**
  ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id ++, //column_id
      2, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
   */
  ADD_COLUMN_SCHEMA("table_id", //column_name
      column_id ++, //column_id
      3, //modify dolphin [database manager]@20150609
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  //modifye:e
  ADD_COLUMN_SCHEMA("priv_all", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_alter", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_create", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_create_user", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_delete", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_drop", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_grant_option", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_insert", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_update", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_select", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("priv_replace", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  //add dolphin [database manager]@20150609
  ADD_COLUMN_SCHEMA("db_id", //column_name
      column_id ++, //column_id
      2, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      true); //is nullable
  //add:e
  ADD_COLUMN_SCHEMA("gm_create", //column_name
      OB_CREATE_TIME_COLUMN_ID,//column_id
      0, //rowkey_id
      ObCreateTimeType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("gm_modify", //column_name
      OB_MODIFY_TIME_COLUMN_ID, //column_id
      0, //rowkey_id
      ObModifyTimeType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  return ret;
}

int ObExtraTablesSchema::all_trigger_event_schema(TableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_ALL_TRIGGER_EVENT_TABLE_NAME);
  table_schema.table_id_ = OB_TRIGGER_EVENT_TID;
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + 5;
  table_schema.rowkey_column_num_ = 1;
  table_schema.max_rowkey_length_ = sizeof(int64_t);

  int column_id = OB_APP_MIN_COLUMN_ID;
  ADD_COLUMN_SCHEMA("event_ts", //column_name
      column_id ++, //column_id
      1, //rowkey_id
      ObPreciseDateTimeType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("src_ip", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      SERVER_IP_LENGTH,
      false); //is nullable
  ADD_COLUMN_SCHEMA("event_type", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("event_param", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("extra", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      5000, //column length
      true); //is nullable
  ADD_COLUMN_SCHEMA("gm_create", //column_name
      OB_CREATE_TIME_COLUMN_ID,//column_id
      0, //rowkey_id
      ObCreateTimeType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("gm_modify", //column_name
      OB_MODIFY_TIME_COLUMN_ID, //column_id
      0, //rowkey_id
      ObModifyTimeType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  return ret;
}

int ObExtraTablesSchema::all_sys_stat_schema(TableSchema &table_schema)
{
  int ret = OB_SUCCESS;

  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_ALL_SYS_STAT_TABLE_NAME);
  table_schema.table_id_ = OB_ALL_SYS_STAT_TID;
  table_schema.rowkey_column_num_ = 2;
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + 4;
  table_schema.max_rowkey_length_ = OB_MAX_TABLE_NAME_LENGTH + sizeof(int64_t);
  strcpy(table_schema.compress_func_name_, OB_DEFAULT_COMPRESS_FUNC_NAME);
  table_schema.rowkey_split_ = 0;

  int column_id = OB_APP_MIN_COLUMN_ID;
  ADD_COLUMN_SCHEMA("cluster_id", //column_name
      column_id ++, //column_id
      1, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("name", //column_name
      column_id ++, //column_id
      2, //rowkey_id
      ObVarcharType,  //column_type
      OB_MAX_TABLE_NAME_LENGTH, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("data_type", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("value", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      OB_MAX_TABLE_NAME_LENGTH, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("info", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      OB_MAX_TABLE_NAME_LENGTH, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("gm_create", //column_name
      OB_CREATE_TIME_COLUMN_ID, //column_id
      0, //rowkey_id
      ObCreateTimeType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("gm_modify", //column_name
      OB_MODIFY_TIME_COLUMN_ID, //column_id
      0, //rowkey_id
      ObModifyTimeType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  return ret;
}

int ObExtraTablesSchema::all_sys_param_schema(TableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == (ret = all_sys_stat_schema(table_schema)))
  {
    strcpy(table_schema.table_name_, OB_ALL_SYS_PARAM_TABLE_NAME);
    table_schema.table_id_ = OB_ALL_SYS_PARAM_TID;
  }
  return ret;
}

int ObExtraTablesSchema::all_sys_config_schema(TableSchema &table_schema)
{
  int ret = OB_SUCCESS;

  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_ALL_SYS_CONFIG_TABLE_NAME);
  table_schema.table_id_ = OB_ALL_SYS_CONFIG_TID;
  table_schema.rowkey_column_num_ = 5;
  table_schema.max_used_column_id_ = OB_ALL_SYS_STAT_MAX_COLUMN_ID;
  table_schema.max_rowkey_length_ = TEMP_ROWKEY_LENGTH;
  strcpy(table_schema.compress_func_name_, OB_DEFAULT_COMPRESS_FUNC_NAME);
  table_schema.rowkey_split_ = 0;

  int column_id = OB_APP_MIN_COLUMN_ID;
  ADD_COLUMN_SCHEMA("cluster_id", //column_name
      column_id ++, //column_id
      1, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("svr_type", //column_name
      column_id ++, //column_id
      2, //rowkey_id
      ObVarcharType,  //column_type
      SERVER_TYPE_LENGTH,
      false); //is nullable
  ADD_COLUMN_SCHEMA("svr_ip", //column_name
      column_id ++, //column_id
      3, //rowkey_id
      ObVarcharType,  //column_type
      SERVER_IP_LENGTH,
      false); //is nullable
  ADD_COLUMN_SCHEMA("svr_port", //column_name
      column_id ++, //column_id
      4, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("name", //column_name
      column_id ++, //column_id
      5, //rowkey_id
      ObVarcharType,  //column_type
      OB_MAX_TABLE_NAME_LENGTH, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("section", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      OB_MAX_TABLE_NAME_LENGTH, //column length
      true); //is nullable
  ADD_COLUMN_SCHEMA("data_type", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      OB_MAX_TABLE_NAME_LENGTH, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("value", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      OB_MAX_TABLE_NAME_LENGTH, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("value_strict", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      2 * OB_MAX_TABLE_NAME_LENGTH, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("info", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      2 * OB_MAX_TABLE_NAME_LENGTH, //column length
      true); //is nullable
  ADD_COLUMN_SCHEMA("gm_create", //column_name
      OB_CREATE_TIME_COLUMN_ID, //column_id
      0, //rowkey_id
      ObCreateTimeType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("gm_modify", //column_name
      OB_MODIFY_TIME_COLUMN_ID, //column_id
      0, //rowkey_id
      ObModifyTimeType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  return ret;
}

int ObExtraTablesSchema::all_sys_config_stat_schema(TableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == (ret = all_sys_config_schema(table_schema)))
  {
    strcpy(table_schema.table_name_, OB_ALL_SYS_CONFIG_STAT_TABLE_NAME);
    table_schema.table_id_ = OB_ALL_SYS_CONFIG_STAT_TID;
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////
//                     OCEANBASE VIRTUAL TABLES                           //
////////////////////////////////////////////////////////////////////////////
int ObExtraTablesSchema::all_server_stat_schema(TableSchema & table_schema)
{
  int ret = OB_SUCCESS;

  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_ALL_SERVER_STAT_TABLE_NAME);
  table_schema.table_id_ = OB_ALL_SERVER_STAT_TID;
  table_schema.rowkey_column_num_ = 4;
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + 5;
  table_schema.max_rowkey_length_ = TEMP_ROWKEY_LENGTH;

  int column_id = OB_APP_MIN_COLUMN_ID;
  ADD_COLUMN_SCHEMA("svr_type", //column_name
      column_id ++, //column_id
      1, //rowkey_id
      ObVarcharType,  //column_type
      SERVER_TYPE_LENGTH, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("svr_ip", //column_name
      column_id ++, //column_id
      2, //rowkey_id
      ObVarcharType,  //column_type
      SERVER_IP_LENGTH, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("svr_port", //column_name
      column_id ++, //column_id
      3, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("name", //column_name
      column_id ++, //column_id
      4, //rowkey_id
      ObVarcharType,  //column_type
      64, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("value", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  return ret;
}

int ObExtraTablesSchema::all_server_session_schema(TableSchema & table_schema)
{
  int ret = OB_SUCCESS;

  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_ALL_SERVER_SESSION_TABLE_NAME);
  table_schema.table_id_ = OB_ALL_SERVER_SESSION_TID;
  table_schema.rowkey_column_num_ = 1;
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + 9;
  table_schema.max_rowkey_length_ = TEMP_ROWKEY_LENGTH;

/*
  |------------------------ columns same as mysql ----------------------------| extra column of Oceanbase|
  +-----+------+-----------+------+---------+------+-------+------------------+--------------+-----------+
  | Id  | User | Host      | db   | Command | Time | State | Info             | MergeServer  | Index     |
  +-----+------+-----------+------+---------+------+-------+------------------+--------------+-----------+
*/
 int column_id = OB_APP_MIN_COLUMN_ID;
  ADD_COLUMN_SCHEMA("id", //column_name
      column_id ++, //column_id
      1, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("username", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      512, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("host", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      128, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("db", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      128, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("command", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      1024, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("timeelapse", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("state", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      128, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("info", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      128, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("mergeserver", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      128, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("index", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  return ret;
}


int ObExtraTablesSchema::all_statement_schema(TableSchema & table_schema)
{
  int ret = OB_SUCCESS;

  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_ALL_STATEMENT_TABLE_NAME);
  table_schema.table_id_ = OB_ALL_STATEMENT_TID;
  table_schema.rowkey_column_num_ = 3;
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + 9;
  table_schema.max_rowkey_length_ = TEMP_ROWKEY_LENGTH;

 int column_id = OB_APP_MIN_COLUMN_ID;
  ADD_COLUMN_SCHEMA("svr_ip", //column_name
      column_id ++, //column_id
      1, //rowkey_id
      ObVarcharType,  //column_type
      SERVER_IP_LENGTH, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("svr_port", //column_name
      column_id ++, //column_id
      2, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("statement", //column_name
      column_id ++, //column_id
      3, //rowkey_id
      ObVarcharType,  //column_type
      1024, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("id", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("prepare_count", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("execute_count", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("avg_execute_usec", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("slow_count", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("create_time", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObPreciseDateTimeType,  //column_type
      sizeof(ObPreciseDateTimeType), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("last_active_time", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObPreciseDateTimeType,  //column_type
      sizeof(ObPreciseDateTimeType), //column length
      false); //is nullable
  return ret;
}
//add wenghaixing [secondary index.cluster] 20150629
/*
*构造index_process的的表schema
*/
int ObExtraTablesSchema::all_index_process_schema(TableSchema & table_schema)
{
    int ret=OB_SUCCESS;
    table_schema.init_as_inner_table();
    strcpy(table_schema.table_name_, OB_INDEX_PROCESS_TABLE_NAME);
    table_schema.table_id_ = OB_INDEX_PROCESS_TID;
    table_schema.rowkey_column_num_ = 2;
    table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + 2;
    table_schema.max_rowkey_length_ = TEMP_ROWKEY_LENGTH;
    int column_id = OB_APP_MIN_COLUMN_ID;

    ADD_COLUMN_SCHEMA("index_id", //column_name
        column_id ++, //column_id
        1, //rowkey_id
        ObIntType,  //column_type
        sizeof(int64_t), //column length
        false); //is nullable

    ADD_COLUMN_SCHEMA("cluster_id", //column_name
        column_id ++, //column_id
        2, //rowkey_id
        ObIntType,  //column_type
        sizeof(int64_t), //column length
        false); //is nullable

    ADD_COLUMN_SCHEMA("status", //column_name
        column_id ++, //column_id
        0, //rowkey_id
        ObIntType,  //column_type
        sizeof(int64_t), //column length
        false); //is nullable

    ADD_COLUMN_SCHEMA("gm_create", //column_name
                      OB_CREATE_TIME_COLUMN_ID,//column_id
                      0, //rowkey_id
                      ObCreateTimeType,  //column_type
                      sizeof(int64_t), //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("gm_modify", //column_name
                      OB_MODIFY_TIME_COLUMN_ID, //column_id
                      0, //rowkey_id
                      ObModifyTimeType,  //column_type
                      sizeof(int64_t), //column length
                      false); //is nullable

    return ret;
}
//add e

//add liuxiao [secondary index col checksum] 20150316
/*
*定义内部表__all_cchecksum_info
*存放全局索引的tablet checksum信息
*/

int ObExtraTablesSchema::all_cchecksum_info(TableSchema & table_schema)
{
    int ret = OB_SUCCESS;
    table_schema.init_as_inner_table();
    strcpy(table_schema.table_name_, OB_ALL_CCHECKSUM_INFO_TABLE_NAME);
    table_schema.table_id_ = OB_ALL_CCHECKSUM_INFO_TID; //tid 601
    table_schema.rowkey_column_num_ = 4;
    table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + 4;
    table_schema.max_rowkey_length_ = TEMP_ROWKEY_LENGTH;
    int column_id = OB_APP_MIN_COLUMN_ID;

    ADD_COLUMN_SCHEMA("table_id", //column_name
        column_id ++, //column_id
        1, //rowkey_id
        ObIntType,  //column_type
        sizeof(int64_t), //column length
        false); //is nullable
    ADD_COLUMN_SCHEMA("culster_id", //column_name
        column_id ++, //column_id
        2, //rowkey_id
        ObIntType,  //column_type
        sizeof(int64_t), //column length
        false); //is nullable
    ADD_COLUMN_SCHEMA("version", //column_name
        column_id ++, //column_id
        3, //rowkey_id
        ObIntType,  //column_type
        sizeof(int64_t), //column length
        false); //is nullable
    ADD_COLUMN_SCHEMA("range", //column_name
        column_id ++, //column_id
        4, //rowkey_id
        ObVarcharType,  //column_type
        1024, //column length
        false); //is nullable

    ADD_COLUMN_SCHEMA("column_checksum", //column_name
        column_id ++, //column_id
        0, //rowkey_id
        ObVarcharType,  //column_type
        1024, //column length
        false); //is nullable

    return ret;
}

//add e

//add dolphin [database manager]@20150605:b
int ObExtraTablesSchema::all_database_schema(TableSchema& table_schema)
{
    int ret = OB_SUCCESS;
    table_schema.init_as_inner_table();
    strcpy(table_schema.table_name_, OB_ALL_DATABASE_NAME);
    table_schema.table_id_ = OB_ALL_DATABASE_TID;
    //modify dolphin [database manager]@20150609
    //table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + 2;
    table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + 3;
    //modify:e
    table_schema.rowkey_column_num_ = 1;
    table_schema.max_rowkey_length_ = OB_MAX_DATBASE_NAME_LENGTH;
    int column_id = OB_APP_MIN_COLUMN_ID;
    ADD_COLUMN_SCHEMA("db_name", //column_name
                      column_id ++, //column_id
                      1, //rowkey_id
                      ObVarcharType,  //column_type
                      OB_MAX_DATBASE_NAME_LENGTH, //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("db_id", //column_name
                      column_id ++, //column_id
                      0, //rowkey_id
                      ObIntType,  //column_type
                      sizeof(int64_t), //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("stat", //column_name
                      column_id ++, //column_id
                      0, //rowkey_id
                      ObIntType,  //column_type
                      sizeof(int8_t), //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("gm_create", //column_name
                      OB_CREATE_TIME_COLUMN_ID,//column_id
                      0, //rowkey_id
                      ObCreateTimeType,  //column_type
                      sizeof(int64_t), //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("gm_modify", //column_name
                      OB_MODIFY_TIME_COLUMN_ID, //column_id
                      0, //rowkey_id
                      ObModifyTimeType,  //column_type
                      sizeof(int64_t), //column length
                      false); //is nullable
    return ret;
}

int ObExtraTablesSchema::all_database_privilege_schema(TableSchema& table_schema)
{
    int ret = OB_SUCCESS;
    table_schema.init_as_inner_table();
    strcpy(table_schema.table_name_, OB_ALL_DATABASE_PRIVILEGE_NAME);
    table_schema.table_id_ = OB_ALL_DATABASE_PRIVILEGE_TID;
    table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + 14;
    table_schema.rowkey_column_num_ = 2;
    table_schema.max_rowkey_length_ = 2 * sizeof(int64_t);
    int column_id = OB_APP_MIN_COLUMN_ID;
    ADD_COLUMN_SCHEMA("user_id", //column_name
                      column_id ++, //column_id
                      1, //rowkey_id
                      ObIntType,  //column_type
                      sizeof(int64_t), //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("db_id", //column_name
                      column_id ++, //column_id
                      2, //rowkey_id
                      ObIntType,  //column_type
                      sizeof(int64_t), //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("priv_all", //column_name
                      column_id ++, //column_id
                      0, //rowkey_id
                      ObIntType,  //column_type
                      sizeof(int8_t), //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("priv_alter", //column_name
                      column_id ++, //column_id
                      0, //rowkey_id
                      ObIntType,  //column_type
                      sizeof(int8_t), //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("priv_create", //column_name
                      column_id ++, //column_id
                      0, //rowkey_id
                      ObIntType,  //column_type
                      sizeof(int8_t), //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("priv_create_user", //column_name
                      column_id ++, //column_id
                      0, //rowkey_id
                      ObIntType,  //column_type
                      sizeof(int8_t), //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("priv_create", //column_name
                      column_id ++, //column_id
                      0, //rowkey_id
                      ObIntType,  //column_type
                      sizeof(int8_t), //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("priv_delete", //column_name
                      column_id ++, //column_id
                      0, //rowkey_id
                      ObIntType,  //column_type
                      sizeof(int8_t), //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("priv_drop", //column_name
                      column_id ++, //column_id
                      0, //rowkey_id
                      ObIntType,  //column_type
                      sizeof(int8_t), //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("priv_grant_option", //column_name
                      column_id ++, //column_id
                      0, //rowkey_id
                      ObIntType,  //column_type
                      sizeof(int8_t), //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("priv_insert", //column_name
                      column_id ++, //column_id
                      0, //rowkey_id
                      ObIntType,  //column_type
                      sizeof(int8_t), //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("priv_update", //column_name
                      column_id ++, //column_id
                      0, //rowkey_id
                      ObIntType,  //column_type
                      sizeof(int8_t), //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("priv_select", //column_name
                      column_id ++, //column_id
                      0, //rowkey_id
                      ObIntType,  //column_type
                      sizeof(int8_t), //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("priv_replace", //column_name
                      column_id ++, //column_id
                      0, //rowkey_id
                      ObIntType,  //column_type
                      sizeof(int8_t), //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("is_locked", //column_name
                      column_id ++, //column_id
                      0, //rowkey_id
                      ObIntType,  //column_type
                      sizeof(int8_t), //column length
                      false); //is nullable

    ADD_COLUMN_SCHEMA("gm_create", //column_name
                      OB_CREATE_TIME_COLUMN_ID,//column_id
                      0, //rowkey_id
                      ObCreateTimeType,  //column_type
                      sizeof(int64_t), //column length
                      false); //is nullable
    ADD_COLUMN_SCHEMA("gm_modify", //column_name
                      OB_MODIFY_TIME_COLUMN_ID, //column_id
                      0, //rowkey_id
                      ObModifyTimeType,  //column_type
                      sizeof(int64_t), //column length
                      false); //is nullable
    return ret;
}
//add:e
