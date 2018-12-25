/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_show_schema_manager.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_show_schema_manager.h"
#include "common/ob_schema_macro_define.h"
#include "common/ob_schema_service.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObSchemaManagerV2* ObShowSchemaManager::show_schema_mgr_ = NULL;
tbsys::CThreadMutex ObShowSchemaManager::mutex_;

const ObSchemaManagerV2* ObShowSchemaManager::get_show_schema_manager()
{
  if (!show_schema_mgr_)
  {
    mutex_.lock();
    if (!show_schema_mgr_)
    {
      void *ptr = NULL;
      ObSchemaManagerV2 *schema_mgr = NULL;
      if ((ptr = ob_malloc(sizeof(ObSchemaManagerV2), ObModIds::OB_SCHEMA)) == NULL)
      {
        TBSYS_LOG(WARN, "Out of memory");
      }
      else if ((schema_mgr = new(ptr) ObSchemaManagerV2()) == NULL)
      {
        TBSYS_LOG(WARN, "Construct ObShowSchemaManager failed");
        ob_free(ptr);
      }
      else if (add_show_schemas(*schema_mgr) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Load show tables' schemas failed");
        schema_mgr->~ObSchemaManagerV2();
        ob_free(ptr);
      }
      else
      {
        show_schema_mgr_ = schema_mgr;
      }
    }
    mutex_.unlock();
  }
  return show_schema_mgr_;
}

int ObShowSchemaManager::add_show_tables_schema(ObSchemaManagerV2& schema_mgr)
{
  int ret = OB_SUCCESS;
  TableSchema table_schema;
  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_TABLES_SHOW_TABLE_NAME);
  table_schema.table_id_ = OB_TABLES_SHOW_TID;
  table_schema.rowkey_column_num_ = 1;
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID;
  table_schema.max_rowkey_length_ = 128;
  int column_id = OB_APP_MIN_COLUMN_ID;

  ADD_COLUMN_SCHEMA("table_name", //column_name
      column_id ++, //column_id
      1, //rowkey_id
      ObVarcharType,  //column_type
      128, //column length
      false); //is nullable

  if (ret == OB_SUCCESS
    && (ret = schema_mgr.add_new_table_schema(table_schema)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Add schema of %s faild, ret=%d", OB_TABLES_SHOW_TABLE_NAME, ret);
  }
  return ret;
}

// add by zhangcd [multi_database.show_databases] 20150617:b
int ObShowSchemaManager::add_show_databases_schema(ObSchemaManagerV2& schema_mgr)
{
  int ret = OB_SUCCESS;
  TableSchema table_schema;
  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_DATABASES_SHOW_TABLE_NAME);
  table_schema.table_id_ = OB_DATABASES_SHOW_TID;
  table_schema.rowkey_column_num_ = 1;
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID;
  table_schema.max_rowkey_length_ = 128;
  int column_id = OB_APP_MIN_COLUMN_ID;

  ADD_COLUMN_SCHEMA("db_name", //column_name
      column_id ++, //column_id
      1, //rowkey_id
      ObVarcharType,  //column_type
      128, //column length
      false); //is nullable

  if (ret == OB_SUCCESS
    && (ret = schema_mgr.add_new_table_schema(table_schema)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Add schema of %s faild, ret=%d", OB_DATABASES_SHOW_TABLE_NAME, ret);
  }
  return ret;
}
// add by zhangcd [multi_database.show_databases] 20150617:e

/* add liumengzhan_show_index [20141208]
 * add virtual table __index_show's schema to schema_mgr
 */
int ObShowSchemaManager::add_show_index_schema(ObSchemaManagerV2& schema_mgr)
{
  int ret = OB_SUCCESS;
  TableSchema table_schema;
  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_INDEX_SHOW_TABLE_NAME);
  table_schema.table_id_ = OB_INDEX_SHOW_TID;
  table_schema.rowkey_column_num_ = 1;
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + 1;
  table_schema.max_rowkey_length_ = 128;
  int column_id = OB_APP_MIN_COLUMN_ID;

  ADD_COLUMN_SCHEMA("index_name", //column_name
      column_id ++, //column_id
      1, //rowkey_id
      ObVarcharType,  //column_type
      128, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("status", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int32_t), //column length
      false); //is nullable

  if (ret == OB_SUCCESS
    && (ret = schema_mgr.add_new_table_schema(table_schema)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Add schema of %s faild, ret=%d", OB_INDEX_SHOW_TABLE_NAME, ret);
  }
  return ret;
}
//add:e

int ObShowSchemaManager::add_show_variables_schema(ObSchemaManagerV2& schema_mgr)
{
  int ret = OB_SUCCESS;
  TableSchema table_schema;
  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_VARIABLES_SHOW_TABLE_NAME);
  table_schema.table_id_ = OB_VARIABLES_SHOW_TID;
  table_schema.rowkey_column_num_ = 1;
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + 1;
  table_schema.max_rowkey_length_ = 128;

  int column_id = OB_APP_MIN_COLUMN_ID;
  ADD_COLUMN_SCHEMA("variable_name", //column_name
      column_id ++, //column_id
      1, //rowkey_id
      ObVarcharType,  //column_type
      128, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("value", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      512, //column length
      false); //is nullable

  if (ret == OB_SUCCESS
    && (ret = schema_mgr.add_new_table_schema(table_schema)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Add schema of %s faild, ret=%d", OB_VARIABLES_SHOW_TABLE_NAME, ret);
  }
  return ret;
}

int ObShowSchemaManager::add_show_columns_schema(ObSchemaManagerV2& schema_mgr)
{
  int ret = OB_SUCCESS;
  TableSchema table_schema;
  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_COLUMNS_SHOW_TABLE_NAME);
  table_schema.table_id_ = OB_COLUMNS_SHOW_TID;
  table_schema.rowkey_column_num_ = 1;
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + 5;
  table_schema.max_rowkey_length_ = 128;

  int column_id = OB_APP_MIN_COLUMN_ID;
  ADD_COLUMN_SCHEMA("field", //column_name
      column_id ++, //column_id
      1, //rowkey_id
      ObVarcharType,  //column_type
      128, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("type", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      128, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("nullable", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("key", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("default", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      5000, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("extra", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      5000, //column length
      false); //is nullable

  if (ret == OB_SUCCESS
    && (ret = schema_mgr.add_new_table_schema(table_schema)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Add schema of %s faild, ret=%d", OB_COLUMNS_SHOW_TABLE_NAME, ret);
  }
  return ret;
}

int ObShowSchemaManager::add_show_create_table_schema(ObSchemaManagerV2& schema_mgr)
{
  int ret = OB_SUCCESS;
  TableSchema table_schema;
  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_CREATE_TABLE_SHOW_TABLE_NAME);
  table_schema.table_id_ = OB_CREATE_TABLE_SHOW_TID;
  table_schema.rowkey_column_num_ = 1;
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID + 1;
  table_schema.max_rowkey_length_ = 128;
  int column_id = OB_APP_MIN_COLUMN_ID;

  ADD_COLUMN_SCHEMA("table_name", //column_name
      column_id ++, //column_id
      1, //rowkey_id
      ObVarcharType,  //column_type
      128, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("table_definition", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      4096, //column length
      false); //is nullable

  if (ret == OB_SUCCESS
    && (ret = schema_mgr.add_new_table_schema(table_schema)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Add schema of %s faild, ret=%d", OB_CREATE_TABLE_SHOW_TABLE_NAME, ret);
  }
  return ret;
}

int ObShowSchemaManager::add_show_parameters_schema(ObSchemaManagerV2& schema_mgr)
{
  int ret = OB_SUCCESS;
  TableSchema table_schema;
  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_PARAMETERS_SHOW_TABLE_NAME);
  table_schema.table_id_ = OB_PARAMETERS_SHOW_TID;
  table_schema.rowkey_column_num_ = 3;
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID+6;
  table_schema.max_rowkey_length_ = 128 + 128 * sizeof(int64_t);

  int column_id = OB_APP_MIN_COLUMN_ID;
  ADD_COLUMN_SCHEMA("name", //column_name
      column_id ++, //column_id
      1, //rowkey_id
      ObVarcharType,  //column_type
      128, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("server_ip", //column_name
      column_id ++, //column_id
      2, //rowkey_id
      ObVarcharType,  //column_type
      128, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("server_port", //column_name
      column_id ++, //column_id
      3, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("server_type", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("cluster_id", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("value", //column_name
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
      false); //is nullable

  if (ret == OB_SUCCESS
    && (ret = schema_mgr.add_new_table_schema(table_schema)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Add schema of %s faild, ret=%d", OB_PARAMETERS_SHOW_TABLE_NAME, ret);
  }
  return ret;
}

int ObShowSchemaManager::add_show_table_status_schema(ObSchemaManagerV2& schema_mgr)
{
  int ret = OB_SUCCESS;
  TableSchema table_schema;
  table_schema.init_as_inner_table();
  strcpy(table_schema.table_name_, OB_TABLE_STATUS_SHOW_TABLE_NAME);
  table_schema.table_id_ = OB_TABLE_STATUS_SHOW_TID;
  table_schema.rowkey_column_num_ = 1;
  table_schema.max_used_column_id_ = OB_APP_MIN_COLUMN_ID+17;
  table_schema.max_rowkey_length_ = 128;

  int column_id = OB_APP_MIN_COLUMN_ID;
  ADD_COLUMN_SCHEMA("name", //column_name
      column_id ++, //column_id
      1, //rowkey_id
      ObVarcharType,  //column_type
      64, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("engine", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      64, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("version", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("row_format", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      10, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("rows", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("avg_row_length", //column_name
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
  ADD_COLUMN_SCHEMA("max_data_length", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("index_length", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("data_free", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("auto_increment", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("create_time", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObPreciseDateTimeType,  //column_type
      sizeof(ObPreciseDateTime), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("update_time", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObPreciseDateTimeType,  //column_type
      sizeof(ObPreciseDateTime), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("check_time", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObPreciseDateTimeType,  //column_type
      sizeof(ObPreciseDateTime), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("collation", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      64, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("checksum", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObIntType,  //column_type
      sizeof(int64_t), //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("create_options", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      255, //column length
      false); //is nullable
  ADD_COLUMN_SCHEMA("comment", //column_name
      column_id ++, //column_id
      0, //rowkey_id
      ObVarcharType,  //column_type
      80, //column length
      false); //is nullable

  if (ret == OB_SUCCESS
    && (ret = schema_mgr.add_new_table_schema(table_schema)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Add schema of %s faild, ret=%d", OB_PARAMETERS_SHOW_TABLE_NAME, ret);
  }
  return ret;
}


int ObShowSchemaManager::add_show_schemas(ObSchemaManagerV2& schema_mgr)
{
  int ret = OB_SUCCESS;
  for (int32_t type = ObBasicStmt::T_SHOW_TABLES;
       ret == OB_SUCCESS && type <= ObBasicStmt::T_SHOW_SERVER_STATUS;
       type++)
  {
    ret = add_show_schema(schema_mgr, type);
  }
  if (ret == OB_SUCCESS)
  {
    ret = schema_mgr.sort_column();
  }
  return ret;
}

int ObShowSchemaManager::add_show_schema(ObSchemaManagerV2& schema_mgr, int32_t stmt_type)
{
  int ret = OB_SUCCESS;
  switch (stmt_type)
  {
    case ObBasicStmt::T_SHOW_TABLES:
      ret = add_show_tables_schema(schema_mgr);
      break;
    // add by zhangcd [multi_database.show_databases] 20150617:b
    case ObBasicStmt::T_SHOW_DATABASES:
      ret = add_show_databases_schema(schema_mgr);
      break;
    // add by zhangcd [multi_database.show_databases] 20150617:e
    //add liumengzhan_show_index [20141208]
    case ObBasicStmt::T_SHOW_INDEX:
      ret = add_show_index_schema(schema_mgr);
      break;
    //add:e
    case ObBasicStmt::T_SHOW_VARIABLES:
      ret = add_show_variables_schema(schema_mgr);
      break;
    case ObBasicStmt::T_SHOW_COLUMNS:
      ret = add_show_columns_schema(schema_mgr);
      break;
    case ObBasicStmt::T_SHOW_CREATE_TABLE:
      ret = add_show_create_table_schema(schema_mgr);
      break;
    case ObBasicStmt::T_SHOW_PARAMETERS:
      ret = add_show_parameters_schema(schema_mgr);
      break;
    case ObBasicStmt::T_SHOW_TABLE_STATUS:
      ret = add_show_table_status_schema(schema_mgr);
      break;
    case ObBasicStmt::T_SHOW_SCHEMA:
    case ObBasicStmt::T_SHOW_SERVER_STATUS:
    case ObBasicStmt::T_SHOW_WARNINGS:
    case ObBasicStmt::T_SHOW_GRANTS:
      /* no schema need */
      break;
    // add by zhangcd [multi_database.show_databases] 20150617:b
    case ObBasicStmt::T_SHOW_CURRENT_DATABASE:
      /* reuse the schema of __databases_show */
      break;
    case ObBasicStmt::T_SHOW_SYSTEM_TABLES:
      /* reuse the schema of __tables_show */
      break;
    // add by zhangcd [multi_database.show_databases] 20150617:e
    default:
      ret = OB_ERR_ILLEGAL_TYPE;
      break;
  }
  return ret;
}
