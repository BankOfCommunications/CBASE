/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_schema_checker.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */

#include "ob_schema_checker.h"
#include "ob_show_schema_manager.h"
#include "common/ob_tsi_factory.h"
#include <config.h>
#include <tblog.h>

using namespace oceanbase::sql;
using namespace oceanbase::common;

const char* SCHEMA_FILE = "./MyTestSchema.ini";

ObSchemaChecker::ObSchemaChecker()
  : schema_mgr_(NULL), show_schema_mgr_(NULL)
{
#if 0
  ObSchemaManagerV2 *schema_mgr = GET_TSI_MULT(ObSchemaManagerV2, 1);
  if (schema_mgr)
  {
    tbsys::CConfig cfg;
    schema_mgr->parse_from_file(SCHEMA_FILE, cfg);
    schema_mgr_ = schema_mgr;
  }
#endif
}

ObSchemaChecker::~ObSchemaChecker()
{
  schema_mgr_ = NULL;
}

void ObSchemaChecker::set_schema(const common::ObSchemaManagerV2& schema_mgr)
{
  if ((show_schema_mgr_ = ObShowSchemaManager::get_show_schema_manager()) == NULL)
  {
    TBSYS_LOG(WARN, "Add show table schemas faild");
  }
  schema_mgr_ = &schema_mgr;
}

bool ObSchemaChecker::column_exists(const ObString& table_name, const ObString& column_name) const
{
  bool ret = false;
  if ((schema_mgr_ && schema_mgr_->get_column_schema(table_name, column_name) != NULL)
    || (show_schema_mgr_ && show_schema_mgr_->get_column_schema(table_name, column_name) != NULL))
    ret = true;
  return ret;
}

const ObColumnSchemaV2* ObSchemaChecker::get_column_schema(
    const ObString& table_name,
    const ObString& column_name) const
{
  const ObColumnSchemaV2 *col = NULL;
  if (schema_mgr_)
    col = schema_mgr_->get_column_schema(table_name, column_name);
  if (!col && show_schema_mgr_)
    col = show_schema_mgr_->get_column_schema(table_name, column_name);
  return col;
}

uint64_t ObSchemaChecker::get_column_id(
    const ObString& table_name,
    const ObString& column_name) const
{
  uint64_t column_id = OB_INVALID_ID;
  const ObColumnSchemaV2 *col = get_column_schema(table_name, column_name);
  if (col != NULL)
    column_id = col->get_id();
  return column_id;
}

const ObTableSchema* ObSchemaChecker::get_table_schema(const char* table_name) const
{
  const ObTableSchema *table_schema = NULL;
  if (schema_mgr_)
    table_schema = schema_mgr_->get_table_schema(table_name);
  if (!table_schema && show_schema_mgr_)
    table_schema = show_schema_mgr_->get_table_schema(table_name);
  return table_schema;
}

const ObTableSchema* ObSchemaChecker::get_table_schema(const ObString& table_name) const
{
  const ObTableSchema *table_schema = NULL;
  if (schema_mgr_)
    table_schema = schema_mgr_->get_table_schema(table_name);
  if (!table_schema && show_schema_mgr_)
    table_schema = show_schema_mgr_->get_table_schema(table_name);
  return table_schema;
}

const ObTableSchema* ObSchemaChecker::get_table_schema(const uint64_t table_id) const
{
  const ObTableSchema *table_schema = NULL;
  if (!IS_SHOW_TABLE(table_id))
  {
    if (schema_mgr_)
      table_schema = schema_mgr_->get_table_schema(table_id);
  }
  else
  {
    if (show_schema_mgr_)
      table_schema = show_schema_mgr_->get_table_schema(table_id);
  }
  return table_schema;
}

const ObColumnSchemaV2* ObSchemaChecker::get_column_schema(const uint64_t table_id, const uint64_t column_id) const
{
  const ObColumnSchemaV2 *col = NULL;
  if (!IS_SHOW_TABLE(table_id))
    col =  schema_mgr_->get_column_schema(table_id, column_id);
  else
    col =  show_schema_mgr_->get_column_schema(table_id, column_id);
  return col;
}

// show tables in consideration
uint64_t ObSchemaChecker::get_local_table_id(const ObString& table_name) const
{
  uint64_t table_id = get_table_id(table_name);
  if (table_id == OB_INVALID_ID && show_schema_mgr_)
  {
    const ObTableSchema *table_schema = show_schema_mgr_->get_table_schema(table_name);
    if (table_schema != NULL)
    {
      table_id = table_schema->get_table_id();
    }
  }
  return table_id;
}

// only this function excludes show tables
uint64_t ObSchemaChecker::get_table_id(const ObString& table_name) const
{
  uint64_t table_id = OB_INVALID_ID;
  if (schema_mgr_)
  {
    const ObTableSchema *table_schema = schema_mgr_->get_table_schema(table_name);
    if (table_schema != NULL)
    {
      table_id = table_schema->get_table_id();
    }
    else
    {
      TBSYS_LOG(DEBUG, "fail to get table schema for table[%.*s]", table_name.length(), table_name.ptr());
    }
  }
  return table_id;
}

const ObColumnSchemaV2* ObSchemaChecker::get_table_columns(
    const uint64_t table_id,
    int32_t& size) const
{
  const ObColumnSchemaV2 *col = NULL;
  if (!IS_SHOW_TABLE(table_id))
    col = schema_mgr_->get_table_schema(table_id, size);
  else
    col = show_schema_mgr_->get_table_schema(table_id, size);
  return col;
}

bool ObSchemaChecker::is_join_column(
    uint64_t table_id,
    uint64_t column_id) const
{
  bool ret = false;
  const common::ObColumnSchemaV2* column_schema = schema_mgr_->get_column_schema(table_id, column_id);
  if (NULL != column_schema)
  {
    ret = NULL != column_schema->get_join_info();
  }
  return ret;
}

bool ObSchemaChecker::is_rowkey_column(const ObString& table_name, const ObString& column_name) const
{
  bool is_rowkey = false;
  const ObTableSchema *table_schema = NULL;
  uint64_t column_id = OB_INVALID_ID;
  if ((table_schema = get_table_schema(table_name)) == NULL)
  {
    TBSYS_LOG(DEBUG, "Get table '%.*s' schema failed", table_name.length(), table_name.ptr());
  }
  else if ((column_id = get_column_id(table_name, column_name)) == OB_INVALID_ID)
  {
    TBSYS_LOG(DEBUG, "Get column id failed, column name = %.*s",
        column_name.length(), column_name.ptr());
  }
  else if (table_schema->get_rowkey_info().is_rowkey_column(column_id))
  {
    is_rowkey = true; 
  }
  return is_rowkey;
}

//add wenghaixing [secondary index] 20141105
int ObSchemaChecker::is_index_full(uint64_t table_id,bool& is_full)
{
  //bool ret=false;
  int ret = OB_SUCCESS;
  IndexList il;
  const hash::ObHashMap<uint64_t,IndexList,hash::NoPthreadDefendMode>* idx_hash=schema_mgr_->get_index_hash();
  if(idx_hash == NULL)
  {
    ret=OB_ERROR;
    TBSYS_LOG(ERROR,"NULL pointer of index_hash_map!");
  }
  else
  {
    if(hash::HASH_NOT_EXIST==idx_hash->get(table_id,il))
    {
    }
    else if(hash::HASH_EXIST==idx_hash->get(table_id,il))
    {
      if(il.get_count()>=OB_MAX_INDEX_NUMS)
      {
        is_full=true;
      }
      else
      {
      }
    }
  }
  return ret;
}
//add e

//add dragon [varchar limit] 2016-8-10 09:43:34
int ObSchemaChecker::varchar_len_check(ObColumnInfo &cinfo, int64_t length)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *col_sche = NULL;
  if(NULL != (col_sche = schema_mgr_->get_column_schema(cinfo.table_id_, cinfo.column_id_)))
  {
    //如果是varchar类型，则进行长度判断
    if(ObVarcharType == (col_sche->get_type()))
    {
      if(length > col_sche->get_size())
      {
        TBSYS_LOG(WARN, "length can't large than giving in the schema.tid[%ld], cid[%ld], "
                  "length[%ld], size in schema[%ld]", cinfo.table_id_, cinfo.column_id_,
                  length, col_sche->get_size());
        ret = OB_ERR_VARCHAR_TOO_LONG;
      }
    }
  }
  return ret;
}
//add e
