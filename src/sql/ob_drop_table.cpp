/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_drop_table.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "sql/ob_drop_table.h"
#include "common/utility.h"
#include "mergeserver/ob_rs_rpc_proxy.h"
#include "sql/ob_sql.h"//add liumz, [drop table -> clean table priv]20150902

using namespace oceanbase::sql;
using namespace oceanbase::common;
ObDropTable::ObDropTable()
  :if_exists_(false), rpc_(NULL)
{
}

ObDropTable::~ObDropTable()
{
}

void ObDropTable::reset()
{
  if_exists_ = false;
  rpc_ = NULL;
  local_context_.rs_rpc_proxy_ = NULL;//add liumz, [drop table -> clean table priv]20150902
}

void ObDropTable::reuse()
{
  if_exists_ = false;
  rpc_ = NULL;
  local_context_.rs_rpc_proxy_ = NULL;//add liumz, [drop table -> clean table priv]20150902
}

//add liumz, [drop table -> clean table priv]20150902:b
void ObDropTable::set_sql_context(const ObSqlContext &context)
{
  local_context_ = context;
  local_context_.schema_manager_ = NULL;
}
//add:e

void ObDropTable::set_if_exists(bool if_exists)
{
  if_exists_ = if_exists;
}

int ObDropTable::add_table_name(const ObString &tname)
{
  return tables_.add_string(tname);
}

//add liumz, [drop table -> clean table priv]20150902:b
int ObDropTable::add_table_id(const uint64_t &tid)
{
  return table_ids_.push_back(tid);
}
//add:e

int ObDropTable::open()
{
  int ret = OB_SUCCESS;
  if (NULL == rpc_ || 0 >= tables_.count())
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "not init, rpc_=%p", rpc_);
  }
  else if (OB_SUCCESS != (ret = rpc_->drop_table(if_exists_, tables_)))
  {
    TBSYS_LOG(WARN, "failed to create table, err=%d", ret);
  }
  else
  {
    TBSYS_LOG(INFO, "drop table succ, tables=[%s] if_exists=%c",
              to_cstring(tables_), if_exists_?'Y':'N');
    clean_owner_privilege();//add liumz, [drop table -> clean table priv]20150902
  }
  return ret;
}

//add liumz, [drop table -> clean table priv]20150902
void ObDropTable::clean_owner_privilege()
{
  int ret = OB_SUCCESS;
  ObString delete_stmt;
  char delete_buff[512];
  int64_t pos = 0;
  uint64_t tid = OB_INVALID_ID;
  //ObString table_name;
  //const ObTableSchema *table_schema = NULL;
  ObArray<ObPrivilege::UserIdDatabaseId> udi_array;

  local_context_.schema_manager_ = local_context_.merger_schema_mgr_->get_user_schema(0);
  if (NULL == local_context_.schema_manager_)
  {
    ret = OB_SCHEMA_ERROR;
    TBSYS_LOG(WARN, "get schema mgr failed, ret=%d", ret);
  }

  for (int64_t i = 0; i < table_ids_.count() && OB_SUCCESS == ret; i++)
  {
    tid = table_ids_.at(i);
    TBSYS_LOG(DEBUG,"test::lmz, delete table privilege,tid = %lu", tid);
    /*
    if (OB_SUCCESS != (ret = tables_.get_string(i, table_name)))
    {
      TBSYS_LOG(WARN, "get table name failed, ret=%d", ret);
    }
    else if (NULL == (table_schema = schema_mgr->get_table_schema(table_name)))
    {
      ret = OB_SCHEMA_ERROR;
      TBSYS_LOG(WARN, "get table schema failed, ret=%d", ret);
    }s
    else
    {
      tid = table_schema->get_table_id();
      */
    if (OB_SUCCESS != (ret = (*(local_context_.pp_privilege_))->get_user_db_id(tid, udi_array)))
    {
      TBSYS_LOG(WARN, "get user_db_id by table id failed, ret=%d", ret);
    }
    else
    {
      for (int64_t j = 0; j < udi_array.count(); j++)
      {
        ObPrivilege::UserIdDatabaseId &udi = udi_array.at(j);
        pos = 0;
        databuff_printf(delete_buff, 512, pos, "delete from __all_table_privilege where user_id=%lu and db_id=%lu and table_id=%lu",
                        udi.userid_, udi.dbid_, tid);
        if (pos >= 511)
        {
          //overflow
          ret = OB_BUF_NOT_ENOUGH;
          TBSYS_LOG(WARN, "buffer overflow ret=%d", ret);
        }
        else
        {
          delete_stmt.assign_ptr(delete_buff, static_cast<ObString::obstr_size_t>(pos));
          TBSYS_LOG(DEBUG,"test::lmz, delete table privilege,sql = %.*s", delete_stmt.length(), delete_stmt.ptr());
          local_context_.disable_privilege_check_ = true;
          ObResultSet local_result;
          if (OB_SUCCESS != (ret = local_result.init()))
          {
            TBSYS_LOG(WARN, "init result set failed,ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = ObSql::direct_execute(delete_stmt, local_result, local_context_)))
          {
            TBSYS_LOG(WARN, "clean privilege from dropped table failed, sql=%.*s, ret=%d", delete_stmt.length(), delete_stmt.ptr(), ret);
          }
          else if (OB_SUCCESS != (ret = local_result.open()))
          {
            TBSYS_LOG(WARN, "open result set failed,ret=%d", ret);
          }
          else
          {
            OB_ASSERT(local_result.is_with_rows() == false);
            int64_t affected_rows = local_result.get_affected_rows();
            if(affected_rows == 0)
            {
              TBSYS_LOG(DEBUG,"test::lmz, 0 rows affected,sql = %.*s", delete_stmt.length(), delete_stmt.ptr());
            }
            int err = local_result.close();
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "close result set failed,ret=%d", ret);
            }
            local_result.reset();
          }
        }
      }//end for
    }
  }//end for
  if (local_context_.schema_manager_ != NULL)
  {
    int err = local_context_.merger_schema_mgr_->release_schema(local_context_.schema_manager_);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "release schema failed,ret=%d", err);
    }
    else
    {
      local_context_.schema_manager_ = NULL;
    }
  }
}
//add:e

int ObDropTable::close()
{
  int ret = OB_SUCCESS;
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObDropTable, PHY_DROP_TABLE);
  }
}

int64_t ObDropTable::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "DropTables(if_exists=%c tables=[", if_exists_?'Y':'N');
  pos += tables_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, "])\n");
  return pos;
}
