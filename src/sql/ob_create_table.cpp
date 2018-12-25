/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_create_table.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "common/ob_privilege.h"
#include "ob_create_table.h"
#include "common/utility.h"
#include "mergeserver/ob_rs_rpc_proxy.h"
#include "sql/ob_result_set.h"
#include "sql/ob_sql.h"
#include "common/ob_privilege_type.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObCreateTable::ObCreateTable()
  : if_not_exists_(false)
{
}

ObCreateTable::~ObCreateTable()
{
}

void ObCreateTable::reset()
{
  if_not_exists_ = false;
  local_context_.rs_rpc_proxy_ = NULL;
}

void ObCreateTable::reuse()
{
  if_not_exists_ = false;
  local_context_.rs_rpc_proxy_ = NULL;
}

void ObCreateTable::set_sql_context(const ObSqlContext &context)
{
  local_context_ = context;
  local_context_.schema_manager_ = NULL;
}

int ObCreateTable::open()
{
  int ret = OB_SUCCESS;
  if (NULL == local_context_.rs_rpc_proxy_ || 0 >= strlen(table_schema_.table_name_))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "not init, rpc_=%p", local_context_.rs_rpc_proxy_);
  }
  else if (OB_SUCCESS != (ret = local_context_.rs_rpc_proxy_->create_table(if_not_exists_, table_schema_)))
  {
    TBSYS_LOG(WARN, "failed to create table, err=%d", ret);
  }
  else
  {
    // 还没有赋予权限即成功,如果创建成功了，但是赋予权限失败，由DBA介入
    TBSYS_LOG(INFO, "create table succ, table_name=%s if_not_exists=%c",
              table_schema_.table_name_, if_not_exists_?'Y':'N');
    grant_owner_privilege();
  }
  return ret;
}

//add liumz, [multi_database.create_table]20150708:b
void construct_grant_table_priv_stmt(char *buf, int buf_size, int64_t &pos, const ObString &user_name, const ObString db_name, const ObString table_name, const ObBitSet<> &privileges)
{
  databuff_printf(buf, buf_size, pos, "GRANT ");
  if (privileges.has_member(OB_PRIV_ALL))
  {
    databuff_printf(buf, buf_size, pos, "ALL PRIVILEGES,");
    if (privileges.has_member(OB_PRIV_GRANT_OPTION))
    {
      databuff_printf(buf, buf_size, pos, "GRANT OPTION,");
    }
  }
  else
  {
    if (privileges.has_member(OB_PRIV_ALTER))
    {
      databuff_printf(buf, buf_size, pos, "ALTER,");
    }
    //mod liumz, [multi_database.UT_bugfix]20150715:b
    if (privileges.has_member(OB_PRIV_CREATE))
    {
      databuff_printf(buf, buf_size, pos, "CREATE,");
    }
    if (privileges.has_member(OB_PRIV_CREATE_USER))
    {
      databuff_printf(buf, buf_size, pos, "CREATE USER,");
    }
    if (privileges.has_member(OB_PRIV_DELETE))
    {
      databuff_printf(buf, buf_size, pos, "DELETE,");
    }
    if (privileges.has_member(OB_PRIV_DROP))
    {
      databuff_printf(buf, buf_size, pos, "DROP,");
    }
    if (privileges.has_member(OB_PRIV_GRANT_OPTION))
    {
      databuff_printf(buf, buf_size, pos, "GRANT OPTION,");
    }
    if (privileges.has_member(OB_PRIV_INSERT))
    {
      databuff_printf(buf, buf_size, pos, "INSERT,");
    }
    if (privileges.has_member(OB_PRIV_UPDATE))
    {
      databuff_printf(buf, buf_size, pos, "UPDATE,");
    }
    if (privileges.has_member(OB_PRIV_SELECT))
    {
      databuff_printf(buf, buf_size, pos, "SELECT,");
    }
    if (privileges.has_member(OB_PRIV_REPLACE))
    {
      databuff_printf(buf, buf_size, pos, "REPLACE,");
    }
    //mod:e
  }
  pos = pos - 1;
  databuff_printf(buf, buf_size, pos, " ON \"%.*s\".\"%.*s\" TO '%.*s'", db_name.length(), db_name.ptr(),
                  table_name.length(), table_name.ptr(), user_name.length(), user_name.ptr());
}
//add:e

void ObCreateTable::grant_owner_privilege()
{
  int ret = OB_SUCCESS;
  ObString user_name = my_phy_plan_->get_result_set()->get_session()->get_user_name();
  char grant_buff[512];
  ObString grant_stmt;
  int64_t pos = 0;  
  ObString table_name;
  //add liumz, [multi_database.create_table]20150708:b
  bool is_regrant_priv = my_phy_plan_->get_result_set()->get_session()->is_regrant_priv();
  ObString db_name;
  db_name.assign_ptr(table_schema_.dbname_, static_cast<ObString::obstr_size_t>(strlen(table_schema_.dbname_)));
  //add:e
  table_name.assign_ptr(table_schema_.table_name_, static_cast<ObString::obstr_size_t>(strlen(table_schema_.table_name_)));
  //modify dolphin [database manager]@20150616
  databuff_printf(grant_buff, 512, pos, "GRANT ALL PRIVILEGES, GRANT OPTION ON \"%s\".\"%.*s\" to '%.*s'",
                  table_schema_.dbname_, table_name.length(), table_name.ptr(),
                  user_name.length(), user_name.ptr());
  //modify:e
  if (pos >= 511)
  {
    //overflow
    ret = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(WARN, "privilege buffer overflow ret=%d", ret);
  }
  else
  {
    grant_stmt.assign_ptr(grant_buff, static_cast<ObString::obstr_size_t>(pos));

    // 重新获取schema
    // 如果获取10次都获取不到最新的含有刚刚创建的表的schema，则由DBA来进行处理
    static const int retry_times = 10;
    const ObSchemaManagerV2 *curr_schema_mgr = NULL;
    int i = 1;
    for (;i <= retry_times;++i)
    {
      curr_schema_mgr = local_context_.merger_schema_mgr_->get_user_schema(0);
      if (NULL == curr_schema_mgr)
      {
        TBSYS_LOG(WARN, "%s 's schema is not available,retry", table_schema_.table_name_);
        usleep(10 * 1000);// 10ms
      }
      else
      {
        // new table 's schema still not available

        //add dolphin [database manger]@20150616:b
        char buf[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1];//mod liumz [name_length]
        size_t l = strlen(table_schema_.dbname_);
        if(l > 0)
        {
          memcpy(buf,table_schema_.dbname_,l);
          buf[l] = '.';
          memcpy(buf+l+1,table_schema_.table_name_,strlen(table_schema_.table_name_));
          buf[l+1+strlen(table_schema_.table_name_)] = 0;
        }
        else
        {
          memcpy(buf,table_schema_.table_name_,strlen(table_schema_.table_name_));
          buf[strlen(table_schema_.table_name_)] = 0;
        }
        //add:e
        if (NULL == curr_schema_mgr->get_table_schema(/** modify dolphin [database manager]@20150616 table_schema_.table_name_ */buf))
        {
          TBSYS_LOG(WARN, "%s 's schema is not available,retry", table_schema_.table_name_);
          int err = local_context_.merger_schema_mgr_->release_schema(curr_schema_mgr);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "release schema failed,ret=%d", err);
          }
        }
        else
        {
          TBSYS_LOG(INFO, "get created table %s's schema success", table_schema_.table_name_);
          local_context_.schema_manager_ = curr_schema_mgr;
          break;
        }
      }
    } // end for
    if (i == retry_times + 1)
    {
      ret = OB_ERR_GRANT_PRIVILEGES_TO_CREATE_TABLE;
      //报警，让DBA使用admin账户来处理，给新建的表手工加权限
      //mod liumz, [multi_database.create_table]20150708:b
      /*TBSYS_LOG(ERROR, "User: %.*s create table %s success, but grant all privileges, grant option on %s to '%.*s' failed, ret=%d",
                user_name.length(), user_name.ptr(), table_schema_.table_name_, table_schema_.table_name_, user_name.length(), user_name.ptr(), ret);*/
      TBSYS_LOG(ERROR, "User: %.*s create table %s success, but grant all privileges, grant option on %s.%s to '%.*s' failed, ret=%d",
                user_name.length(), user_name.ptr(), table_schema_.table_name_, table_schema_.dbname_, table_schema_.table_name_, user_name.length(), user_name.ptr(), ret);
      //mod:e
    }
    if (OB_SUCCESS == ret)
    {
      local_context_.disable_privilege_check_ = true;
      ObResultSet local_result;
      if (OB_SUCCESS != (ret = local_result.init()))
      {
        TBSYS_LOG(WARN, "init result set failed,ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = ObSql::direct_execute(grant_stmt, local_result, local_context_)))
      {
        TBSYS_LOG(WARN, "grant privilege to created table failed, sql=%.*s, ret=%d", grant_stmt.length(), grant_stmt.ptr(), ret);
      }
      else if (OB_SUCCESS != (ret = local_result.open()))
      {
        TBSYS_LOG(WARN, "open result set failed,ret=%d", ret);
      }
      else
      {
        OB_ASSERT(local_result.is_with_rows() == false);
        int err = local_result.close();
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "close result set failed,ret=%d", ret);
        }
        local_result.reset();
      }
    }
    //add liumz, [multi_database.create_table]20150708:b
    //TBSYS_LOG(ERROR, "test::lmz, is_regrant_priv[%d]", is_regrant_priv);
    if (OB_SUCCESS == ret && is_regrant_priv)
    {
      //TBSYS_LOG(ERROR, "test::lmz, is_regrant_priv[%d]", is_regrant_priv);
      ObPrivilege::UserDbPriMap *user_db_priv= const_cast<ObPrivilege*>(*(local_context_.pp_privilege_))->get_user_database_privilege_map();
      ObPrivilege::UserDbPriMap::const_iterator it = user_db_priv->begin();
      for (; it != user_db_priv->end() && OB_LIKELY(OB_SUCCESS == ret); it++)
      {
        if (OB_INVALID_ID == it->first.dbid_ || OB_DSADMIN_UID == it->first.userid_)
          continue;//ignore sys db & dsadmin
        pos = 0;//reset buf
        ObString username;
        ObString dbname;//add liumz, [multi_database.UT_bugfix]20150715
        ObBitSet<> db_priv;
        //add liumz, [multi_database.UT_bugfix]20150715:b
        if(OB_SUCCESS != (ret = (*(local_context_.pp_privilege_))->get_db_name(it->first.dbid_, dbname)))
        {
          TBSYS_LOG(WARN, "get db name failed, db_id[%lu], ret=%d", it->first.dbid_, ret);
        }
        else if (dbname == db_name)
        {//add:e
          if (OB_SUCCESS != (ret = (*(local_context_.pp_privilege_))->get_user_name(it->first.userid_, username)))
          {
            TBSYS_LOG(WARN, "get user name failed, user_id[%lu], ret=%d", it->first.userid_, ret);
          }
          else if (username != user_name)
          {
            db_priv = it->second.privileges_;
            if (!db_priv.is_empty())
            {
              construct_grant_table_priv_stmt(grant_buff, 512, pos, username, db_name, table_name, db_priv);
              if (pos >= 511)
              {
                //overflow
                ret = OB_BUF_NOT_ENOUGH;
                TBSYS_LOG(WARN, "privilege buffer overflow ret=%d", ret);
              }
              else
              {
                grant_stmt.assign_ptr(grant_buff, static_cast<ObString::obstr_size_t>(pos));
                local_context_.disable_privilege_check_ = true;
                ObResultSet local_result;
                if (OB_SUCCESS != (ret = local_result.init()))
                {
                  TBSYS_LOG(WARN, "init result set failed,ret=%d", ret);
                }
                else if (OB_SUCCESS != (ret = ObSql::direct_execute(grant_stmt, local_result, local_context_)))
                {
                  TBSYS_LOG(WARN, "grant privilege to created table failed, sql=%.*s, ret=%d", grant_stmt.length(), grant_stmt.ptr(), ret);
                }
                else if (OB_SUCCESS != (ret = local_result.open()))
                {
                  TBSYS_LOG(WARN, "open result set failed,ret=%d", ret);
                }
                else
                {
                  OB_ASSERT(local_result.is_with_rows() == false);
                  int err = local_result.close();
                  if (OB_SUCCESS != err)
                  {
                    TBSYS_LOG(WARN, "close result set failed,ret=%d", ret);
                  }
                  local_result.reset();
                }
              }
            }
          }//end else
        }//end else
      }//enf for
      //add liumz, [multi_database.UT_bugfix]20150715:b
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "User: %.*s create table %s success, but regrant table privs to db owners failed, ret=%d",
                  user_name.length(), user_name.ptr(), table_schema_.table_name_, ret);
      }
      //add:e
    }
    //add:e
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
}

int ObCreateTable::close()
{
  return OB_SUCCESS;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObCreateTable, PHY_CREATE_TABLE);
  }
}

int64_t ObCreateTable::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "CreateTable(if_not_exists=%s, ", if_not_exists_ ? "TRUE" : "FALSE");
  databuff_printf(buf, buf_len, pos, "table_name=%s, ", table_schema_.table_name_);
  databuff_printf(buf, buf_len, pos, "compress_func_name=%s, ", table_schema_.compress_func_name_);
  databuff_printf(buf, buf_len, pos, "table_id=%lu, ", table_schema_.table_id_);
  // we didn't set these type, so just show the type number here
  databuff_printf(buf, buf_len, pos, "table_type=%d, ", table_schema_.table_type_);
  databuff_printf(buf, buf_len, pos, "load_type=%d, ", table_schema_.load_type_);
  databuff_printf(buf, buf_len, pos, "table_def_type=%d, ", table_schema_.table_def_type_);
  databuff_printf(buf, buf_len, pos, "is_use_bloomfilter=%s, ", table_schema_.is_use_bloomfilter_ ? "TRUE" : "FALSE");
  databuff_printf(buf, buf_len, pos, "write_sstable_version=%ld, ", table_schema_.merge_write_sstable_version_);
  databuff_printf(buf, buf_len, pos, "is_pure_update_table=%s, ", table_schema_.is_pure_update_table_ ? "TRUE" : "FALSE");
  databuff_printf(buf, buf_len, pos, "rowkey_split=%ld, ", table_schema_.rowkey_split_);
  databuff_printf(buf, buf_len, pos, "rowkey_column_num=%d, ", table_schema_.rowkey_column_num_);
  databuff_printf(buf, buf_len, pos, "replica_num=%d, ", table_schema_.replica_num_);
  databuff_printf(buf, buf_len, pos, "max_used_column_id=%ld, ", table_schema_.max_used_column_id_);
  databuff_printf(buf, buf_len, pos, "create_mem_version=%ld, ", table_schema_.create_mem_version_);
  databuff_printf(buf, buf_len, pos, "tablet_max_size=%ld, ", table_schema_.tablet_max_size_);
  databuff_printf(buf, buf_len, pos, "tablet_block_size_=%ld, ", table_schema_.tablet_block_size_);
  databuff_printf(buf, buf_len, pos, "max_rowkey_length=%ld, ", table_schema_.max_rowkey_length_);
  if (table_schema_.expire_condition_[0] != '\0')
  {
    databuff_printf(buf, buf_len, pos, "expire_info=%s, ", table_schema_.expire_condition_);
  }
  if (table_schema_.comment_str_[0] != '\0')
  {
    databuff_printf(buf, buf_len, pos, "comment_str=%s, ", table_schema_.comment_str_);
  }
  databuff_printf(buf, buf_len, pos, "create_time_column_id=%lu, ", table_schema_.create_time_column_id_);
  databuff_printf(buf, buf_len, pos, "modify_time_column_id_=%lu, ", table_schema_.modify_time_column_id_);

  databuff_printf(buf, buf_len, pos, "columns=[");
  for (int64_t i = 0; i < table_schema_.columns_.count(); ++i)
  {
    const ColumnSchema& col = table_schema_.columns_.at(i);
    databuff_printf(buf, buf_len, pos, "(column_name=%s, ", col.column_name_);
    databuff_printf(buf, buf_len, pos, "column_id_=%lu, ", col.column_id_);
    databuff_printf(buf, buf_len, pos, "column_group_id=%lu, ", col.column_group_id_);
    databuff_printf(buf, buf_len, pos, "rowkey_id=%ld, ", col.rowkey_id_);
    databuff_printf(buf, buf_len, pos, "join_table_id=%lu, ", col.join_table_id_);
    databuff_printf(buf, buf_len, pos, "join_column_id=%lu, ", col.join_column_id_);
    databuff_printf(buf, buf_len, pos, "data_type=%d, ", col.data_type_);
    databuff_printf(buf, buf_len, pos, "data_length_=%ld, ", col.data_length_);
    databuff_printf(buf, buf_len, pos, "data_precision=%ld, ", col.data_precision_);
    databuff_printf(buf, buf_len, pos, "nullable=%s, ", col.nullable_ ? "TRUE" : "FALSE");
    databuff_printf(buf, buf_len, pos, "length_in_rowkey=%ld, ", col.length_in_rowkey_);
    databuff_printf(buf, buf_len, pos, "gm_create=%ld, ", col.gm_create_);
    databuff_printf(buf, buf_len, pos, "gm_modify=%ld)", col.gm_modify_);
    if (i != table_schema_.columns_.count())
      databuff_printf(buf, buf_len, pos, ", ");
  } // end for

  // No JoinInfo here
  databuff_printf(buf, buf_len, pos, "])\n");

  return pos;
}
