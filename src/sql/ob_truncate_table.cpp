/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_TRUNCATE_table.cpp
 *
 *
 */
#include "sql/ob_truncate_table.h"
#include "common/utility.h"
#include "mergeserver/ob_rs_rpc_proxy.h"
#include "sql/ob_sql.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
ObTruncateTable::ObTruncateTable()
  :if_exists_(false), rpc_(NULL)
{
}

ObTruncateTable::~ObTruncateTable()
{
}

void ObTruncateTable::reset()
{
  if_exists_ = false;
  rpc_ = NULL;
  local_context_.rs_rpc_proxy_ = NULL;
}

void ObTruncateTable::reuse()
{
  if_exists_ = false;
  rpc_ = NULL;
  local_context_.rs_rpc_proxy_ = NULL;
}


void ObTruncateTable::set_sql_context(const ObSqlContext &context)
{
  local_context_ = context;
  local_context_.schema_manager_ = NULL;
}

void ObTruncateTable::set_if_exists(bool if_exists)
{
  if_exists_ = if_exists;
}

void ObTruncateTable::set_comment(const ObString & comment)
{
  comment_ = comment;
}

int ObTruncateTable::add_table_name(const ObString &tname)
{
  return tables_.add_string(tname);
}

int ObTruncateTable::add_table_id(const uint64_t &tid)
{
  return table_ids_.push_back(tid);
}
//add:e

int ObTruncateTable::open()
{
  int ret = OB_SUCCESS;
  if (NULL == rpc_ || 0 >= tables_.count())
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "not init, rpc_=%p", rpc_);
  }
  else if (local_context_.session_info_->get_autocommit() == false || local_context_.session_info_->get_trans_id().is_valid())
  {
    TBSYS_LOG(WARN, "truncate table is not allowed in transaction, err=%d", ret);
    ret = OB_ERR_TRANS_ALREADY_STARTED;
  }
  else if (OB_SUCCESS != (ret = rpc_->truncate_table(if_exists_, tables_, local_context_.session_info_->get_user_name(), comment_)))
  {
    TBSYS_LOG(WARN, "failed to truncate table, err=%d", ret);
  }
  else
  {
    TBSYS_LOG(INFO, "truncate table succ, tables=[%s] if_exists=%c",
              to_cstring(tables_), if_exists_?'Y':'N');
  }
  return ret;
}

int ObTruncateTable::close()
{
  int ret = OB_SUCCESS;
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObTruncateTable, PHY_TRUNCATE_TABLE);
  }
}

int64_t ObTruncateTable::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Truncate Tables(if_exists=%c tables=[", if_exists_?'Y':'N');
  pos += tables_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, "])\n");
  return pos;
}
