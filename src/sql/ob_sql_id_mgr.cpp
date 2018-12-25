/*
 * (C) 2013-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_sql_id_mgr.h$
 *
 * Authors:
 *   Fusheng Han <yanran.hfs@alipay.com>
 *
 */

#include "ob_sql_id_mgr.h"
#include "common/ob_define.h"
#include "common/ob_statistics.h"
#include "common/ob_common_stat.h"
#include <tbsys.h>

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::sql;

ObSQLIdMgr::ObSQLIdMgr()
  : last_sql_id_(0), sql_str_buf_(ObModIds::OB_SQL_ID_MGR)
{
}

ObSQLIdMgr::~ObSQLIdMgr()
{
}

int ObSQLIdMgr::init()
{
  int ret = OB_SUCCESS;
  ret = sql2id_map_.create(SQL_ID_MAX_BUCKET_NUM);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "ObHashMap create error, ret: %d", ret);
  }
  else if (OB_SUCCESS != (ret = id2sql_map_.create(SQL_ID_MAX_BUCKET_NUM)))
  {
    TBSYS_LOG(WARN, "id2sql_map_ init error, err=%d", ret);
  }
  return ret;
}

int ObSQLIdMgr::get_sql_id(const ObString & sql, int64_t & sql_id)
{
  int ret = OB_SUCCESS;
  int err = sql2id_map_.get(sql, sql_id);
  if (HASH_EXIST == err)
  {
    TBSYS_LOG(DEBUG, "got sql_id from sqlidmgr sql(%.*s), sqlid=%ld", sql.length(), sql.ptr(), sql_id);
  }
  else if (HASH_NOT_EXIST == err)
  {
    tbsys::CThreadGuard guard(&lock_);
    err = sql2id_map_.get(sql, sql_id);
    if (HASH_EXIST == err)
    {
      TBSYS_LOG(DEBUG, "got sql_id from sqlidmgr sql(%.*s), sqlid=%ld", sql.length(), sql.ptr(), sql_id);
    }
    else if (HASH_NOT_EXIST == err)
    {
      ObString stored_sql;
      sql_id = allocate_new_id();
      err = sql_str_buf_.write_string(sql, &stored_sql);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "ObStringBufT write_string error, err: %d", err);
        ret = OB_ERROR;
      }
      else
      {
        err = sql2id_map_.set(stored_sql, sql_id);
        if (HASH_INSERT_SUCC == err)
        {
          err = sql2id_map_.get(sql, sql_id);
          if (HASH_NOT_EXIST == err)
          {
            TBSYS_LOG(ERROR, "This should not be reached");
            sql_id = 0;
            ret = OB_ERR_UNEXPECTED;
          }
          else
          {
            // set id2sql map
            SQLStat stat;
            stat.sql_ = stored_sql;
            stat.create_timestamp_ = tbsys::CTimeUtil::getTime();
            if (HASH_INSERT_SUCC != (err = id2sql_map_.set(sql_id, stat)))
            {
              // rollback
              sql2id_map_.erase(stored_sql);
              TBSYS_LOG(ERROR, "failed to insert id2sql map, err=%d", ret);
              ret = OB_ERROR;
            }
            else
            {
              OB_STAT_INC(SQL, SQL_DISTINCT_STMT_COUNT);
            }
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "insert into sql2id failed ret is %d", err);
          ret = OB_ERR_UNEXPECTED;
        }
      }
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "ObSQLIdMgr get error, sql(%.*s) err: %d", sql.length(), sql.ptr(), err);
    ret = OB_ERROR;
  }
  return ret;
}

int64_t ObSQLIdMgr::allocate_new_id()
{
  return __sync_add_and_fetch(&last_sql_id_, 1);
}

struct ObStatPrepareOp
{
  void operator() (oceanbase::common::hash::HashMapPair<SQLIdValue, SQLStat> &entry)
  {
    ++entry.second.prepare_count_;
    entry.second.last_active_timestamp_ = tbsys::CTimeUtil::getTime();
  };
};

int ObSQLIdMgr::stat_prepare(int64_t sql_id)
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS;
  ObStatPrepareOp op;
  if (HASH_EXIST != (err = id2sql_map_.atomic(sql_id, op)))
  {
    TBSYS_LOG(WARN, "failed to get sql_id=%ld err=%d", sql_id, err);
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

struct ObStatExecuteOp
{
  ObStatExecuteOp()
    :elapsed_us_(0),
     is_slow_(false)
  {
  }
  void operator() (oceanbase::common::hash::HashMapPair<SQLIdValue, SQLStat> &entry)
  {
    ++entry.second.execute_count_;
    entry.second.execute_time_ += elapsed_us_;
    entry.second.last_active_timestamp_ = tbsys::CTimeUtil::getTime();
    if (is_slow_)
    {
      ++entry.second.slow_count_;
    }
  };
  int64_t elapsed_us_;
  bool is_slow_;
};

int ObSQLIdMgr::stat_execute(int64_t sql_id, int64_t elapsed_us, bool is_slow)
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS;
  ObStatExecuteOp op;
  op.elapsed_us_ = elapsed_us;
  op.is_slow_ = is_slow;
  if (HASH_EXIST != (err = id2sql_map_.atomic(sql_id, op)))
  {
    TBSYS_LOG(WARN, "failed to get sql_id=%ld err=%d", sql_id, err);
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObSQLIdMgr::get_scanner(const ObServer &addr, ObNewScanner &scanner) const
{
  int ret = OB_SUCCESS;

  ObRowDesc row_desc;
  for (int i = 0; i < 10; ++i)
  {
    row_desc.add_column_desc(OB_ALL_STATEMENT_TID, i+OB_APP_MIN_COLUMN_ID);
  }
  row_desc.set_rowkey_cell_count(3);
  ObRow row;
  row.set_row_desc(row_desc);

  char ipbuf[OB_IP_STR_BUFF] = {0};
  addr.ip_to_string(ipbuf, sizeof (ipbuf));
  ObString ipstr = ObString::make_string(ipbuf);
  const int32_t port = addr.get_port();

  ObObj obj;
  int64_t count = 0;
  IdSqlMap::const_iterator it = id2sql_map_.begin();
  for (; it != id2sql_map_.end(); ++it)
  {
    int64_t column_id = OB_APP_MIN_COLUMN_ID;
    row.reset(false, ObRow::DEFAULT_NULL);
    obj.set_varchar(ipstr);
    row.set_cell(OB_ALL_STATEMENT_TID, column_id++, obj);
    obj.set_int(port);
    row.set_cell(OB_ALL_STATEMENT_TID, column_id++, obj);
    obj.set_varchar(it->second.sql_);
    row.set_cell(OB_ALL_STATEMENT_TID, column_id++, obj);
    obj.set_int(it->first);
    row.set_cell(OB_ALL_STATEMENT_TID, column_id++, obj);
    obj.set_int(it->second.prepare_count_);
    row.set_cell(OB_ALL_STATEMENT_TID, column_id++, obj);
    obj.set_int(it->second.execute_count_);
    row.set_cell(OB_ALL_STATEMENT_TID, column_id++, obj);
    int64_t avg_execute_us = 0;
    if (0 < it->second.execute_count_)
    {
      avg_execute_us = it->second.execute_time_/it->second.execute_count_;
    }
    obj.set_int(avg_execute_us);
    row.set_cell(OB_ALL_STATEMENT_TID, column_id++, obj);
    obj.set_int(it->second.slow_count_);
    row.set_cell(OB_ALL_STATEMENT_TID, column_id++, obj);
    obj.set_precise_datetime(it->second.create_timestamp_);
    row.set_cell(OB_ALL_STATEMENT_TID, column_id++, obj);
    obj.set_precise_datetime(it->second.last_active_timestamp_);
    row.set_cell(OB_ALL_STATEMENT_TID, column_id++, obj);
    scanner.add_row(row);
    ++count;
  }
  scanner.set_is_req_fullfilled(true, count);
  return ret;
}

int ObSQLIdMgr::get_sql(int64_t sql_id, common::ObString &sql) const
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS;
  SQLStat stat;
  if (HASH_EXIST != (err = id2sql_map_.get(sql_id, stat)))
  {
    ret = OB_ENTRY_NOT_EXIST;
  }
  else
  {
    sql = stat.sql_;
  }
  return ret;
}

void ObSQLIdMgr::print() const
{
  int64_t count = 0;
  TBSYS_LOG(INFO, "================sql statistics begin================");
  IdSqlMap::const_iterator it = id2sql_map_.begin();
  for (; it != id2sql_map_.end(); ++it)
  {
    int64_t avg_execute_us = 0;
    if (0 < it->second.execute_count_)
    {
      avg_execute_us = it->second.execute_time_/it->second.execute_count_;
    }
    TBSYS_LOG(INFO, "[SQL] idx=%ld id=%ld stmt=`%.*s' prepared_count=%ld executed_count=%ld "
              "avg_execute_us=%ld slow_count=%ld create_timestamp=%ld last_active_timestamp=%ld",
              count, it->first, it->second.sql_.length(), it->second.sql_.ptr(),
              it->second.prepare_count_, it->second.execute_count_,
              avg_execute_us, it->second.slow_count_, it->second.create_timestamp_,
              it->second.last_active_timestamp_);
    ++count;
  }
  TBSYS_LOG(INFO, "================sql statistics end================");
}
