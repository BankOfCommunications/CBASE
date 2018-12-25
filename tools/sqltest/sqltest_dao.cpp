/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: sqltest_dao.cpp,v 0.1 2012/02/24 10:08:24 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "sqltest_dao.h"
#include "util.h"

SqlTestDao::SqlTestDao()
{
  memset(ob_host_, 0x00, sizeof(ob_host_));
  ob_port_ = 0;
  memset(mysql_host_, 0x00, sizeof(mysql_host_));
  mysql_port_ = 0;
  ob_client_ = NULL;
  mysql_client_ = NULL;
}

SqlTestDao::~SqlTestDao()
{
  if (NULL != ob_client_)
  {
    delete ob_client_;
    ob_client_ = NULL;
  }

  if (NULL != mysql_client_)
  {
    delete mysql_client_;
    mysql_client_ = NULL;
  }
}

int SqlTestDao::init(char* ob_host, int ob_port, char* mysql_host, int mysql_port,
    char* mysql_user, char* mysql_pass, char* mysql_db)
{
  int err = OB_SUCCESS;

  if (NULL != ob_client_ || NULL != mysql_client_)
  {
    TBSYS_LOG(WARN, "init twice");
    err = OB_ERROR;
  }
  else
  {
    ob_client_ = new ObSqlClient();
    mysql_client_ = new MysqlClient();

    err = ((ObSqlClient*) ob_client_)->init(ob_host, ob_port);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to init ob client, err=%d", err);
    }
    else
    {
      err = ((MysqlClient*) mysql_client_)->init(mysql_host, mysql_port, mysql_user, mysql_pass, mysql_db);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to init mysql client, err=%d", err);
      }
    }

    if (OB_SUCCESS == err)
    {
      // init prefix info
      err = prefix_info_.init(*((ObSqlClient*) ob_client_));
      if (0 != err)
      {
        TBSYS_LOG(WARN, "failed to init prefix info, err=%d", err);
      }
    }

    if (OB_SUCCESS == err)
    {
      strcpy(ob_host_, ob_host);
      strcpy(mysql_host_, mysql_host);
      ob_port_ = ob_port;
      mysql_port_ = mysql_port;
    }
  }

  return err;
}

int SqlTestDao::exec_query(char* obsql_query, char* mysql_query, uint64_t prefix)
{
  int err = OB_SUCCESS;

  assert(NULL != obsql_query && NULL != mysql_query);
  assert(NULL != ob_client_ && NULL != mysql_client_);

  err = set_flag(prefix, SQLTEST_READING);
  if (0 != err && READ_WRITE_CONFLICT != err)
  {
    TBSYS_LOG(WARN, "failed to set read flag, err=%d", err);
  }

  if (0 == err)
  {
    Array mysql_res;
    Array ob_res;

    err = ob_client_->exec_query(obsql_query, ob_res);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "failed to query ob, sql=%s, err=%d", obsql_query, err);
    }
    else
    {
      err = mysql_client_->exec_query(mysql_query, mysql_res);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "failed to query mysql, sql=%s, err=%d", mysql_query, err);
      }
    }

    if (OB_SUCCESS == err)
    {
      err = compare_result(ob_res, mysql_res);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to compare result, err=%d", err);
      }
    }
  }

  if (0 == err)
  {
    err = set_flag(prefix, 0);
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to reset flag, err=%d", err);
    }
  }
  else if (READ_WRITE_CONFLICT == err)
  {
    err = OB_SUCCESS;
  }

  return err;
}

int SqlTestDao::exec_update(char* obsql_update, char* mysql_update, uint64_t prefix)
{
  int err = OB_SUCCESS;

  assert(NULL != obsql_update && NULL != mysql_update);
  assert(NULL != ob_client_ && NULL != mysql_client_);

  err = set_flag(prefix, SQLTEST_UPDATING);
  if (READ_WRITE_CONFLICT != err && 0 != err)
  {
    TBSYS_LOG(WARN, "failed to set update flag to true, err=%d", err);
  }

  if (0 == err)
  {
    err = ob_client_->exec_update(obsql_update);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "failed to update ob, update_sql=%s, err=%d", obsql_update, err);
    }
    else
    {
      err = mysql_client_->exec_update(mysql_update);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "failed to update mysql, update_sql=%s, err=%d", mysql_update, err);
      }
    }
  }

  if (0 == err)
  {
    err = set_flag(prefix, 0);
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to reset flag, err=%d", err);
    }
  }
  else if (READ_WRITE_CONFLICT == err)
  {
    err = OB_SUCCESS;
  }

  return err;
}

int SqlTestDao::exec_query_ob(char* obsql_query, Array& ob_res)
{
  int err = 0;
  assert(NULL != obsql_query && NULL != ob_client_);

  err = ob_client_->exec_query(obsql_query, ob_res);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "failed to query ob, sql=%s, err=%d", obsql_query, err);
  }

  return err;
}

int SqlTestDao::exec_update_ob(char* obsql_update)
{
  int err = 0;

  assert(NULL != obsql_update && NULL != ob_client_);

  err = ob_client_->exec_update(obsql_update);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "failed to update ob, update_sql=%s, err=%d", obsql_update, err);
  }

  return err;
}

int SqlTestDao::compare_result(Array& ob_res, Array& mysql_res)
{
  int err = OB_SUCCESS;

  int cmp_val = ob_res.compare(mysql_res);
  if (cmp_val != 0)
  {
    TBSYS_LOG(ERROR, "result is not equal, cmp_val=%d", cmp_val);
    err = OB_ERROR;
    ob_res.print();
    mysql_res.print();
    if (ob_res.get_row_num() != mysql_res.get_row_num() ||
        ob_res.get_column_num() != mysql_res.get_column_num())
    {
      TBSYS_LOG(ERROR, "exit program");
      exit(0);
    }
  }

  TBSYS_LOG(INFO, "compare result, err=%d", err);
  return err;
}

int SqlTestDao::set_flag(uint64_t prefix, int flag)
{
  int err = 0;

  err = prefix_info_.set_read_write_status(prefix, flag);
  if (READ_WRITE_CONFLICT == err)
  {
    // does nothing
  }
  else if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to set flag, prefix=%lu, flag=%d, err=%d",
        prefix, flag, err);
  }

  return err;
}

