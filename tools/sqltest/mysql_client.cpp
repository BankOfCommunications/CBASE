/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: mysql_api.cpp,v 0.1 2012/02/23 17:27:43 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "mysql_client.h"

namespace
{
  const pthread_key_t INVALID_THREAD_KEY = UINT32_MAX;
}

MysqlClient::MysqlClient()
{
  //conn_ = NULL;
  key_ = INVALID_THREAD_KEY;
  port_ = 0;
  memset(host_, 0x00, sizeof(host_));
  create_thread_key();
  memset(mysql_user_, 0x00, sizeof(mysql_user_));
  memset(mysql_pass_, 0x00, sizeof(mysql_pass_));
  memset(mysql_db_, 0x00, sizeof(mysql_db_));
}

MysqlClient::~MysqlClient()
{
  close();
}

int MysqlClient::init(char* host, int port, char* mysql_user, char* mysql_pass, char* mysql_db)
{
  int err = OB_SUCCESS;

  if (NULL == host || port <= 0 || NULL == mysql_user || NULL == mysql_pass || NULL == mysql_db)
  {
    TBSYS_LOG(WARN, "invalid param, host=%p, port=%d, mysql_user=%p, mysql_pass=%p, mysql_db=%p",
        host, port, mysql_user, mysql_pass, mysql_db);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    port_ = port;
    strcpy(host_, host);
    strcpy(mysql_user_, mysql_user);
    strcpy(mysql_pass_, mysql_pass);
    strcpy(mysql_db_, mysql_db);
    /*
    err = connect();
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to connect, err=%d", err);
    }
    */
  }

  return err;
}

int MysqlClient::close()
{
  /*
  if (NULL != conn_)
  {
    mysql_close(conn_);
    conn_ = NULL;
  }
  */
  void* ptr = pthread_getspecific(key_);
  if (NULL != ptr)
  {
    mysql_close((MYSQL*) ptr);
    ptr = NULL;
  }
  delete_thread_key();

  return 0;
}

int MysqlClient::exec_query(char* query_sql, Array& res)
{
  int err = OB_SUCCESS;

  MYSQL* conn = get_conn();
  assert(query_sql != NULL && query_sql[0] != '\0');
  assert(NULL != conn);

  err = mysql_query(conn, query_sql);
  if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to exec query, query_sql=%s, err_msg=%s", query_sql, mysql_error(conn));
    err = OB_ERROR;
  }
  else
  {
    MYSQL_RES* result = mysql_store_result(conn);
    MYSQL_ROW row;
    int row_num = mysql_num_rows(result);
    int col_num = mysql_num_fields(result);
    err = res.init(row_num, col_num);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to init array, row_num=%d, col_num=%d, err=%d",
          row_num, col_num, err);
    }
    else
    {
      int row_idx = 0;
      int type = 0;

      while (NULL != (row = mysql_fetch_row(result)))
      {
        mysql_field_seek(result, 0);
        for (int col_idx = 0; col_idx < col_num; ++col_idx)
        {
          MYSQL_FIELD* field = mysql_fetch_field(result);
          assert(NULL != field);
          switch (field->type)
          {
            case MYSQL_TYPE_SHORT:
            case MYSQL_TYPE_LONG:
            case MYSQL_TYPE_LONGLONG:
                type = Array::INT_TYPE;
                break;
            case MYSQL_TYPE_FLOAT:
            case MYSQL_TYPE_DOUBLE:
            case MYSQL_TYPE_NEWDECIMAL:
            case MYSQL_TYPE_DECIMAL:
                type = Array::FLOAT_TYPE;
                break;
            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_TIME:
            case MYSQL_TYPE_DATETIME:
                type = Array::DATETIME_TYPE;
                break;
            case MYSQL_TYPE_STRING:
            case MYSQL_TYPE_VAR_STRING:
                type = Array::VARCHAR_TYPE;
                break;
            default:
                TBSYS_LOG(ERROR, "invalid type, type=%d", field->type);
                err = OB_ERROR;
                break;
          }
          res.set_value(row_idx, col_idx, row[col_idx], type);
        }
        ++row_idx;
      }
    }

    mysql_free_result(result);
  }

  TBSYS_LOG(INFO, "[MYSQL:exec_query]: sql=%s, err=%d", query_sql, err);
  return err;
}

int MysqlClient::exec_update(char* update_sql)
{
  int err = OB_SUCCESS;

  MYSQL* conn = get_conn();
  assert(NULL != update_sql && update_sql[0] != '\0');
  assert(NULL != conn);

  err = mysql_query(conn, update_sql);
  if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to exec update, update_sql=%s, err_msg=%s", update_sql, mysql_error(conn));
    err = OB_ERROR;
  }

  TBSYS_LOG(INFO, "[MYSQL:exec_update]: sql=%s, err=%d", update_sql, err);
  return err;
}

int MysqlClient::connect()
{
  int err = OB_SUCCESS;

  MYSQL* conn = mysql_init(NULL);
  if (NULL == conn)
  {
    TBSYS_LOG(WARN, "failed to init conn");
    err = OB_ERROR;
  }
  else
  {
    if (NULL == mysql_real_connect(conn, host_, mysql_user_, mysql_pass_, mysql_db_, port_, NULL, 0))
    {
      TBSYS_LOG(WARN, "failed to connect, host_=%p, port_=%d, mysql_user_=%s, mysql_pass=%s, mysql_db=%s, err_msg=%s",
          host_, port_, mysql_user_, mysql_pass_, mysql_db_, mysql_error(conn));
      err = OB_ERROR;
    }
    else
    {
      err = pthread_setspecific(key_, (void*) conn);
      if (0 != err)
      {
        TBSYS_LOG(WARN, "pthread_setspecific failed, err=%d", err);
      }
    }
  }

  return err;
}

int MysqlClient::create_thread_key()
{
  int ret = pthread_key_create(&key_, destroy_thread_key);
  if (0 != ret)
  {
    TBSYS_LOG(ERROR, "cannot create thread key:%d", ret);
  }
  return (0 == ret) ? OB_SUCCESS : OB_ERROR;
}

int MysqlClient::delete_thread_key()
{
  int ret = -1;
  if (INVALID_THREAD_KEY != key_)
  {
    ret = pthread_key_delete(key_);
  }
  if (0 != ret)
  {
    TBSYS_LOG(WARN, "delete thread key key_ failed.");
  }
  return (0 == ret) ? OB_SUCCESS : OB_ERROR;
}

void MysqlClient::destroy_thread_key(void* ptr)
{
  TBSYS_LOG(DEBUG, "delete thread specific ptr:%p", ptr);
  if (NULL != ptr)
  {
    mysql_close((MYSQL*) ptr);
    ptr = NULL;
  }
}

MYSQL* MysqlClient::get_conn()
{
  void* ptr = pthread_getspecific(key_);
  if (NULL == ptr)
  {
    int err = connect();
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to connect, err=%d", err);
    }
    else
    {
      ptr = pthread_getspecific(key_);
    }
  }
  
  return (MYSQL*) ptr;
}


