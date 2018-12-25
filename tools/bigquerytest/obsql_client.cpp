/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_api.cpp,v 0.1 2012/02/22 17:19:06 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "obsql_client.h"
#include "libpq-fe.h"

namespace
{
  const pthread_key_t INVALID_THREAD_KEY = UINT32_MAX;
}

ObSqlClient::ObSqlClient()
{
  is_init_ = false;
  memset(host_addr_, 0x00, sizeof(host_addr_));
  port_ = 0;
  key_ = INVALID_THREAD_KEY;
  //conn_ = NULL;
  create_thread_key();
}

ObSqlClient::~ObSqlClient()
{
  close();
}

int ObSqlClient::init(char* hostaddr, int port)
{
  int err = OB_SUCCESS;
  if (NULL == hostaddr || port <= 0)
  {
    TBSYS_LOG(WARN, "invalid param, hostaddr=%p, port=%d", hostaddr, port);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    int thread_safe = PQisthreadsafe();
    TBSYS_LOG(INFO, "pgsql thread_safety=%s", thread_safe ? "true" : "false");
    sprintf(host_addr_, "%s", hostaddr);
    port_ = port;
    /*
    err = connect();
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to connect, err=%d", err);
    }
    */
  }

  if (OB_SUCCESS == err)
  {
    is_init_ = true;
  }

  return err;
}

int ObSqlClient::close()
{
  int err = OB_SUCCESS;
  void* ptr = pthread_getspecific(key_);
  if (NULL != ptr)
  {
    PQfinish((PGconn*) ptr);
    ptr = NULL;
  }
  delete_thread_key();

  return err;
}

int ObSqlClient::exec_query(char* query_sql, Array& array)
{
  int err = OB_SUCCESS;

  //PGconn* conn = (PGconn*) pthread_get_specific(key_);
  PGconn* conn = get_conn();
  assert(query_sql != NULL && query_sql[0] != '\0');
  assert(true == is_init_ && NULL != conn);

  PGresult* res = PQexec(conn, query_sql);
  if (PQresultStatus(res) != PGRES_TUPLES_OK)
  {
    TBSYS_LOG(WARN, "query failed, query_sql=%s, err_msg=%s, err=%d", query_sql, PQerrorMessage(conn),
        PQresultStatus(res));
    err = OB_ERROR;
  }
  else
  {
    err = translate_res(res, array);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to translate res, err=%d", err);
    }
  }

  PQclear(res);

  TBSYS_LOG(INFO, "[OBSQL:exec_query], sql=%s, err=%d", query_sql, err);
  return err;
}

int ObSqlClient::exec_update(char* update_sql)
{
  int err = OB_SUCCESS;

  //PGconn* conn = (PGconn*) pthread_get_specific(key_);
  PGconn* conn = get_conn();
  assert(NULL != update_sql && update_sql[0] != '\0');
  assert(true == is_init_ && NULL != conn);

  PGresult* res = PQexec(conn, update_sql);
  if (PQresultStatus(res) != PGRES_COMMAND_OK)
  {
    TBSYS_LOG(WARN, "update sql failed, update_sql=%s, err_msg=%s, code=%d", update_sql, PQerrorMessage(conn),
        PQresultStatus(res));
    err = OB_ERROR;
  }

  PQclear(res);

  TBSYS_LOG(INFO, "[OBSQL:exec_update], sql=%s, err=%d", update_sql, err);
  return err;
}

int ObSqlClient::connect()
{
  int err = OB_SUCCESS;

  char conn_info[256];
  sprintf(conn_info, "hostaddr=%s port=%d", host_addr_, port_);
  PGconn* conn = PQconnectdb(conn_info);

  TBSYS_LOG(INFO, "[GET_CONN] host_addr=%s, port=%d, conn_=%p", host_addr_, port_, conn);

  if (PQstatus(conn) != CONNECTION_OK)
  {
    TBSYS_LOG(WARN, "Failed to connnect to obconnector: hostaddr=%s, port=%d, conn_info=%s, err_msg=%s",
        host_addr_, port_, conn_info, PQerrorMessage(conn));
    err = OB_ERROR;
  }
  else
  {
    err = pthread_setspecific(key_, (void*) conn);
    if (0 != err)
    {
      TBSYS_LOG(ERROR, "pthread_setspecific failed, err=%d", err);
    }
  }

  return err;
}

int ObSqlClient::translate_res(PGresult* res, Array& array)
{
  int err = OB_SUCCESS;

  if (NULL == res)
  {
    TBSYS_LOG(WARN, "invalid param");
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    int row_num = PQntuples(res);
    int col_num = PQnfields(res);
    err = array.init(row_num, col_num);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to init result array, row_num=%d, col_num=%d, err=%d",
          row_num, col_num, err);
    }
    else
    {
      for (int i = 0; OB_SUCCESS == err && i < row_num; ++i)
      {
        for (int j = 0; OB_SUCCESS == err && j < col_num; ++j)
        {
          int type = 0;
          int type_id = PQftype(res, j);
          switch (type_id)
          {
            case 700:
            case 701:
              type = Array::FLOAT_TYPE;
              break;
            case 1043:
              type = Array::VARCHAR_TYPE;
              break;
            case 20:
            case 23:
              type = Array::INT_TYPE;
              break;
            case 1082:
            case 1083:
            case 1114:
              type = Array::DATETIME_TYPE;
              break;
            default:
              TBSYS_LOG(ERROR, "invalid type, type_id=%d", type_id);
              err = OB_ERROR;
              break;
          }

          if (0 == err)
          {
            err = array.set_value(i, j, PQgetvalue(res, i, j), type);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to set value, i=%d, j=%d", i, j);
            }
          }
        }
      }
    }
  }

  return err;
}

int ObSqlClient::create_thread_key()
{
  int ret = pthread_key_create(&key_, destroy_thread_key);
  if (0 != ret)
  {
    TBSYS_LOG(ERROR, "cannot create thread key:%d", ret);
  }
  return (0 == ret) ? OB_SUCCESS : OB_ERROR;
}

int ObSqlClient::delete_thread_key()
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

void ObSqlClient::destroy_thread_key(void* ptr)
{
  TBSYS_LOG(DEBUG, "delete thread specific ptr:%p", ptr);
  if (NULL != ptr)
  {
    PQfinish((PGconn*) ptr);
  }
}

PGconn* ObSqlClient::get_conn()
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
  
  return (PGconn*) ptr;
}


