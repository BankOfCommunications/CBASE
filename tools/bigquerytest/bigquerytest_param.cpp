/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: sqltest_param.h,v 0.1 2012/02/24 10:08:18 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "bigquerytest_param.h"

namespace 
{
  static const char* SQLTEST_SECTION = "bigquerytest";
  static const char* CLIENT_ID = "client_id";
  static const char* OB_IP = "ob_ip";
  static const char* OB_PORT = "ob_port";
  static const char* OB_RS_IP = "ob_rs_ip";
  static const char* OB_RS_PORT = "ob_rs_port";
  static const char* OB_ADDR = "ob_addr";
  static const char* WRITE_THREAD_COUNT = "write_thread_count";
  static const char* READ_THREAD_COUNT = "read_thread_count";
}

BigqueryTestParam::BigqueryTestParam()
{
  memset(this, 0, sizeof(BigqueryTestParam));
}

BigqueryTestParam::~BigqueryTestParam()
{
}

int BigqueryTestParam::load_string(char* dest, const int64_t size, 
    const char* section, const char* name, 
    bool not_null)
{
  int ret           = OB_SUCCESS;
  const char* value = NULL;

  if (NULL == dest || 0 >= size || NULL == section || NULL == name)
  {
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret)
  {
    value = TBSYS_CONFIG.getString(section, name);
    if (not_null && (NULL == value || 0 >= strlen(value)))
    {
      TBSYS_LOG(WARN, "%s.%s has not been set.", section, name);
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret && NULL != value)
  {
    if ((int32_t)strlen(value) >= size)
    {
      TBSYS_LOG(WARN, "%s.%s too long, length (%lu) > %ld", 
          section, name, strlen(value), size);
      ret = OB_SIZE_OVERFLOW;
    }
    else
    {
      memset(dest, 0, size);
      strncpy(dest, value, strlen(value));
    }
  }

  return ret;
}

int BigqueryTestParam::load_from_config()
{
  int err = 0;

  client_id_ = TBSYS_CONFIG.getInt(SQLTEST_SECTION, CLIENT_ID, 0);
  assert(client_id_ >= 0);

  /*
  if (0 == err)
  {
    ob_port_ = TBSYS_CONFIG.getInt(SQLTEST_SECTION, OB_PORT, 0);
    assert(ob_port_ > 0);
    err = load_string(ob_ip_, sizeof(ob_ip_), SQLTEST_SECTION, OB_IP, true);
  }

  if (0 == err)
  {
    ob_rs_port_ = TBSYS_CONFIG.getInt(SQLTEST_SECTION, OB_RS_PORT, 0);
    assert(ob_rs_port_ > 0);
    err = load_string(ob_rs_ip_, sizeof(ob_rs_ip_), SQLTEST_SECTION, OB_RS_IP, true);
  }
  */
  if (0 == err)
  {
    err = load_string(ob_addr_, sizeof(ob_addr_), SQLTEST_SECTION, OB_ADDR, true);
  }

  if (0 == err)
  {
    read_thread_count_ = TBSYS_CONFIG.getInt(SQLTEST_SECTION, READ_THREAD_COUNT, 0);
    write_thread_count_ = TBSYS_CONFIG.getInt(SQLTEST_SECTION, WRITE_THREAD_COUNT, 0);
    assert(write_thread_count_ <= 1); // less than one thread
  }

  return err;
}

void BigqueryTestParam::dump_param()
{
  fprintf(stderr, "    ob_addr: [%s] write_thread_count: %ld read_thread_count: %ld \n",
      ob_addr_, write_thread_count_, read_thread_count_);
}
