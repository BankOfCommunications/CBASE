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
#include "sqltest_param.h"

namespace 
{
  static const char* SQLTEST_SECTION = "sqltest";
  static const char* OB_IP = "ob_ip";
  static const char* OB_PORT = "ob_port";
  static const char* MYSQL_IP = "mysql_ip";
  static const char* MYSQL_PORT = "mysql_port";
  static const char* MYSQL_USER = "mysql_user";
  static const char* MYSQL_PASS = "mysql_pass";
  static const char* MYSQL_DB = "mysql_db";
  static const char* PATTERN_FILE = "pattern_file";
  static const char* SCHEMA_FILE = "schema_file";

  static const char* WRITE_THREAD_COUNT = "write_thread_count";
  static const char* READ_THREAD_COUNT = "read_thread_count";
}

SqlTestParam::SqlTestParam()
{
  memset(this, 0, sizeof(SqlTestParam));
}

SqlTestParam::~SqlTestParam()
{
}

int SqlTestParam::load_string(char* dest, const int64_t size, 
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

int SqlTestParam::load_from_config()
{
  int err = 0;

  if (0 == err)
  {
    ob_port_ = TBSYS_CONFIG.getInt(SQLTEST_SECTION, OB_PORT, 0);
    assert(ob_port_ > 0);
    err = load_string(ob_ip_, sizeof(ob_ip_), SQLTEST_SECTION, OB_IP, true);
  }

  if (0 == err)
  {
    mysql_port_ = TBSYS_CONFIG.getInt(SQLTEST_SECTION, MYSQL_PORT, 0);
    assert(mysql_port_ > 0);
    err = load_string(mysql_ip_, sizeof(mysql_ip_), SQLTEST_SECTION, MYSQL_IP, true);
    if (0 == err)
    {
      err = load_string(mysql_db_, sizeof(mysql_db_), SQLTEST_SECTION, MYSQL_DB, true);
    }

    if (0 == err)
    {
      err = load_string(mysql_user_, sizeof(mysql_user_), SQLTEST_SECTION, MYSQL_USER, true);
    }

    if (0 == err)
    {
      err = load_string(mysql_pass_, sizeof(mysql_pass_), SQLTEST_SECTION, MYSQL_PASS, false);
    }
  }

  if (0 == err)
  {
    read_thread_count_ = TBSYS_CONFIG.getInt(SQLTEST_SECTION, READ_THREAD_COUNT, 0);
    write_thread_count_ = TBSYS_CONFIG.getInt(SQLTEST_SECTION, WRITE_THREAD_COUNT, 0);
    assert(write_thread_count_ <= 1); // less than one thread
  }

  if (0 == err)
  {
    err = load_string(pattern_file_, sizeof(pattern_file_),
        SQLTEST_SECTION, PATTERN_FILE, true);
  }

  if (0 == err)
  {
    err = load_string(schema_file_, sizeof(schema_file_), SQLTEST_SECTION,
        SCHEMA_FILE, true);
  }

  return err;
}

void SqlTestParam::dump_param()
{
  fprintf(stderr, "    ob_addr: [%s, %d]\n"
      "    mysql_addr: [%s, %d]\n"
      "    mysql_info: [user:%s, pass:%s, db:%s]\n"
      "    write_thread_count: %ld \n"
      "    read_thread_count: %ld \n"
      "    schema_file:%s\n"
      "    pattern_file:%s\n",
      ob_ip_, ob_port_, mysql_ip_, mysql_port_, mysql_user_, mysql_pass_, mysql_db_,
      write_thread_count_, read_thread_count_, schema_file_, pattern_file_);
}
