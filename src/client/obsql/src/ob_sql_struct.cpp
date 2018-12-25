/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Authors:
 *   Author XiuMing     2014-03-16
 *   Email: wanhong.wwh@alibaba-inc.com
 *     -some work detail if you want
 *
 */

#include "ob_sql_struct.h"
#include "ob_sql_define.h"
#include "common/ob_define.h"
#include "tblog.h"

using namespace oceanbase::common;

ObSQLServerBlackList::ObSQLServerBlackList() : size_(0)
{
  memset(ip_array_, 0, sizeof(ip_array_));
}

bool ObSQLServerBlackList::exist(const uint32_t ip)
{
  bool exist = false;
  for (int32_t index = 0; index < size_; index++)
  {
    if (ip == ip_array_[index])
    {
      exist = true;
      break;
    }
  }

  return exist;
}

int ObSQLServerBlackList::add(const uint32_t ip)
{
  int ret = OB_SQL_SUCCESS;

  if (!exist(ip))
  {
    if (size_ >= OB_SQL_MAX_SERVER_BLACK_LIST_SIZE)
    {
      TBSYS_LOG(ERROR, "cannot add new server(ip=%d), black list is full. size=%d", ip, size_);
      ret = OB_SQL_ERROR;
    }
    else
    {
      ip_array_[size_++] = ip;
    }
  }

  return ret;
}

void ObSQLServerBlackList::del(const uint32_t ip)
{
  int index = 0;

  for (index = 0; index < size_; index++)
  {
    if (ip == ip_array_[index])
    {
      break;
    }
  }

  if (index < size_)
  {
    for (int j = index + 1; j < size_; j++)
    {
      ip_array_[j - 1] = ip_array_[j];
    }

    size_--;
  }
}
