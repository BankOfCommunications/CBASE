/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_privilege_type.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_privilege_type.h"

#ifdef __cplusplus
extern "C" {
#endif

const char* OB_PRIV_TYPE_STR[OB_PRIV_NUM] =
{
  "INVALID",
  "ALL",
  "ALTER",
  "CREATE",
  "CREATE_USER",
  "DELETE",
  "DROP",
  "GRANT_OPTION",
  "INSERT",
  "UPDATE",
  "SELECT"
};

const char* ob_priv_type_str(const ObPrivilegeType priv_type)
{
  const char* ret = "Unknown";
  if (priv_type < OB_PRIV_NUM && priv_type >= OB_PRIV_INVALID)
  {
    ret = OB_PRIV_TYPE_STR[priv_type];

  }
  return ret;
}

#ifdef __cplusplus
}
#endif
