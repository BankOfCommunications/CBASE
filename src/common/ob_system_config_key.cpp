/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Filename: ob_system_config_key.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@taobao.com>
 *
 */

#include "ob_system_config_key.h"

using namespace oceanbase;
using namespace oceanbase::common;
const char * ObSystemConfigKey::DEFAULT_VALUE = "ANY";

int ObSystemConfigKey::set_varchar(const ObString &key, const char * strvalue)
{
  int ret = OB_SUCCESS;
  if (NULL == strvalue)
  {
    TBSYS_LOG(WARN, "check varchar value failed");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (key == ObString::make_string("svr_type"))
  {
    strncpy(server_type_, strvalue, sizeof (server_type_));
    server_type_[OB_SERVER_TYPE_LENGTH - 1] = '\0';
  }
  else if (key == ObString::make_string("svr_ip"))
  {
    strncpy(server_ip_, strvalue, sizeof (server_ip_));
    server_ip_[OB_MAX_SERVER_ADDR_SIZE - 1] = '\0';
  }
  else
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "unknow sys config column name: %.*s",
        key.length(), key.ptr());
  }
  return ret;
}


int ObSystemConfigKey::set_int(const ObString &key, int64_t intval)
{
  int ret = OB_SUCCESS;
  if (key == ObString::make_string("cluster_id"))
  {
    cluster_id_ = intval;
  }
  else if (key == ObString::make_string("svr_port"))
  {
    server_port_ = intval;
  }
  else
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "unknow sys config column name: %.*s",
              key.length(), key.ptr());
  }
  return ret;
}

void ObSystemConfigKey::set_version(const int64_t version)
{
  version_ = version;
}

int64_t ObSystemConfigKey::get_version() const
{
  return version_;
}

int64_t ObSystemConfigKey::to_string(char* buf, const int64_t len) const
{
  return snprintf(buf, len, "cluster_id: [%ld]"
                  " server_type: [%s] server_ip: [%s] server_port: [%ld]",
                  cluster_id_, server_type_, server_ip_, server_port_);
}
