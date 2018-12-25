/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_inner_table_operator.h,  03/01/2013 17:27:08 PM zhidong Exp $
 *
 * Author:
 *   xielun.szd <xielun.szd@alipay.com>
 * Description:
 *   Util for inner Table
 *
 */

#include "ob_define.h"
#include "ob_string.h"
#include "ob_server.h"
#include "ob_obi_role.h"
#include "ob_inner_table_operator.h"

using namespace oceanbase::common;
//mod bingo [Paxos Cluster.Balance] 20161019:b
/*int ObInnerTableOperator::update_all_cluster(ObString & sql, const int64_t cluster_id,
    const ObServer & server, const ObiRole role, const int64_t flow_percent)*/
int ObInnerTableOperator::update_all_cluster(ObString & sql, const int64_t cluster_id,
    const ObServer & server, const int32_t master_cluster_id, const int64_t flow_percent)
//mod:e
{
  int ret = OB_SUCCESS;
  if ((sql.ptr() == NULL) || (0 == sql.size()))
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "Check SQL buffer size failed! ptr[%p], size[%d]", sql.ptr(), sql.size());
  }
  else
  {
    char buf[OB_MAX_SERVER_ADDR_SIZE];
    memset(buf, 0 , sizeof(buf));
    if (server.ip_to_string(buf, sizeof(buf)) != true)
    {
      ret = OB_CONVERT_ERROR;
      TBSYS_LOG(ERROR, "server ip is invalid, ret=%d", ret);
    }
    else
    {
      const char * format = "REPLACE INTO %s"
        "(cluster_id, cluster_role, cluster_vip, cluster_flow_percent, read_strategy, rootserver_port) "
        "VALUES(%d,%d,'%s',%d,%d,%d);";
      // random read read_strategy
      //mod bingo [Paxos Cluster.Balance] 20161019:b
      /*      int size = snprintf(sql.ptr(), sql.size(), format, OB_ALL_CLUSTER, cluster_id,
                          role.get_role() == ObiRole::MASTER ? 1 : 2, buf, flow_percent, 0, server.get_port());*/
      int size = snprintf(sql.ptr(), sql.size(), format, OB_ALL_CLUSTER, cluster_id,
                          cluster_id == master_cluster_id ? 1 : 2, buf, flow_percent, 0, server.get_port());
      //mod:e
      if (size >= sql.size())
      {
        TBSYS_LOG(ERROR, "SQL buffer size not enough! size: [%d], need: [%d], sql: [%.*s]",
                  sql.size(), size, sql.length(), sql.ptr());
        ret = OB_SIZE_OVERFLOW;
      }
      else
      {
        sql.assign_ptr(sql.ptr(), size);
        TBSYS_LOG(INFO, "XXX=%.*s", sql.length(), sql.ptr());
      }
    }
  }
  return ret;
}

int ObInnerTableOperator::update_all_trigger_event(ObString & sql, const int64_t timestamp,
    const ObServer & server, const int64_t type, const int64_t param)
{
  UNUSED(param);
  int ret = OB_SUCCESS;
  if ((sql.ptr() == NULL) || (0 == sql.size()))
  {
    TBSYS_LOG(WARN, "Check SQL buffer size failed! ptr[%p], size[%d]", sql.ptr(), sql.size());
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    char buf[OB_MAX_SERVER_ADDR_SIZE] = "";
    if (server.ip_to_string(buf, sizeof(buf)) != true)
    {
      ret = OB_CONVERT_ERROR;
    }
    else
    {
      const char * format = "REPLACE INTO %s"
        "(event_ts, src_ip, event_type, event_param) "
        "values (%ld, '%s', %ld, %ld);";
      int size = snprintf(sql.ptr(), sql.size(), format,
          OB_ALL_TRIGGER_EVENT_TABLE_NAME, timestamp, buf, type, param);
      if (size >= sql.size())
      {
        TBSYS_LOG(ERROR, "SQL buffer size not enough! size: [%d], need: [%d], sql: [%.*s]",
            sql.size(), size, sql.length(), sql.ptr());
        ret = OB_SIZE_OVERFLOW;
      }
      else
      {
        sql.assign_ptr(sql.ptr(), size);
      }
    }
  }
  return ret;
}

int ObInnerTableOperator::update_all_server(ObString & sql, const int64_t cluster_id,
    const char * server_type, const ObServer & server, const uint32_t inner_port, const char * version)
{
  int ret = OB_SUCCESS;
  if ((sql.ptr() == NULL) || (0 == sql.size()))
  {
    TBSYS_LOG(WARN, "Check SQL buffer size failed! ptr[%p], size[%d]", sql.ptr(), sql.size());
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    char buf[OB_MAX_SERVER_ADDR_SIZE] = "";
    if (server.ip_to_string(buf, sizeof(buf)) != true)
    {
      ret = OB_CONVERT_ERROR;
    }
    else
    {
      const char * format = "REPLACE INTO %s"
        "(cluster_id, svr_type, svr_ip, svr_port, inner_port, svr_role, svr_version) "
        "values (%d, '%s', '%s', %u, %u, %d, '%s');";
      int size = snprintf(sql.ptr(), sql.size(), format, OB_ALL_SERVER, cluster_id,
          server_type, buf, server.get_port(), inner_port, 0, version);
      if (size >= sql.size())
      {
        TBSYS_LOG(ERROR, "SQL buffer size not enough! size: [%d], need: [%d], sql: [%.*s]",
            sql.size(), size, sql.length(), sql.ptr());
        ret = OB_SIZE_OVERFLOW;
      }
      else
      {
        sql.assign_ptr(sql.ptr(), size);
      }
    }
  }
  return ret;
}

int ObInnerTableOperator::delete_all_server(ObString & sql, const int64_t cluster_id,
    const char * server_type, const ObServer & server)
{
  int ret = OB_SUCCESS;
  if ((sql.ptr() == NULL) || (0 == sql.size()))
  {
    TBSYS_LOG(WARN, "Check SQL buffer size failed! ptr[%p], size[%d]", sql.ptr(), sql.size());
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    char buf[OB_MAX_SERVER_ADDR_SIZE] = "";
    if (server.ip_to_string(buf, sizeof (buf)) != true)
    {
      ret = OB_CONVERT_ERROR;
    }
    else
    {
      const char * format = "DELETE FROM %s WHERE cluster_id = %d AND svr_type = '%s' "
        "AND svr_ip = '%s' AND svr_port = %u;";
      int size = snprintf(sql.ptr(), sql.size(), format, OB_ALL_SERVER,
          cluster_id, server_type, buf, server.get_port());
      if (size >= sql.size())
      {
        TBSYS_LOG(ERROR, "SQL buffer size not enough! size: [%d], need: [%d], sql: [%.*s]",
            sql.size(), size, sql.length(), sql.ptr());
        ret = OB_SIZE_OVERFLOW;
      }
      else
      {
        sql.assign_ptr(sql.ptr(), size);
      }
    }
  }
  return ret;
}

