/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_trigger_event_util.h,  02/28/2013 06:32:08 PM zhidong Exp $
 *
 * Author:
 *   xielun.szd <xielun.szd@alipay.com>
 * Description:
 *   Util for trigger
 *
 */

#include "ob_trigger_event.h"
#include "ob_trigger_event_util.h"
#include "ob_inner_table_operator.h"

using namespace oceanbase::common;

int ObTriggerEventUtil::format(ObString & sql, const int64_t type, const int64_t param)
{
  ObServer server;
  int64_t timestamp = tbsys::CTimeUtil::getTime();
  server.set_ipv4_addr(tbsys::CNetUtil::getLocalAddr(NULL), 0);
  int ret = ObInnerTableOperator::update_all_trigger_event(sql, timestamp, server, type, param);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "update trigger event sql failed:type[%ld], param[%ld], err[%d]", type, param, ret);
  }
  return ret;
}

int ObTriggerEventUtil::execute(const int64_t type, const int64_t param, ObTriggerEvent & trigger)
{
  ObString sql;
  char sql_buf[OB_MAX_SQL_LENGTH] = "";
  sql.assign(sql_buf, sizeof(sql_buf));
  int ret = format(sql, type, param);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "format trigger event sql failed:type[%ld], ret[%d]", type, ret);
  }
  else
  {
    ret = trigger.execute_sql(sql);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "execute sql failed:sql[%.*s], ret[%d]", sql.length(), sql.ptr(), ret);
    }
  }
  return ret;
}

