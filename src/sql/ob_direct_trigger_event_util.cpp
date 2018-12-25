/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_direct_trigger_event_util.cpp,  02/28/2013 05:48:18 PM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:
 *
 *
 */
#include <tbsys.h>
#include "common/ob_trigger_msg.h"
#include "common/ob_trigger_event_util.h"
#include "ob_sql.h"
#include "ob_direct_trigger_event_util.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObDirectTriggerEventUtil::refresh_new_config(ObResultSet &result, ObSqlContext &context)
{
  int err = OB_SUCCESS;
  // fire an event to tell all clusters
  char buf[OB_MAX_SQL_LENGTH] = "";
  ObString stmt;
  stmt.assign_buffer(buf, sizeof (buf));
  if (OB_SUCCESS != (err = ObTriggerEventUtil::format(stmt, REFRESH_NEW_CONFIG_TRIGGER, 0)))
  {
    TBSYS_LOG(WARN, "fail to format sql. err=%d", err);
  }
  else if (OB_SUCCESS != (err = ObSql::direct_execute(stmt, result, context)))
  {
    TBSYS_LOG(ERROR, "fail to trigger refresh_new_config event. err[%d]", err);
  }
  else
  {
    TBSYS_LOG(INFO, "trigger refresh_new_config event succ: err[%d]", err);
  }
  return err;
}


