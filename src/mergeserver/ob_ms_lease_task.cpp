/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: 0.1: ob_ms_lease_task.h,v 0.1 2011/05/25 12:02:10 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_ms_lease_task.h"
#include "ob_merge_server_service.h"
#include "common/ob_malloc.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerLeaseTask::ObMergerLeaseTask()
{
  service_ = NULL;
}

ObMergerLeaseTask::~ObMergerLeaseTask()
{
}

// WARN: if not registered succ then other task can not executed
void ObMergerLeaseTask::runTimerTask(void)
{
  int ret = OB_SUCCESS;
  if (true != check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check lease timer task inner stat failed");
  }
  else if (false == service_->check_lease())
  {
    // register lease
    TBSYS_LOG(WARN, "%s", "check lease expired register again");
    // if not registered will be blocked until stop or registered 
    ret = service_->register_root_server();
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "register root server failed:ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "%s", "register to root server succ");
    }
  }
  else
  {
    TBSYS_LOG(DEBUG, "check merge server lease ok");
  }
}


