/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: 0.1: ob_ms_ups_task.h,v 0.1 2011/10/24 16:48:10 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_ms_ups_task.h"
#include "ob_merge_server_service.h"
#include "common/ob_malloc.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerUpsTask::ObMergerUpsTask()
{
  rpc_proxy_ = NULL;
}

ObMergerUpsTask::~ObMergerUpsTask()
{
}

void ObMergerUpsTask::runTimerTask(void)
{
  int ret = OB_SUCCESS;
  if (true != check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check fetch ups list timer task inner stat failed");
  }
  else
  {
    // update the update_server list should add version for reduce lock contention
    int32_t count = 0;
    ret = rpc_proxy_->fetch_update_server_list(count);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fetch update server list failed:ret[%d]", ret);
    }
  }
}


