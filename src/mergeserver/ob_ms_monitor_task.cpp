/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_monitor_task.h,v 0.1 2011/05/25 15:08:32 zhidong Exp $
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_merge_server.h"
#include "ob_ms_monitor_task.h"
#include "common/ob_malloc.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerMonitorTask::ObMergerMonitorTask()
{
  old_drop_counter_ = 0;
  min_drop_error_count_ = 1;
}

ObMergerMonitorTask::~ObMergerMonitorTask()
{
}

int ObMergerMonitorTask::init(const ObMergeServer * server)
{
  int ret = OB_SUCCESS;
  if (NULL == server)
  {
    ret = OB_INPUT_PARAM_ERROR;
    TBSYS_LOG(WARN, "check merge server failed:server[%p]", server);
  }
  else
  {
    server_ = server;
  }
  return ret;
}

void ObMergerMonitorTask::runTimerTask(void)
{
  if (true != check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check monitor timer task inner stat failed");
  }
  else
  {
    uint64_t new_drop_counter = server_->get_drop_packet_count();
    if (new_drop_counter > old_drop_counter_)
    {
      if (new_drop_counter > old_drop_counter_ + min_drop_error_count_)
      {
        TBSYS_LOG(ERROR, "check dropped packet count:drop[%lu], current[%lu], old[%lu], min[%ld]",
            new_drop_counter - old_drop_counter_, new_drop_counter, old_drop_counter_, min_drop_error_count_);
      }
      else
      {
        TBSYS_LOG(WARN, "check dropped packet count:drop[%lu], current[%lu], old[%lu], min[%ld]",
            new_drop_counter - old_drop_counter_, new_drop_counter, old_drop_counter_, min_drop_error_count_);
      }
    }
    old_drop_counter_ = new_drop_counter;
    /// print all module memory usage
    ob_print_mod_memory_usage();
  }
}
