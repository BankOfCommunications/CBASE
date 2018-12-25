/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_ups_heartbeat_runnable.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_ups_heartbeat_runnable.h"
#include <sys/types.h>
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObUpsHeartbeatRunnable::ObUpsHeartbeatRunnable(ObUpsManager& ups_manager)
  :ups_manager_(ups_manager)
{
}

ObUpsHeartbeatRunnable::~ObUpsHeartbeatRunnable()
{
}

void ObUpsHeartbeatRunnable::run(tbsys::CThread* thread, void* arg)
{
  UNUSED(thread);
  UNUSED(arg);
  TBSYS_LOG(INFO, "[NOTICE] ups heartbeat thread start, tid=%ld", syscall(__NR_gettid));
  while (!_stop)
  {
     ups_manager_.grant_lease();
     usleep(CHECK_INTERVAL_US);
  }
  //mod pangtianze [Paxos rs_election] 20150825:b
  /*
  if (OB_SUCCESS == ups_manager_.grant_eternal_lease())
  {
    TBSYS_LOG(INFO, "grant eternal ups lease");
  }
  */
  if (OB_SUCCESS == ups_manager_.grant_lease())
  {
    TBSYS_LOG(INFO, "grant ups lease");
  }
  //mod:e
  else
  {
    TBSYS_LOG(ERROR, "failed to grant eternal ups lease");
  }
  TBSYS_LOG(INFO, "[NOTICE] ups heartbeat thread exit");
}
