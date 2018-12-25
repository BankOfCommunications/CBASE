/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_ups_check_runnable.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_ups_check_runnable.h"
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObUpsCheckRunnable::ObUpsCheckRunnable(ObUpsManager& ups_manager)
  :ups_manager_(ups_manager)
{
}

ObUpsCheckRunnable::~ObUpsCheckRunnable()
{
}

void ObUpsCheckRunnable::run(tbsys::CThread* thread, void* arg)
{
  UNUSED(thread);
  UNUSED(arg);
  TBSYS_LOG(INFO, "[NOTICE] ups check thread start, tid=%ld", syscall(__NR_gettid));
  while (!_stop)
  {
    ups_manager_.check_ups_master_exist();
    ups_manager_.check_lease();
    usleep(CHECK_INTERVAL_US);
  }
  TBSYS_LOG(INFO, "[NOTICE] ups check thread exit");
}

