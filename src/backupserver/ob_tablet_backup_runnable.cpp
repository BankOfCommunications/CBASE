/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_root_balancer_runnable.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_tablet_backup_runnable.h"
#include "tbsys.h"
using namespace oceanbase::common;
using namespace oceanbase::chunkserver;

namespace oceanbase
{
  namespace backupserver
  {
    ObTabletBackupRunnable::ObTabletBackupRunnable(ObChunkServerConfig &config,
                                               ObTabletBackupManager *mgr)
      : config_(config), backup_mgr_(mgr)
    {
      backup_mgr_->set_backup_thread(this);
    }

    ObTabletBackupRunnable::~ObTabletBackupRunnable()
    {

    }
    void ObTabletBackupRunnable::wakeup()
    {
      backup_worker_sleep_cond_.broadcast();
    }

    void ObTabletBackupRunnable::run(tbsys::CThread *thread, void *arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      TBSYS_LOG(INFO, "[NOTICE] backup worker thread start, waiting [%s]",
           backup_mgr_->get_backup_server()->get_backup_config().migrate_wait_time.str());
      const int wait_second = (int)backup_mgr_->get_backup_server()->get_backup_config().migrate_wait_time / 1000L / 1000L;
      for (int64_t i = 0; i < wait_second && !_stop; i++)
      {
        sleep(1);
      }
      TBSYS_LOG(INFO, "[NOTICE] backup thread is working");
      while (!_stop)
      {
        if(backup_mgr_->backup_started())
        {
          backup_mgr_->do_dispatch_backup_task();
        }
        int64_t sleep_us = backup_mgr_->get_backup_server()->get_backup_config().backup_worker_idle_time;
        TBSYS_LOG(INFO, "backup worker idle, sleep [%s]",
                  backup_mgr_->get_backup_server()->get_backup_config().backup_worker_idle_time.str());
        int sleep_ms = static_cast<int32_t>(sleep_us/1000);
        backup_worker_sleep_cond_.wait(sleep_ms);
      } // end while
      TBSYS_LOG(INFO, "[NOTICE] backup worker thread exit");
    }
  }
}

