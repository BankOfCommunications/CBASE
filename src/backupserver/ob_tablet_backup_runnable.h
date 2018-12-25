/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_root_balancer_runnable.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_TABLET_BACKUP_RUNNABLE_H
#define _OB_TABLET_BACKUP_RUNNABLE_H 1
#include "ob_tablet_backup_manager.h"
#include "chunkserver/ob_chunk_server_config.h"

namespace oceanbase
{
  namespace backupserver
  {
    using oceanbase::chunkserver::ObChunkServerConfig;
    class ObTabletBackupManager;
    class ObTabletBackupRunnable : public tbsys::CDefaultRunnable
    {
      public:
        ObTabletBackupRunnable(ObChunkServerConfig &config,
                               ObTabletBackupManager * mgr);
        virtual ~ObTabletBackupRunnable();
        void run(tbsys::CThread *thread, void *arg);
        void wakeup();


      private:
        // disallow copy
        ObTabletBackupRunnable(const ObTabletBackupRunnable &other);
        ObTabletBackupRunnable& operator=(const ObTabletBackupRunnable &other);
      private:
        // data members
        ObChunkServerConfig &config_;
        ObTabletBackupManager * backup_mgr_;
        tbsys::CThreadCond backup_worker_sleep_cond_;
    };
  } // end namespace backupserver
} // end namespace oceanbase

#endif /* _OB_TABLET_BACKUP_RUNNABLE_H */

