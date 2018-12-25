/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_ups_heartbeat_runnable.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_UPS_HEARTBEAT_RUNNABLE_H
#define _OB_UPS_HEARTBEAT_RUNNABLE_H 1

#include "tbsys.h"
#include "rootserver/ob_ups_manager.h"
namespace oceanbase
{
  namespace rootserver
  {
    class ObUpsHeartbeatRunnable: public tbsys::CDefaultRunnable
    {
      public:
        ObUpsHeartbeatRunnable(ObUpsManager& ups_manager);
        virtual ~ObUpsHeartbeatRunnable();
        virtual void run(tbsys::CThread* thread, void* arg);
      private:
        // disallow copy
        ObUpsHeartbeatRunnable(const ObUpsHeartbeatRunnable &other);
        ObUpsHeartbeatRunnable& operator=(const ObUpsHeartbeatRunnable &other);
      private:
        static const int64_t CHECK_INTERVAL_US = 50000LL; // 50ms
        // data members
        ObUpsManager &ups_manager_;
    };
  } // end namespace rootserver
} // end namespace oceanbase
#endif /* _OB_UPS_HEARTBEAT_RUNNABLE_H */

