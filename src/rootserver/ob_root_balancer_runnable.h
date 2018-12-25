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
#ifndef _OB_ROOT_BALANCER_RUNNABLE_H
#define _OB_ROOT_BALANCER_RUNNABLE_H 1
#include "ob_root_balancer.h"
#include "ob_root_server_config.h"
#include "common/ob_role_mgr.h"
namespace oceanbase
{
  namespace rootserver
  {
    class ObRootBalancer;
    class ObRootBalancerRunnable : public tbsys::CDefaultRunnable
    {
      public:
        ObRootBalancerRunnable(ObRootServerConfig &config,
                               ObRootBalancer &balancer,
                               common::ObRoleMgr &role_mgr);
        virtual ~ObRootBalancerRunnable();
        void run(tbsys::CThread *thread, void *arg);
        void wakeup();
      private:
        bool is_master() const;
        // disallow copy
        ObRootBalancerRunnable(const ObRootBalancerRunnable &other);
        ObRootBalancerRunnable& operator=(const ObRootBalancerRunnable &other);
      private:
        // data members
        ObRootServerConfig &config_;
        ObRootBalancer &balancer_;
        common::ObRoleMgr &role_mgr_;
        tbsys::CThreadCond balance_worker_sleep_cond_;
    };
  } // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_ROOT_BALANCER_RUNNABLE_H */

