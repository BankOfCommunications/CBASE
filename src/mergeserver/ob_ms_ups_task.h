/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_ups_task .h,v 0.1 2011/10/24 16:44:10 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OB_MERGER_FETCH_UPS_TASK_H_
#define OB_MERGER_FETCH_UPS_TASK_H_


#include "common/ob_timer.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerRpcProxy;
    /// @brief fetch ups list timer task
    class ObMergerUpsTask: public common::ObTimerTask
    {
    public:
      ObMergerUpsTask();
      ~ObMergerUpsTask();

    public:
      // set rpc proxy for run task
      void init(ObMergerRpcProxy * rpc_proxy);

      // main routine
      void runTimerTask(void);

    private:
      bool check_inner_stat(void) const;
      ObMergerRpcProxy * rpc_proxy_;
    };

    inline bool ObMergerUpsTask::check_inner_stat(void) const
    {
      return (NULL != rpc_proxy_);
    }

    inline void ObMergerUpsTask::init(ObMergerRpcProxy * rpc_proxy)
    {
      rpc_proxy_ = rpc_proxy;
    }
  }
}


#endif //OB_MERGER_FETCH_UPS_TASK_H_

