/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_lease_task.h,v 0.1 2011/05/25 12:02:10 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OB_MERGER_CHECK_LEASE_TASK_H_
#define OB_MERGER_CHECK_LEASE_TASK_H_


#include "common/ob_timer.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergeServerService;
    /// @brief check and fetch new schema timer task
    class ObMergerLeaseTask: public common::ObTimerTask
    {
    public:
      ObMergerLeaseTask();
      ~ObMergerLeaseTask();
    
    public:
      // set service for run task
      void init(ObMergeServerService * service);

      // main routine
      void runTimerTask(void);
    
    private:
      bool check_inner_stat(void) const; 
      ObMergeServerService * service_;
    };

    inline bool ObMergerLeaseTask::check_inner_stat(void) const
    {
      return (NULL != service_);
    }

    inline void ObMergerLeaseTask::init(ObMergeServerService * service)
    {
      service_ = service;
    }
  }
}


#endif //OB_MERGER_CHECK_LEASE_TASK_H_

