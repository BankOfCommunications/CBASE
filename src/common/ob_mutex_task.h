/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#ifndef __OB_COMMON_OB_MUTEX_TASK_H__
#define __OB_COMMON_OB_MUTEX_TASK_H__
#include "tbsys.h"

namespace oceanbase
{
  namespace common
  {
    class ObMutexTask
    {
      public:
        ObMutexTask(): lock_(), is_running_(false), finished_count_(0) {}
        virtual ~ObMutexTask() {}
        int run(void* arg);
        virtual int do_work(void* arg) = 0;
        int64_t get_finished_count() const;
        bool is_running() const;
      protected:
        tbsys::CThreadMutex lock_;
        bool is_running_;
      public:
        int64_t finished_count_;
    };
  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_MUTEX_TASK_H__ */
