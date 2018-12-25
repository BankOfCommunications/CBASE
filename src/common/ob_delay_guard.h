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
#ifndef __OB_COMMON_OB_DELAY_GUARD_H__
#define __OB_COMMON_OB_DELAY_GUARD_H__
#include "Time.h"

namespace oceanbase
{
  namespace common
  {
    struct ObDelayGuard
    {
      public:
        ObDelayGuard(const int64_t delay_time, const bool enable = false):
          delay_time_(delay_time), enable_(enable), start_time_(tbsys::CTimeUtil::getTime()) {}
        ~ObDelayGuard()
        {
          int64_t left_time = start_time_ + delay_time_ - tbsys::CTimeUtil::getTime();
          if (enable_ && left_time > 0)
          {
            usleep((useconds_t)left_time);
          }
        }
        void enable() { enable_ = true; }
      private:
        int64_t delay_time_;
        bool enable_;
        int64_t start_time_;
    };
  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_DELAY_GUARD_H__ */
