/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef __OB_UPDATESERVER_OB_LOG_LOCATOR_H__
#define __OB_UPDATESERVER_OB_LOG_LOCATOR_H__

#include "common/ob_define.h"

namespace oceanbase
{
  namespace updateserver
  {
    struct ObLogLocation
    {
      ObLogLocation(): log_id_(0), file_id_(0), offset_(0) {}
      ~ObLogLocation() {}
      int64_t log_id_;
      int64_t file_id_;
      int64_t offset_;
      bool is_valid() const
      {
        return log_id_ > 0 && file_id_ > 0 && offset_ >= 0;
      }
    };

    class IObLogLocator
    {
      public:
        IObLogLocator(){}
        virtual ~IObLogLocator(){}
        virtual int get_location(const int64_t log_id, ObLogLocation& location) = 0;
    };
  } // end namespace updateserver
} // end namespace oceanbase
#endif /* __OB_UPDATESERVER_OB_LOG_LOCATOR_H__ */

