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
#ifndef __OB_COMMON_OB_TASK_H__
#define __OB_COMMON_OB_TASK_H__
namespace oceanbase
{
  namespace common
  {
    class IObAsyncTaskSubmitter
    {
      public:
        IObAsyncTaskSubmitter(){}
        virtual ~IObAsyncTaskSubmitter(){}
        virtual int submit_task(void* arg) = 0;
    };
  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_TASK_H__ */
