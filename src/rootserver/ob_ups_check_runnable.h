/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_ups_check_runnable.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_UPS_CHECK_RUNNABLE_H
#define _OB_UPS_CHECK_RUNNABLE_H 1
#include <tbsys.h>
#include "rootserver/ob_ups_manager.h"
namespace oceanbase
{
  namespace rootserver
  {
    class ObUpsCheckRunnable: public tbsys::CDefaultRunnable
    {
    public:
      ObUpsCheckRunnable(ObUpsManager& ups_manager);
      virtual ~ObUpsCheckRunnable();
      virtual void run(tbsys::CThread* thread, void* arg);
    private:
      // disallow copy
      ObUpsCheckRunnable(const ObUpsCheckRunnable &other);
      ObUpsCheckRunnable& operator=(const ObUpsCheckRunnable &other);
    private:
      // data members
      ObUpsManager &ups_manager_;
      static const int64_t CHECK_INTERVAL_US = 50000LL; // 100ms
    };
  } // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_UPS_CHECK_RUNNABLE_H */

