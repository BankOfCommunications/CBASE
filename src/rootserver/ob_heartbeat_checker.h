/*
 * Copyright (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *     - some work details here
 */

#ifndef _OB_HEARTBEAT_CHECKER_H_
#define _OB_HEARTBEAT_CHECKER_H_

#include "tbsys.h"

namespace oceanbase
{
  namespace rootserver
  {
    class ObRootServer2;
    class ObHeartbeatChecker : public tbsys::CDefaultRunnable
    {
    public:
      ObHeartbeatChecker(ObRootServer2 * root_server);
      virtual ~ObHeartbeatChecker();
    public:
      void run(tbsys::CThread * thread, void * arg);
    private:
      static const int64_t HB_RETRY_FACTOR = 100 * 1000;
      static const int64_t CHECK_INTERVAL_US = 100000LL; // 100ms
    private:
      ObRootServer2 * root_server_;
    };
  }
}

#endif //_OB_HEARTBEAT_CHECKER_H_
