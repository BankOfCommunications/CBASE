/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_trigger_event_util.h,  02/28/2013 06:32:08 PM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 *   xielun.szd <xielun.szd@alipay.com>
 * Description:
 *   Util for trigger
 *
 */
#ifndef __OB_COMMON_TRIGGER_EVENT_UTIL__
#define __OB_COMMON_TRIGGER_EVENT_UTIL__

#include <tbsys.h>
#include "common/ob_string.h"
#include "common/ob_server.h"
#include "common/ob_trigger_msg.h"

namespace oceanbase
{
  namespace common
  {
    class ObTriggerEvent;
    class ObTriggerEventUtil
    {
    protected:
      // format the sql before execute
      static int format(ObString & sql, const int64_t type, const int64_t param);
      // according to the event type format the sql and execute the update query
      static int execute(const int64_t type, const int64_t param, ObTriggerEvent & trigger);
    }; // end of class
  }
}

#endif
