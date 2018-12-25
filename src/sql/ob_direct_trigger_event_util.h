/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_direct_trigger_event_util.h,  02/28/2013 05:46:22 PM xiaochu Exp $
 * 
 * Author:  
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:  
 *   Direct trigger event using direct_execute()
 * 
 */
#ifndef __MERGESERVER_OB_DIRECT_TRIGGER_EVENT_UTIL_H__
#define __MERGESERVER_OB_DIRECT_TRIGGER_EVENT_UTIL_H__

#include "common/ob_string.h"
#include "common/ob_trigger_event_util.h"
#include "ob_result_set.h"
#include "ob_sql_context.h"

namespace oceanbase
{
  namespace sql 
  {
    class ObDirectTriggerEventUtil : public ObTriggerEventUtil
    {
      public:
        static int refresh_new_config(ObResultSet &result, ObSqlContext &context);
      private:
        ObDirectTriggerEventUtil();
        ~ObDirectTriggerEventUtil();
    };
  } // end namespace sql
}
#endif
