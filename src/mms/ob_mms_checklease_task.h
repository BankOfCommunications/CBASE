/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 * First Create_time: 2011-8-5
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_MMS_OB_MMS_CHECKLEASE_TASK_H_
#define OCEANBASE_MMS_OB_MMS_CHECKLEASE_TASK_H_

#include "common/ob_timer.h"
//#include "ob_node.h"

namespace oceanbase
{
  namespace mms
  {
    class ObNode;
    class ObMMSCheckleaseTask : public common::ObTimerTask
    {
      public:
        static const int64_t SCHEDULE_PERIOD = 100000L;
        ObMMSCheckleaseTask(ObNode *node);
        ~ObMMSCheckleaseTask();

        void runTimerTask(void);
      private:
       ObNode* node_;
    };
  }
}
#endif


