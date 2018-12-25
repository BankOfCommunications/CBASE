/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_rs_trigger_event_util.h,  12/19/2012 10:09:10 PM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 *   xielun.szd <xielun.szd@alipay.com>
 * Description:
 *
 *
 */
#ifndef __ROOTSERVER_OB_ROOT_TRIGGER_UTIL_H__
#define __ROOTSERVER_OB_ROOT_TRIGGER_UTIL_H__

#include "common/ob_trigger_event.h"
#include "common/ob_trigger_event_util.h"
#include "common/ob_general_rpc_stub.h"
#include "ob_chunk_server_manager.h"

namespace oceanbase
{
  namespace rootserver
  {
    class ObRootTriggerUtil:public ObTriggerEventUtil
    {
    private:
      static const int64_t DEFAULT_PARAM = 0;
    public:
      static int slave_boot_strap(common::ObTriggerEvent & trigger);
      static int notify_slave_refresh_schema(common::ObTriggerEvent & trigger);
      static int create_table(common::ObTriggerEvent & trigger, const uint64_t table_id = 0);
      static int alter_table(common::ObTriggerEvent & trigger);
      static int drop_tables(common::ObTriggerEvent & trigger, const uint64_t table_id);
      //add liuxiao [secondary index] 20150323
      static int slave_create_all_checksum(common::ObTriggerEvent & trigger);
      //add e
    };
  } // end namespace rootserver
}
#endif
