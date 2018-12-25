/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_trigger_msg.h,  12/14/2012 11:09:36 AM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:
 *
 *
 */
#ifndef __COMMON_OB_TRIGGER_MSG_
#define __COMMON_OB_TRIGGER_MSG_

#include "ob_define.h"
#include "ob_string.h"

namespace oceanbase
{
  namespace common
  {
    const static int64_t REFRESH_NEW_SCHEMA_TRIGGER = 1;
    const static int64_t UPDATE_PRIVILEGE_TIMESTAMP_TRIGGER = 2;
    const static int64_t SLAVE_BOOT_STRAP_TRIGGER = 3;
    const static int64_t REFRESH_NEW_CONFIG_TRIGGER = 4;
    const static int64_t CREATE_TABLE_TRIGGER = 5;
    const static int64_t DROP_TABLE_TRIGGER = 6;
    //add liuxiao [secondary index] 20150321
    //备集群建内部表的trigger信息
    const static int64_t SLAVE_CREATE_ALL_CCHECKSUM_INFO = 7;
    //add e

    class ObTriggerMsg{
     public:
       ObTriggerMsg(){}
       ~ObTriggerMsg(){}
       NEED_SERIALIZE_AND_DESERIALIZE;
     public:
       ObString src;
       int64_t type;
       int64_t param;
    };

    //add zhaoqiong [Schema Manager] 20150327:b
    class ObDdlTriggerMsg{
     public:
      ObDdlTriggerMsg(){}
      ~ObDdlTriggerMsg(){}
      NEED_SERIALIZE_AND_DESERIALIZE;
    public:
      int64_t type;
      int64_t param;
      int64_t version;
    };
    //add:e
  }; // end namespace common
};

#endif

