/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_rs_trigger_event_util.cpp,  12/19/2012 10:09:10 PM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:
 *
 *
 */
#include "ob_rs_trigger_event_util.h"
#include "ob_root_ms_provider.h"
#include "ob_chunk_server_manager.h"
#include "common/ob_general_rpc_stub.h"
#include "common/ob_trigger_event.h"
#include "common/ob_define.h"
#include "tbsys.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;

int ObRootTriggerUtil::slave_boot_strap(ObTriggerEvent & trigger)
{
  int err = ObTriggerEventUtil::execute(SLAVE_BOOT_STRAP_TRIGGER, DEFAULT_PARAM, trigger);
  if (err != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "fail to trigger slave_boot_strap event. err[%d]", err);
  }
  else
  {
    TBSYS_LOG(TRACE, "trigger slave_boot_strap event succ");
  }
  return err;
}

//add liuxiao [secondary index] 20150323
int ObRootTriggerUtil::slave_create_all_checksum(ObTriggerEvent & trigger)
{
  int err = ObTriggerEventUtil::execute(SLAVE_CREATE_ALL_CCHECKSUM_INFO, DEFAULT_PARAM, trigger);
  if (err != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "fail to trigger create slave all_ccheck_sum. err[%d]", err);
  }
  else
  {
    TBSYS_LOG(TRACE, "trigger create slave all_ccheck_sum succ");
  }
  return err;
}
//add e


int ObRootTriggerUtil::notify_slave_refresh_schema(ObTriggerEvent & trigger)
{
  int err = ObTriggerEventUtil::execute(REFRESH_NEW_SCHEMA_TRIGGER, DEFAULT_PARAM, trigger);
  if (err != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "fail to trigger notify_slave_refresh_schema event. err[%d]", err);
  }
  else
  {
    TBSYS_LOG(TRACE, "trigger notify_slave_refresh_schema event succ");
  }
  return err;
}

int ObRootTriggerUtil::create_table(ObTriggerEvent & trigger, const uint64_t table_id)
{
  int err = ObTriggerEventUtil::execute(CREATE_TABLE_TRIGGER, table_id, trigger);
  if (err != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "fail to trigger create_table event. err[%d]", err);
  }
  else
  {
    TBSYS_LOG(TRACE, "trigger create_table event succ");
  }
  return err;
}

int ObRootTriggerUtil::alter_table(ObTriggerEvent & trigger)
{
  int err = ObTriggerEventUtil::execute(REFRESH_NEW_SCHEMA_TRIGGER, DEFAULT_PARAM, trigger);
  if (err != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "fail to trigger alter_table event. err[%d]", err);
  }
  else
  {
    TBSYS_LOG(TRACE, "trigger alter_table event succ");
  }
  return err;
}

int ObRootTriggerUtil::drop_tables(ObTriggerEvent & trigger, const uint64_t table_id)
{
  int err = ObTriggerEventUtil::execute(DROP_TABLE_TRIGGER, (int64_t)table_id, trigger);
  if (err != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "fail to trigger drop_table event. err[%d]", err);
  }
  else
  {
    TBSYS_LOG(TRACE, "trigger drop_table event succ");
  }
  return err;
}

