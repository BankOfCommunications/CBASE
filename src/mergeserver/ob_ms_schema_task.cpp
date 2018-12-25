/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_timer_task.h,v 0.1 2010/10/29 10:25:10 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_ms_schema_task.h"
#include "ob_ms_schema_proxy.h"
#include "common/ob_schema_manager.h"
#include "common/ob_malloc.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerSchemaTask::ObMergerSchemaTask()
{
  schema_proxy_ = NULL;
  schema_ = NULL;
  local_version_ = 0;
  remote_version_ = 0;
}

ObMergerSchemaTask::~ObMergerSchemaTask()
{
}

void ObMergerSchemaTask::runTimerTask(void)
{
  int ret = OB_SUCCESS;
  if (true != check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check schema timer task inner stat failed");
  }
  else if (remote_version_ > local_version_)
  {
    const ObSchemaManagerV2 * new_schema = NULL;
    ret = schema_proxy_->fetch_user_schema(remote_version_, &new_schema);
    if ((ret != OB_SUCCESS) || (NULL == new_schema))
    {
      TBSYS_LOG(WARN, "fetch new schema version failed:schema[%p], local[%ld], "
          "new[%ld], ret[%d]", new_schema, local_version_, remote_version_, ret);
    }
    else
    {
      ret = schema_proxy_->release_schema(new_schema);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "release schema failed:ret[%d]", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "fetch new schema succ:local[%ld], new[%ld]",
            local_version_, remote_version_);
      }
    }
  }
  else
  {
    TBSYS_LOG(WARN, "check new version lt than local version:local[%ld], new[%ld]",
        local_version_, remote_version_);
  }
}

