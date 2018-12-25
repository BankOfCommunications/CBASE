/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_schema_task.cpp for update multi-version schemas form
 * rootserver.
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_schema_task.h"
#include "ob_rpc_proxy.h"
#include "common/ob_schema_manager.h"
#include "common/ob_malloc.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    ObMergerSchemaTask::ObMergerSchemaTask()
    {
      task_scheduled_ = false;
      rpc_proxy_ = NULL;
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
        TBSYS_LOG(WARN, "check schema timer task inner stat failed");
      }
      else if (remote_version_ > local_version_)
      {
        const ObSchemaManagerV2 * new_schema = NULL;
        ret = rpc_proxy_->fetch_new_schema(remote_version_, &new_schema);
        if ((ret != OB_SUCCESS) || (NULL == new_schema))
        {
          TBSYS_LOG(WARN, "fetch new schema version failed:schema[%p], local[%ld], "
              "new[%ld], ret[%d]", new_schema, local_version_, remote_version_, ret);
        }
        else
        {
          ret = rpc_proxy_->release_schema(new_schema);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "release schema failed:ret[%d]", ret);
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
      unset_scheduled();
    }
  } // end namespace chunkserver
} // end namespace oceanbase
