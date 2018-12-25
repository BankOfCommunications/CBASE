/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_sql_context.h,  08/24/2012 10:28:35 AM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:
 *   sql context params
 *
 */

#ifndef _OB_SQL_CONTEXT_H
#define _OB_SQL_CONTEXT_H

#include "common/ob_schema.h"
#include "common/ob_privilege.h"
#include "mergeserver/ob_ms_async_rpc.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "mergeserver/ob_rs_rpc_proxy.h"
#include "ob_sql_session_info.h"
#include "ob_sql_session_mgr.h"
#include "ob_sql_id_mgr.h"
#include "ob_ps_store.h"
#include "common/location/ob_tablet_location_cache_proxy.h"
#include "common/ob_privilege_manager.h"
#include "common/ob_schema_manager.h"
#include "common/ob_statistics.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergeServerService;
  }

  namespace sql
  {
    struct ObSqlContext
    {
      oceanbase::common::ObTabletLocationCacheProxy * cache_proxy_;
      oceanbase::mergeserver::ObMergerAsyncRpcStub * async_rpc_;
      const oceanbase::common::ObSchemaManagerV2 *schema_manager_;
      oceanbase::mergeserver::ObMergerRpcProxy *merger_rpc_proxy_;
      oceanbase::mergeserver::ObMergerRootRpcProxy *rs_rpc_proxy_;
      const oceanbase::mergeserver::ObMergeServerService *merge_service_;
      ObPsStore *ps_store_;     /* Ps Global Store */
      ObSQLSessionInfo *session_info_;
      ObSQLSessionMgr *session_mgr_;
      ObSQLIdMgr * sql_id_mgr_;
      const oceanbase::common::ObPrivilege **pp_privilege_;
      bool disable_privilege_check_;
      bool is_prepare_protocol_;
      // lide.wd: 两个全局指针，析构的时候不要碰
      common::ObMergerSchemaManager *merger_schema_mgr_;
      common::ObPrivilegeManager *privilege_mgr_;
      common::ObStatManager *stat_mgr_;

      // the following members are used by SQL module internally
      ObIAllocator *transformer_allocator_;

      ObSqlContext() :
        cache_proxy_(NULL),
        async_rpc_(NULL),
        schema_manager_(NULL),
        merger_rpc_proxy_(NULL),
        rs_rpc_proxy_(NULL),
        merge_service_(NULL),
        session_info_(NULL),
        session_mgr_(NULL),
        pp_privilege_(NULL),
        disable_privilege_check_(false),
        is_prepare_protocol_(false),
        merger_schema_mgr_(NULL),
        privilege_mgr_(NULL),
        stat_mgr_(NULL),
        transformer_allocator_(NULL)
      {
      }
    };
  }
}
#endif //_OB_SQL_CONTEXT_H
