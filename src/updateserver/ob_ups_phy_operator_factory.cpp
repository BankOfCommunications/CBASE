/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#include "ob_ups_phy_operator_factory.h"
#include "ob_ups_lock_filter.h"
#include "ob_ups_inc_scan.h"
#include "ob_memtable_modify.h"

#define new_operator(__type__, __allocator__, ...)      \
  ({                                                    \
    __type__ *ret = NULL;                               \
    void* buf = __allocator__.alloc(sizeof(__type__));  \
    if (NULL != buf)                                    \
    {                                                   \
      ret = new(buf) __type__(__VA_ARGS__);             \
    }                                                   \
    ret;                                                \
   })

namespace oceanbase
{
  using namespace sql;
  namespace updateserver
  {
    ObPhyOperator *ObUpsPhyOperatorFactory::get_one(ObPhyOperatorType type, common::ModuleArena &allocator)
    {
      ObPhyOperator *ret = NULL;
      if (NULL == session_ctx_ || NULL == table_mgr_)
      {
        TBSYS_LOG(ERROR, "param not set: session_ctx=%p, table_mgr=%p", session_ctx_, table_mgr_);
      }
      else
      {
        switch(type)
        {
          case PHY_LOCK_FILTER:
            ret = new_operator(ObUpsLockFilter, allocator, *session_ctx_);
            break;
          case PHY_INC_SCAN:
            {
              ObUpsIncScan *ic = tc_rp_alloc(ObUpsIncScan);
              ic->set_session_ctx(session_ctx_);
              ret = ic;
              break;
            }
          case PHY_UPS_MODIFY:
            ret = new_operator(MemTableModify, allocator, *session_ctx_, *table_mgr_);
            break;
          case PHY_UPS_MODIFY_WITH_DML_TYPE:
            ret = new_operator(MemTableModifyWithDmlType, allocator, *session_ctx_, *table_mgr_);
            break;
          default:
            ret = ObPhyOperatorFactory::get_one(type, allocator);
            break;
        }
        // add by maosy [Delete_Update_Function_isolation_RC] 20161218
        if (NULL != ret && type ==PHY_MULTIPLE_GET_MERGE)
        {
            dynamic_cast<ObMultipleGetMerge*>(ret)->set_start_time (session_ctx_->get_start_time_for_batch ());
        }
        // add e
      }
      return ret;
    }

    void ObUpsPhyOperatorFactory::release_one(sql::ObPhyOperator *opt)
    {
      if (NULL != opt)
      {
        switch (opt->get_type())
        {
          case PHY_INC_SCAN:
            tc_rp_free(dynamic_cast<ObUpsIncScan*>(opt));
            break;
          default:
            ObPhyOperatorFactory::release_one(opt);
            break;
        }
      }
    }

  }; // end namespace updateserver
}; // end namespace oceanbase
