/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_table_rename.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_table_rename.h"
#include "ob_sql_expression.h"
#include "common/utility.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;


namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObTableRename, PHY_TABLE_RENAME);
  }
}
int ObTableRename::cons_row_desc()
{
  int ret = OB_SUCCESS;
  uint64_t org_tid = OB_INVALID_ID;
  uint64_t org_cid = OB_INVALID_ID;

  if (NULL == child_op_)
  {
    TBSYS_LOG(ERROR, "child op is NULL");
    ret = OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (ret = child_op_->get_row_desc(org_row_desc_)))
  {
    TBSYS_LOG(WARN, "failed to get original row desc, err=%d", ret);
  }
  else
  {
    OB_ASSERT(org_row_desc_);
    for (int64_t idx = 0; idx < org_row_desc_->get_column_num(); idx++)
    {
      if(OB_SUCCESS != (ret = org_row_desc_->get_tid_cid(idx, org_tid, org_cid)))
      {
        TBSYS_LOG(WARN, "get org desc tid cid fail:ret[%d]", ret);
      }

      if(OB_SUCCESS == ret)
      {
        uint64_t new_table_id = table_id_;
        if(OB_INVALID_ID == org_tid) //如果是invalid id，表示是复合列，不做转换
        {
          new_table_id = OB_INVALID_ID;
        }

        if (OB_SUCCESS != (ret = row_desc_.add_column_desc(new_table_id, org_cid)))
        {
          TBSYS_LOG(WARN, "fail to add tid and cid to row desc. idx=%ld, tid=%lu, ret=%d, cid=%lu",
                    idx, table_id_, ret, org_cid);
          break;
        }
      }
    }
  }
  return ret;
}

ObPhyOperatorType ObTableRename::get_type() const
{
  return PHY_TABLE_RENAME;
}
