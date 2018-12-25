/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_dual_table_scan.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_dual_table_scan.h"
#include "common/utility.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

/*int ObDualTableScan::open() 
{
  int ret = common::OB_SUCCESS;
  is_opend_ = true;
  has_data_ = true;
  if ((ret = row_desc_.add_column_desc(common::OB_INVALID_ID, common::OB_INVALID_ID)) 
    != common::OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "Construct row descriptor failed, ret=%d", ret);
  }
  else
  {
    row_.set_row_desc(row_desc_);
  }
  return ret;
}

int ObDualTableScan::close()
{
  is_opend_ = false;
  has_data_ = false;
  row_.reset(false, common::ObRow::DEFAULT_NULL);
  row_desc_.reset();
  return common::OB_SUCCESS;
}

int ObDualTableScan::get_next_row(const common::ObRow *&row)
{
  int ret = common::OB_SUCCESS;
  row = NULL;
  if (OB_UNLIKELY(!is_opend_))
  {
    ret = common::OB_NOT_INIT;
    TBSYS_LOG(ERROR, "Operator is not opened, ret=%d", ret);
  }
  else if (has_data_)
  {
    common::ObObj val;
    has_data_ = false;
    if ((ret = row_.set_cell(common::OB_INVALID_ID, common::OB_INVALID_ID, val))
      != common::OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "Get row failed, ret=%d", ret);
    }
    else
    {
      row = &row_;
    }
  }
  else
  {
    ret = common::OB_ITER_END;
  }
  return ret;
  }*/

int64_t ObDualTableScan::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "DualTableScan()\n");
  return pos;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObDualTableScan, PHY_DUAL_TABLE_SCAN);
  }
}

PHY_OPERATOR_ASSIGN(ObDualTableScan)
{
  UNUSED(other);
  return common::OB_SUCCESS;
}

