/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_get_fuse.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_tablet_get_fuse.h"
#include "common/ob_row_fuse.h"

using namespace oceanbase;
using namespace common;
using namespace sql;

ObTabletGetFuse::ObTabletGetFuse()
  :sstable_get_(NULL),
  incremental_get_(NULL),
  data_version_(0),
  last_rowkey_(NULL)
{
}

void ObTabletGetFuse::reset()
{
  sstable_get_ = NULL;
  incremental_get_ = NULL;
  data_version_ = 0;
  last_rowkey_ = NULL;
}

void ObTabletGetFuse::reuse()
{
  sstable_get_ = NULL;
  incremental_get_ = NULL;
  data_version_ = 0;
  last_rowkey_ = NULL;
}

int ObTabletGetFuse::set_sstable_get(ObRowkeyPhyOperator *sstable_get)
{
  int ret = OB_SUCCESS;
  if (NULL == sstable_get)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "sstable_get is null");
  }
  else
  {
    sstable_get_ = sstable_get;
  }
  return ret;
}

int ObTabletGetFuse::set_incremental_get(ObRowkeyPhyOperator *incremental_get)
{
  int ret = OB_SUCCESS;
  if (NULL == incremental_get)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "incremental_get is null");
  }
  else
  {
    incremental_get_ = incremental_get;
  }
  return ret;
}

int ObTabletGetFuse::open()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = sstable_get_->open()))
  {
    TBSYS_LOG(WARN, "fail to open sstable get:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = incremental_get_->open()))
  {
    TBSYS_LOG(WARN, "fail to open incremental get:ret[%d]", ret);
  }
  FILL_TRACE_LOG("open get fuse op done ret =%d", ret);
  return ret;
}

int ObTabletGetFuse::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *sstable_row = NULL;
  const ObRowkey *sstable_rowkey = NULL;
  const ObRow *tmp_row = NULL;
  const ObUpsRow *incremental_row = NULL;
  const ObRowkey *incremental_rowkey = NULL;
  ret = sstable_get_->get_next_row(sstable_rowkey, sstable_row);
  if (OB_SUCCESS != ret && OB_ITER_END != ret)
  {
    TBSYS_LOG(WARN, "fail to get sstable next row:ret[%d]", ret);
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = incremental_get_->get_next_row(incremental_rowkey, tmp_row)))
    {
      TBSYS_LOG(WARN, "fail to get increment next row:ret[%d]", ret);
      if (OB_ITER_END == ret)
      {
        ret = OB_ERROR;
      }
    }
  }
  // del by maosy [Delete_Update_Function_isolation_RC] 20161218
  //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//  if (NULL != sstable_row)
//      TBSYS_LOG(DEBUG,"mul_del::debug,orig_sstable_row=[%s],ret=%d",
//                to_cstring(*sstable_row),ret);
//  if (NULL != tmp_row)
//      TBSYS_LOG(DEBUG,"mul_del::debug,orig_inc_row=[%s],ret=%d",
//                to_cstring(*tmp_row),ret);
  //add duyr 20160531:e
//del by maosy
  if (OB_SUCCESS == ret)
  {
    incremental_row = dynamic_cast<const ObUpsRow *>(tmp_row);
    if (NULL == incremental_row)
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "should be ups row");
    }
  }

  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "tablet get fuse incr[%s] sstable row[%s]", to_cstring(*incremental_row), to_cstring(*sstable_row));
    if (OB_SUCCESS != (ret = ObRowFuse::fuse_row(incremental_row, sstable_row, &curr_row_)))
    {
      TBSYS_LOG(WARN, "fail to fuse row:ret[%d]", ret);
    }
    else
    {
      row = &curr_row_;
      last_rowkey_ = sstable_rowkey;
    }
  }
  return ret;
}

int ObTabletGetFuse::close()
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS;
  if (OB_SUCCESS != (err = sstable_get_->close()))
  {
    ret = err;
    TBSYS_LOG(WARN, "fail to close sstable get:err[%d]", err);
  }
  if (OB_SUCCESS != (err = incremental_get_->close()))
  {
    ret = err;
    TBSYS_LOG(WARN, "fail to close incremental get:err[%d]", err);
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObTabletGetFuse, PHY_TABLET_GET_FUSE);
  }
}

int64_t ObTabletGetFuse::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObTabletGetFuse");
  return pos;
}

int ObTabletGetFuse::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(child_idx);
  UNUSED(child_operator);
  return ret;
}

int ObTabletGetFuse::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  return sstable_get_->get_row_desc(row_desc);
}

int ObTabletGetFuse::get_last_rowkey(const common::ObRowkey *&rowkey)
{
  rowkey = last_rowkey_;
  return OB_SUCCESS;
}
