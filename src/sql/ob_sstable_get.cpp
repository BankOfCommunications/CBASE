/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sstable_get.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_sstable_get.h"
#include "common/ob_new_scanner_helper.h"

ObSSTableGet::ObSSTableGet()
  :tablet_manager_(NULL),
  get_param_(NULL),
  tablet_version_(0),
  row_iter_(false),
  last_rowkey_(NULL)
{
}

void ObSSTableGet::reset()
{
  tablet_manager_ = NULL;
  get_param_ = NULL;
  tablet_version_ = 0;
  row_desc_.reset();
  last_rowkey_ = NULL;
}

void ObSSTableGet::reuse()
{
  reset();
}

int ObSSTableGet::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  row_desc = &row_desc_;
  return ret;
}

int ObSSTableGet::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(child_idx);
  UNUSED(child_operator);
  return ret;
}

int ObSSTableGet::open_tablet_manager(chunkserver::ObTabletManager *tablet_manager,
                                      const ObGetParam *get_param)
{
  int ret = OB_SUCCESS;
  if (NULL == tablet_manager || NULL == get_param)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "tablet_manager[%p] get_param[%p]", tablet_manager, get_param);
  }
  else
  {
    tablet_manager_ = tablet_manager;
    get_param_ = get_param;
    if (OB_SUCCESS != (ret = tablet_manager_->gen_sstable_getter(*get_param_, &sstable_getter_, tablet_version_) ))
    {
      TBSYS_LOG(WARN, "fail to gen sstable getter:ret[%d]", ret);
    }
  }
  return ret;
}

int ObSSTableGet::open()
{
  int ret = OB_SUCCESS;
  row_desc_.reset();
  if (NULL == tablet_manager_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "tablet_manager_ is null");
  }
  else if (OB_SUCCESS != (ret = ObNewScannerHelper::get_row_desc(*get_param_, true, row_desc_)))
  {
    TBSYS_LOG(WARN, "fail to get param:ret[%d]", ret);
  }
  else
  {
    TBSYS_LOG(DEBUG, "ObSSTableGet row desc [%s]", to_cstring(row_desc_));
  }

  if (OB_SUCCESS == ret)
  {
    row_iter_.set_cell_iter(&sstable_getter_, row_desc_, true);
  }

  return ret;
}

int ObSSTableGet::close()
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS;
  if (NULL != tablet_manager_)
  {
    if (OB_SUCCESS != (err = tablet_manager_->end_get()))
    {
      TBSYS_LOG(WARN, "fail to end get :err[%d]", err);
      ret = err;
    }
  }
  if (OB_SUCCESS != (err = row_iter_.close() ))
  {
    TBSYS_LOG(WARN, "fail to close row iter:err[%d]", err);
    ret = err;
  }
  return ret;
}

int ObSSTableGet::get_next_row(const common::ObRowkey *&rowkey, const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  ret = row_iter_.get_next_row(rowkey, row);
  if (OB_SUCCESS != ret && OB_ITER_END != ret)
  {
    TBSYS_LOG(WARN, "fail to get next row:ret[%d]", ret);
  }
  else if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "sstable get row[%s]", to_cstring(*row));
    last_rowkey_ = rowkey;
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObSSTableGet, PHY_SSTABLE_GET);
  }
}

int64_t ObSSTableGet::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObSSTableGet");
  return pos;
}

int ObSSTableGet::get_tablet_data_version(int64_t &data_version)
{
  data_version = tablet_version_;
  return OB_SUCCESS;
}

int ObSSTableGet::get_last_rowkey(const common::ObRowkey *&rowkey)
{
  rowkey = last_rowkey_;
  return OB_SUCCESS;
}
