/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_direct_join.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_tablet_direct_join.h"
#include "common/utility.h"

using namespace oceanbase;
using namespace common;
using namespace sql;

ObTabletDirectJoin::ObTabletDirectJoin()
  :last_ups_rowkey_(NULL),
  last_ups_row_(NULL)
{
}

ObTabletDirectJoin::~ObTabletDirectJoin()
{
}

int ObTabletDirectJoin::open()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ObTabletJoin::open()))
  {
    TBSYS_LOG(WARN, "ObTabletJoin open:ret[%d]", ret);
  }
  else
  {
    last_ups_rowkey_ = NULL;
    last_ups_row_ = NULL;
  }
  return ret;
}

void ObTabletDirectJoin::reset()
{
  ObTabletJoin::reset();
  last_ups_rowkey_ = NULL;
  last_ups_row_ = NULL;
}

int ObTabletDirectJoin::gen_get_param(ObGetParam &get_param, const ObRow &fused_row)
{
  int ret = OB_SUCCESS;
  ObRowkey rowkey;
  ObObj rowkey_obj[OB_MAX_ROWKEY_COLUMN_NUMBER];

  if(OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = get_right_table_rowkey(fused_row, rowkey, rowkey_obj)))
    {
      TBSYS_LOG(WARN, "get rowkey from tmp_row fail:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = compose_get_param(table_join_info_.right_table_id_, rowkey, get_param)))
    {
      if (OB_SIZE_OVERFLOW != ret)
      {
        TBSYS_LOG(WARN, "compose get param fail:ret[%d]", ret);
      }
    }
  }
  return ret;
}

int ObTabletDirectJoin::fetch_fused_row_prepare()
{
  int ret = OB_SUCCESS;
  last_ups_rowkey_ = NULL;
  last_ups_row_ = NULL;
  return ret;
}

int ObTabletDirectJoin::get_ups_row(const ObRowkey &rowkey, ObUpsRow &ups_row, const ObGetParam &get_param)
{
  int ret = OB_SUCCESS;
  if (NULL == last_ups_rowkey_)
  {
    if(OB_SUCCESS == ret && get_param.get_cell_size() > 0)
    {
      ups_multi_get_.reuse();
      ups_multi_get_.set_get_param(get_param);
      ups_multi_get_.set_row_desc(ups_row_desc_);

      if(OB_SUCCESS != (ret = ups_multi_get_.open()))
      {
        TBSYS_LOG(WARN, "open ups multi get fail:ret[%d]", ret);
      }
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "get param is empty");
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (NULL == last_ups_rowkey_ || *last_ups_rowkey_ != rowkey)
    {
      const ObRow *tmp_row_ptr = NULL;
      if (OB_SUCCESS != (ret = ups_multi_get_.get_next_row(last_ups_rowkey_, tmp_row_ptr)))
      {
        TBSYS_LOG(WARN, "ups multi get next row fail:ret[%d], rowkey[%s]", ret, to_cstring(rowkey));
      }
      else if (NULL == (last_ups_row_ = dynamic_cast<const ObUpsRow *>(tmp_row_ptr)))
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "shoud be ObUpsRow");
      }
      else if (*last_ups_rowkey_ != rowkey)
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "cannot find rowkey:rowkey[%s], last_ups_rowkey_[%s]", to_cstring(rowkey), to_cstring(*last_ups_rowkey_));
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    ups_row = *last_ups_row_;
  }

  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObTabletDirectJoin, PHY_TABLET_DIRECT_JOIN);
  }
}
