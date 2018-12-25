/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_join.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_tablet_join.h"
#include "common/ob_compact_cell_iterator.h"
#include "common/ob_compact_cell_writer.h"
#include "common/ob_row_util.h"
#include "common/ob_row_fuse.h"
#include "common/utility.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObTabletJoin::JoinInfo::JoinInfo()
  :left_column_id_(OB_INVALID_ID),
  right_column_id_(OB_INVALID_ID)
{
}

ObTabletJoin::TableJoinInfo::TableJoinInfo()
  :left_table_id_(OB_INVALID_ID)
{
}

void ObTabletJoin::reset()
{
  ups_row_desc_.reset();
  ups_row_desc_for_join_.reset();

  fused_row_store_.clear();
  fused_row_iter_end_ = false;
  fused_row_ = NULL;
  fused_row_array_.clear();
  fused_row_idx_ = 0;
  valid_fused_row_count_ = 0;
}


void ObTabletJoin::reuse()
{
  ups_row_desc_.reset();
  ups_row_desc_for_join_.reset();

  fused_row_store_.clear();
  fused_row_iter_end_ = false;
  fused_row_ = NULL;
  fused_row_array_.clear();
  fused_row_idx_ = 0;
  valid_fused_row_count_ = 0;
}

ObTabletJoin::ObTabletJoin()
  :batch_count_(0),
  fused_row_iter_end_(false),
  fused_scan_(NULL),
  fused_row_store_(),
  fused_row_(NULL),
  fused_row_idx_(0),
  is_read_consistency_(true),
  valid_fused_row_count_(0)
{
}

ObTabletJoin::~ObTabletJoin()
{
}

bool ObTabletJoin::check_inner_stat()
{
  bool ret = true;
  if(NULL == fused_scan_ || 0 == batch_count_)
  {
    ret = false;
    TBSYS_LOG(WARN, "check inner stat fail:fused_scan_[%p], batch_count_[%ld]", fused_scan_, batch_count_);
  }
  return ret;
}

int ObTabletJoin::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_SUCCESS;
  switch(child_idx)
  {
    case 0:
      fused_scan_ = dynamic_cast<ObTabletFuse *>(&child_operator);
      if(NULL == fused_scan_)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "child operator is not ObTabletFuse");
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

int ObTabletJoin::open()
{
  int ret = OB_SUCCESS;
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }
  else if(OB_SUCCESS != (ret = fused_scan_->open()))
  {
    TBSYS_LOG(WARN, "open fused scan fail:ret[%d]", ret);
  }
  else if(OB_SUCCESS != (ret = gen_ups_row_desc()))
  {
    TBSYS_LOG(WARN, "gen ups row desc:ret[%d]", ret);
  }

  if(OB_SUCCESS == ret)
  {
    fused_row_store_.clear();
    fused_row_iter_end_ = false;
    fused_row_ = NULL;
    fused_row_array_.clear();
    fused_row_idx_ = 0;
    valid_fused_row_count_ = 0;
  }

  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "table_join_info_[%s]", to_cstring(table_join_info_));
  }

  return ret;
}

int ObTabletJoin::close()
{
  int ret = OB_SUCCESS;
  if(NULL != fused_scan_)
  {
    if(OB_SUCCESS != (ret = fused_scan_->close()))
    {
      TBSYS_LOG(WARN, "close fused scan fail:ret[%d]", ret);
    }
  }
  return ret;
}

int ObTabletJoin::compose_get_param(uint64_t table_id, const ObRowkey &rowkey, ObGetParam &get_param)
{
  int ret = OB_SUCCESS;

  ObCellInfo cell_info;
  cell_info.table_id_ = table_id;
  cell_info.row_key_ = rowkey;
  JoinInfo join_info;

  if(get_param.get_cell_size() > 0 && (rowkey == get_param[get_param.get_cell_size() - 1]->row_key_))
  {
    //相同的内容，省略，不只是优化，必须省略
  }
  else
  {
    for(int64_t i=0; (OB_SUCCESS == ret) && i<table_join_info_.join_column_.count();i++)
    {
      if(OB_SUCCESS != (ret = table_join_info_.join_column_.at(i, join_info)))
      {
        TBSYS_LOG(WARN, "get join info fail:ret[%d], i[%ld]", ret, i);
      }
      else
      {
        cell_info.column_id_ = join_info.right_column_id_;
        ret = get_param.add_cell(cell_info);
        if (OB_SIZE_OVERFLOW == ret)
        {
          if (0 != i)
          {
            if (OB_SUCCESS != (ret = get_param.rollback(i)))
            {
              TBSYS_LOG(WARN, "fail to rollback get param:ret[%d], i[%ld]", ret, i);
            }
            else
            {
              ret = OB_SIZE_OVERFLOW;
            }
          }
          TBSYS_LOG(INFO, "batch count is too large[%ld], get param capability[%ld]", batch_count_, get_param.get_row_size());
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "add cell info to get_param fail:ret[%d]", ret);
        }
      }
    }
  }
  return ret;
}

int ObTabletJoin::get_right_table_rowkey(const ObRow &row, ObRowkey &rowkey, ObObj *rowkey_obj) const
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  int64_t count = table_join_info_.join_condition_.count();
  uint64_t left_column_id = OB_INVALID_ID;

  if(NULL == rowkey_obj)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "rowkey_obj is null");
  }

  if(OB_SUCCESS == ret)
  {
    for(int32_t i=0;OB_SUCCESS == ret && i<count;i++)
    {
      if(OB_SUCCESS != (ret = table_join_info_.join_condition_.at(i, left_column_id)))
      {
        TBSYS_LOG(WARN, "get join condition fail:ret[%d], i[%d]", ret, i);
      }
      else if(OB_SUCCESS != (ret = row.get_cell(table_join_info_.left_table_id_, left_column_id, cell)))
      {
        TBSYS_LOG(WARN, "row get cell fail:ret[%d]", ret);
      }
      else if( i >= OB_MAX_ROWKEY_COLUMN_NUMBER )
      {
        ret = OB_SIZE_OVERFLOW;
        TBSYS_LOG(WARN, "join condition array length is great than right table rowkey obj length:i[%d]", i);
      }
      else
      {
        rowkey_obj[i] = *cell;
      }
    }
  }

  if(OB_SUCCESS == ret)
  {
    rowkey.assign(rowkey_obj, count);
  }

  return ret;
}

int ObTabletJoin::fetch_fused_row_prepare()
{
  return OB_SUCCESS;
}

int ObTabletJoin::fetch_fused_row(ObGetParam *get_param)
{
  int ret = OB_SUCCESS;
  const ObRowStore::StoredRow *stored_row = NULL;

  fused_row_store_.clear();
  fused_row_array_.clear();
  fused_row_idx_ = 0;
  valid_fused_row_count_ = 0;

  if(NULL == get_param)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "get param is null");
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = fetch_fused_row_prepare()))
    {
      TBSYS_LOG(WARN, "fetch fused row prepare fail:ret[%d]", ret);
    }
  }

  for(int64_t i=0;(OB_SUCCESS == ret) && i < batch_count_;i++)
  {
    if(NULL == fused_row_)
    {
      ret = fused_scan_->get_next_row(fused_row_);
      if(OB_ITER_END == ret)
      {
        break;
      }
      else if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "get next row from fused scan fail:ret[%d]", ret);
      }
    }

    if(OB_SUCCESS == ret)
    {
      ret = fused_row_store_.add_row(*fused_row_, stored_row);
      if(OB_SIZE_OVERFLOW == ret)
      {
        TBSYS_LOG(DEBUG, "fused row store size overflow");
        ret = OB_SUCCESS;
        break;
      }
      else if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "add row to row store fail:ret[%d]", ret);
      }
      else if (OB_SUCCESS != (ret = fused_row_array_.push_back(stored_row)))
      {
        TBSYS_LOG(WARN, "add item to fused row array fail:ret[%d]", ret);
      }
      else
      {
        valid_fused_row_count_ ++;
      }
    }

    if (OB_SUCCESS == ret)
    {
      ret = gen_get_param(*get_param, *fused_row_);
      if (OB_SIZE_OVERFLOW == ret)
      {
        valid_fused_row_count_ = fused_row_array_.count() - 1;
        ret = OB_SUCCESS;
        break;
      }
      else if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "gen get param fail:ret[%d]", ret);
      }
      else
      {
        fused_row_ = NULL;
      }
    }
  }

  TBSYS_LOG(DEBUG, "fetch fused row count[%ld]", fused_row_array_.count());

  return ret;
}

int ObTabletJoin::gen_ups_row_desc()
{
  int ret = OB_SUCCESS;
  JoinInfo join_info;
  for(int64_t i=0;(OB_SUCCESS == ret) && i<table_join_info_.join_column_.count();i++)
  {
    if(OB_SUCCESS != (ret = table_join_info_.join_column_.at(i, join_info)))
    {
      TBSYS_LOG(WARN, "get join column fail:ret[%d], i[%ld]", ret, i);
    }
    else if(OB_SUCCESS != (ret = ups_row_desc_.add_column_desc(table_join_info_.right_table_id_, join_info.right_column_id_)))
    {
      TBSYS_LOG(WARN, "ups_row_desc_ add column desc fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = ups_row_desc_for_join_.add_column_desc(table_join_info_.left_table_id_, join_info.left_column_id_)))
    {
      TBSYS_LOG(WARN, "ups_row_desc_for_join_ add column desc fail:ret[%d]", ret);
    }
  }
  return ret;
}

int ObTabletJoin::fetch_next_batch_row(ObGetParam *get_param)
{
  int ret = OB_SUCCESS;

  if(NULL == get_param)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "get param is null");
  }

  if(OB_SUCCESS == ret)
  {
    get_param->reset(true);
    get_param->set_version_range(version_range_);
    get_param->set_is_read_consistency(is_read_consistency_);
    if(OB_SUCCESS != (ret = fetch_fused_row(get_param)))
    {
      if(OB_ITER_END != ret)
      {
        TBSYS_LOG(ERROR, "fetch_fused_row fail :ret[%d]", ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "fused row iterate end.");
        fused_row_iter_end_ = true;
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObTabletJoin::get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  ObRowkey rowkey;
  ObObj rowkey_obj[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObRow tmp_row;
  ObUpsRow tmp_ups_row;
  const ObRowStore::StoredRow *stored_row = NULL;
  ObGetParam *get_param = NULL;
  const ObRowDesc *row_desc = NULL;

  tmp_ups_row.set_row_desc(ups_row_desc_);

  if(NULL == fused_scan_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "fuse_scan_ is null");
  }
  else if (OB_SUCCESS != (ret = fused_scan_->get_row_desc(row_desc) ))
  {
    TBSYS_LOG(WARN, "fail to get row desc:ret[%d]", ret);
  }
  else
  {
    tmp_row.set_row_desc(*row_desc);
  }

  if(OB_SUCCESS == ret)
  {
    if (fused_row_idx_ >= valid_fused_row_count_)
    {
      if (fused_row_iter_end_)
      {
        TBSYS_LOG(DEBUG, "no more data in fused_row_.");
        ret = OB_ITER_END;
      }
      else
      {
        if(NULL == (get_param = GET_TSI_MULT(ObGetParam, common::TSI_SQL_GET_PARAM_1)))
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(WARN, "get tsi get_param fail:ret[%d]", ret);
        }
        else
        {
          get_param->reset(true);
          get_param->set_is_read_consistency(is_read_consistency_);
        }
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = fetch_next_batch_row(get_param)))
          {
            TBSYS_LOG(WARN, "fetch next batch row fail:ret[%d]", ret);
          }
          else if(0 == valid_fused_row_count_)
          {
            TBSYS_LOG(DEBUG, "no more data in fused_row_.");
            ret = OB_ITER_END;
          }
        }
      }
    }
    else if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "get next row from fused row store fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = fused_row_array_.at(fused_row_idx_, stored_row)))
    {
      TBSYS_LOG(WARN, "get item from fused row array fail:ret[%d], fused_row_idx_[%ld], array_count[%ld]", ret, fused_row_idx_, fused_row_array_.count());
    }
    else if(OB_SUCCESS != (ret = ObRowUtil::convert(stored_row->get_compact_row(), tmp_row)))
    {
      TBSYS_LOG(WARN, "convert ob row fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = get_right_table_rowkey(tmp_row, rowkey, rowkey_obj)))
    {
      TBSYS_LOG(WARN, "get rowkey from tmp_row fail:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = get_ups_row(rowkey, tmp_ups_row, *get_param)))
    {
      TBSYS_LOG(WARN, "get ups row fail:rowkey[%s], ret[%d]", to_cstring(rowkey), ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    fused_row_idx_ ++;
    tmp_ups_row.set_row_desc(ups_row_desc_for_join_);
    TBSYS_LOG(DEBUG, "fuse_row: incrow:%s, sstable row:%s",
        to_cstring(tmp_ups_row), to_cstring(tmp_row));
    if(OB_SUCCESS != (ret = ObRowFuse::join_row(&tmp_ups_row, &tmp_row, &curr_row_)))
    {
      TBSYS_LOG(WARN, "fused ups row to row fail:ret[%d]", ret);
    }
    else
    {
      row = &curr_row_;
    }
    TBSYS_LOG(DEBUG, "fuse_row dest row:%s", to_cstring(curr_row_));
  }

  return ret;
}

int64_t ObTabletJoin::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "TabletJoin()\n");
  if (NULL != fused_scan_)
  {
    pos += fused_scan_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}

int ObTabletJoin::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (NULL == fused_scan_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "fused_scan_ is null");
  }
  else if (OB_SUCCESS != (ret = fused_scan_->get_row_desc(row_desc)))
  {
    TBSYS_LOG(WARN, "fail to get row desc:ret[%d]", ret);
  }
  return ret;
}

void ObTabletJoin::set_ts_timeout_us(int64_t ts_timeout_us)
{
  ups_multi_get_.set_ts_timeout_us(ts_timeout_us);
}
