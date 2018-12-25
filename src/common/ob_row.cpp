/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_row.h"
#include "common/utility.h"
#include "ob_obj_cast.h" //add lbzhong [Update rowkey] 20160106:b:e

using namespace oceanbase::common;

ObRow::ObRow()
  :row_desc_(NULL)
  //add lbzhong [Update rowkey] 20151221:b
  ,new_rowkey_(NULL)
  , is_new_row_(false)
  //add:e
  //add huangcc [Update rowkey] bug#1216 20161019:b
  ,is_update_rowkey_(false)
  //add:e
{
}

ObRow::~ObRow()
{
}

int ObRow::reset(bool skip_rowkey, enum ObRow::DefaultValue default_value)
{
  int ret = OB_SUCCESS;
  //add lbzhong [Update rowkey] 20151221:b
  is_new_row_ = false;
  new_rowkey_ = NULL;
  //new_rowkey_row_.clear();
  //add:e
  ObObj null_cell;
  ObObj nop_cell;
  ObObj row_not_exist_cell;
  null_cell.set_null();
  nop_cell.set_ext(ObActionFlag::OP_NOP);
  row_not_exist_cell.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
  int64_t flag_column_idx = 0;
  int64_t start = 0;

  ObObj *default_obj = NULL;
  if (ObRow::DEFAULT_NULL == default_value)
  {
    default_obj = &null_cell;
  }
  else if (ObRow::DEFAULT_NOP == default_value)
  {
    default_obj = &nop_cell;
  }
  else
  {
    ret = OB_INVALID_ARGUMENT;
  }
  
  if (OB_SUCCESS == ret && NULL != row_desc_)
  {
    if (OB_SUCCESS == ret)
    {
      if(NULL == row_desc_)
      {
        TBSYS_LOG(ERROR, "row_desc_ must not be null");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        flag_column_idx = row_desc_->get_idx(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
      }
    }

    if (OB_SUCCESS == ret)
    {
      raw_row_.clear();
      start = skip_rowkey ? row_desc_->get_rowkey_cell_count() : 0;
      for (int64_t i=start;OB_SUCCESS == ret && i<row_desc_->get_column_num();i++)
      {
        if (i != flag_column_idx)
        {
          if (OB_SUCCESS != (ret = this->raw_set_cell(i, *default_obj)))
          {
            TBSYS_LOG(WARN, "fail to set cell null:ret[%d], i[%ld]", ret, i);
          }
        }
        else
        {
          if (OB_SUCCESS != (ret = this->raw_set_cell(i, row_not_exist_cell) ))
          {
            TBSYS_LOG(WARN, "set action flag cell fail:ret[%d]", ret);
          }
        }
      }
    }
  }
  return ret;
}

void ObRow::assign(const ObRow &other)
{
  //add lbzhong [Update rowkey] 20160510:b
  this->is_new_row_ = other.is_new_row_;
  //add:e
  this->raw_row_.assign(other.raw_row_);
  this->row_desc_ = other.row_desc_;
}

ObRow::ObRow(const ObRow &other)
{
  this->assign(other);
}

ObRow &ObRow::operator= (const ObRow &other)
{
  this->assign(other);
  return *this;
}

int ObRow::get_cell(const uint64_t table_id, const uint64_t column_id, const common::ObObj *&cell) const
{
  int ret = OB_SUCCESS;
  int64_t cell_idx = OB_INVALID_INDEX;
  if (NULL == row_desc_)
  {
    TBSYS_LOG(WARN, "row_desc_ is null, tid=%lu cid=%lu", table_id, column_id);
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_INVALID_INDEX == (cell_idx = row_desc_->get_idx(table_id, column_id)))
  {
    TBSYS_LOG(WARN, "failed to find cell, tid=%lu cid=%lu, row_desc_=%s", table_id, column_id, to_cstring(*row_desc_));
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ret = raw_row_.get_cell(cell_idx, cell);
  }
  return ret;
}

int ObRow::get_cell(const uint64_t table_id, const uint64_t column_id, common::ObObj *&cell)
{
  int ret = OB_SUCCESS;
  int64_t cell_idx = OB_INVALID_INDEX;
  if (NULL == row_desc_
      || OB_INVALID_INDEX == (cell_idx = row_desc_->get_idx(table_id, column_id)))
  {
    TBSYS_LOG(WARN, "failed to find cell, tid=%lu cid=%lu", table_id, column_id);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ret = raw_row_.get_cell(cell_idx, cell);
  }
  return ret;
}

int ObRow::set_cell(const uint64_t table_id, const uint64_t column_id, const common::ObObj &cell)
{
  int ret = OB_SUCCESS;
  int64_t cell_idx = OB_INVALID_INDEX;
  if (NULL == row_desc_
      || OB_INVALID_INDEX == (cell_idx = row_desc_->get_idx(table_id, column_id)))
  {
    TBSYS_LOG(WARN, "failed to find cell, tid=%lu cid=%lu row_desc=%s", table_id, column_id, to_cstring(*row_desc_));
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = raw_row_.set_cell(cell_idx, cell)))
  {
    TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
  }
  return ret;
}

int ObRow::raw_get_cell(const int64_t cell_idx, const common::ObObj *&cell, uint64_t &table_id, uint64_t &column_id) const
{
  int ret = OB_SUCCESS;
  int64_t cells_count = get_column_num();
  if (NULL == row_desc_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "row_desc_ is NULL");
  }
  else if (cell_idx >= cells_count)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid cell_idx=%ld cells_count=%ld", cell_idx, cells_count);
  }
  else if (OB_SUCCESS != (ret = row_desc_->get_tid_cid(cell_idx, table_id, column_id)))
  {
    TBSYS_LOG(WARN, "failed to get tid and cid, err=%d", ret);
  }
  else
  {
    ret = raw_row_.get_cell(cell_idx, cell);
  }
  return ret;
}

int ObRow::raw_set_cell(const int64_t cell_idx, const common::ObObj &cell)
{
  int ret = OB_SUCCESS;
  int64_t cells_count = get_column_num();
  if (NULL == row_desc_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "row_desc_ is NULL");
  }
  else if (cell_idx >= cells_count)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid cell_idx=%ld cells_count=%ld", cell_idx, cells_count);
  }
  else if (OB_SUCCESS != (ret = raw_row_.set_cell(cell_idx, cell)))
  {
    TBSYS_LOG(WARN, "failed to get cell, err=%d idx=%ld", ret, cell_idx);
  }
  return ret;
}

void ObRow::dump() const
{
  int64_t cells_count = get_column_num();
  int64_t cell_idx = 0;
  uint64_t tid = 0, cid = 0;
  const ObObj *cell = NULL;
  TBSYS_LOG(DEBUG, "[obrow.dump begin]");
  for (cell_idx = 0; cell_idx < cells_count; cell_idx++)
  {
    if (OB_SUCCESS != raw_get_cell(cell_idx, cell, tid, cid))
    {
      TBSYS_LOG(WARN, "fail to dump ObRow");
      break;
    }
    if (NULL != cell)
    {
      TBSYS_LOG(DEBUG, "-------  tid=%lu, cid=%lu  -------", tid, cid);
      cell->dump();
    }
  }
  TBSYS_LOG(DEBUG, "[obrow.dump end]");
}

int64_t ObRow::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t cells_count = get_column_num();
  uint64_t tid = 0;
  uint64_t cid = 0;
  const ObObj *cell = NULL;
  for (int64_t cell_idx = 0; cell_idx < cells_count; cell_idx++)
  {
    if (OB_SUCCESS != raw_get_cell(cell_idx, cell, tid, cid))
    {
      TBSYS_LOG(WARN, "fail to get cell");
      break;
    }
    if (NULL != cell)
    {
      databuff_printf(buf, buf_len, pos, " <%lu.%lu>=", tid, cid);
      pos += cell->to_string(buf+pos, buf_len-pos);
    }
  }
  //add lbzhong [Update rowkey] 20151221:b
  /*
  int64_t tmp_size;
  const ObObj* update_rowkey = new_rowkey_row_.get_obj_array(tmp_size);
  for(int64_t i = 0; i < tmp_size; i++)
  {
    databuff_printf(buf, buf_len, pos, " <NULL.NULL>=");
    pos += update_rowkey[i].to_string(buf+pos, buf_len-pos);
  }
  */
  //add:e
  return pos;
}

int ObRow::get_rowkey(const ObRowkey *&rowkey) const
{
  int ret = OB_SUCCESS;
  if (NULL == row_desc_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "row_desc_ is NULL");
  }
  else if (row_desc_->get_rowkey_cell_count() <= 0)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "rowkey count[%ld]", row_desc_->get_rowkey_cell_count());
  }
  else
  {
    rowkey = &rowkey_;
  }
  return ret;
}

int ObRow::get_is_row_empty(bool &is_row_empty) const
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  int64_t action_column_idx = 0;
  if (NULL == row_desc_)
  {
    ret = OB_NOT_INIT;
  }
  else
  {
    action_column_idx = row_desc_->get_idx(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_INVALID_INDEX == action_column_idx)
    {
      is_row_empty = false;
    }
    else
    {
      ret = this->get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, cell);
      if (OB_SUCCESS == ret)
      {
        if (ObExtendType != cell->get_type())
        {
          ret = OB_ERROR;
        }
        else
        {
          is_row_empty = (ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell->get_ext());
        }
      }
    }
  }
  return ret;
}

//add lbzhong [Update rowkey] 20151221:b
int ObRow::get_new_rowkey(const common::ObObj *&cell) const
{
  int ret = OB_SUCCESS;
  if(NULL == new_rowkey_)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "new_rowkey_ is NULL");
  }
  else
  {
    cell = &new_rowkey_[0];
  }
  return ret;
}

int ObRow::set_new_rowkey(ObRawRow*& new_rowkey_row)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = new_rowkey_row->get_cell(0, new_rowkey_)))
  {
    TBSYS_LOG(ERROR, "Fail new_rowkey_row.get_cell(0, cell), ret=%d", ret);
  }
  return ret;
}

ObRow::ObRow(const ObRow &other, ModuleArena &row_alloc)
{
  this->raw_row_.deep_copy(other.raw_row_, row_alloc);
  this->row_desc_ = other.row_desc_;
}

int64_t ObRow::rowkey_to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t rowkey_count = row_desc_->get_rowkey_cell_count();

  uint64_t tid = 0;
  uint64_t cid = 0;
  const ObObj *cell = NULL;
  for (int64_t cell_idx = 0; cell_idx < rowkey_count; cell_idx++)
  {
    if (OB_SUCCESS != raw_get_cell(cell_idx, cell, tid, cid))
    {
      TBSYS_LOG(WARN, "fail to get cell");
      break;
    }
    if (NULL != cell)
    {
      databuff_printf(buf, buf_len, pos, " <%lu.%lu>=", tid, cid);
      pos += cell->to_string(buf+pos, buf_len-pos);
    }
  }
  return pos;
}
//add:e

// add by maosy [Delete_Update_Function_isolation_RC] 20161228
int ObRow::get_action_flag (int64_t &action_flag) const
{
    int ret = OB_SUCCESS ;
    action_flag = ObActionFlag::OP_NOP ;
    uint64_t tid = OB_INVALID_ID;
    uint64_t cid = OB_ACTION_FLAG_COLUMN_ID;
    const ObObj * cell = NULL ;
    if(OB_SUCCESS != (ret = this->get_cell (tid,cid,cell)))
    {
        TBSYS_LOG(WARN,"failed to get action flag cell ,ret = %d",ret);
    }
    else if(cell->get_type () != ObExtendType)
    {
        ret = OB_ERR_UNEXPECTED ;
        TBSYS_LOG(ERROR,"row action flag type error,type is = %d",cell->get_type ());
    }
    else if(OB_SUCCESS !=(ret = cell->get_ext (action_flag)))
    {
        TBSYS_LOG(WARN,"failed to get flag cell ");
    }
    return ret ;
}
// add e

