/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_fuse.c
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_row_fuse.h"

using namespace oceanbase::common;

/*
 * 将一个ups的行付给一个普通行
 * 主要是把OP_NOP转换成null
 * 没有处理is_add的情况，如果原来的ObObj带有is_add，转换后还将带有is_add
 */
int ObRowFuse::assign(const ObUpsRow &incr_row, ObRow &result)
{
  int ret = OB_SUCCESS;
  result.raw_row_.clear();

  const ObObj *cell = NULL;
  ObObj null_cell;
  null_cell.set_null();

  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;

  if (NULL == result.get_row_desc())
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "result row desc is null");
  }
  else
  {
    for(int64_t i=0;(OB_SUCCESS == ret) && i<incr_row.get_column_num();i++)
    {
      if(OB_SUCCESS != (ret = incr_row.raw_get_cell(i, cell, table_id, column_id)))
      {
        TBSYS_LOG(WARN, "incr row get cell fail:ret[%d]", ret);
      }
      else 
      {
        if(ObExtendType == cell->get_type() && cell->get_ext() == ObActionFlag::OP_NOP)
        {
          cell = &null_cell;
        }

        if(OB_SUCCESS != (ret = result.set_cell(table_id, column_id, *cell)))
        {
          TBSYS_LOG(WARN, "result set_cell fail:ret[%d]", ret);
        }
      }
    }
  }
  return ret;
}

int ObRowFuse::join_row(const ObUpsRow *incr_row, const ObRow *sstable_row, ObRow *result)
{
  int ret = OB_SUCCESS;
  const ObObj *ups_cell = NULL;
  ObObj *result_cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;

  if(NULL == incr_row || NULL == sstable_row || NULL == result)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "incr_row[%p], sstable_row[%p], result[%p]", incr_row, sstable_row, result);
  }

  if (OB_SUCCESS == ret)
  {
    result->assign(*sstable_row);
    for (int64_t i = 0; OB_SUCCESS == ret && i < incr_row->get_column_num(); i ++)
    {
      if (OB_SUCCESS != (ret = incr_row->raw_get_cell(i, ups_cell, table_id, column_id)))
      {
        TBSYS_LOG(WARN, "fail to get ups cell:ret[%d]", ret);
      }
      else if (OB_SUCCESS != (ret = result->get_cell(table_id, column_id, result_cell)))
      {
        TBSYS_LOG(WARN, "fail to get result cell:ret[%d]", ret);
      }
      else
      {
        if (incr_row->get_is_delete_row())
        {
          result_cell->set_null();
        }
        if (OB_SUCCESS != (ret = result_cell->apply(*ups_cell)))
        {
          TBSYS_LOG(WARN, "fail to apply ups cell to result cell:ret[%d]", ret);
        }
      }
    }
  }
  return ret;
}

int ObRowFuse::fuse_row(const ObUpsRow *incr_row, const ObRow *sstable_row, ObRow *result)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  const ObObj *ups_cell = NULL;
  ObObj *result_cell = NULL;
  ObObj *result_action_flag_cell = NULL;
  const ObRowDesc *row_desc = NULL;
  const ObObj *sstable_action_flag_cell = NULL;
  bool is_row_not_exist = false;
  if(NULL == incr_row || NULL == sstable_row || NULL == result)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "incr_row[%p], sstable_row[%p], result[%p]", incr_row, sstable_row, result);
  }

  if (OB_SUCCESS == ret)
  {
    row_desc = sstable_row->get_row_desc();
    if (NULL == row_desc)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(WARN, "result row desc is null");
    }
    else
    {
      result->set_row_desc(*row_desc);
      if (OB_INVALID_INDEX != row_desc->get_idx(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID))
      {
        if (OB_SUCCESS != (ret = result->get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, result_action_flag_cell)))
        {
          TBSYS_LOG(WARN, "fail to get result action flag column:ret[%d]", ret);
        }
        else if (OB_SUCCESS != (ret = sstable_row->get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, sstable_action_flag_cell)))
        {
          TBSYS_LOG(WARN, "fail to get sstable action flag column:ret[%d]", ret);
        }
        else
        {
          result_action_flag_cell->set_ext(sstable_action_flag_cell->get_ext());
        }
      }
    }
  }

  if (OB_SUCCESS == ret && NULL != result_action_flag_cell)
  {
    if (ObExtendType != result_action_flag_cell->get_type())
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "invalid type[%d]", result_action_flag_cell->get_type());
    }
    else
    {
      is_row_not_exist = (ObActionFlag::OP_ROW_DOES_NOT_EXIST == result_action_flag_cell->get_ext());
    }
  }

  if (OB_SUCCESS == ret)
  {
    result->assign(*sstable_row);
    if (incr_row->get_is_delete_row() || is_row_not_exist)
    {
      if (OB_SUCCESS != (ret = result->reset(true, ObRow::DEFAULT_NULL)))
      {
        TBSYS_LOG(WARN, "fail to reset result row:ret[%d]", ret);
      }
    }
  }

  for (int64_t i = 0; OB_SUCCESS == ret && i < incr_row->get_column_num(); i ++)
  {
    if (OB_SUCCESS != (ret = incr_row->raw_get_cell(i, ups_cell, table_id, column_id)))
    {
      TBSYS_LOG(WARN, "fail to get ups cell:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = result->get_cell(table_id, column_id, result_cell)))
    {
      TBSYS_LOG(WARN, "fail to get result cell:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = result_cell->apply(*ups_cell)))
    {
      TBSYS_LOG(WARN, "fail to apply ups cell to result cell:ret[%d]", ret);
    }
    else if ( (ups_cell->get_type() != ObExtendType || ups_cell->get_ext() != ObActionFlag::OP_NOP) 
      && NULL != result_action_flag_cell )
    {
      result_action_flag_cell->set_ext(ObActionFlag::OP_VALID);
    }
  }
  return ret;
}

int ObRowFuse::apply_row(const ObRow &row, ObRow &result_row, bool copy)
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  ObObj *result_cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  const ObRowDesc *row_desc = NULL;

  if (NULL == (row_desc = row.get_row_desc()))
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "row_desc is null");
  }

  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0; OB_SUCCESS == ret && i < row.get_column_num(); i ++)
    {
      if (OB_SUCCESS != (ret = row.raw_get_cell(i, cell, table_id, column_id) ))
      {
        TBSYS_LOG(WARN, "fail to get cell:ret[%d]", ret);
      }
      else if (OB_ACTION_FLAG_COLUMN_ID != column_id)
      {
        if (OB_SUCCESS != (ret = result_row.get_cell(table_id, column_id, result_cell) ))
        {
          TBSYS_LOG(WARN, "fail to get cell:ret[%d]", ret);
        }
        else
        {
          if (copy)
          {
            *result_cell = *cell;
          }
          else
          {
            result_cell->apply(*cell);
          }
        }
      }
    }
  }
  return ret;
}

int ObRowFuse::fuse_row(const ObRow &row, ObRow &result_row, bool &is_row_empty, bool is_ups_row)
{
  int ret = OB_SUCCESS;
  if (!is_ups_row)
  {
    ret = fuse_sstable_row(row, result_row, is_row_empty);
  }
  else
  {
    ret = fuse_ups_row(row, result_row, is_row_empty);
  }
  return ret;
}

int ObRowFuse::fuse_sstable_row(const ObRow &row, ObRow &result_row, bool &is_row_empty)
{
  int ret = OB_SUCCESS;
  const ObObj *action_flag_cell = NULL;
  ObObj *result_action_flag_cell = NULL;
  int64_t row_action_flag = 0;
  int64_t idx = 0;
  int64_t result_idx = 0;

  if (NULL == row.get_row_desc() || NULL == result_row.get_row_desc())
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "row desc[%p], result row desc[%p]", 
      row.get_row_desc(), result_row.get_row_desc());
  }
  else
  {
    idx = row.get_row_desc()->get_idx(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
    result_idx = result_row.get_row_desc()->get_idx(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
    if (OB_INVALID_INDEX != result_idx)
    {
      if (OB_SUCCESS != (ret = result_row.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, result_action_flag_cell)))
      {
        TBSYS_LOG(WARN, "fail to get cell:ret[%d]", ret);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_INVALID_INDEX == idx)
    {
      result_row.assign(row);
      is_row_empty = false;
    }
    else
    {
      if (OB_SUCCESS != (ret = row.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, action_flag_cell) ))
      {
        TBSYS_LOG(WARN, "fail to get action flag cell:ret[%d]", ret);
      }
      else if (ObExtendType != action_flag_cell->get_type())
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "action_flag_cell_type[%d]", action_flag_cell->get_type());
      }
      else
      {
        row_action_flag = action_flag_cell->get_ext();
      }

      if (OB_SUCCESS == ret)
      {
        switch (row_action_flag)
        {
          case ObActionFlag::OP_ROW_DOES_NOT_EXIST:
            // do nothing
            break;
          case ObActionFlag::OP_DEL_ROW:
            is_row_empty = true;
            if (OB_SUCCESS != (ret = result_row.reset(true, ObRow::DEFAULT_NULL) ))
            {
              TBSYS_LOG(WARN, "reset result fail:ret[%d]", ret);
            }
            else if (NULL != result_action_flag_cell)
            {
              result_action_flag_cell->set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
            }
            break;
          case ObActionFlag::OP_NEW_ADD:
            is_row_empty = false;
            if (OB_SUCCESS != (ret = result_row.reset(true, ObRow::DEFAULT_NULL) )) // set to all null
            {
              TBSYS_LOG(WARN, "fail to reset result_row:ret[%d]", ret);
            }
            else if (OB_SUCCESS != (ret = apply_row(row, result_row, false) ))
            {
              TBSYS_LOG(WARN, "fail to apply row:ret[%d]", ret);
            }
            else if (NULL != result_action_flag_cell)
            {
              result_action_flag_cell->set_ext(ObActionFlag::OP_VALID);
            }
            break;
          case ObActionFlag::OP_VALID:
            is_row_empty = false;
            if (OB_SUCCESS != (ret = apply_row(row, result_row, false) ))
            {
              TBSYS_LOG(WARN, "fail to apply row:ret[%d]", ret);
            }
            else if (NULL != result_action_flag_cell)
            {
              result_action_flag_cell->set_ext(ObActionFlag::OP_VALID);
            }
            break;
          default:
            ret = OB_NOT_SUPPORTED;
            TBSYS_LOG(WARN, "unsupported ext type[%ld]", row_action_flag);
        }
        //add lbzhong [Update rowkey] 20160420:b
        if(OB_SUCCESS == ret)
        {
          result_row.set_is_new_row(row.is_new_row());
        }
        //add:e
      }
    }
  }
  return ret;
}

int ObRowFuse::fuse_ups_row(const ObRow &row, ObRow &result_row, bool &is_row_empty)
{
  int ret = OB_SUCCESS;
  const ObObj *action_flag_cell = NULL;
  ObObj *result_action_flag_cell = NULL;
  bool copy = false;
  int64_t row_action_flag = 0;
  int64_t result_row_action_flag = 0;

  if (OB_SUCCESS != (ret = row.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, action_flag_cell) ))
  {
    TBSYS_LOG(WARN, "fail to get action flag cell:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = result_row.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, result_action_flag_cell) ))
  {
    TBSYS_LOG(WARN, "fail to get result action flag cell:ret[%d]", ret);
  }
  else if (ObExtendType != action_flag_cell->get_type() || ObExtendType != result_action_flag_cell->get_type())
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "action_flag_cell_type[%d], result_row_action_flag_type[%d]", action_flag_cell->get_type(),
      result_action_flag_cell->get_type());
  }
  else
  {
    row_action_flag = action_flag_cell->get_ext();
    result_row_action_flag = result_action_flag_cell->get_ext();
  }

  if (OB_SUCCESS == ret)
  {
    switch (row_action_flag)
    {
      case ObActionFlag::OP_NEW_ADD:
        result_action_flag_cell->set_ext(ObActionFlag::OP_NEW_ADD);
        if (OB_SUCCESS != (ret = apply_row(row, result_row, true) ))
        {
          TBSYS_LOG(WARN, "fail to apply row:ret[%d]", ret);
        }
        break;
      case ObActionFlag::OP_DEL_ROW:
        result_action_flag_cell->set_ext(ObActionFlag::OP_DEL_ROW);
        break;
      case ObActionFlag::OP_VALID:
        if (result_row_action_flag == ObActionFlag::OP_VALID
          || result_row_action_flag == ObActionFlag::OP_ROW_DOES_NOT_EXIST)
        {
          result_action_flag_cell->set_ext(ObActionFlag::OP_VALID);
        }
        else
        {
          result_action_flag_cell->set_ext(ObActionFlag::OP_NEW_ADD);
        }
        copy = result_row_action_flag == ObActionFlag::OP_DEL_ROW
          || result_row_action_flag == ObActionFlag::OP_ROW_DOES_NOT_EXIST;
        if (OB_SUCCESS != (ret = apply_row(row, result_row, copy) ))
        {
          TBSYS_LOG(WARN, "fail to apply row:ret[%d]", ret);
        }
        break;
      case ObActionFlag::OP_ROW_DOES_NOT_EXIST:
        // do nothing
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        TBSYS_LOG(WARN, "unsupported ext type[%ld]", row_action_flag);
    }
  }

  if (OB_SUCCESS == ret)
  {
    is_row_empty = (ObActionFlag::OP_ROW_DOES_NOT_EXIST == result_action_flag_cell->get_ext());
  }
  
  return ret;
}

