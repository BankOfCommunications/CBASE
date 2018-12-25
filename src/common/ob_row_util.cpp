/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_util.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_row_util.h"
#include "common/ob_compact_cell_writer.h"
#include "common/ob_compact_cell_iterator.h"

using namespace oceanbase::common;

int ObRowUtil::convert(const ObRow &row, ObString &compact_row)
{
  int ret = OB_SUCCESS;
  ObCompactCellWriter cell_writer;
  cell_writer.init(compact_row.ptr(), compact_row.size());

  const ObObj *cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  ObObj cell_clone;
  for (int64_t i = 0; i < row.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = row.raw_get_cell(i, cell, table_id, column_id)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
      break;
    }
    else if (OB_SUCCESS != (ret = cell_writer.append(column_id, *cell)))
    {
      if (OB_SIZE_OVERFLOW != ret)
      {
        TBSYS_LOG(WARN, "failed to append cell, err=%d", ret);
      }
      break;
    }
  } // end for i
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = cell_writer.row_finish()))
    {
      if (OB_SIZE_OVERFLOW != ret)
      {
        TBSYS_LOG(WARN, "failed to append cell, err=%d", ret);
      }
    }
    else
    {
      compact_row.assign_ptr(compact_row.ptr(), static_cast<int32_t>(cell_writer.size()));
    }
  }
  return ret;
}

int ObRowUtil::convert(const ObRow &row, ObString &compact_row, ObRow &out_row)
{
  int ret = OB_SUCCESS;
  ObCompactCellWriter cell_writer;
  cell_writer.init(compact_row.ptr(), compact_row.size());

  const ObObj *cell = NULL;
  const ObObj *clone_cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  ObObj cell_clone;

  // shallow copy
  out_row = row;

  for (int64_t i = 0; i < row.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = row.raw_get_cell(i, cell, table_id, column_id)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
      break;
    }
    else if (OB_SUCCESS != (ret = out_row.raw_get_cell(i, clone_cell, table_id, column_id)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
      break;
    }
    else if (OB_SUCCESS != (ret = cell_writer.append(column_id, *cell, const_cast<ObObj *>(clone_cell))))
    {
      if (OB_SIZE_OVERFLOW != ret)
      {
        TBSYS_LOG(WARN, "failed to append cell, err=%d size=%ld", ret, cell_writer.size());
      }
      break;
    }
  } // end for i
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = cell_writer.row_finish()))
    {
      if (OB_SIZE_OVERFLOW != ret)
      {
        TBSYS_LOG(WARN, "failed to append cell, err=%d", ret);
      }
    }
    else
    {
      compact_row.assign_ptr(compact_row.ptr(), static_cast<int32_t>(cell_writer.size()));
    }
  }
  return ret;
}

int ObRowUtil::convert(const ObString &compact_row, ObRow &row)
{
  return convert(compact_row, row, NULL, NULL);
}

int ObRowUtil::convert(const ObString &compact_row, ObRow &row, ObRowkey *rowkey, ObObj *rowkey_buf
                       //add lbzhong [Update rowkey] 20151221:b
                       , const bool is_update_second_index
                       //add:e
                       )
{
  int ret = OB_SUCCESS;
  ObCompactCellIterator cell_reader;
  ObString compact_row2 = compact_row;
  uint64_t column_id = OB_INVALID_ID;
  ObObj cell;
  bool is_row_finished = false;
  int64_t cell_idx = 0;

  const ObObj *rowkey_obj = NULL;

  if(NULL == rowkey)
  {
    cell_reader.init(compact_row2, SPARSE);
  }
  else
  {
    cell_reader.init(compact_row2, DENSE_SPARSE);
  }


  if(OB_SUCCESS == ret && NULL != rowkey)
  {
    if(NULL == rowkey_buf)
    {
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "rowkey_buf should not be null");
    }

    int64_t rowkey_cnt = 0;
    while(OB_SUCCESS == ret)
    {
      if(OB_SUCCESS != (ret = cell_reader.next_cell()))
      {
        TBSYS_LOG(WARN, "next cell fail:ret[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = cell_reader.get_cell(rowkey_obj, &is_row_finished)))
      {
        TBSYS_LOG(WARN, "get cell fail:ret[%d]", ret);
      }

      if(OB_SUCCESS == ret && is_row_finished)
      {
        break;
      }

      if(OB_SUCCESS == ret)
      {
        rowkey_buf[rowkey_cnt ++] = *rowkey_obj;
      }
    }

    if(OB_SUCCESS == ret)
    {
      rowkey->assign(rowkey_buf, rowkey_cnt);
    }
  }
  //add lbzhong [Update rowkey] 20151221:b
  const ObRowDesc *row_desc = row.get_row_desc();
  //add:e
  while(OB_SUCCESS == (ret = cell_reader.next_cell()))
  {
    if (OB_SUCCESS != (ret = cell_reader.get_cell(column_id, cell, &is_row_finished)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
      break;
    }
    else if (is_row_finished)
    {
      ret = OB_SUCCESS;
      break;
    }

    else if(
            //add lbzhong [Update rowkey] 20151221:b
            !is_update_second_index &&
            //add:e
            OB_SUCCESS != (ret = row.raw_set_cell(cell_idx++, cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d", ret);
      break;
    }
    //add lbzhong [Update rowkey] 20160510:b
    else if(is_update_second_index && row_desc->exist_column_id(column_id) &&
            OB_SUCCESS != (ret = row.raw_set_cell(cell_idx++, cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d", ret);
      break;
    }
    //add:e
  }
  if (OB_UNLIKELY(OB_SUCCESS != ret))
  {
    TBSYS_LOG(WARN, "fail to read next cell[%d]", ret);
  }
  if (cell_idx != row.get_column_num())
  {
    //const ObRowDesc *row_desc = row.get_row_desc(); //del lbzhong [Update rowkey] 20151221:b:e
    TBSYS_LOG(ERROR, "corrupted row data, row_desc[%s] col=%ld cell_num=%ld", to_cstring(*row_desc), row.get_column_num(), cell_idx);
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}


int ObRowUtil::convert(uint64_t table_id, const ObString &compact_row, ObRow &row, bool is_ups_row)
{
  int ret = OB_SUCCESS;
  ObCompactCellIterator cell_reader;
  uint64_t column_id = OB_INVALID_ID;
  bool is_row_finished = false;
  const ObObj *value = NULL;

  if (is_ups_row)
  {
    row.reset(false, ObRow::DEFAULT_NOP);
  }
  else
  {
    row.reset(false, ObRow::DEFAULT_NULL);
  }

  cell_reader.init(compact_row, SPARSE);

  do
  {
    if(OB_SUCCESS != (ret = cell_reader.next_cell()))
    {
      TBSYS_LOG(WARN, "next cell fail:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = cell_reader.get_cell(column_id, value, &is_row_finished)))
    {
      TBSYS_LOG(WARN, "failed to get cell, column_id=%lu err=%d", column_id, ret);
      break;
    }
    else if (is_row_finished)
    {
      ret = OB_SUCCESS;
      break;
    }
    else if(ObExtendType == value->get_type())
    {
      if (OB_SUCCESS !=
          (ret = row.set_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, *value)))
      {
        TBSYS_LOG(WARN, "failed to set action flag cell, table_id=%lu column_id=%lu "
            " value=%s err=%d", OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, to_cstring(*value), ret);
        break;
      }
    }
    else if (OB_SUCCESS != (ret = row.set_cell(table_id, column_id, *value)))
    {
      TBSYS_LOG(WARN, "failed to set cell, table_id=%lu column_id=%lu err=%d",
          table_id, column_id, ret);
      break;
    }
  } while(OB_SUCCESS == ret);
  return ret;
}

int64_t ObRowUtil::get_row_serialize_size(const common::ObRow& row)
{
  int64_t length = 0;
  const ObObj* cell = NULL;
  for (int64_t i = 0; i < row.get_column_num(); ++i)
  {
    if (OB_SUCCESS == row.raw_get_cell(i, cell) && NULL != cell)
    {
      length += cell->get_serialize_size();
    }
  }
  return length;
}

int ObRowUtil::serialize_row(const common::ObRow& row, char* buf, int64_t buf_len, int64_t &pos)
{
  int ret = 0;
  const ObObj* cell = NULL;
  for (int64_t i = 0; i < row.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = row.raw_get_cell(i, cell)) || NULL == cell)
    {
      TBSYS_LOG(WARN, "serialize_row cell[%ld], ret=%d, cell=%p", i, ret, cell);
    }
    else
    {
      ret = cell->serialize(buf, buf_len, pos);
    }
  }
  return ret;
}

