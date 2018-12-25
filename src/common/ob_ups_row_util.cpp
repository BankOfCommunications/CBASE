/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_row_util.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_ups_row_util.h"
#include "common/ob_compact_cell_writer.h"
#include "common/ob_compact_cell_iterator.h"

using namespace oceanbase::common;

int ObUpsRowUtil::convert(const ObUpsRow &row, ObString &compact_row)
{
  int ret = OB_SUCCESS;
  ObCompactCellWriter cell_writer;
  cell_writer.init(compact_row.ptr(), compact_row.size());

  const ObObj *cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  ObObj cell_clone;

  if(row.get_is_delete_row())
  {
    if(OB_SUCCESS != (ret = cell_writer.row_delete()))
    {
      TBSYS_LOG(WARN, "append delete row fail:ret[%d]", ret);
    }
  }

  for (int64_t i = 0; (OB_SUCCESS == ret) && i < row.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = row.raw_get_cell(i, cell, table_id, column_id)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
      break;
    }
    else
    {
      if(ObExtendType == cell->get_type() && ObActionFlag::OP_NOP == cell->get_ext())
      {
        //不用处理，OP_NOP不序列化
      }
      else if (OB_SUCCESS != (ret = cell_writer.append(column_id, *cell)))
      {
        if (OB_SIZE_OVERFLOW != ret)
        {
          TBSYS_LOG(WARN, "failed to append cell, err=%d", ret);
        }
        break;
      }
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
      compact_row.assign_ptr(compact_row.ptr(), (int32_t)(cell_writer.size()));
    }
  }
  return ret;
}

int ObUpsRowUtil::convert(uint64_t table_id, const ObString &compact_row, ObUpsRow &row)
{
  return convert(table_id, compact_row, row, NULL, NULL);
}

int ObUpsRowUtil::convert(uint64_t table_id, const ObString &compact_row, ObUpsRow &row, ObRowkey *rowkey, ObObj *rowkey_buf)
{
  int ret = OB_SUCCESS;
  ObCompactCellIterator cell_reader;
  uint64_t column_id = OB_INVALID_ID;
  bool is_row_finished = false;
  const ObObj *rowkey_obj = NULL;
  const ObObj *value = NULL;

  row.reuse();

  if(NULL == rowkey)
  {
    cell_reader.init(compact_row, SPARSE);
  }
  else
  {
    cell_reader.init(compact_row, DENSE_SPARSE);
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

  while(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = cell_reader.next_cell()))
    {
      TBSYS_LOG(WARN, "next cell fail:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = cell_reader.get_cell(column_id, value, &is_row_finished)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
      break;
    }
    else if (is_row_finished)
    {
      ret = OB_SUCCESS;
      break;
    }
    else if(ObExtendType == value->get_type() && ObActionFlag::OP_DEL_ROW == value->get_ext())
    {
      row.set_is_delete_row(true);
    }
    else if (OB_SUCCESS != (ret = row.set_cell(table_id, column_id, *value)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d", ret);
      break;
    }
    else
    {
      row.set_is_all_nop(false);
    }
  }
  return ret;
}

