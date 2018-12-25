/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_new_scanner_helper.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_new_scanner_helper.h"
#include "utility.h"

using namespace oceanbase;
using namespace common;

int ObNewScannerHelper::print_new_scanner(ObNewScanner &new_scanner, ObRow &row, bool has_rowkey /* = false */)
{
  int ret = OB_SUCCESS;
  const ObRowkey *rowkey = NULL;

  while (OB_SUCCESS == ret)
  {
    if (has_rowkey)
    {
      ret = new_scanner.get_next_row(rowkey, row);
    }
    else
    {
      ret = new_scanner.get_next_row(row);
    }

    if (OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
      break;
    }
    else if (OB_SUCCESS == ret)
    {
      if (has_rowkey)
      {
        TBSYS_LOG(INFO, "new scanner rowkey:%s row:%s", to_cstring(*rowkey), to_cstring(row));
      }
      else
      {
        TBSYS_LOG(INFO, "new scanner row:%s", to_cstring(row));
      }
    }
    else
    {
      TBSYS_LOG(WARN, "new scanner get next row fail");
    }
  }

  return ret;
}

int ObNewScannerHelper::add_cell(ObRow &row, const ObCellInfo &cell, bool is_ups_row)
{
  int ret = OB_SUCCESS;
  ObObj *action_flag_cell = NULL;
  ObObj *value = NULL;
  
  if (OB_SUCCESS != (ret = row.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, action_flag_cell) ))
  {
    TBSYS_LOG(WARN, "fail to get action flag cell:ret[%d]", ret);
  }

  if (OB_SUCCESS == ret)
  {
    if (!is_ups_row) //normal row
    {
      if (ObExtendType == cell.value_.get_type())
      {
        if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell.value_.get_ext())
        {
          if (OB_SUCCESS != (ret = put_rowkey_to_row(row, cell) ))
          {
            TBSYS_LOG(WARN, "fail to put rowkey to row:ret[%d]", ret);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "unsupported ext type[%ld]", cell.value_.get_ext());
        }
      }
      else
      {
        if (OB_SUCCESS != (ret = row.get_cell(cell.table_id_, cell.column_id_, value) ))
        {
          TBSYS_LOG(WARN, "fail to get cell:ret[%d]", ret);
        }
        else if (OB_SUCCESS != (ret = value->apply(cell.value_) ))
        {
          TBSYS_LOG(WARN, "fail to apply cell to value:ret[%d]", ret);
        }
        else
        {
          action_flag_cell->set_ext(ObActionFlag::OP_VALID);
        }
      }
    }
    else //ups row
    {
      if (OB_SUCCESS == ret)
      {
        if (ObExtendType != cell.value_.get_type() || ObActionFlag::OP_NOP == cell.value_.get_ext())
        {
          if (action_flag_cell->get_ext() == ObActionFlag::OP_DEL_ROW 
            || action_flag_cell->get_ext() == ObActionFlag::OP_NEW_ADD)
          {
            action_flag_cell->set_ext(ObActionFlag::OP_NEW_ADD);
          }
          else
          {
            action_flag_cell->set_ext(ObActionFlag::OP_VALID);
          }

          if (ObExtendType != cell.value_.get_type())
          {
            if (OB_SUCCESS != (ret = row.get_cell(cell.table_id_, cell.column_id_, value) ))
            {
              TBSYS_LOG(WARN, "fail to get cell:ret[%d]", ret);
            }
            else if (OB_SUCCESS != (ret = value->apply(cell.value_) ))
            {
              TBSYS_LOG(WARN, "fail to apply cell to value:ret[%d]", ret);
            }
          }
        }
        else if (ObActionFlag::OP_DEL_ROW == cell.value_.get_ext())
        {
          action_flag_cell->set_ext(ObActionFlag::OP_DEL_ROW);
          if (OB_SUCCESS != (ret = put_rowkey_to_row(row, cell) ))
          {
            TBSYS_LOG(WARN, "fail to put rowkey to row:ret[%d]", ret);
          }
        }
        else if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell.value_.get_ext())
        {
          if (OB_SUCCESS != (ret = put_rowkey_to_row(row, cell) ))
          {
            TBSYS_LOG(WARN, "fail to put rowkey to row:ret[%d]", ret);
          }
        }
        else
        {
          ret = OB_NOT_SUPPORTED;
          TBSYS_LOG(WARN, "unsupport ext type[%ld]", cell.value_.get_ext());
        }
      }
    }
  }

  return ret;
}

int ObNewScannerHelper::put_rowkey_to_row(ObRow &row, const ObCellInfo &cell)
{
  int ret = OB_SUCCESS;
  // put rowkey to row
  const ObRowDesc *row_desc = NULL;
  row_desc = row.get_row_desc();
  if (OB_UNLIKELY(NULL == row_desc))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "row_desc is null");
  }
  else if(row_desc->get_rowkey_cell_count() > 0)
  {
    if (row_desc->get_rowkey_cell_count() != cell.row_key_.length())
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "rowkey count is wrong row_desc rowkey count [%ld], actual rowkey count[%ld]",
      row_desc->get_rowkey_cell_count(), 
      cell.row_key_.length());
    }
    else
    {
      for (int64_t i = 0; i < cell.row_key_.length(); i ++)
      {
        row.raw_set_cell(i, cell.row_key_.ptr()[i]);
      }
    }
  }
  return ret;
}


int ObNewScannerHelper::add_ups_cell(ObUpsRow &ups_row, const ObCellInfo &cell, ObStringBuf *str_buf /* = NULL */)
{
  int ret = OB_SUCCESS;
  ObObj value;
  ObObj stored_value;
  ObObj *val_ptr = &value;
  const ObObj *got_cell = NULL;

  if(ObExtendType == cell.value_.get_type())
  {
    if(ObActionFlag::OP_DEL_ROW == cell.value_.get_ext())
    {
      ups_row.set_is_delete_row(true);
      if(OB_SUCCESS != (ret = ups_row.reset()))
      {
        TBSYS_LOG(WARN, "ups row set all nop fail:ret[%d]", ret);
      }
    }
    else if(ObActionFlag::OP_NOP == cell.value_.get_ext())
    {
       //do nothing
    }
    else if(ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell.value_.get_ext())
    {
       //do nothing
    }
    else
    {
      ret = OB_NOT_SUPPORTED;
      TBSYS_LOG(WARN, "not support ext value:[%ld]", cell.value_.get_ext());
    }
  }
  else
  {
    if(OB_SUCCESS != (ret = ups_row.get_cell(cell.table_id_, cell.column_id_, got_cell)))
    {
      TBSYS_LOG(WARN, "ups row get cell fail:ret[%d]", ret);
    }
    else
    {
      value = *got_cell;
    }

    if(OB_SUCCESS == ret)
    {
      if(OB_SUCCESS != (ret = value.apply(cell.value_)))
      {
        TBSYS_LOG(WARN, "obj apply fail:ret[%d]", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      if(NULL != str_buf)
      {
        if (OB_SUCCESS != (ret = str_buf->write_obj(value, &stored_value)))
        {
          TBSYS_LOG(WARN, "write obj fail:ret[%d]", ret);
        }
        else
        {
          val_ptr = &stored_value;
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      if(OB_SUCCESS != (ret = ups_row.set_cell(cell.table_id_, cell.column_id_, *val_ptr)))
      {
        TBSYS_LOG(WARN, "ups set value fail:ret[%d]", ret);
      }
    }
  }
  
  return ret;
}

//没有处理ups_row全部是OP_NOP的情况，这种情况下还是会当成存在的一行
int ObNewScannerHelper::convert(ObScanner &scanner, const ObRowDesc *row_desc, ObNewScanner &new_scanner)
{
  int ret = OB_SUCCESS;
  ObCellInfo *cell = NULL;
  bool is_fullfilled = false;
  int64_t fullfilled_item_num = 0;
  int64_t fullfilled_row_num = 0;
  ObUpsRow ups_row;
  ObRowkey last_rowkey;
  ObRowkey last_succ_added_rowkey;
  bool ups_row_empty = true;
  bool is_row_changed = false;
  bool is_size_overflow = false;
  CharArena allocator;

  if(NULL == row_desc)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "row_desc is null");
  }
  else
  {
    ups_row.set_row_desc(*row_desc);
    ups_row.reuse();
  }

  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_item_num)))
    {
      TBSYS_LOG(WARN, "get scanner fullfilled fail:ret[%d]", ret);
    }
  }

  while(OB_SUCCESS == ret)
  {
    ret = scanner.next_cell();
    if(OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
      break;
    }
    else if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "scanner next cell fail:ret[%d]", ret);
    }

    if(OB_SUCCESS == ret)
    {
      if(OB_SUCCESS != (ret = scanner.get_cell(&cell, &is_row_changed)))
      {
        TBSYS_LOG(WARN, "scanner get cell fail:ret[%d]", ret);
      }
    }

    if(OB_SUCCESS == ret)
    {
      if(is_row_changed && !ups_row_empty)
      {
        ret = new_scanner.add_row(last_rowkey, ups_row);
        if(OB_SIZE_OVERFLOW == ret)
        {
          ret = OB_SUCCESS;
          is_size_overflow = true;
          break;
        }
        else if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "new scanner add row fail:ret[%d]", ret);
        }
        else
        {
          last_succ_added_rowkey = last_rowkey;
          ++fullfilled_row_num;
        }

        if(OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret = ups_row.reuse()))
          {
            TBSYS_LOG(WARN, "ups row reuse fail:ret[%d]", ret);
          }
          else
          {
            ups_row_empty = true;
          }
        }
      }

      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = add_ups_cell(ups_row, *cell)))
        {
          TBSYS_LOG(WARN, "add cell to ups row fail:ret[%d]", ret);
        }
        else
        {
          if(is_row_changed)
          {
            if(OB_SUCCESS != (ret = cell->row_key_.deep_copy(last_rowkey, allocator)))
            {
              TBSYS_LOG(WARN, "rowkey deep copy fail:ret[%d]", ret);
            }
            else
            {
              ups_row_empty = false;
            }
          }
        }
      }
    }
  }
  
  if(OB_SUCCESS == ret && !ups_row_empty && !is_size_overflow)
  {
    ret = new_scanner.add_row(last_rowkey, ups_row);
    if(OB_SUCCESS == ret)
    {
      last_succ_added_rowkey = last_rowkey;
      ++fullfilled_row_num;
    }
    else if(OB_SIZE_OVERFLOW != ret)
    {
      TBSYS_LOG(WARN, "new scanner add row fail:ret[%d]", ret);
    }
    else
    {
      ret = OB_SUCCESS;
      is_size_overflow = true;
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(is_size_overflow)
    {
      is_fullfilled = false;
    }

    if(fullfilled_row_num <= 0 && !is_fullfilled)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "when not fullfilled, fullfilled row num at least be one");
    }
    else if(OB_SUCCESS != (ret = new_scanner.set_is_req_fullfilled(is_fullfilled, fullfilled_row_num)))
    {
      TBSYS_LOG(WARN, "new scanner set is req fullfilled fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = new_scanner.set_last_row_key(last_succ_added_rowkey)))
    {
      TBSYS_LOG(WARN, "set last rowkey fail:ret[%d]", ret);
    }
  }

  ObNewRange range;
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS == scanner.get_range(range) 
        && OB_SUCCESS != (ret = new_scanner.set_range(range)))
    {
      TBSYS_LOG(WARN, "new scanner set range fail:ret[%d]", ret);
    }
  }

  return ret;
}

int ObNewScannerHelper::get_row_desc(const ObScanParam &scan_param, ObRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = scan_param.get_table_id();

  for(int64_t i=0;OB_SUCCESS == ret && i<scan_param.get_column_id_size();i++)
  {
    if(OB_SUCCESS != (ret = row_desc.add_column_desc(table_id, scan_param.get_column_id()[i])))
    {
      TBSYS_LOG(WARN, "add column desc fail:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = row_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID) ))
    {
      TBSYS_LOG(WARN, "fail to add OB_ACTION_FLAG_COLUMN_ID:ret[%d]", ret);
    }
  }

  //todo 添加其他类型的列

  return ret;
}

int ObNewScannerHelper::get_row_desc(const ObGetParam &get_param, bool has_flag_column, ObRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  ObCellInfo *cell = NULL;

  if(get_param.get_row_size() <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "get param row should be positive:actual[%ld]", get_param.get_row_size());
  }

  if(OB_SUCCESS == ret)
  {
    ObGetParam::ObRowIndex row_index = get_param.get_row_index()[0];
    for(int64_t i=0;OB_SUCCESS == ret && i<row_index.size_;i++)
    {
      cell = get_param[row_index.offset_ + i];
      if(OB_SUCCESS != (ret = row_desc.add_column_desc(cell->table_id_, cell->column_id_)))
      {
        TBSYS_LOG(WARN, "add column desc fail:ret[%d], row_index.size_[%d], offset=%ld, row_num=%ld",
                  ret, row_index.size_, row_index.offset_ + i, get_param.get_row_size());
      }
    }
  }

  if (OB_SUCCESS == ret && has_flag_column)
  {
    if (OB_SUCCESS != (ret = row_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID) ))
    {
      TBSYS_LOG(WARN, "fail to add OB_ACTION_FLAG_COLUMN_ID:ret[%d]", ret);
    }
  }

  return ret;
}

