/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_file_table.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_ups_file_table.h"

using namespace oceanbase::sql;
using namespace oceanbase::sql::test;
using namespace oceanbase::common;

ObUpsFileTable::ObUpsFileTable(const char *file_name)
  :ObFileTable(file_name)
{
}

int ObUpsFileTable::parse_line(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj value;
  int64_t int_value;
  ObString str_value;

  if (column_count_ + 1 == count_)
  {

    for (int32_t i=1;(OB_SUCCESS == ret) && i<count_;i++)
    {
      if( tokens_[0][0] == '-' )
      {
        curr_ups_row_.set_is_delete_row(true);
      }
      else
      {
        curr_ups_row_.set_is_delete_row(false);
      }

      if(0 == strcmp(tokens_[i], "NOP"))
      {
        value.set_ext(ObActionFlag::OP_NOP);
      }
      else
      {
        if(ObIntType == column_type_[i-1])
        {
          bool is_add = false;
          if( tokens_[i][strlen(tokens_[i]) - 1] == '+' )
          {
            tokens_[i][strlen(tokens_[i]) - 1] = '\0';
            is_add = true;
          }
          int_value = atoi(tokens_[i]);
          value.set_int(int_value, is_add);
        }
        else if(ObVarcharType == column_type_[i-1])
        {
          str_value.assign_ptr(tokens_[i], (int32_t)strlen(tokens_[i]));
          value.set_varchar(str_value);
        }
        else if(ObExtendType == column_type_[i-1])
        {
          value.set_ext(ObActionFlag::OP_DEL_ROW);
        }
        else
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "unsupport type [%d]", column_type_[i-1]);
        }
      }

      if(OB_SUCCESS == ret)
      {
        int64_t index = 0;
        ObRowkeyColumn rk_col;
        if(OB_SUCCESS == rowkey_info_.get_index(column_ids_[i-1], index , rk_col))
        {
          rowkey_obj[index] = value;
        }
        curr_ups_row_.set_cell(table_id_, column_ids_[i-1], value);
      }
    }
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "column_count_[%d], count_[%d], line_[%s]", column_count_, count_, line_);
  }

  if (OB_SUCCESS == ret)
  {
    row = &curr_ups_row_;
  }
  return ret;
}

ObRow* ObUpsFileTable::get_curr_row()
{
  return &curr_ups_row_;
}
