/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_cell_array_helper.cpp for help get cell from cell array or
 * get param easier. 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_cell_stream.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    ObCellArrayHelper::ObCellArrayHelper()
    :cell_array_(NULL), get_param_(NULL)
    {
    }
    
    ObCellArrayHelper::ObCellArrayHelper(ObCellArray &cell_array)
    :cell_array_(&cell_array), get_param_(NULL)
    {
    }
    
    ObCellArrayHelper::ObCellArrayHelper(const ObGetParam &get_param)
    :cell_array_(NULL), get_param_(&get_param)
    {
    }
    
    int64_t ObCellArrayHelper::get_cell_size() const
    {
      int64_t result = 0;
      if (NULL != get_param_)
      {
        result = get_param_->get_cell_size();
      }
      else if (NULL != cell_array_)
      {
        result = cell_array_->get_cell_size();
      }
      else
      {
        result = 0;
      }
      return result;
    }
    
    const ObCellInfo & ObCellArrayHelper::operator [](int64_t offset)const
    {
      const ObCellInfo * result = NULL;
      if (NULL != get_param_)
      {
        if (offset < get_param_->get_cell_size())
        {
          result = get_param_->operator [](offset);
        }
        else
        {
          TBSYS_LOG(ERROR, "logic error, offset out of range [offset:%ld,cell_num:%ld]",
                    offset,get_param_->get_cell_size());
          result = &default_cell_;
        }
      }
      else if (NULL != cell_array_)
      {
        ObInnerCellInfo & temp = cell_array_->operator [](offset);
        ugly_temp_cell_.table_id_ = temp.table_id_;
        ugly_temp_cell_.column_id_ = temp.column_id_;
        ugly_temp_cell_.row_key_ = temp.row_key_;
        ugly_temp_cell_.value_ = temp.value_;
        result = &ugly_temp_cell_;
      }
      else
      {
        TBSYS_LOG(ERROR, "logic error, offset out of range [offset:%ld,cell_num:%d]", offset, 0);
        result = &default_cell_;
      } 
      return *result;
    }
  } // end namespace chunkserver
} // end namespace oceanbase
