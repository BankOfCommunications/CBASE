/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_return_operator.cpp for implementation of return operator.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_return_operator.h"

namespace oceanbase
{
  namespace common
  {
    ObReturnOperator::ObReturnOperator()
    {
      initialize();
    }

    ObReturnOperator::~ObReturnOperator()  
    {
      clear();
    }

    void ObReturnOperator::initialize()
    {
      iter_ = NULL;
      return_columns_map_ = NULL;
      cur_column_idx_in_row_ = 0;
      row_width_ = 0;
      limit_offset_ = 0;
      limit_count_ = 0;
      max_avail_cell_num_ = -1;
      row_changed_ = false;
      row_first_cell_got_ = false;
    }

    void ObReturnOperator::clear()
    {
      initialize();
    }

    int ObReturnOperator::start_return(
      ObInnerIterator& iter, 
      const ObArrayHelpers<bool>& return_columns_map, 
      const int64_t row_width, 
      const int64_t limit_offset, 
      const int64_t limit_count)
    {
      int ret = OB_SUCCESS;

      clear();
      if (row_width <= 0 || limit_offset < 0 || limit_count < 0)
      {
        TBSYS_LOG(WARN, "invalid rparam, row_width=%ld, limit_offset=%ld, "
          "limit_count=%ld", 
          row_width, limit_offset, limit_count);
        ret = OB_ERROR;
      }
      else
      {
        iter_ = &iter;
        return_columns_map_ = &return_columns_map;
        row_width_ = row_width;
        limit_offset_ = limit_offset;
        limit_count_ = limit_count;
      }

      if (OB_SUCCESS == ret && limit_count_ > 0)
      {
        max_avail_cell_num_ = row_width_ * limit_count_;
      }

      if (OB_SUCCESS == ret && limit_offset_ > 0)
      {
        ret = jump_limit_offset();
        if (OB_ITER_END == ret)
        {
          max_avail_cell_num_ = 0;
          ret = OB_SUCCESS;
        }
      }

      return ret;
    }

    int ObReturnOperator::jump_limit_offset()
    {
      int ret = OB_SUCCESS;
      int64_t jump_cell_num = limit_offset_ * row_width_;

      for (int64_t i = 0; i < jump_cell_num && OB_SUCCESS == ret; ++i)
      {
        ret = iter_->next_cell();
      }      

      return ret;
    }

    int ObReturnOperator::next_cell()
    {
      int ret = OB_SUCCESS;

      for(;;)
      {
        cur_column_idx_in_row_ = cur_column_idx_in_row_ % row_width_;
        ret = iter_->next_cell();
        if (OB_SUCCESS == ret)
        {
          if (cur_column_idx_in_row_ == 0)
          {
            row_changed_ = true;
            row_first_cell_got_ = false;
          }
          else if (row_first_cell_got_)
          {
            row_changed_ = false;
          }

          if (max_avail_cell_num_ >= 0)
          {
            if (max_avail_cell_num_ == 0)
            {
              ret = OB_ITER_END;
              break;
            }
            else
            {
              max_avail_cell_num_ --;
            }
          }
        }

        if (OB_SUCCESS == ret
          && !*return_columns_map_->at(cur_column_idx_in_row_++))
        {
          continue;
        }
        else
        {
          break;
        }
      }

      return ret;
    }

    int ObReturnOperator::get_cell(ObInnerCellInfo** cell)
    {
      return iter_->get_cell(cell);
    }

    int ObReturnOperator::get_cell(ObInnerCellInfo** cell, bool* is_row_changed)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == (ret = iter_->get_cell(cell, is_row_changed)))
      {
        if (NULL != is_row_changed)
        {
          *is_row_changed = row_changed_;
          row_first_cell_got_ = true;
        }
      }
      return ret;
    }
  } // end namespace common
} // end namespace oceanbase
