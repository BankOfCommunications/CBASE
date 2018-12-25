/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_compose_operator.cpp for implementation of compose 
 * operator. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */

#include "ob_compose_operator.h"
namespace oceanbase
{
  namespace common
  {
    ObComposeOperator::ObComposeOperator()
    {
      initialize();
    }
    
    ObComposeOperator::~ObComposeOperator()  
    {
      clear();
    }

    void ObComposeOperator::initialize()
    {
      result_array_ = NULL;
      composite_columns_ = NULL;
      row_width_ = 0;
    }
    
    void ObComposeOperator::clear()
    {
      initialize();
    }

    int ObComposeOperator::start_compose(
        ObCellArray& result_array, 
        const ObArrayHelper<ObCompositeColumn>& composite_columns, 
        const int64_t row_width)
    {
      int ret = OB_SUCCESS;

      clear();
      result_array_ = &result_array;
      composite_columns_ = &composite_columns;

      if (row_width <= composite_columns.get_array_index())
      {
        TBSYS_LOG(WARN, "the row column size is less than composite column size,"
                        "row_column_size=%ld, composite_column_size=%ld",
                  row_width, composite_columns.get_array_index());
        ret = OB_ERROR;
      }
      else
      {
        row_width_ = row_width;
      }

      if (OB_SUCCESS == ret)
      {
        ret = do_compose(); 
      }

      return ret;
    }

    int ObComposeOperator::do_compose()
    {
      int ret = OB_SUCCESS;

      if (result_array_->get_cell_size() % row_width_ != 0)
      {
        TBSYS_LOG(WARN, "the result array includes part row, total_cell_size=%ld, "
                        "row_width=%ld", result_array_->get_cell_size(), row_width_);
        ret = OB_ERROR;
      }
      else
      {
        for (int64_t i = result_array_->get_consumed_cell_num(); 
             i < result_array_->get_cell_size() && OB_SUCCESS == ret; 
             i += row_width_)
        {
          ret = compose_one_row(i, i + row_width_ -1);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to caculate composite column for row=%ld", i);
          }
        }
      }

      return ret;
    }

    int ObComposeOperator::compose_one_row(
        const int64_t row_beg, 
        const int64_t row_end)
    {
      int ret = OB_SUCCESS;
      int64_t comp_column_size = composite_columns_->get_array_index();
      int64_t comp_column_beg = row_width_ - comp_column_size;
      
      for (int64_t i = 0; i < comp_column_size && OB_SUCCESS == ret; ++i)
      {
        ObObj& comp_val = (result_array_->operator [](row_beg + comp_column_beg + i)).value_;
        ret = composite_columns_->at(i)->calc_composite_val(
          comp_val, *result_array_, row_beg, row_end);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to caculate composite column, row_beg=%ld, "
                          "row_end=%ld, composite_column_size=%ld, row_width=%ld, "
                          "composite_column_index=%ld", 
                    row_beg, row_end, comp_column_size, row_width_, i);
        }
        else
        {
          TBSYS_LOG(DEBUG, "success to calculate composite val. value is:");
          comp_val.dump();
        }
      }

      return ret;
    }
        
    int ObComposeOperator::next_cell()
    {
      return result_array_->next_cell();
    }

    int ObComposeOperator::get_cell(ObInnerCellInfo** cell)
    {
      return result_array_->get_cell(cell, NULL);
    }

    int ObComposeOperator::get_cell(ObInnerCellInfo** cell, bool* is_row_changed)
    {
      return result_array_->get_cell(cell, is_row_changed);
    }
  } // end namespace common
} // end namespace oceanbase
