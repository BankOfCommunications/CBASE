/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: mock_mem_iterator.h,v 0.1 2010/09/21 11:07:00 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_COMMON_MOCK_MEM_ITERATOR_H__
#define __OCEANBASE_COMMON_MOCK_MEM_ITERATOR_H__

#include "common/ob_read_common_data.h"
#include "common/ob_string_buf.h"

using namespace oceanbase::common;

class MockMemIterator : public ObIterator
{
  public:
    MockMemIterator()
    {
      cell_num_ = 0;
      iter_idx_ = -1;
      err_cell_idx_ = -1;
      err_code_ = 0;
    }
    ~MockMemIterator()
    {
      cell_num_ = 0;
      iter_idx_ = -1;
      err_cell_idx_ = -1;
      err_code_ = 0;
    }

    // reset iter
    void reset()
    {
      iter_idx_ = -1;
      err_cell_idx_ = -1;
      err_code_ = 0;
    }

  public:
    int add_cell(const ObCellInfo& cell)
    {
      int err = OB_SUCCESS;

      if (cell_num_ >= MAX_MOCK_CELL_NUM)
      {
        TBSYS_LOG(WARN, "cell array full, cell_num=%ld", cell_num_);
        err = OB_ERROR;
      }
      else
      {
        cells_[cell_num_].table_id_ = cell.table_id_;
        cells_[cell_num_].column_id_ = cell.column_id_;
        //cells_[cell_num_].op_info_ = cell.op_info_;

        // store row key
        err = str_buf_.write_string(cell.row_key_, &cells_[cell_num_].row_key_);
        if (OB_SUCCESS == err)
        {
          err = str_buf_.write_obj(cell.value_, &cells_[cell_num_].value_);
        }

        if (OB_SUCCESS == err)
        {
          ++cell_num_;
        }
      }

      return err;
    }

    void set_err(const int64_t err_cell_idx, int err_code)
    {
      err_cell_idx_ = err_cell_idx;
      err_code_ = err_code;
    }

  public:
    int next_cell()
    {
      int err = OB_SUCCESS;
      ++iter_idx_;
      if (iter_idx_ >= cell_num_)
      {
        err = OB_ITER_END;
      }

      return err;
    }

    int get_cell(ObCellInfo** cell)
    {
      int err = OB_SUCCESS;
      if (NULL == cell)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (err_cell_idx_ != -1 && iter_idx_ == err_cell_idx_)
      {
        err = err_code_;
      }
      else
      {
        *cell = &cells_[iter_idx_];
      }

      return err;
    }

    int get_cell(ObCellInfo** cell, bool* is_row_changed)
    {
      int err = OB_SUCCESS;
      if (NULL == cell)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (err_cell_idx_ != -1 && iter_idx_ == err_cell_idx_)
      {
        err = err_code_;
      }
      else
      {
        *cell = &cells_[iter_idx_];
        if (NULL != is_row_changed)
        {
          if (0 == iter_idx_ ||
              cells_[iter_idx_].row_key_ != cells_[iter_idx_ - 1].row_key_)
          {
            *is_row_changed = true;
          }
          else
          {
            *is_row_changed = false;
          }
        }
      }

      return err;
    }

  public:
    static const int64_t MAX_MOCK_CELL_NUM = 1024;
  private:
    ObCellInfo cells_[MAX_MOCK_CELL_NUM];
    int64_t cell_num_;
    int64_t iter_idx_;
    ObStringBuf str_buf_;
    int64_t err_cell_idx_;
    int err_code_;
};

#endif //__MOCK_MEM_ITERATOR_H__

