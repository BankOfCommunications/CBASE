/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_sort.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_merge_sort.h"
#include "common/ob_compact_cell_iterator.h"
#include <algorithm>
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObMergeSort::ObMergeSort()
  :final_run_(NULL), sort_columns_(NULL),
   need_push_heap_(false),
   dump_run_count_(0), row_desc_(NULL)
{
  run_filename_buf_[0] = '\0';
}

ObMergeSort::~ObMergeSort()
{
}

void ObMergeSort::reset()
{
  int ret = OB_SUCCESS;
  if (run_file_.is_opened())
  {
    if (OB_SUCCESS != (ret = run_file_.close()))
    {
      TBSYS_LOG(WARN, "failed to close run file, err=%d", ret);
    }
  }
  if ('\0' != run_filename_buf_[0])
  {
    struct stat stat_buf;
    if (0 == stat(run_filename_buf_, &stat_buf))
    {
      if (0 != unlink(run_filename_buf_))
      {
        TBSYS_LOG(WARN, "failed to remove tmp run file, err=%s", strerror(errno));
      }
    }
  }
  heap_array_.clear();
  dump_run_count_ = 0;
  row_desc_ = NULL;
}

void ObMergeSort::reuse()
{
  int ret = OB_SUCCESS;
  if (run_file_.is_opened())
  {
    if (OB_SUCCESS != (ret = run_file_.close()))
    {
      TBSYS_LOG(WARN, "failed to close run file, err=%d", ret);
    }
  }
  struct stat stat_buf;
  if (0 == stat(run_filename_buf_, &stat_buf))
  {
    if (0 != unlink(run_filename_buf_))
    {
      TBSYS_LOG(WARN, "failed to remove tmp run file, err=%s", strerror(errno));
    }
  }
  heap_array_.clear();
  dump_run_count_ = 0;
  row_desc_ = NULL;
}

void ObMergeSort::set_sort_columns(const common::ObArray<ObSortColumn> &sort_columns)
{
  sort_columns_ = &sort_columns;
}

int ObMergeSort::set_run_filename(const common::ObString &filename)
{
  int ret = OB_SUCCESS;
  if (filename.length() >= OB_MAX_FILE_NAME_LENGTH)
  {
    TBSYS_LOG(ERROR, "filename is too long, filename=%.*s", filename.length(), filename.ptr());
    ret = OB_BUF_NOT_ENOUGH;
  }
  else
  {
    snprintf(run_filename_buf_, OB_MAX_FILE_NAME_LENGTH, "%.*s", filename.length(), filename.ptr());
    run_filename_.assign_ptr(run_filename_buf_, filename.length());
  }
  return ret;
}

int ObMergeSort::dump_run(ObInMemorySort &rows)
{
  int ret = OB_SUCCESS;
  if (!run_file_.is_opened())
  {
    if (OB_SUCCESS != (ret = run_file_.open(run_filename_)))
    {
      TBSYS_LOG(WARN, "failed to open run file, err=%d filename=%.*s",
                ret, run_filename_.length(), run_filename_.ptr());
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = run_file_.begin_append_run(SORT_RUN_FILE_BUCKET_ID)))
    {
      TBSYS_LOG(WARN, "failed to begin dump run, err=%d", ret);
    }
    else
    {
      ObString compact_row;
      while (OB_SUCCESS == rows.get_next_compact_row(compact_row))
      {
        if (OB_SUCCESS != (ret = run_file_.append_row(compact_row)))
        {
          TBSYS_LOG(WARN, "failed to append row, err=%d", ret);
          break;
        }
      }
      if (OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = run_file_.end_append_run()))
        {
          TBSYS_LOG(WARN, "failed to end dump run, err=%d", ret);
        }
        else
        {
          if (NULL == row_desc_)
          {
            row_desc_ = rows.get_row_desc();
          }
          TBSYS_LOG(INFO, "dump run, row_count=%ld", rows.get_row_count());
        }
      }
    }
  }
  return ret;
}

void ObMergeSort::set_final_run(ObInMemorySort &rows)
{
  final_run_ = &rows;
}

struct ObMergeSort::HeapComparer
{
  HeapComparer(const common::ObArray<ObSortColumn> &sort_columns)
    :sort_columns_(sort_columns)
  {
  }
  bool operator()(const RowRun &r1, const RowRun &r2) const
  {
    bool ret = false;
    const ObObj *cell1 = NULL;
    const ObObj *cell2 = NULL;
    for (int32_t i = 0; i < sort_columns_.count(); ++i)
    {
      if (OB_SUCCESS != (ret = r1.first.get_cell(sort_columns_.at(i).table_id_,
                                                 sort_columns_.at(i).column_id_, cell1)))
      {
        TBSYS_LOG(ERROR, "failed to get cell, err=%d", ret);
        break;
      }
      else if (OB_SUCCESS != (ret = r2.first.get_cell(sort_columns_.at(i).table_id_,
                                                      sort_columns_.at(i).column_id_, cell2)))
      {
        TBSYS_LOG(ERROR, "failed to get cell, err=%d", ret);
      }
      else
      {
        if (*cell1 < *cell2)
        {
          ret = !sort_columns_.at(i).is_ascending_;
          break;
        }
        else if (*cell1 > *cell2)
        {
          ret = sort_columns_.at(i).is_ascending_;
          break;
        }
      }
    } // end for
    return ret;
  }
  private:
    const common::ObArray<ObSortColumn> &sort_columns_;
};

int ObMergeSort::build_merge_heap()
{
  int ret = OB_SUCCESS;
  common::ObRow row;
  OB_ASSERT(row_desc_);
  row.set_row_desc(*row_desc_);
  if (OB_SUCCESS != (ret = run_file_.begin_read_bucket(SORT_RUN_FILE_BUCKET_ID, dump_run_count_)))
  {
    TBSYS_LOG(WARN, "failed to begin to read backet, err=%d", ret);
  }
  else
  {
    heap_array_.clear();
    heap_array_.reserve(static_cast<int32_t>(dump_run_count_+1));
    for (int64_t i = 0; i < dump_run_count_; ++i)
    {
      if (OB_SUCCESS != (ret = run_file_.get_next_row(i, row)))
      {
        TBSYS_LOG(WARN, "failed to read next row, err=%d i=%ld", ret, i);
        break;
      }
      else if (OB_SUCCESS != (ret = heap_array_.push_back(std::make_pair(row, i))))
      {
        TBSYS_LOG(WARN, "failed to push back to array, err=%d", ret);
        break;
      }
    } // end for
  }

  if (OB_SUCCESS == ret)
  {
    if (NULL != final_run_ && 0 < final_run_->get_row_count())
    {
      if (OB_SUCCESS != (ret = final_run_->get_next_row(row)))
      {
        TBSYS_LOG(WARN, "failed th get the first row, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = heap_array_.push_back(std::make_pair(row, dump_run_count_))))
      {
        TBSYS_LOG(WARN, "failed to push back to array, err=%d", ret);
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    OB_ASSERT(2 <= heap_array_.count());
    OB_ASSERT(sort_columns_);
    TBSYS_LOG(INFO, "build merge heap, size=%ld sort_columns_count=%ld",
              heap_array_.count(), sort_columns_->count());
    need_push_heap_ = false;
    RowRun *first_row = &heap_array_.at(0);
    std::make_heap(first_row, first_row+heap_array_.count(), HeapComparer(*sort_columns_));
  }
  return ret;
}


int ObMergeSort::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (0 >= heap_array_.count())
  {
    if (OB_SUCCESS != (ret = run_file_.end_read_bucket()))
    {
      TBSYS_LOG(WARN, "failed to end read backet, err=%d", ret);
    }
    ret = OB_ITER_END;
    TBSYS_LOG(INFO, "end of merge sort");
  }
  else if (need_push_heap_)
  {
    RowRun *first_row = &heap_array_.at(0);
    RowRun *last_pop_row = first_row+heap_array_.count()-1;
    int64_t run_idx = last_pop_row->second;
    // get the next row from the run_idx run
    if (run_idx < dump_run_count_)
    {
      if (OB_SUCCESS != (ret = run_file_.get_next_row(run_idx, last_pop_row->first)))
      {
        if (OB_ITER_END == ret)
        {
          heap_array_.pop_back();
          TBSYS_LOG(INFO, "end of dumped run, run_idx=%ld run_count=%ld", run_idx, heap_array_.count());
        }
        else
        {
          TBSYS_LOG(WARN, "failed to read next row, err=%d i=%ld", ret, run_idx);
        }
      }
    }
    else
    {
      // the last in-memory run
      if (OB_SUCCESS != (ret = final_run_->get_next_row(last_pop_row->first)))
      {
        if (OB_ITER_END == ret)
        {
          TBSYS_LOG(INFO, "end of the in-memory run, run_idx=%ld run_count=%ld", run_idx, heap_array_.count());
          heap_array_.pop_back();
        }
        else
        {
          TBSYS_LOG(WARN, "failed to get next row, err=%d i=%ld", ret, run_idx);
        }
      }
    }
    // push heap
    if (OB_SUCCESS == ret)
    {
      std::push_heap(first_row, first_row+heap_array_.count(), HeapComparer(*sort_columns_));
    }
    else if (OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCCESS == ret && 0 >= heap_array_.count())
  {
    if (OB_SUCCESS != (ret = run_file_.end_read_bucket()))
    {
      TBSYS_LOG(WARN, "failed to end read backet, err=%d", ret);
    }
    ret = OB_ITER_END;
    TBSYS_LOG(INFO, "end of merge sort");
  }

  if (OB_SUCCESS == ret)
  {
    RowRun *first_row = &heap_array_.at(0);
    OB_ASSERT(std::__is_heap(first_row, first_row+heap_array_.count(), HeapComparer(*sort_columns_)));
    std::pop_heap(first_row, first_row+heap_array_.count(), HeapComparer(*sort_columns_));
    RowRun *next_row = first_row+heap_array_.count()-1;
    row = &next_row->first;
    need_push_heap_ = true;
  }
  return ret;
}
