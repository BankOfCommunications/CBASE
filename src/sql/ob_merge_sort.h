/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_sort.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_MERGE_SORT_H
#define _OB_MERGE_SORT_H 1
#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_array.h"
#include "common/ob_row.h"
#include "ob_run_file.h"
#include "ob_in_memory_sort.h"
#include "ob_sort_helper.h"

namespace oceanbase
{
  namespace sql
  {
    // on-disk merge sort, used by ObSort
    class ObMergeSort: public ObSortHelper
    {
      public:
        ObMergeSort();
        virtual ~ObMergeSort();
        virtual void reset();
        virtual void reuse();
        int set_run_filename(const common::ObString &filename);
        void set_sort_columns(const common::ObArray<ObSortColumn> &sort_columns);
        int dump_run(ObInMemorySort &rows);
        void set_final_run(ObInMemorySort &rows);
        int build_merge_heap();
        /// @pre build_heap()
        virtual int get_next_row(const common::ObRow *&row);
      private:
        // types and constants
        static const int64_t SORT_RUN_FILE_BUCKET_ID = 0;
        struct HeapComparer;
        typedef std::pair<common::ObRow, int64_t> RowRun;
      private:
        // disallow copy
        ObMergeSort(const ObMergeSort &other);
        ObMergeSort& operator=(const ObMergeSort &other);
      private:
        // data members
        char run_filename_buf_[common::OB_MAX_FILE_NAME_LENGTH];
        common::ObString run_filename_;
        ObRunFile run_file_;
        common::ObArray<RowRun> heap_array_;
        ObInMemorySort *final_run_;
        const common::ObArray<ObSortColumn> *sort_columns_;
        bool need_push_heap_;
        int64_t dump_run_count_;
        const common::ObRowDesc *row_desc_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_MERGE_SORT_H */

