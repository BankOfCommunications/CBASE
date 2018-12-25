/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_merger.h,v 0.1 2010/09/19 18:01:06 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_COMMON_OB_MERGER_H__
#define __OCEANBASE_COMMON_OB_MERGER_H__

#include "ob_define.h"
#include "ob_iterator.h"

namespace oceanbase
{
  namespace common
  {
    // ObMerger is used to merge multi iterator and get cell info row by row.
    class ObMerger : public ObIterator
    {
      public:
        ObMerger(bool is_asc = true);
        virtual ~ObMerger();
        void reset();

      public:
        // add iterator
        //mod zhaoqiong [Truncate Table]:20170519
//        int add_iterator(ObIterator* iterator);
        int add_iterator(ObIterator* iterator, bool trun_status = false);
        //mod:e
        void set_asc(bool is_asc)
        {
          is_asc_ = is_asc;
        }

      public:
        int next_cell();
        int get_cell(ObCellInfo** cell, bool* is_row_changed);
        int get_cell(ObCellInfo** cell)
        {
          return this->get_cell(cell, NULL);
        };

      private:
        int find_next_row_();
        int next_cell_(const int64_t iter_idx);
        int get_cell_(const int64_t iter_idx, ObCellInfo** cell, bool* is_row_changed);
        int pop_current_row_(const int64_t iter_idx);
        int cmp_row_key_(const ObCellInfo* first_cell, const ObCellInfo* second_cell, int& cmp);
        bool is_del_row_(const ObCellInfo& cell_info);

      private:
        enum IterStatus
        {
          ITER_BEFORE_NEXT = 0,  // before next_cell called
          ITER_AFTER_NEXT,       // after next_call called
          ITER_END,
        };

      private:
        static const int64_t MAX_ITER_NUM = OB_UPS_MAX_MINOR_VERSION_NUM;
        ObIterator* iters_[MAX_ITER_NUM];
        IterStatus iter_status_[MAX_ITER_NUM];
        int64_t iter_num_;
        bool is_asc_;

        bool iter_row_changed_;
        int64_t trun_iter_idx_; //add zhaoqiong [Truncate Table]:20170519
        ObCellInfo* cur_iter_cell_;
        int64_t cur_iter_arr_[MAX_ITER_NUM];
        int64_t cur_iter_arr_idx_;
        int64_t cur_iter_arr_num_;
        ObCellInfo rdel_cell_; //add zhaoqiong [Truncate Table]:20170519
    };
  }
}

#endif //__OB_MERGER_H__

