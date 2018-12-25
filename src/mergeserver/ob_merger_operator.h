/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_merger_operator.cpp for 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef MERGESERVER_OB_MERGER_OPERATOR_CPP_ 
#define MERGESERVER_OB_MERGER_OPERATOR_CPP_
#include "common/ob_iterator.h"
#include "common/ob_groupby_operator.h"
#include "common/ob_return_operator.h"
#include "common/ob_compose_operator.h"
#include "common/ob_cell_array.h"
#include "ob_merger_reverse_operator.h"
#include "ob_merger_sorted_operator.h"
#include "ob_merger_groupby_operator.h"
namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerSortedOperator;
    class ObMergerReverseOperator;
    class ObMergerOperator : public common::ObInnerIterator
    {
    public:
      ObMergerOperator();
      ~ObMergerOperator();
      /// initialize
      int set_param(const int64_t max_memory_size, const common::ObScanParam & scan_param);
      /// add a subscanrequest's result
      int add_sharding_result(common::ObScanner & sharding_res, const common::ObNewRange & query_range, 
          const int64_t limit_offset, bool &is_finish, bool &can_free_res);
      /// finish processing result, like orderby grouped result
      int seal();
      /// set max sharding source size -1 for not restrict
      void set_max_res_size(const int64_t size);
      int64_t get_max_res_size(void) const;
      /// get max memory size used
      int get_mem_size_used() const;
      void reset();

      int64_t get_result_row_width()const ;
      int64_t get_whole_result_row_count()const;
    public:
      virtual int next_cell();
      virtual int get_cell(common::ObInnerCellInfo** cell);
      virtual int get_cell(common::ObInnerCellInfo** cell, bool* is_row_changed);
    private:
      enum
      {
        NOT_INIT,
        USE_SORTED_OPERATOR,
        USE_REVERSE_OPERATOR,
        USE_GROUPBY_OPERATOR
      };
      int status_;
      bool sealed_;
      common::ObCellArray cells_;
      const common::ObScanParam *scan_param_;
      common::ObReturnOperator  return_operator_;
      ObMergerSortedOperator    sorted_operator_;
      ObMergerReverseOperator   reversed_operator_; 
      ObMergerGroupByOperator   groupby_operator_;
      common::ObInnerIterator   *res_iterator_;
      common::ObScanner         *last_sharding_res_;
      int64_t                   sharding_res_count_;
      int64_t                   max_sharding_res_size_;
      int64_t                   total_sharding_res_size_;
    };
  }
}

#endif /* MERGESERVER_OB_MERGER_OPERATOR_CPP_ */
