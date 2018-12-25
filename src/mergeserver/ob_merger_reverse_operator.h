/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_merger_reverse_operator.h for 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef MERGESERVER_OB_MERGER_REVERSE_OPERATOR_H_ 
#define MERGESERVER_OB_MERGER_REVERSE_OPERATOR_H_
#include "ob_merger_sorted_operator.h"
#include "common/ob_iterator.h"
#include "common/ob_return_operator.h"
namespace oceanbase
{
  namespace common
  {
    class ObCellArray;
  }
  namespace mergeserver
  {
    class ObMergerReverseOperator : public common::ObInnerIterator
    {
    public:
      ObMergerReverseOperator();
      ~ObMergerReverseOperator();
      /// initialize
      int set_param(const common::ObScanParam & scan_param, common::ObCellArray & cells);
      /// add a subscanrequest's result
      int add_sharding_result(common::ObScanner & sharding_res, const common::ObNewRange & query_range, bool &is_finish);
      /// finish processing result, like orderby grouped result
      int seal();

      int64_t get_mem_size_used()const;

    public:
      virtual int next_cell();
      virtual int get_cell(common::ObInnerCellInfo** cell);
      virtual int get_cell(common::ObInnerCellInfo** cell, bool* is_row_changed);

      void reset()
      {
        sorted_operator_.reset();
        reversed_cells_ = NULL;
        scan_param_ = NULL;
      }

    private:
      ObMergerSortedOperator          sorted_operator_;
      common::ObCellArray             *reversed_cells_;
      const common::ObScanParam       *scan_param_;
    };
  }
}  
#endif /* MERGESERVER_OB_MERGER_REVERSE_OPERATOR_H_ */
