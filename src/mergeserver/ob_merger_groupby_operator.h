/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_merger_groupby_operator.h for 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef MERGESERVER_OB_MERGER_GROUPBY_OPERATOR_H_ 
#define MERGESERVER_OB_MERGER_GROUPBY_OPERATOR_H_
#include "common/ob_groupby_operator.h"
#include "common/ob_compose_operator.h"
#include "common/ob_cell_array.h"
#include "common/ob_composite_column.h"
#include "common/ob_array_helper.h"
#include "common/ob_scanner.h"
#include "common/ob_range2.h"
namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerGroupByOperator :public common::ObInnerIterator
    {
    public:
      ObMergerGroupByOperator();
      ~ObMergerGroupByOperator();

      int set_param(const int64_t max_memory_size, const common::ObScanParam & param);
      int add_sharding_result(common::ObScanner & sharding_res, 
        const common::ObNewRange & query_range, const int64_t limit_offset, bool &is_finish);

      int seal();

      int get_mem_size_used()const;
      void reset();   


      int64_t get_result_row_width()const;

      int64_t get_whole_result_row_count()const;

    public:
      virtual int next_cell();
      virtual int get_cell(common::ObInnerCellInfo** cell);
      virtual int get_cell(common::ObInnerCellInfo** cell, bool* is_row_changed);

    private:
      int having_condition_filter(common::ObGroupByOperator& result_array,
        const int64_t row_width, const common::ObGroupByParam &param);

      int compose(common::ObCellArray& result_array, 
        const common::ObArrayHelper<common::ObCompositeColumn>& composite_columns, 
        const int64_t row_width);

      const common::ObScanParam    *scan_param_;
      common::ObGroupByOperator    operator_;
      common::ObComposeOperator    compose_operator_;
      common::ObCellArray::OrderDesc  orderby_desc_[common::OB_MAX_COLUMN_NUMBER*4];

      int64_t cs_whole_row_count_;

      bool    sealed_;
      common::ObCellArray row_cells_;
    };
  }
}   
#endif /* MERGESERVER_OB_MERGER_GROUPBY_OPERATOR_H_ */
