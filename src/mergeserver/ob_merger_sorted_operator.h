/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_merger_sorted_operator.h for
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef MERGESERVER_OB_MERGER_SORTED_OPERATOR_H_
#define MERGESERVER_OB_MERGER_SORTED_OPERATOR_H_
#include "common/ob_iterator.h"
#include "ob_ms_request.h"

namespace oceanbase
{
  namespace common
  {
    class ObScanParam;
    class ObScanner;
    class ObNewRange;
    class ObCellArray;
    class ObString;
  }
  namespace mergeserver
  {
    class ObMergerSortedOperator : public common::ObInnerIterator
    {
    public:
      ObMergerSortedOperator();
      ~ObMergerSortedOperator();

      /// initialize
      int set_param(const common::ObScanParam & scan_param);
      /// add a subscanrequest's result
      int add_sharding_result(common::ObScanner & sharding_res, const common::ObNewRange & query_range, bool &is_finish);
      /// finish processing result, like orderby grouped result
      int seal();

      int64_t get_mem_size_used()const
      {
        return 0;
      }

    public:
      virtual int next_cell();
      virtual int get_cell(common::ObInnerCellInfo** cell);
      virtual int get_cell(common::ObInnerCellInfo** cell, bool* is_row_changed);
      void reset();
    private:
      static const int64_t FULL_SCANNER_RESERVED_BYTE_COUNT  = 200;
      void sort(bool &is_finish, common::ObScanner * last_sharding_res = NULL);
      struct sharding_result_t
      {
        common::ObScanner *sharding_res_;
        const common::ObNewRange   *sharding_query_range_;
        const common::ObScanParam *param_;
        int64_t                   fullfilled_item_num_;
        common::ObRowkey last_row_key_;

        void init(common::ObScanner & sharding_res, const common::ObNewRange & query_range, const common::ObScanParam &param,
          common::ObRowkey & last_proces_rowkey, const int64_t fullfilled_item_num);
        bool operator<(const sharding_result_t & other)const;
      };
      static const int64_t MAX_SHARDING_RESULT_COUNT = mergeserver::ObMergerRequest::MAX_SUBREQUEST_NUM;
      sharding_result_t sharding_result_arr_[MAX_SHARDING_RESULT_COUNT];
      int64_t           sharding_result_count_;
      int64_t           seamless_result_count_;
      int64_t           cur_sharding_result_idx_;
      const common::ObScanParam    *scan_param_;
      common::ObNewRange              scan_range_;
    };
  }
}

#endif /* MERGESERVER_OB_MERGER_SORTED_OPERATOR_H_ */
