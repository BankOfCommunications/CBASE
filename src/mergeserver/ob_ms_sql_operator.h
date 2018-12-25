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
#ifndef MERGESERVER_OB_MS_SQL_OPERATOR_CPP_ 
#define MERGESERVER_OB_MS_SQL_OPERATOR_CPP_
#include "common/ob_new_scanner.h"
#include "common/ob_row_iterator.h"
#include "common/ob_groupby_operator.h"
#include "common/ob_return_operator.h"
#include "common/ob_compose_operator.h"
#include "common/ob_cell_array.h"
#include "sql/ob_sql_scan_param.h"
#include "ob_merger_reverse_operator.h"
#include "ob_ms_sql_sorted_operator.h"
#include "ob_merger_groupby_operator.h"
namespace oceanbase
{
  namespace mergeserver
  {
    class ObMsSqlSortedOperator;
    class ObMergerReverseOperator;
    class ObMsSqlOperator : public common::ObRowIterator
    {
    public:
      ObMsSqlOperator();
      ~ObMsSqlOperator();
      /// initialize
      int set_param(const sql::ObSqlScanParam & scan_param);
      //added by fyd  IN_EXPR  [PrefixKeyQuery_for_INstmt] 2014.4.4
      int set_param_only(const sql::ObSqlScanParam & scan_param);
	  int    store_rowkey_to_sorted_operator(common::ObNewRange  rowkey)
      {
    	  int ret = OB_SUCCESS;
    	  sorted_operator_.used_rowkey_.push_back(rowkey);
    	  return ret;
      };
      //add:e
      /// add a subscanrequest's result
      int add_sharding_result(ObMsRequestResult & sharding_res, const common::ObNewRange & query_range, bool &is_finish);
      /// finish processing result, like orderby grouped result

      int64_t get_mem_size_used()const;

      void reset();

      int64_t get_result_row_width()const ;
      int64_t get_whole_result_row_count()const;

      int64_t get_sharding_result_count() const;
      int64_t get_cur_sharding_result_idx() const;
      
      int get_next_row(common::ObRow &row);
    private:
      enum
      {
        NOT_INIT,
        USE_SORTED_OPERATOR,
      };
      int status_;
      const sql::ObSqlScanParam *scan_param_;
      ObMsSqlSortedOperator    sorted_operator_;
      ObMsRequestResult         *last_sharding_res_;
      int64_t                   sharding_res_count_;
    };
  }
}

#endif /* MERGESERVER_OB_MS_SQL_OPERATOR_CPP_ */
