/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql_read_strategy.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_SQL_READ_STRATEGY_H
#define _OB_SQL_READ_STRATEGY_H 1

#include "ob_sql_expression.h"
#include "common/ob_schema.h"
#include "common/ob_obj_cast.h"
#include "common/ob_range2.h"
#include "common/ob_vector.h"
//#include <algorithm>
using namespace oceanbase::common;

// added by fyd  IN_EX [PrefixKeyQuery_for_INstmt] 2014.5.26
namespace oceanbase
{
  namespace tests
  {
    namespace sql
    {
      class ObSqlReadStrategyTest;
    }
  }
}
//add:e
namespace oceanbase
{
  namespace common
  {
  // class ObVector< ObArray<ObRowkey> >;
  }
  namespace sql
  {
    /*
     * 通过sql语句的where条件判断使用get还是使用scan
     *
     * exmaple: select * from t where pk1 = 0 and pk2 = 1;
     * exmaple: select * from t where pk1, pk2 in ((0,1))
     *
     */
   // Note:
   // 这个函数其中有关优化的操作 ，仅仅针对AND条件中的表达式，如果是 表达式为 (....) AND (...) AND 形式，则可以优化
   // 如果  (....) OR (...) OR 则直接是全表扫描.
   // 这里和具体的(。。。)内容没有关系
   // fyd  2014.4.10
    class ObSqlReadStrategy
    {
      public:
        ObSqlReadStrategy();
        virtual ~ObSqlReadStrategy();

        void reset();
        //add wanglei [semi join update] 20160523:b
        void reset_for_semi_join();
        //add wanglei [semi join update] 20160523:b
        inline void set_rowkey_info(const common::ObRowkeyInfo &rowkey_info)
        {
          rowkey_info_ = &rowkey_info;
        }
        //add wanglei [semi join update] 20160411:b
        //mod dragon [type_err] 2016-12-6 b
//        void remove_last_inexpr();
        int remove_last_inexpr();
        //mod e
        void remove_last_expr();
        //add wanglei:e
        int add_filter(const ObSqlExpression &expr);
        int get_read_method(ObIArray<ObRowkey> &rowkey_array, PageArena<ObObj,common::ModulePageAllocator> &objs_allocator, int32_t &read_method);
        void destroy();

        int find_rowkeys_from_equal_expr(bool real_val, ObIArray<ObRowkey> &rowkey_array, PageArena<ObObj,common::ModulePageAllocator> &objs_allocator);
        int find_rowkeys_from_in_expr(bool real_val, ObIArray<ObRowkey> &rowkey_array, common::PageArena<ObObj,common::ModulePageAllocator> &objs_allocator);
        int find_scan_range(ObNewRange &range, bool &found, bool single_row_only);
        // added by fyd  IN_EX [PrefixKeyQuery_for_INstmt] 2014.3.25
        // find all the ranges,and fetch the range one by one
        int find_scan_range(bool &found, bool single_row_only);
        bool has_next_range();
        int get_next_scan_range(ObNewRange &range,bool &has_next);// must called  after find_scan_range
        friend class ObSqlReadStrategyTest;
        friend class TestSqlStrategy;
        //add:end
        int assign(const ObSqlReadStrategy *other, ObPhyOperator *owner_op = NULL);
        int64_t to_string(char* buf, const int64_t buf_len) const;


        // added by fyd  IN_EX [PrefixKeyQuery_for_INstmt] 2014.3.25
        // 当完成scan或是GET 后，应该调用此函数，释放申请的空间
        // it should not to realse the mem?
        int release_rowkey_objs();
        // the ob_malloc and ob _free may have some problems, but it works
        // 将所有内存开辟工作集中到一块儿？ TODO
        //TODO
      int malloc_rowkey_objs_space( int64_t num);
      int print_ranges() const;
      typedef struct Rowkey_Objs
      {
          common::ObObj row_key_objs_[OB_MAX_ROWKEY_COLUMN_NUMBER];

      }RowKey_Objs,*pRowKey_Objs;
      struct Comparer ;
      public :///test
      int sort_mutiple_range() ;
      int merge_and_sort_ranges() const;
      int compare_range(const sql::ObSqlReadStrategy::RowKey_Objs objs1,const sql::ObSqlReadStrategy::RowKey_Objs objs2) const;
      int eraseDuplicate(pRowKey_Objs tmp_start_rowkeys, pRowKey_Objs tmp_end_rowkeys, bool forward, int64_t position) ;
      //add:end
		void reset_stuff();//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140911 /*Exp:reset*/
        void clear_cursor_where_condition();//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160314
      public:
        static const int32_t USE_METHOD_UNKNOWN = 0;
        static const int32_t USE_SCAN = 1;
        static const int32_t USE_GET = 2;

      private:
        static const int64_t COMMON_FILTER_NUM = 8;
        //static const int64_t COMMON_ROWKEY_RANGE_NUM = 64;//  最大的主键range 个数
        ObSEArray<ObSqlExpression, COMMON_FILTER_NUM,common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > simple_in_filter_list_;
        ObSEArray<ObSqlExpression, COMMON_FILTER_NUM,common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > simple_cond_filter_list_;
        const common::ObRowkeyInfo *rowkey_info_;
        //mod fyd IN_EX [PrefixKeyQuery_for_INstmt]  2014.4.25  以下四个变量已经计划弃用--b
        common::ObObj start_key_objs_[OB_MAX_ROWKEY_COLUMN_NUMBER];
        common::ObObj end_key_objs_[OB_MAX_ROWKEY_COLUMN_NUMBER];
        char* start_key_mem_hold_[OB_MAX_ROWKEY_COLUMN_NUMBER];
        char* end_key_mem_hold_[OB_MAX_ROWKEY_COLUMN_NUMBER];
        //mod:e
        // added  by fyd IN_EX [PrefixKeyQuery_for_INstmt]  2014.3.25
        // the default value is 0 ,it means we do not malloc any space ;
        //if the value  > = 1,it means there are  more than  one ranges
        int64_t range_count_ ;
		//add peioy IN_EX [PrefixKeyQuery_for_INstmt]  20140930:b
		//range_count_cons_保存最初申请内存大小(range_count_)，用以最终释放开辟的内存
		//因为切分的多个range，可能执行merge，即将相同range合并。合并操作会将range_count_减小。
		//因此如果用range_count_会致使内存无法完全释放，致使泄露。
		int64_t range_count_cons_ ;
		//add 20140930:e
        //it is used as the index of ranges when fecth the range from rpc .the is inited as -1;
        int64_t idx_key_  ;
       // these address are used to store the mutiple ranges
        common::ObObj * mutiple_start_key_objs_;
        common::ObObj *mutiple_end_key_objs_ ;
        char**mutiple_start_key_mem_hold_ ;
        char**mutiple_end_key_mem_hold_ ;
        //add:end
        int64_t in_sub_query_idx_;//add dyr [Insert_Subquery_Function] [test] 20151029
      public://test
        int find_single_column_range(bool real_val, int64_t idx, uint64_t column_id, bool &found);
        int find_closed_column_range(bool real_val, int64_t idx, uint64_t column_id, bool &found_start, bool &found_end, bool single_row_only);
        // added  by fyd  IN_EX [PrefixKeyQuery_for_INstmt] 2014.3.24
        // resolve simple expr ,like equal,le,et; only one range's one column  once
        int find_closed_column_range_simple_con(bool real_val, const ObRowkeyColumn *column,int64_t column_idx, int64_t idx_of_ranges, int64_t cond_op, ObObj cond_val, bool &found_start, bool &found_end, bool single_row_only);
        // resolve  btw ; only one range's one column  each
        int find_closed_column_range_simple_btw(bool real_val, const ObRowkeyColumn *column, int64_t column_idx, int64_t idx_of_ranges, /*int64_t cond_op, ObObj cond_val,*/ ObObj cond_start, ObObj cond_end,  bool &found_start, bool &found_end, bool single_row_only);
        // resolve in expr ;each time ,it handles the  spicified column of one range
        int  resolve_close_column_range_in(bool real_val, const ObRowkeyColumn *column, ObObj cond_val,int64_t column_idx, int64_t idx_of_ranges, bool &found_start, bool &found_end, bool single_row_only);
        int find_closed_column_range_ex(bool real_val, int64_t idx, uint64_t column_id, bool &found_start, bool &found_end, bool single_row_only/*,bool ignore_in_expr*/);
        //add: end
    };
  }
}

#endif /* _OB_SQL_READ_STRATEGY_H */
