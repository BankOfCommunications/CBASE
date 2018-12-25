/**
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_semi_join.h
 *
 * Authors:
 *   LEI WANG <seilwang@163.com>
 *
 */
#ifndef _OB_SEMI_JOIN_H
#define _OB_SEMI_JOIN_H 1

#include "ob_join.h"
#include "common/ob_row.h"
#include "common/ob_array.h"
#include "common/ob_row_store.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_custom_allocator.h"
#include "ob_in_memory_sort.h"
#include "ob_merge_sort.h"
#include "ob_filter.h"
#include "ob_stmt.h"
//add wanglei:b
namespace oceanbase
{
  namespace sql
  {
    typedef hash::ObHashMap<ObString,bool,hash::NoPthreadDefendMode> distinct_hash;
    typedef  ObArray<distinct_hash*> item_hash_array;
    class ObSemiJoin: public ObJoin
    {
    public:
      ObSemiJoin();
      virtual ~ObSemiJoin();
      virtual void reset();
      virtual void reuse();
      virtual int open();
      int semi_join_open();//open left
      int semi_join_open_right();//open right
      virtual int close();
      virtual ObPhyOperatorType get_type() const { return PHY_SEMI_JOIN; }
      virtual int set_join_type(const ObJoin::JoinType join_type);
      virtual int get_next_row(const common::ObRow *&row);
      virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
      virtual int64_t to_string(char* buf, const int64_t buf_len) const;
      void set_is_can_use_semi_join(const bool & is_can_use_semi_join);
      bool is_in_hash_map(int & curr_buket, ObString &str, item_hash_array &is_confilct);
      bool is_use_second_index();
      //add dragon [Bugfix 1224] 2016-8-25 09:20:02
      /**
       * @brief set_alias_table
       *  判断给定tid的表是不是设置了别名，同时设置成员变量is_alias_table_
       * @param tid
       * @param stmt
       * @param aliasT 输出值 如不需要返回，则缺省
       * @return
       */
      int set_alias_table(uint64_t tid, oceanbase::sql::ObSelectStmt *&stmt, bool alias = false);
      bool get_aliasT() const;
      //add e
      //add dragon [Bugfix 1224] 2016-8-30 11:58:01
      bool is_use_second_index_storing();
      bool is_use_second_index_without_storing();
      int set_is_use_second_index_storing(const bool &is_use_index_storing);
      int set_is_use_second_index_without_storing(const bool &is_use_index_without_storing);
      //add 2016-8-30 11:58:06e
      int set_is_use_second_index(const bool &is_use_index);
      int set_index_table_id(uint64_t &first_index_table_id,uint64_t &second_index_table_id);
      int add_right_table_filter(ObSqlExpression* ose);
      void set_right_table_filter(ObFilter *r_filter);
      void set_id(int id);
      void set_use_in(bool value);
      void set_use_btw(bool value);
      DECLARE_PHY_OPERATOR_ASSIGN;
    private:
      //public : //for UT
      int inner_semi_get_next_row(const common::ObRow *&row);
      int inner_semi_hash_get_next_row(const common::ObRow *&row);
      int left_semi_get_next_row(const common::ObRow *&row);
      int right_semi_get_next_row(const common::ObRow *&row);
      int left_anti_semi_get_next_row(const common::ObRow *&row);
      int right_anti_semi_get_next_row(const common::ObRow *&row);
      int compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const;
      int left_row_compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const;
      int reset_join_type();
      int inner_ge_get_next_row(const common::ObRow *&row);
      int left_outer_ge_get_next_row(const common::ObRow *&row);
      int right_outer_ge_get_next_row(const common::ObRow *&row);
      int full_outer_ge_get_next_row(const common::ObRow *&row);
      int right_row_compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const;
      int curr_row_is_qualified(bool &is_qualified);
      /**
       * @brief cons_row_desc
       * 构造的结果实际是semi join的成员变量row_desc_
       * @param rd1
       * @param rd2
       * @return
       */
      int cons_row_desc(const ObRowDesc &rd1, const ObRowDesc &rd2);
      int join_rows(const ObRow& r1, const ObRow& r2);
      int left_join_rows(const ObRow& r1);
      int right_join_rows(const ObRow& r2);
      int process_sub_query();
      int get_data_for_distinct(const common::ObObj *cell,
                                ObObjType ll_type,
                                char *buff,
                                int buff_size,
                                int& fliter_data_type);
      int get_tid_cid_for_cons_filter(ObArray<int64_t> &left_join_tid,
                                      ObArray<int64_t> &left_join_cid,
                                      ObArray<int64_t> &right_join_tid,
                                      ObArray<int64_t> &right_join_cid, const int flag);
      int cons_filter_for_right_table(ObSqlExpression *&table_filter_expr_,
                                      ObSqlExpression *&src_table_filter_expr_,
                                      int &index_param_count,
                                      ObArray<int64_t> &left_join_tid,
                                      ObArray<int64_t> &left_join_cid,
                                      ObArray<int64_t> &right_join_tid,
                                      ObArray<int64_t> &right_join_cid);
      int cons_filter_for_left_table(ObSqlExpression *&table_filter_expr_,
                                     int &index_param_count,
                                     ObArray<int64_t> &left_join_tid,
                                     ObArray<int64_t> &left_join_cid,
                                     ObArray<int64_t> &right_join_tid,
                                     ObArray<int64_t> &right_join_cid);
      int add_filter_to_rpc_scan(int &index_param_count,
                                 ObSqlExpression *&table_filter_expr,
                                 ObSqlExpression *&src_table_filter_expr,
                                 const int flag);

      int add_sort_column(const uint64_t tid, const uint64_t cid, bool is_ascending_order);
      void set_mem_size_limit(const int64_t limit);
      int set_run_filename(const common::ObString &filename);
      int store_row_in_merge_sort(const common::ObRow *&input_row, bool &need_merge);
      inline bool need_dump() const
      {
        return mem_size_limit_ <= 0 ? false : (in_mem_sort_.get_used_mem_size() >= mem_size_limit_);
      }
      ObSemiJoin(const ObSemiJoin &other);
      ObSemiJoin& operator=(const ObSemiJoin &other);
    private:
      static const int64_t DEFAULT_MEMORY_LIMIT = 20*1024*1024LL;
      static const int64_t COMMON_FILTER_NUM = 8;
      static const int64_t MAX_SINGLE_ROW_SIZE = common::OB_ROW_MAX_COLUMNS_COUNT*(common::OB_MAX_VARCHAR_LENGTH+4);
      typedef int (ObSemiJoin::*get_next_row_func_type)(const common::ObRow *&row);
      get_next_row_func_type get_next_row_func_;
      const common::ObRow *last_left_row_;
      const common::ObRow *last_right_row_;
      common::ObRow last_join_left_row_;
      common::ObString last_join_left_row_store_;
      common::ObRowStore right_cache_;
      common::ObRow curr_cached_right_row_;
      common::ObRow curr_row_;
      common::ObRowDesc row_desc_;
      bool right_cache_is_valid_;
      bool is_right_iter_end_;
      bool is_unequal_join_;
      bool last_left_row_has_printed_;
      bool last_right_row_has_printed_;
      bool left_cache_is_valid_;
      common::ObRowStore left_cache_;
      common::ObRow last_join_right_row_;
      common::ObString last_join_right_row_store_;
      common::ObRow curr_cached_left_row_;
      bool is_left_iter_end_;
      static const int MAX_SUB_QUERY_NUM = 5;
      static const int64_t HASH_BUCKET_NUM = 100000;
      static const int BIG_RESULTSET_THRESHOLD = 50;
      int hashmap_num_ ;
      common::CustomAllocator arena_;
      common::ObArray<common::ObRowkey>  sub_result_;
      common::hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode> sub_query_map_[MAX_SUB_QUERY_NUM];
      common::ObRow cached_left_row_;
      common::ObRowStore left_cache_for_store_;
      common::ObRow cached_right_row_;
      common::ObRowStore right_cache_for_store_;
      int TwoMpackect_limit;
      bool is_use_semi_join_;
      bool is_can_use_semi_join_;
      common::ObArray<ObSortColumn> sort_columns_;
      int64_t mem_size_limit_;
      ObInMemorySort in_mem_sort_;
      ObMergeSort merge_sort_;
      ObSortHelper *sort_reader_;
      bool is_use_index_;
      uint64_t first_index_table_id_;
      uint64_t second_index_table_id_;
      ObSqlExpression *table_filter_expr_ ;
      ObSqlExpression *src_table_filter_expr_;
      //wanglei [semi join update] 20160406:b
      const ObRowDesc *left_row_desc_;
      ObFilter *right_table_filter_;
      //wanglei :e
      int id_;//add wanglei [semi join on expr] 20160511
      bool use_in_expr_;
      bool use_btw_expr_;

      //add dragon [Bugfix 1224] 2016-8-25 09:47:44
    private:
      bool is_alias_table_; //对应修改构造函数,close(),reset(),reuse()
      //add e
      //add dragon [Bugfix 1224] 2016-8-30 11:55:13
      bool is_use_second_index_storing_; //使用不回表的二级索引查询
      bool is_use_second_index_without_storing_; //使用回表的二级索引查询
      //add 2016-8-30 11:55:35e
    };
  } // end namespace sql
} // end namespace oceanbase
//add:e
#endif /* _OB_SEMI_JOIN_H */
