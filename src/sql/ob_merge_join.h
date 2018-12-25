/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_join.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_MERGE_JOIN_H
#define _OB_MERGE_JOIN_H 1

#include "ob_join.h"
#include "common/ob_row.h"
#include "common/ob_array.h"
#include "common/ob_row_store.h"
//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140610 :b
#include "common/hash/ob_hashmap.h"
#include "common/ob_custom_allocator.h"
//add 20140610:e

namespace oceanbase
{
  namespace sql
  {
    // 要求两个输入left_child和right_child在等值join列上排好序
    // 支持所有join类型
    class ObMergeJoin: public ObJoin
    {
      public:
        ObMergeJoin();
        virtual ~ObMergeJoin();
        virtual void reset();
        virtual void reuse();
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const { return PHY_MERGE_JOIN; }
        virtual int set_join_type(const ObJoin::JoinType join_type);
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        
        DECLARE_PHY_OPERATOR_ASSIGN;
      private:
        int normal_get_next_row(const common::ObRow *&row);
        int inner_get_next_row(const common::ObRow *&row);
        int left_outer_get_next_row(const common::ObRow *&row);
        int right_outer_get_next_row(const common::ObRow *&row);
        int full_outer_get_next_row(const common::ObRow *&row);
        int left_semi_get_next_row(const common::ObRow *&row);
        int right_semi_get_next_row(const common::ObRow *&row);
        int left_anti_semi_get_next_row(const common::ObRow *&row);
        int right_anti_semi_get_next_row(const common::ObRow *&row);
        int compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const;
        int left_row_compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const;
        /* add liumz, [unequal_join]20150924
         * TODO, optimize unequal_join, don't use cross join
         */
        int reset_join_type();
        int inner_ge_get_next_row(const common::ObRow *&row);
        int left_outer_ge_get_next_row(const common::ObRow *&row);
        int right_outer_ge_get_next_row(const common::ObRow *&row);
        int full_outer_ge_get_next_row(const common::ObRow *&row);
        //add:e
        //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140611:b
        int right_row_compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const;
        //add [JHOBv0.1] 20140611:e

        int curr_row_is_qualified(bool &is_qualified);
        int cons_row_desc(const ObRowDesc &rd1, const ObRowDesc &rd2);
        int join_rows(const ObRow& r1, const ObRow& r2);
        int left_join_rows(const ObRow& r1);
        int right_join_rows(const ObRow& r2);
        //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140610:b 
        int process_sub_query();
        //add 20140610:e
        // disallow copy
        ObMergeJoin(const ObMergeJoin &other);
        ObMergeJoin& operator=(const ObMergeJoin &other);
      private:
        static const int64_t MAX_SINGLE_ROW_SIZE = common::OB_ROW_MAX_COLUMNS_COUNT*(common::OB_MAX_VARCHAR_LENGTH+4);
        // data members
        typedef int (ObMergeJoin::*get_next_row_func_type)(const common::ObRow *&row);
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

        bool is_unequal_join_;//add liumz, [unequal_join]20150924

        //add dyr [using_AND_in_ONstmt] [JHOBv0.1] 20140610:b
        bool last_left_row_has_printed_;
        bool last_right_row_has_printed_;
        bool left_cache_is_valid_;
        common::ObRowStore left_cache_;
        common::ObRow last_join_right_row_;
        common::ObString last_join_right_row_store_;
        common::ObRow curr_cached_left_row_;
        bool is_left_iter_end_;
        //add 20140610:e
        //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140610:b 
        static const int MAX_SUB_QUERY_NUM = 5;//restrict num of sub_query which use HASHMAP
        static const int64_t HASH_BUCKET_NUM = 100000;
		static const int BIG_RESULTSET_THRESHOLD = 50; //´óÊý¾Ý¼¯ãÐÖµ
        int hashmap_num_ ;
        common::CustomAllocator arena_;
        common::ObArray<common::ObRowkey>  sub_result_;
        common::hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode> sub_query_map_[MAX_SUB_QUERY_NUM];
        //add 20140610:e
        common::ObArray<ObObjType> sub_query_map_and_bloomfilter_column_type[MAX_SUB_QUERY_NUM];  //add peiouya [IN_TYPEBUG_FIX] 20151225
        bool is_subquery_result_contain_null[MAX_SUB_QUERY_NUM];  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_MERGE_JOIN_H */
