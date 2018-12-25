/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_filter.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_FILTER_H
#define _OB_FILTER_H 1
#include "ob_single_child_phy_operator.h"
#include "ob_sql_expression.h"
#include "common/dlist.h"
//add liumengzhan_delete_index
#include "ob_multiple_get_merge.h"
//add:e
//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20151016:b
#include "common/hash/ob_hashmap.h"
#include "common/ob_custom_allocator.h"
//add 20151016:e

namespace oceanbase
{
  namespace sql
  {
    class ObFilter: public ObSingleChildPhyOperator
    {
      public:
        ObFilter();
        virtual ~ObFilter();
        virtual void reset();
        virtual void reuse();
        /**
         * 添加一个filter
         * 多个filter之间为AND关系
         * @param expr [in] 过滤表达式
         *
         * @return OB_SUCCESS或错误码
         */
        int add_filter(ObSqlExpression* expr);
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual ObPhyOperatorType get_type() const;
        //add liumz, [fix delete where bug] 20150128:b
        void reset_iterator();
        //add:e
		//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140408:b
		
		/*Exp:use sub_query's number in main_query to find its index in physical_plan
		* @param idx[in] sub_query's number in main_query
		* @param index[out] sub_query index in physical_plan
		*/
		int get_sub_query_index(int32_t idx,int32_t& index);
		
		/*Exp:get ObSqlExpression from readparam
		* @param idx[in] idx, sub_query's number in main_query
		* @param index[out] expr, ObSqlExpression which has corresponding sub_query
		* @param index[out] query_index, sub_query's order in current expression
		*/
		int get_sub_query_expr(int32_t idx,ObSqlExpression* &expr,int &query_index);
		
		/*Exp:remove expression which has sub_query but not use bloomfilter 
		* make stmt pass the first check 
		*/
		int remove_or_sub_query_expr();
		
		/*Exp:update sub_query num
		* if direct bind sub_query result to main query,
		* do not treat it as a sub_query any more
		*/
		void update_sub_query_num();
		
		/**Exp: return filters size*/
		int count(){return filters_.get_size();};
		
		/*Exp:copy filter expression to object filter
		* used for ObMultiBind second check 
		* @param object[in,out] object filter
		*/
		int copy_filter(ObFilter &object);
		// add 20140408:e

        //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20151016:b
        int process_sub_query();

        void set_need_handle_sub_query(bool need_handle)
        {
           need_handle_sub_query_ = need_handle;
        }

        bool get_need_handle_sub_query() const
        {
           return need_handle_sub_query_;
        }
        //add tianz 20151016:e
        //add wanglei [semi join update] 20160405:b
        common::DList & get_filter()
        {
            return filters_;
        }
        //mod dragon [type_err] 2016-12-6 b
        int remove_last_filter();
//        void remove_last_filter()
//        {
//            filters_.remove_last ();
//        }
        //mod e
        //add wanglei :e
        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        // disallow copy
        ObFilter(const ObFilter &other);
        ObFilter& operator=(const ObFilter &other);
      private:
        // data members
        common::DList filters_;
        //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20151016:b
        static const int MAX_SUB_QUERY_NUM = 5;//restrict num of sub_query which use HASHMAP
        static const int64_t HASH_BUCKET_NUM = 100000;
        static const int BIG_RESULTSET_THRESHOLD = 50; //´óÊý¾Ý¼¯ãÐÖµ
        bool need_handle_sub_query_;
        int hashmap_num_ ;
        common::CustomAllocator arena_;
        common::ObArray<common::ObRowkey>  sub_result_;
        common::hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode> sub_query_map_[MAX_SUB_QUERY_NUM];
        //add tianz 20151016:e
        common::ObArray<ObObjType> sub_query_map_and_bloomfilter_column_type[MAX_SUB_QUERY_NUM];  //add peiouya [IN_TYPEBUG_FIX] 20151225
        bool is_subquery_result_contain_null[MAX_SUB_QUERY_NUM];  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_FILTER_H */
