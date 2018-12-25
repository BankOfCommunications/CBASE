/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql_expression.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SQL_EXPRESSION_H
#define _OB_SQL_EXPRESSION_H 1
#include "common/ob_object.h"
#include "ob_postfix_expression.h"
#include "common/ob_row.h"
#include "common/dlist.h"
//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140508:b
#include "common/bloom_filter.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_rowkey.h"
//add 20140508:e
class ObAggregateFunctionTest;

namespace oceanbase
{
  namespace sql
  {

    class ObSqlExpression: public common::DLink
    {
      public:
        ObSqlExpression();
        virtual ~ObSqlExpression();

        ObSqlExpression(const ObSqlExpression &other);
        ObSqlExpression& operator=(const ObSqlExpression &other);
        //add wanglei [to date optimization] 20160329:b
        bool is_optimization()
        {
            return is_optimization_;
        }
        void set_is_optimization(bool value)
        {
            is_optimization_ = value;
        }
        //add wanglei :e
        void set_int_div_as_double(bool did);

        void set_tid_cid(const uint64_t tid, const uint64_t cid);
        const uint64_t get_column_id() const;
        const uint64_t get_table_id() const;
        //add lbzhong[Update rowkey] 20151221:b
        int convert_to_equal_expression(const int64_t table_id, const int64_t column_id, const ObObj *&value);
        //add:e
        //add fanqiushi_index
        int change_tid(uint64_t &array_index);
        int get_cid(uint64_t &cid);
        void set_table_id(uint64_t tid);
        //add:e
        //add wanglei [second index fix] 20160425:b
         int get_tid_indexs( ObArray<uint64_t> & index_list);
        //add wanglei [second index fix] 20160425:e
        //add wanglei [to date optimization] 20160328
        int get_result (ObExprObj & result);
        //add wanglei:e
        void set_aggr_func(ObItemType aggr_fun, bool is_distinct);
        int get_aggr_column(ObItemType &aggr_fun, bool &is_distinct) const;
        /**
         * 设置表达式
         * @param expr [in] 表达式，表达方式与实现相关，目前定义为后缀表达式
         *
         * @return error code
         */
        int add_expr_obj(const ObObj &obj);
        int add_expr_item(const ExprItem &item);
        //add dolphin[coalesce return type]@20151126:b
        void set_expr_type(const ObObjType &type);
        int add_expr_item_end();
        void set_listagg_delimeter(ObString &listagg_delimeter){listagg_delimeter_ = listagg_delimeter;}//add gaojt [ListAgg][JHOBv0.1]20150104
        ObString get_listagg_delimeter()const{return listagg_delimeter_;}//add gaojt [ListAgg][JHOBv0.1]20150104
        void reset();

        /**
         * 获取解码后的表达式
         */
        inline const ObPostfixExpression &get_decoded_expression() const;
        //add wenghaixing for fix insert bug decimal key 2014/10/11
        inline  ObPostfixExpression &get_decoded_expression_v2() ;
        //add e
        inline bool is_equijoin_cond(ExprItem::SqlCellInfo &c1, ExprItem::SqlCellInfo &c2) const;
        //add dragon [Bugfix lmz-2017-2-6] 2017-2-7 b
        inline bool is_op_is_null();
        //add dragon e
        /**
         * 根据表达式语义对row的值进行计算
         *
         * @param row [in] 输入行
         * @param result [out] 计算结果
         *
         * @return error code
         */
        //mod peiouya peiouya [IN_TYPEBUG_FIX] 20151225:b
        ////mod tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140508:b
        ////int calc(const common::ObRow &row, const common::ObObj *&result);
        ///*Exp: calc will call static function to do real work,
        //* other params need transfer to static function as input parameters
        //* @param hash_map[in]
        //* @param second_check[in]
        //*/
        // int calc(const common::ObRow &row, const common::ObObj *&result,hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* hash_map = NULL,bool second_check = false);
        ////mod 20140508:e
        int calc(const common::ObRow &row, const common::ObObj *&result,
                   hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* hash_map = NULL,
                   bool is_has_map_contain_null = false,  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
                   common::ObArray<ObObjType> * hash_map_pair_type_desc = NULL,
                   bool second_check = false);
        //mod 20151225:e
        /// 打印表达式
        int64_t to_string(char* buf, const int64_t buf_len) const;

        // check expression type
        inline int is_const_expr(bool &is_const_type) const;
        inline int is_column_index_expr(bool &is_idx_type) const;
        inline int is_simple_condition(bool &is_simple_cond_type) const;
        inline int get_column_index_expr(uint64_t &tid, uint64_t &cid, bool &is_idx_type) const;
        inline int merge_expr(const ObSqlExpression &expr1, const ObSqlExpression &expr2, const ExprItem &op);
        inline bool is_simple_condition(bool real_val, uint64_t &column_id, int64_t &cond_op, ObObj &const_val, ObPostfixExpression::ObPostExprNodeType *val_type = NULL) const;
        //add fanqiushi_index
        inline bool is_have_main_cid(uint64_t mian_column_id);
        inline int find_cid(uint64_t &column_id);
        inline bool is_all_expr_cid_in_indextable(uint64_t index_tid,const ObSchemaManagerV2 *sm_v2);
        inline bool is_this_expr_can_use_index(uint64_t &index_tid,uint64_t main_tid,const ObSchemaManagerV2 *sm_v2);
        inline int get_all_cloumn(ObArray<uint64_t> &column_index);
        //add:e
        //add wanglei [second index fix] 20160425:b
        inline bool is_expr_has_more_than_two_columns();
        //add wanglei [second index fix] 20160425:e
        inline bool is_simple_between(bool real_val, uint64_t &column_id, int64_t &cond_op, ObObj &cond_start, ObObj &cond_end) const;
        inline bool is_simple_in_expr(bool real_val, const ObRowkeyInfo &info, ObIArray<ObRowkey> &rowkey_array,
            common::PageArena<ObObj,common::ModulePageAllocator> &allocator) const;
        //add fyd [PrefixKeyQuery_for_INstmt] 2014.4.22:b
	   inline bool is_in_expr_with_ex_rowkey(bool real_val, const ObRowkeyInfo &info, ObIArray<ObRowkey> &rowkey_array,
				   common::PageArena<ObObj,common::ModulePageAllocator> &allocator) const;
     //add lijianqiang [sequence] 20150414:b
     /*Exp:用sequence回填inputvalues时使用*/
     ObSEArray<ObObj, 64> &get_expr_array();
     int64_t get_expr_obj_size();
     //add 20150414:e

	   // for test
	   inline bool set_post_expr( ObPostfixExpression &other)
	   {
		bool ret = true ;
		if ( other.is_empty() )
		{
			TBSYS_LOG(WARN, "post_expr should not be empty!");
			ret = false ;
		}
		else
		{
			post_expr_.reset();
			post_expr_ = other ;
		}
		return ret;
	   }
	   //add:e
        inline bool is_aggr_func() const;
        inline bool is_empty() const;
        inline void set_owner_op(ObPhyOperator *owner_op);
        inline ObPhyOperator* get_owner_op();
        NEED_SERIALIZE_AND_DESERIALIZE;
		//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b
		
		/*Exp:determin whether the expr is sub_query for insert,
		* used for strategy store expr 
		*/
		inline bool is_in_sub_query_expr(const ObRowkeyInfo &info) const;
		
		/*Exp:determin whether the expr is sub_query for insert,
		* used for rpc_scan get filter
		*/
		bool is_rows_filter();
		
		/*Exp:used before specific value replace the mark of sub_query, 
		* pop other information of expression out to temp expression ,
		* then remove the mark of sub_query
		*/
		int delete_sub_query_info();
		
		//add 20140715:e
        //add tianz [SubQuery_for_Instmt] [JHOBv0.1]20140404:b

		/*Exp:count on sub_query num in current expression*/
        void add_sub_query_num(){sub_query_num_++;};

		/*Exp:get sub_query num in current expression
		* @return sub_query_num
		*/
        int64_t get_sub_query_num(){return sub_query_num_;};

        /*Exp:store sub_query physical_plan index to array
        * @param idx[in] sub_query's number in current expression
        * @param index[in] sub_query index in physical_plan
        */
        int set_sub_query_idx(int64_t idx,int32_t index);

		/*Exp:use sub_query's number in main_query to find its index in physical_plan
		* @param idx[in] sub_query's number in main_query
		* @param index[out] sub_query index in physical_plan
		*/
        int32_t get_sub_query_idx(int32_t idx){return sub_query_idx_[idx] ;};

		/*Exp:used before specific value replace the mark of sub_query,
		* pop other information of expression out to temp expression ,
		* then remove the mark of sub_query
		* @param sub_query_index[in] ,sub_query index in current expression
		*/
        int delete_sub_query_info(int sub_query_index);

		/*Exp:used for specific value replace the mark of sub_query,
		* push specific value back to current expression
		* @param obj_value[in] specific value which will replace the mark of sub_query
		*/
        int add_expr_in_obj(ObObj& obj_value){return post_expr_.add_expr_in_obj(obj_value);};

		/*Exp:used after specific value replace the mark of sub_query,
		* push other information of expression back to modified expression
		*/
        int complete_sub_query_info(){return post_expr_.complete_sub_query_info();};

		/*Exp: get address of bloomfilter
		* @param bloom_filter[out]
		*/
        int get_bloom_filter(ObBloomFilterV1 *& bloom_filter){bloom_filter = &bloom_filter_; return OB_SUCCESS;};

		/*Exp: set mark for using bloomfilter*/
        void set_has_bloomfilter();

		/*Exp: get mark of bloomfilter's state of using */
        bool has_bloom_filter(){return use_bloom_filter_;};

		/*Exp:get sub_query selected column number
		* @param sub_query_index[in] ,sub_query's number in current expression
		* @param num[out],sub_query selected column number
		* @param special_sub_query[out],sub_query is a separate expression, not belong to in or not in expression
		*/
        int get_sub_query_column_num(int sub_query_index,int64_t &num,bool &special_sub_query){return post_expr_.get_sub_query_column_num(sub_query_index-direct_bind_query_num_,num,special_sub_query);};

        /*Exp:get sub_query selected column number
        * @param sub_query_index[in] ,sub_query's number in current expression
        * @param num[out],sub_query selected column number
        * @param special_sub_query[out],sub_query is a separate expression, not belong to in or not in expression
        * @param special_case:T_OP_EQ || T_OP_LE ||T_OP_LT || T_OP_GE || T_OP_GT || T_OP_NE
        */
        //add xionghui [fix equal subquery bug] 20150122 b:
        int get_sub_query_column_num_new(int sub_query_index,int64_t &num,bool &special_sub_query,bool &special_case){return post_expr_.get_sub_query_column_num_new(sub_query_index-direct_bind_query_num_,num,special_sub_query,special_case);};
        //add e:
        /*Exp:check whether current expression satisfy the condition of use bloomfilter */
        bool is_satisfy_bloomfilter(){return post_expr_.is_satisfy_bloomfilter();};// add 20140523

		/*Exp:update sub_query num
        * if direct bind sub_query result to main query,
        * do not treat it as a sub_query any more
        */
        void update_sub_query_num();

		/*Exp:copy filter expression to object filter
		* used for ObMultiBind second check
		* @param res[in] res filter
		*/
        int copy(ObSqlExpression* res);

		/*Exp: for free memory of bloomfilter used*/
        void destroy_filter();
        //add 20140404:e

        //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160520:b
        void set_is_update(bool is_update){is_update_ = is_update;};
        bool get_is_update() {return is_update_;};
        void add_question_num(int64_t step){questionmark_num_in_update_assign_list_ += step;};
        int64_t get_question_num(){return questionmark_num_in_update_assign_list_;};
        void reset_question_num(){questionmark_num_in_update_assign_list_ = 0;};
        //add gaojt 20160520:e


      public:
        static ObSqlExpression* alloc();
        static void free(ObSqlExpression* ptr);
      private:
        friend class ::ObAggregateFunctionTest;
        // data members
        ObPostfixExpression post_expr_;
        uint64_t column_id_;
        uint64_t table_id_;
        bool is_aggr_func_;
        bool is_distinct_;
        ObItemType aggr_func_;
        ObString listagg_delimeter_;//add gaojt [ListAgg][JHOBv0.1]20150104
		//add tianz [SubQuery_for_Instmt] [JHOBv0.1]20140404:b
		static const int32_t MAX_SUB_QUERY_NUM = 10;
	/*Exp:one expression of stmt's max sub_query_num, used for avoid package size bigger than 2M*/
		int64_t sub_query_num_ ;						//count current expression's sub_query num
		int direct_bind_query_num_;						//count current expression's specific value replace sub_query num
		int32_t sub_query_idx_[MAX_SUB_QUERY_NUM];		//store sub_query index in physical_plan
		common::ObBloomFilterV1 bloom_filter_;			//bloom filter
		bool use_bloom_filter_;							//mark whether current expression used bloomfilter
		//add 20140404:e
        //add wanglei [to date optimization] 20160328
        bool is_optimization_;
        //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160520:b
        bool is_update_;
        int64_t questionmark_num_in_update_assign_list_;
        //add gaojt 20160520:e
      private:
        // method
        int serialize_basic_param(char* buf, const int64_t buf_len, int64_t& pos) const;
        int deserialize_basic_param(const char* buf, const int64_t data_len, int64_t& pos);
        int64_t get_basic_param_serialize_size(void) const;
    };
    typedef common::ObArray<ObSqlExpression, ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > ObExpressionArray;
    class ObSqlExpressionUtil
    {
      public:
        static int make_column_expr(const uint64_t tid, const uint64_t cid, ObSqlExpression &expr);
      private:
        DISALLOW_COPY_AND_ASSIGN(ObSqlExpressionUtil);
        ObSqlExpressionUtil();
        ~ObSqlExpressionUtil();
    };

    inline void ObSqlExpression::reset(void)
    {
      DLink::reset();
      post_expr_.reset();
      column_id_ = OB_INVALID_ID;
      table_id_ = OB_INVALID_ID;
      is_aggr_func_ = is_distinct_ = false;
	  //add tianz [SubQuery_for_Instmt] [JHOBv0.1]20140404:b
      direct_bind_query_num_ = 0;
      sub_query_num_ = 0;
      post_expr_.free_post_in();

      if(use_bloom_filter_)
	{
	  bloom_filter_.destroy();//add 20140512
	  use_bloom_filter_ = false;//20140610
	}
	  //add 20140404:e
    }

    inline void ObSqlExpression::set_int_div_as_double(bool did)
    {
      post_expr_.set_int_div_as_double(did);
    }

    inline void ObSqlExpression::set_tid_cid(const uint64_t tid, const uint64_t cid)
    {
      table_id_ = tid;
      column_id_ = cid;
    }

    inline const uint64_t ObSqlExpression::get_column_id() const
    {
      return column_id_;
    }

    inline const uint64_t ObSqlExpression::get_table_id() const
    {
      return table_id_;
    }

    //add fanqiushi_index
    inline void ObSqlExpression::set_table_id(uint64_t tid)
    {
            table_id_ = tid;
    }

    //add:e
    inline int ObSqlExpression::get_aggr_column(ObItemType &aggr_fun, bool &is_distinct) const
    {
      int ret = OB_SUCCESS;
      if (!is_aggr_func_)
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "this expression is not an aggr function");
      }
      else
      {
        aggr_fun = aggr_func_;
        is_distinct = is_distinct_;
      }
      return ret;
    }
    //add wanglei [to date optimization] 20160328
    inline int ObSqlExpression::get_result (ObExprObj & result)
    {
        int ret = OB_SUCCESS;
        post_expr_.get_result(result);
        return ret;
    }
    //add wanglei:e
    inline void ObSqlExpression::set_aggr_func(ObItemType aggr_func, bool is_distinct)
    {
      OB_ASSERT(aggr_func >= T_FUN_MAX && aggr_func <= T_FUN_AVG);
      is_aggr_func_ = true;
      aggr_func_ = aggr_func;
      is_distinct_ = is_distinct;
    }

    inline const ObPostfixExpression &ObSqlExpression::get_decoded_expression() const
    {
      return post_expr_;
    }
    //add wenghaixing for fix delete bug decimal key 2014/10/11
     inline  ObPostfixExpression &ObSqlExpression::get_decoded_expression_v2()
    {
      return post_expr_;
    }
    //add e
    inline bool ObSqlExpression::is_equijoin_cond(ExprItem::SqlCellInfo &c1, ExprItem::SqlCellInfo &c2) const
    {
      return post_expr_.is_equijoin_cond(c1, c2);
    }
    //add dragon [Bugfix lmz-2017-2-6] 2017-2-7 b
    inline bool ObSqlExpression::is_op_is_null()
    {
      return post_expr_.is_op_is_null();
    }
    //add dragon e
    inline int ObSqlExpression::is_const_expr(bool &is_const_type) const
    {
      return post_expr_.is_const_expr(is_const_type);
    }
    inline int ObSqlExpression::get_column_index_expr(uint64_t &tid, uint64_t &cid, bool &is_idx_type) const
    {
      return post_expr_.get_column_index_expr(tid, cid, is_idx_type);
    }
    inline int ObSqlExpression::is_column_index_expr(bool &is_idx_type) const
    {
      return post_expr_.is_column_index_expr(is_idx_type);
    }
    inline bool ObSqlExpression::is_simple_condition(bool real_val, uint64_t &column_id, int64_t &cond_op, ObObj &const_val, ObPostfixExpression::ObPostExprNodeType *val_type) const
    {
      return post_expr_.is_simple_condition(real_val, column_id, cond_op, const_val, val_type);
    }
    //add fanqiushi_index
    inline bool ObSqlExpression::is_have_main_cid(uint64_t mian_column_id)
    {
        return post_expr_.is_have_main_cid(mian_column_id);
    }
    inline int ObSqlExpression::find_cid(uint64_t &column_id)
    {
        return post_expr_.find_cid(column_id);
    }
    inline bool ObSqlExpression::is_all_expr_cid_in_indextable(uint64_t index_tid,const ObSchemaManagerV2 *sm_v2)
    {
        return post_expr_.is_all_expr_cid_in_indextable(index_tid,sm_v2);
    }
    inline int ObSqlExpression::get_all_cloumn(ObArray<uint64_t> &column_index)
    {
        return post_expr_.get_all_cloumn(column_index);
    }

    inline bool ObSqlExpression::is_this_expr_can_use_index(uint64_t &index_tid,uint64_t main_tid,const ObSchemaManagerV2 *sm_v2)
    {
        return post_expr_.is_this_expr_can_use_index(index_tid,main_tid,sm_v2);
    }

    //add:e
    //add wanglei [second index fix] 20160425:b
    inline bool ObSqlExpression::is_expr_has_more_than_two_columns()
    {
        return post_expr_.is_expr_has_more_than_two_columns();
    }
    //add wanglei [second index fix] 20160425:e
    inline bool ObSqlExpression::is_simple_between(bool real_val, uint64_t &column_id, int64_t &cond_op, ObObj &cond_start, ObObj &cond_end) const
    {
      return post_expr_.is_simple_between(real_val, column_id, cond_op, cond_start, cond_end);
    }
    inline bool ObSqlExpression::is_simple_in_expr(bool real_val, const ObRowkeyInfo &info, ObIArray<ObRowkey> &rowkey_array,
        common::PageArena<ObObj,common::ModulePageAllocator>  &allocator) const
    {
      return post_expr_.is_simple_in_expr(real_val, info, rowkey_array, allocator);
    }
    //add fyd [PrefixKeyQuery_for_INstmt] 2014.4.23
    inline bool ObSqlExpression::is_in_expr_with_ex_rowkey(bool real_val, const ObRowkeyInfo &info, ObIArray<ObRowkey> &rowkey_array,
        common::PageArena<ObObj,common::ModulePageAllocator>  &allocator) const
    {
      return post_expr_.is_in_expr_with_ex_rowkey(real_val, info, rowkey_array, allocator);
    }
    //add:e
    //add lijianqiang [sequence] 20150414:b
    inline ObSEArray<ObObj, 64> &ObSqlExpression::get_expr_array()
    {
      return post_expr_.get_expr_array();
    }
    inline int64_t ObSqlExpression::get_expr_obj_size()
    {
      return post_expr_.get_expr_obj_size();
    }
    //add 20150414:e
    inline bool ObSqlExpression::is_aggr_func() const
    {
      return is_aggr_func_;
    }
    inline bool ObSqlExpression::is_empty() const
    {
      return post_expr_.is_empty();
    }
    inline void ObSqlExpression::set_owner_op(ObPhyOperator *owner_op)
    {
      post_expr_.set_owner_op(owner_op);
    }
    inline ObPhyOperator* ObSqlExpression::get_owner_op()
    {
      return post_expr_.get_owner_op();
    }
    inline int ObSqlExpression::merge_expr(const ObSqlExpression &expr1, const ObSqlExpression &expr2, const ExprItem &op)
    {
      reset();
      return post_expr_.merge_expr(expr1.post_expr_, expr2.post_expr_, op);
    }
    //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b
    inline bool ObSqlExpression::is_in_sub_query_expr(const ObRowkeyInfo &info) const
    {
      return post_expr_.is_in_sub_query_expr(info);
    }
    //add 20140715:e
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SQL_EXPRESSION_H */
