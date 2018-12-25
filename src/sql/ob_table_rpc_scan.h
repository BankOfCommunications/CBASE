/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_table_rpc_scan.h
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#ifndef _OB_TABLE_RPC_SCAN_H
#define _OB_TABLE_RPC_SCAN_H 1
#include "ob_table_scan.h"
#include "ob_rpc_scan.h"
#include "ob_sql_expression.h"
#include "ob_table_rename.h"
#include "ob_project.h"
#include "ob_filter.h"
#include "ob_scalar_aggregate.h"
#include "ob_merge_groupby.h"
#include "ob_sort.h"
#include "ob_limit.h"
#include "ob_empty_row_filter.h"
#include "ob_sql_context.h"
#include "common/ob_row.h"
#include "common/ob_hint.h"

namespace oceanbase
{
  namespace sql
  {
    class ObTableRpcScan: public ObTableScan
    {
      public:
        ObTableRpcScan();
        virtual ~ObTableRpcScan();
        virtual void reset();
        virtual void reuse();
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual ObPhyOperatorType get_type() const;

        int init(ObSqlContext *context, const common::ObRpcScanHint *hint = NULL);

        /**
         * 添加一个需输出的column
         *
         * @note 只有通过复合列结算新生成的列才需要new_column_id
         * @param expr [in] 需输出的列（这个列可能是个复合列的结果）
         *
         * @return OB_SUCCESS或错误码
         */
        int add_output_column(const ObSqlExpression& expr, bool change_tid_for_storing = false);//mod liumz, [optimize group_order by index]20170419
        //add fanqiushi_index
        void set_main_tid(uint64_t tid);  //存原表的tid
        void set_is_index_for_storing(bool is_use,uint64_t tid);  //设置这次查询是否用到不回表的索引
        inline int get_is_index_without_storing(bool &is_use, uint64_t &tid); //add dragon
        void set_is_index_without_storing(bool is_use,uint64_t tid);//设置这次查询是否用到回表的索引
        void set_is_use_index_without_storing();
        void set_is_use_index_for_storing(uint64_t tid,ObRowDesc &row_desc);
        int add_main_output_column(const ObSqlExpression& expr);
        int add_main_filter(ObSqlExpression* expr);
        int add_index_filter(ObSqlExpression* expr);
        void set_main_rowkey_info(common::ObRowkeyInfo RI);
        int cons_second_row_desc(ObRowDesc &row_desc);
        int set_second_row_desc(ObRowDesc *row_desc);
        //add:e
        //add wanglei [semi join] 20160108:b
         int add_index_filter_ll(ObSqlExpression* expr) ;
         int add_index_output_column_ll(ObSqlExpression& expr) ;
        //add:e
         //add wanglei [semi join update] 20160405:b
         ObRpcScan & get_rpc_scan();
         void reset_select_get_filter();
         bool is_right_table();
         void set_is_right_table(bool flag);
         //add wanglei :e
        /**
         * 设置table_id
         * @note 只有基本表被重命名的情况才会使两个不相同id，其实两者相同时base_table_id可以给个默认值。
         * @param table_id [in] 输出的table_id
         * @param base_table_id [in] 被访问表的id
         *
         * @return OB_SUCCESS或错误码
         */
        int set_table(const uint64_t table_id, const uint64_t base_table_id);


        //add zhaoqiong [TableMemScan_Subquery_BUGFIX] 20151118:b
        bool is_base_table_id_valid(){return true;};
        //add:e
        /**
         * 添加一个filter
         *
         * @param expr [in] 过滤表达式
         *
         * @return OB_SUCCESS或错误码
         */
        int add_filter(ObSqlExpression *expr);
        int add_group_column(const uint64_t tid, const uint64_t cid, bool change_tid_for_storing = false);//mod liumz, [optimize group_order by index]20170419
        int add_aggr_column(const ObSqlExpression& expr);

        /**
         * 指定limit/offset
         *
         * @param limit [in]
         * @param offset [in]
         *
         * @return OB_SUCCESS或错误码
         */
        int set_limit(const ObSqlExpression& limit, const ObSqlExpression& offset);
        int64_t to_string(char* buf, const int64_t buf_len) const;
        void set_phy_plan(ObPhysicalPlan *the_plan);
        int32_t get_child_num() const;
        //add gaojt [ListAgg][JHOBv0.1]20150104:b
        void set_is_listagg(bool is_listagg){is_listagg_ = is_listagg;}
        int add_sort_column_for_listagg(const uint64_t tid, const uint64_t cid,bool is_asc);
        /* add 20150104:e */
		//add liumz, [optimize group_order by index]20170419:b	
        void set_indexed_group(bool indexed)
        {
          is_indexed_group_ = indexed;
          rpc_scan_.set_indexed_group(indexed);
        }
        bool is_indexed_group() const{return is_indexed_group_;}
		//add:e
        void set_rowkey_cell_count(const int64_t rowkey_cell_count)
        {
          rpc_scan_.set_rowkey_cell_count(rowkey_cell_count);
        }

        inline void set_need_cache_frozen_data(bool need_cache_frozen_data)
        {
          rpc_scan_.set_need_cache_frozen_data(need_cache_frozen_data);
        }
        inline void set_cache_bloom_filter(bool cache_bloom_filter)
        {
          rpc_scan_.set_cache_bloom_filter(cache_bloom_filter);
        }
        //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140408:b
		
		/*Exp:use sub_query's number in main_query to find its index in physical_plan
		* @param idx[in] sub_query's number in main_query
		* @param index[out] sub_query index in physical_plan
		*/
        int get_sub_query_index(int32_t idx,int32_t &index);
		
		/*Exp:get ObSqlExpression from ObTableRpcScan's member select_get_filter_ (ObFilter)
		* @param idx[in] idx, sub_query's number in main_query
		* @param index[out] expr, ObSqlExpression which has corresponding sub_query
		* @param index[out] query_index, sub_query's order in current expression
		*/
        int get_sub_query_expr(int32_t idx,ObSqlExpression* &expr,int &query_index){return select_get_filter_.get_sub_query_expr(idx,expr,query_index);};
		
		/*Exp:get ObSqlExpression from readparam
		* @param idx[in] idx, sub_query's number in main_query
		* @param index[out] expr, ObSqlExpression which has corresponding sub_query
		* @param index[out] query_index, sub_query's order in current expression
		*/
        int get_param_sub_query_expr(int32_t idx,ObSqlExpression* &expr,int &query_index){return rpc_scan_.get_param_sub_query_expr(idx,expr,query_index);};
		
		/*Exp:remove expression which has sub_query but not use bloomfilter 
		* make stmt pass the first check 
		*/
        int remove_or_sub_query_expr();
		
		/*Exp:update sub_query num
		* if direct bind sub_query result to main query,
		* do not treat it as a sub_query any more
		*/
        void update_sub_query_num();
		
		/*Exp:copy filter expression to object filter
		* used for ObMultiBind second check 
		* @param object[in,out] object filter
		*/
        int copy_filter(ObFilter &object);
		
		/*Exp:get read method */
        int32_t get_read_method(){return rpc_scan_.get_read_method();};
        //add 20140408:e
        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;
		//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b
		/*Exp:move sub_query expression to simple in expression after process sub_query*/
		int chang_sub_query_to_simple();
        int reset_stuff();/*Exp:call ObRowDesc ObSqlReadParam ObSqlReadStrategy's reset_stuff function*/
		//add 20140715:e
        //add gaojt [Delete_Update_Function] [JHOBv0.1] 20151206:b
        int reset_stuff_for_ud();
         //add 20151206:e
        // del by maosy [Delete_Update_Function_isolation_RC] 20161218
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//        int set_data_mark_param(const ObDataMarkParam &param)
//        {
//            return rpc_scan_.set_data_mark_param(param);
//        }
//        int get_data_mark_param(const ObDataMarkParam *&param)
//        {
//            return rpc_scan_.get_data_mark_param(param);
//        }
        //add duyr 20160531:e
    //del by maosy
      private:
        // disallow copy
        ObTableRpcScan(const ObTableRpcScan &other);
        ObTableRpcScan& operator=(const ObTableRpcScan &other);
      private:
        // data members
        ObRpcScan rpc_scan_;
        ObFilter select_get_filter_;
        ObScalarAggregate *scalar_agg_; // very big
        ObMergeGroupBy *group_; // very big
        ObSort group_columns_sort_;
        ObLimit limit_;
        ObEmptyRowFilter empty_row_filter_;
        bool has_rpc_;
        bool has_scalar_agg_;
        bool has_group_;
        bool has_group_columns_sort_;
        bool is_indexed_group_;//add liumz, [optimize group_order by index]20170419
        bool has_limit_;
        bool is_skip_empty_row_;
        //add fanqiushi_index
        bool is_use_index_rpc_scan_;  //判断是否使用了回表的索引
        bool is_use_index_for_storing; //判断是否使用了不回表的索引
        //ObRpcScan index_rpc_scan_;
        ObProject main_project_;   //存第二次get原表时的输出列
        ObFilter main_filter_;     //存第二次get原表时filter
        uint64_t main_tid_;         //原表的tid
        bool is_use_index_for_storing_for_tostring_;
        uint64_t index_tid_for_storing_for_tostring_;
        bool is_use_index_without_storing_for_tostring_;
        uint64_t index_tid_without_storing_for_tostring_;

        //add:e
        //add wanglei [semi join update] 20160407:b
        bool is_right_table_;
        ObSqlExpression* expr_clone;
        //add wanglei :e
        int32_t read_method_;
        bool is_listagg_;//add gaojt [ListAgg][JHOBv0.1]20150104
        ObSort sort_for_listagg_;//add gaojt [ListAgg][JHOBv0.1]20150104
    };
    inline void ObTableRpcScan::set_phy_plan(ObPhysicalPlan *the_plan)
    {
      ObPhyOperator::set_phy_plan(the_plan);
      rpc_scan_.set_phy_plan(the_plan);
      select_get_filter_.set_phy_plan(the_plan);
      group_columns_sort_.set_phy_plan(the_plan);
      sort_for_listagg_.set_phy_plan(the_plan);//add gaojt [ListAgg][JHOBv0.1]20150104
      limit_.set_phy_plan(the_plan);
      limit_.set_phy_plan(the_plan);
      //add liumz, [bugfix: index memory overflow]20170105:b
      main_project_.set_phy_plan(the_plan);
      main_filter_.set_phy_plan(the_plan);
      //add:e
    }
    inline int32_t ObTableRpcScan::get_child_num() const
    {
      return 0;
    }

    //add dragon [invalid_argument] 2016-12-22
    inline int ObTableRpcScan::get_is_index_without_storing(bool &is_use, uint64_t &tid)
    {
      is_use = is_use_index_without_storing_for_tostring_;
      tid = index_tid_without_storing_for_tostring_;
      return OB_SUCCESS;
    }
    //add e
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TABLE_RPC_SCAN_H */
