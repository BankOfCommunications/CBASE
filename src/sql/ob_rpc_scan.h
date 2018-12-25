/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rpc_scan.h
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#ifndef _OB_RPC_SCAN_H
#define _OB_RPC_SCAN_H 1
#include "ob_phy_operator.h"
#include "ob_sql_expression.h"
#include "common/ob_row.h"
#include "common/ob_hint.h"
#include "common/ob_schema.h"
#include "common/location/ob_tablet_location_cache_proxy.h"
#include "common/ob_string_buf.h"
#include "sql/ob_sql_scan_param.h"
#include "sql/ob_sql_get_param.h"
#include "sql/ob_sql_context.h"
#include "mergeserver/ob_ms_scan_param.h"
#include "mergeserver/ob_ms_sql_scan_request.h"
#include "mergeserver/ob_ms_sql_get_request.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "mergeserver/ob_rs_rpc_proxy.h"
#include "mergeserver/ob_merge_server_service.h"
#include "mergeserver/ob_frozen_data_cache.h"
#include "sql/ob_sql_read_strategy.h"
#include "mergeserver/ob_insert_cache.h"
//wanglei [semi join update] 20160406:b
#include <map>
#include <utility>
#include <string>
#include <vector>
using namespace std;
//wanglei :e
namespace oceanbase
{
//add  by fyd  [PrefixKeyQuery_for_INstmt]2014.4.1:b
  namespace mergeserver
  {
   class ObMsSqlScanRequest;
  }
  //add:e
  namespace sql
  {
    // 用于MS进行全表扫描
    class ObRpcScan : public ObPhyOperator
    {
      public:
        ObRpcScan();
        virtual ~ObRpcScan();
        virtual void reset();
        virtual void reuse();
        int set_child(int32_t child_idx, ObPhyOperator &child_operator)
        {
          UNUSED(child_idx);
          UNUSED(child_operator);
          return OB_ERROR;
        }
        int init(ObSqlContext *context, const common::ObRpcScanHint *hint = NULL);
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const { return PHY_RPC_SCAN; }
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        /**
         * 添加一个需输出的column
         *
         * @note 只有通过复合列结算新生成的列才需要new_column_id
         * @param expr [in] 需输出的列（这个列可能是个复合列的结果）
         *
         * @return OB_SUCCESS或错误码
         *
         * NOTE: 如果传入的expr是一个条件表达式，本函数将不做检查，需要调用者保证
         */		
        int add_output_column(const ObSqlExpression& expr, bool change_tid_for_storing = false);//mod liumz, [optimize group_order by index]20170419
        //add fanqiushi_index
        int set_main_tid(uint64_t main_tid);
        int set_is_use_index_for_storing(uint64_t main_tid,common::ObRowDesc &row_desc);
        int get_next_compact_row_for_index(const common::ObRow *&row);
        int set_main_rowkey_info(common::ObRowkeyInfo RI);
        int add_main_output_column(const ObSqlExpression& expr);
        int add_main_filter(ObSqlExpression* expr);
        int get_other_row_desc(const common::ObRowDesc *&row_desc);
        int set_second_rowdesc(common::ObRowDesc *row_desc);
        int fill_read_param_for_first_scan(ObSqlReadParam &dest_param);
        //add:e
		//add liumz, [optimize group_order by index]20170419:b
        void set_indexed_group(bool indexed)
        {
          read_param_->set_indexed_group(indexed);
        }
		//add:e

        //add wanglei [semi join] 20160108:b
        int add_index_filter_ll(ObSqlExpression* expr);
        int add_index_output_column_ll(const ObSqlExpression& expr);
        //add:e
        //add wanglei [semi join update] 20160415:b
        /*************************************我是华丽丽的分隔符*******************************************/
        //数据处理线程:b
//        static void * process_one_rpc(void * args);
//        static void * process_one_scan_param(void * args);
        //数据处理线程:E
        //使用回表的索引处理流程：b>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        /*************************************我是华丽丽的分隔符*******************************************/
        //使用单rpc 多scan的方法
        int get_table_row_with_more_scan(const common::ObRow *&row);
//      int get_table_row_with_more_scan_thread(const common::ObRow *&row);
        int cons_filter_for_right_table(ObSqlExpression *&table_filter_expr_,ObSqlExpression *&src_table_filter_expr_);
        int init_next_scan_param_for_one_scan_request(ObNewRange &range,
                                                      ObSqlScanParam * scan_param,
                                                      mergeserver::ObMsSqlScanRequest * scan_request);
        int create_scan_param_for_one_scan_request(ObSqlScanParam &scan_param,
                                                   ObSqlReadStrategy &sql_read_strategy);
        int cons_scan_range_for_one_scan_request(ObNewRange &range,
                                                            ObSqlReadStrategy &sql_read_strategy);
        /*************************************我是华丽丽的分隔符*******************************************/
        //通用函数:b>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        //设置左表缓存数据，在rpc scan中迭代到最后，在semi join操作符中重置
        int set_semi_join_left_table_row(common::ObRowStore *semi_join_left_table_row);
        //设置是否使用semi join
        int set_is_use_semi_join(bool is_use_semi_join);
        //设置左表的row desc用于构造右表的get param
        int set_semi_join_left_row_desc(const ObRowDesc *row_desc);
        //设置左表的table id，column id以及右表的column id
        int set_left_table_id_column_id(int64_t table_id,int64_t column_id,int64_t right_column_id,int64_t right_table_id);
        //设置是否为右表，如果是使用semi join并且为右表才走semi join的处理流程
        int set_is_right_table(bool flag);
        //用于重复主键的过滤
        int get_data_for_distinct(const common::ObObj *cell,
                                  ObObjType type,
                                  char *buff,
                                  int buff_size);
        void set_use_in(bool value);
        void set_use_btw(bool value);
		int64_t get_type_num(int64_t idx,int64_t type,ObSEArray<ObObj, 64> &expr_);
        //通用函数:e>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        /*************************************我是华丽丽的分隔符*******************************************/
        /**
         * 设置table_id
         * @param table_id [in] 被访问表的id
         *
         * @return OB_SUCCESS或错误码
         */
        int set_table(const uint64_t table_id, const uint64_t base_table_id);
        /**
         * 添加一个filter
         *
         * @param expr [in] 过滤表达式
         *
         * @return OB_SUCCESS或错误码
         */
        int add_filter(ObSqlExpression* expr);
        int add_group_column(const uint64_t tid, const uint64_t cid);
        int add_aggr_column(const ObSqlExpression& expr);
        //add gaojt [ListAgg][JHOBv0.1]20150104:b
        int add_sort_column_for_listagg(const uint64_t tid, const uint64_t cid,bool is_asc){return read_param_->add_sort_column_for_listagg(tid,cid,is_asc);}
        //add 20150104:e
        /**
         * 指定limit/offset
         *
         * @param limit [in]
         * @param offset [in]
         *
         * @return OB_SUCCESS或错误码
         */
        int set_limit(const ObSqlExpression& limit, const ObSqlExpression& offset);

        //void set_data_version(int64_t data_version);
        int set_scan_range(const ObNewRange &range)
        {
          UNUSED(range);
          return OB_ERROR;
        }

        void set_rowkey_cell_count(const int64_t rowkey_cell_count)
        {
          cur_row_desc_.set_rowkey_cell_count(rowkey_cell_count);
        }
        inline void set_need_cache_frozen_data(bool need_cache_frozen_data)
        {
          need_cache_frozen_data_ = need_cache_frozen_data;
        }
        inline void set_cache_bloom_filter(bool cache_bloom_filter)
        {
          cache_bloom_filter_ = cache_bloom_filter;
        }

        // add  by fyd  [PrefixKeyQuery_for_INstmt]2014.4.1:b
        ObSqlReadStrategy *get_sql_read_trategy()// const
        {
        	return &(this->sql_read_strategy_);
        }
        // 重新设置 scan_param 以及相关参数
        int init_next_scan_param(ObNewRange &range) ;
        //add:e
        int64_t to_string(char* buf, const int64_t buf_len) const;
		//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140409:b
		
		/*Exp:get ObSqlExpression from readparam
		* @param idx[in] idx, sub_query's number in main_query
		* @param index[out] expr, ObSqlExpression which has corresponding sub_query
		* @param index[out] query_index, sub_query's order in current expression
		*/
        int get_param_sub_query_expr(int32_t idx,ObSqlExpression* &expr,int &query_index){return read_param_->get_param_sub_query_expr(idx,expr,query_index);};
		
		/*Exp:remove expression which has sub_query but not use bloomfilter 
		* make stmt pass the first check 
		*/
        int remove_or_sub_query_expr();
		
		/*Exp:update sub_query num
		* if direct bind sub_query result to main query,
		* do not treat it as a sub_query any more
		*/
        void update_sub_query_num();
		
		/*Exp:get read method 
		* @return read_method 
		*/
        int32_t get_read_method(){return hint_.read_method_;};
		//add 20140409:e
		//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b
		
		/*Exp:here are two places need to modify the filter*/
        int get_rows_filter(ObSqlExpression *&rows_filter,ObSqlExpression *&strategy_rows_filter);
		
		/*Exp:move sub_query expression to simple in expression after process sub_query*/
		int chang_sub_query_to_simple();
		//add 20140715:e
        //add gaojt [NPU-2015009-cursor] [NPU-OB-009] 20150416:b
        void reset_stuff();/*Exp:call ObRowDesc ObSqlReadParam ObSqlReadStrategy's reset_stuff function*/
        //add gaojt 20150416:e
        //add gaojt [Delete_Update_Function] [JHOBv0.1] 20151206:b
        void reset_stuff_for_ud();
         //add 20151206:e
        //add zhujun [transaction read uncommit]2016/3/24
        int set_trans_id(const common::ObTransID &trans_id);
        //add:e
        // del by maosy [Delete_Update_Function_isolation_RC] 20161218
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//        int set_data_mark_param(const ObDataMarkParam &param);
//        int get_data_mark_param(const ObDataMarkParam *&param);
        //add duyr 20160531:e
    //del by maosy
        DECLARE_PHY_OPERATOR_ASSIGN;
        bool get_is_use_index() const;
    private:
        // disallow copy
        ObRpcScan(const ObRpcScan &other);
        ObRpcScan& operator=(const ObRpcScan &other);

        // member method
        void destroy();
        int cast_range(ObNewRange &range);
        int create_scan_param(ObSqlScanParam &scan_param);
        int get_next_compact_row(const common::ObRow*& row);
        int cons_scan_range(ObNewRange &range);
        int cons_row_desc(const ObSqlGetParam &sql_get_param, ObRowDesc &row_desc);
        int fill_read_param(ObSqlReadParam &dest_param);
        int get_min_max_rowkey(const ObArray<ObRowkey> &rowkey_array, ObObj *start_key_objs_, ObObj *end_key_objs_,int64_t rowkey_size);

        int create_get_param(ObSqlGetParam &get_param);
        //add fanqiushi_index
        int create_get_param_for_index(ObSqlGetParam &get_param);
        int fill_read_param_for_index(ObSqlReadParam &dest_param);
        int cons_get_rows_for_index(ObSqlGetParam &get_param);
        int reset_read_param_for_index();
        int cons_index_row_desc(const ObSqlGetParam &sql_get_param, ObRowDesc &row_desc);

        //add:e
        int cons_get_rows(ObSqlGetParam &get_param);
        void set_hint(const common::ObRpcScanHint &hint);
        void cleanup_request();


// del by maosy [Delete_Update_Function_isolation_RC] 20161218
//        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//      private:
//        int add_data_mark_cids_();
//        int add_special_output_column_(const uint64_t tid,const uint64_t cid);
//        //add duyr 20160531:e
//del by maosy e

      private:
        static const int64_t REQUEST_EVENT_QUEUE_SIZE = 8192;
        // 等待结果返回的超时时间
        int64_t timeout_us_;
        mergeserver::ObMsSqlScanRequest *sql_scan_request_;
        mergeserver::ObMsSqlGetRequest *sql_get_request_;
        //add fanqiushi_index
        //mergeserver::ObMsSqlGetRequest *index_sql_get_request_;
        bool is_use_index_;         //是否使用回表的索引
        bool is_use_index_for_storing_;  //是否使用不回表的索引
        uint64_t main_table_id_;    //主表的tid
        int64_t get_next_row_count_;    //判断是否是第一次调用get_new_row
        ObProject main_project;     //第二次get原表时的输出列
        ObFilter main_filter_;      //where条件中如果有不在索引表中的列的表达式，就把它存到这里，第二次get原表时用做过滤。
        common::ObRowkeyInfo main_rowkey_info_;    //主表的主键信息
        common::ObRowDesc second_row_desc_;  //回表时，第二次get原表时用到的行描述
        //add:e
        //add wanglei [second index fix] 20160108:b
        ObProject index_project_;
        ObFilter index_filter_;
        //add:e
        //add wanglei [semi join update] 20160405:b
        bool is_use_semi_join_;                         //判断是否使用semi join
        common::ObRowStore * semi_join_left_table_row_; //缓存左表的数据
        const ObRowDesc *left_row_desc_;                //左表row describe
        int64_t left_table_id_;                         //左表table id
        int64_t left_column_id_;                        //左表等值连接条件列
        int64_t right_column_id_;                       //右表等值连接条件列
        int64_t right_table_id_;                        //右表table id
        ObProject project_raw;                          //原表输出列
        ObFilter filter_raw;                            //原表filter
        bool is_right_table_ ;                          //判断是否为右表
        bool is_left_row_cache_complete_;               //判断左表缓存数据是否迭代完毕
        bool has_next_row_;                             //判断是否有下一行
        bool is_first_set_read_stra_;                   //判断是否第一次设置 read stra
        bool is_in_expr_empty_;                         //判断in表达式是否为空
        bool is_first_cons_scan_param_;                 //判断是否第一次构造scan param
        ObSqlExpression *filter_expr_;                  //不使用索引表的filter表达式
        ObSqlExpression *src_filter_expr_;              //使用二级索引的filter表达式
        map<string,bool> is_confilct_;                  //过滤重复数据
        bool use_in_expr_;
        bool use_btw_expr_;
        //多线程
//        common::ObRowStore one_cache;
//        bool is_first_use_thread_;
//        bool thread_complete_;
//        common::ObRow cached_row;
//        pthread_mutex_t mutex;
//        pthread_mutex_t mutex_1;
//        ObArray<pthread_t> thread_id;
        //add wanglei [semi join update] 20160405:e
        ObSqlScanParam *scan_param_;
        ObSqlGetParam *get_param_;
        ObSqlReadParam *read_param_;
        bool is_scan_;
        bool is_get_;
        ObRowDesc get_row_desc_;
        common::ObRowkeyInfo rowkey_info_;
        common::ObTabletLocationCacheProxy * cache_proxy_;
        mergeserver::ObMergerAsyncRpcStub * async_rpc_;
        ObSQLSessionInfo *session_info_;
        const oceanbase::mergeserver::ObMergeServerService *merge_service_;
        common::ObRpcScanHint hint_;
        common::ObObj start_key_objs_[OB_MAX_ROWKEY_COLUMN_NUMBER];
        common::ObObj end_key_objs_[OB_MAX_ROWKEY_COLUMN_NUMBER];
        common::ObRow cur_row_;
        //add fanqiushi_index
        common::ObRow cur_row_for_storing_;  //在不回表的查询中，需要对CS返回的数据修改tid，修改后的行存到cur_row_for_storing_里面
        common::ObRowDesc cur_row_desc_for_storing;
        //add:e
        common::ObRowDesc cur_row_desc_;
        uint64_t table_id_;
        uint64_t base_table_id_;
        char* start_key_buf_;
        char* end_key_buf_;
        ObSqlReadStrategy sql_read_strategy_;
        ObSEArray<ObRowkey, OB_PREALLOCATED_NUM> get_rowkey_array_;
        bool is_rpc_failed_;
        bool need_cache_frozen_data_;
        mergeserver::ObFrozenDataKey    frozen_data_key_;
        mergeserver::ObFrozenDataKeyBuf frozen_data_key_buf_;
        mergeserver::ObFrozenData       frozen_data_;
        mergeserver::ObCachedFrozenData cached_frozen_data_;
        bool cache_bloom_filter_;
        bool rowkey_not_exists_;
        ObObj obj_row_not_exists_;
        ObRow row_not_exists_;
        int insert_cache_iter_counter_;
        mergeserver::InsertCacheWrapValue value_;
        common::ObStringBuf tablet_location_list_buf_;
        //ObSEArray<bool, OB_PREALLOCATED_NUM> rowkeys_exists_;
        bool insert_cache_need_revert_;

        //add zhujun [transaction read uncommit]2016/3/22
        common::ObTransID trans_id_;
        // del by maosy [Delete_Update_Function_isolation_RC] 20161218
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//        bool is_data_mark_cids_added_;
//        bool is_data_mark_param_valid_;
        //add duyr 20160531:e
    //del by maosy
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TABLE_RPC_SCAN_H */
