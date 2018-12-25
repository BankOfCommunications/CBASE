/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rpc_scan.cpp
 *
 * ObRpcScan operator
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#include "ob_rpc_scan.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_obj_cast.h"
#include "mergeserver/ob_merge_server_service.h"
#include "mergeserver/ob_merge_server_main.h"
#include "mergeserver/ob_ms_sql_get_request.h"
#include "ob_sql_read_strategy.h"
#include "ob_duplicate_indicator.h"
#include "common/ob_profile_type.h"
#include "common/ob_profile_log.h"
#include "mergeserver/ob_insert_cache.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::mergeserver;

ObRpcScan::ObRpcScan() :
    timeout_us_(0),
    sql_scan_request_(NULL),
    sql_get_request_(NULL),
    //add fanqiushi_index
    //index_sql_get_request_(NULL),
    // is_use_index(false),
    //index_table_id(OB_INVALID_ID),
    is_use_index_(false),
    is_use_index_for_storing_(false),
    main_table_id_(OB_INVALID_ID),
    get_next_row_count_(0),
    main_project(),
    main_filter_(),
    second_row_desc_(),
    //add:e
    //add wanglei [semi join] 20150108:b
//    index_project_(),
//    index_filter_(),
    //add:e
    //add wanglei [semi join update] 20160405:b
    is_use_semi_join_(false),
    semi_join_left_table_row_(NULL),
    left_row_desc_(NULL),
    left_table_id_(OB_INVALID_INDEX),
    left_column_id_(OB_INVALID_INDEX),
    right_column_id_(OB_INVALID_INDEX),
    is_right_table_ (false),
    is_left_row_cache_complete_(false),
    has_next_row_(false),
    is_first_set_read_stra_(true),
    is_in_expr_empty_(false),
    is_first_cons_scan_param_(true),
    filter_expr_(NULL),
    src_filter_expr_(NULL),
    use_in_expr_(false),
    use_btw_expr_(false),
    //is_first_use_thread_(true),
    //thread_complete_(false),
    //add wanglei [semi join update] 20160405:e
    scan_param_(NULL),
    get_param_(NULL),
    read_param_(NULL),
    is_scan_(false),
    is_get_(false),
    cache_proxy_(NULL),
    async_rpc_(NULL),
    session_info_(NULL),
    merge_service_(NULL),
    cur_row_(),
    cur_row_desc_(),
    table_id_(OB_INVALID_ID),
    base_table_id_(OB_INVALID_ID),
    start_key_buf_(NULL),
    end_key_buf_(NULL),
    is_rpc_failed_(false),
    need_cache_frozen_data_(false),
    cache_bloom_filter_(false),
    rowkey_not_exists_(false),
    insert_cache_iter_counter_(0),
    insert_cache_need_revert_(false)
  // del by maosy [Delete_Update_Function_isolation_RC] 20161218
  //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//    ,is_data_mark_cids_added_(false)
//    ,is_data_mark_param_valid_(false)
  //add duyr 20160531:e
    // del by maosy
{
    sql_read_strategy_.set_rowkey_info(rowkey_info_);
    //add wanglei [semi join update] 20160601:b
//    project_raw = NULL;
//    filter_raw = NULL;
    //add wanglei [semi join update] 20160601:e
}


ObRpcScan::~ObRpcScan()
{
    //add wanglei [semi join update] 20160601:b
//    if(is_right_table_)
//    {
//        if(project_raw != NULL)
//        {
//            project_raw->~ObProject ();
//            ob_free (project_raw);
//            project_raw = NULL;
//        }
//        if(filter_raw != NULL)
//        {
//            filter_raw->~ObFilter ();
//            ob_free (filter_raw);
//            filter_raw = NULL;
//        }
//    }
    //for memory leak check:b
    //int64_t usage = 0;
    //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION);
    //TBSYS_LOG(INFO,"wanglei::~ObRpcScan OB_SEMI_JOIN_SQL_EXPRESSION clear malloc usage = %ld",usage);
    //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
    //TBSYS_LOG(INFO,"wanglei::~ObRpcScan OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER free malloc usage = %ld",usage);
    //for memory leak check:e
    //add wanglei [semi join update] 20160601:e
    this->destroy();
}

void ObRpcScan::reset()
{
    this->destroy();
    timeout_us_ = 0;
    read_param_ = NULL;
    get_param_ = NULL;
    scan_param_ = NULL;
    is_scan_ = false;
    is_get_ = false;
    cache_proxy_ = NULL;
    async_rpc_ = NULL;
    session_info_ = NULL;
    merge_service_ = NULL;
    table_id_ = OB_INVALID_ID;
    base_table_id_ = OB_INVALID_ID;
    start_key_buf_ = NULL;
    end_key_buf_ = NULL;
    is_rpc_failed_ = false;
    need_cache_frozen_data_ = false;
    cache_bloom_filter_ = false;
    rowkey_not_exists_ = false;
    frozen_data_.clear();
    cached_frozen_data_.reset();
    insert_cache_iter_counter_ = 0;
    cleanup_request();
    //cur_row_.reset(false, ObRow::DEFAULT_NULL);
    cur_row_desc_.reset();
    sql_read_strategy_.destroy();
    insert_cache_need_revert_ = false;
    //add fanqiushi_index
    is_use_index_=false;
    is_use_index_for_storing_=false;
    get_next_row_count_=0;
    main_table_id_=OB_INVALID_ID;
    main_project.reset();
    main_filter_.reset();
    second_row_desc_.reset();
    //add:e
    //add wanglei [semi join update] 20160601:b
    is_use_semi_join_ = false;
//    if(is_right_table_)
//    {
//        if(project_raw != NULL)
//        {
//            project_raw->~ObProject ();
//            ob_free (project_raw);
//            project_raw = NULL;
//        }
//        if(filter_raw != NULL)
//        {
//            filter_raw->~ObFilter ();
//            ob_free (filter_raw);
//            filter_raw = NULL;
//        }
//    }
    //for memory leak check:b
    //int64_t usage = 0;
    //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION);
    //TBSYS_LOG(INFO,"wanglei::reset OB_SEMI_JOIN_SQL_EXPRESSION clear malloc usage = %ld",usage);
    //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
    //TBSYS_LOG(INFO,"wanglei::reset OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER free malloc usage = %ld",usage);
    //for memory leak check:e
    //add wanglei [semi join update] 20160601:e
    // del by maosy [Delete_Update_Function_isolation_RC] 20161218
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//    is_data_mark_cids_added_  = false;
//    is_data_mark_param_valid_ = false;
    //add duyr 20160531:e
  //del by maosy
}

void ObRpcScan::reuse()
{
    reset();
}

int ObRpcScan::init(ObSqlContext *context, const common::ObRpcScanHint *hint)
{
    int ret = OB_SUCCESS;
    if (NULL == context)
    {
        ret = OB_INVALID_ARGUMENT;
    }
    else if (NULL == context->cache_proxy_
             || NULL == context->async_rpc_
             || NULL == context->schema_manager_
             || NULL == context->merge_service_)
    {
        ret = OB_INVALID_ARGUMENT;
    }
    else if (base_table_id_ == OB_INVALID_ID)
    {
        TBSYS_LOG(WARN, "must set table_id_ first. table_id_=%ld", table_id_);
        ret = OB_NOT_INIT;
    }
    else
    {
        // init rowkey_info
        const ObTableSchema * schema = NULL;
        if (NULL == (schema = context->schema_manager_->get_table_schema(base_table_id_)))
        {
            TBSYS_LOG(WARN, "fail to get table schema. table_id[%ld]", base_table_id_);
            ret = OB_ERROR;
        }
        else
        {
            cache_proxy_ = context->cache_proxy_;
            async_rpc_ = context->async_rpc_;
            session_info_ = context->session_info_;
            merge_service_ = context->merge_service_;
            // copy
            rowkey_info_ = schema->get_rowkey_info();
        }
    }
    obj_row_not_exists_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
    if (hint)
    {
        this->set_hint(*hint);
        if (hint_.read_method_ == ObSqlReadStrategy::USE_SCAN)
        {
            OB_ASSERT(NULL == scan_param_);
            scan_param_ = OB_NEW(ObSqlScanParam, ObModIds::OB_SQL_SCAN_PARAM);
            if (NULL == scan_param_)
            {
                TBSYS_LOG(WARN, "no memory");
                ret = OB_ALLOCATE_MEMORY_FAILED;
            }
            else
            {
                scan_param_->set_phy_plan(get_phy_plan());
                read_param_ = scan_param_;
            }
            //add liumz, [secondary_index, null_row bugfix]20170523:b
            if (OB_SUCCESS == ret && hint_.is_get_skip_empty_row_ && is_use_index_)
            {
                ObSqlExpression special_column;
                special_column.set_tid_cid(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
                if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_column_expr(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, special_column)))
                {
                    TBSYS_LOG(WARN, "fail to create column expression. ret=%d", ret);
                }
                else if (OB_SUCCESS != (ret = main_project.add_output_column(special_column)))
                {
                    TBSYS_LOG(WARN, "fail to add special is-row-empty-column to project. ret=%d", ret);
                }
                else
                {
                    TBSYS_LOG(DEBUG, "add special column to read param");
                }
            }
            //add:e
        }
        else if (hint_.read_method_ == ObSqlReadStrategy::USE_GET)
        {
            OB_ASSERT(NULL == get_param_);
            get_param_ = OB_NEW(ObSqlGetParam, ObModIds::OB_SQL_GET_PARAM);
            if (NULL == get_param_)
            {
                TBSYS_LOG(WARN, "no memory");
                ret = OB_ALLOCATE_MEMORY_FAILED;
            }
            else
            {
                get_param_->set_phy_plan(get_phy_plan());
                read_param_ = get_param_;
            }
            if (OB_SUCCESS == ret && hint_.is_get_skip_empty_row_)
            {
                ObSqlExpression special_column;
                special_column.set_tid_cid(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
                if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_column_expr(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, special_column)))
                {
                    TBSYS_LOG(WARN, "fail to create column expression. ret=%d", ret);
                }
                else if (OB_SUCCESS != (ret = get_param_->add_output_column(special_column)))
                {
                    TBSYS_LOG(WARN, "fail to add special is-row-empty-column to project. ret=%d", ret);
                }
                else
                {
                    TBSYS_LOG(DEBUG, "add special column to read param");
                }
            }
        }
        else
        {
            ret = OB_INVALID_ARGUMENT;
            TBSYS_LOG(WARN, "read method must be either scan or get. method=%d", hint_.read_method_);
        }
    }
    return ret;
}

//add fanqiushi_index
int ObRpcScan::set_main_tid(uint64_t main_tid)
{
    is_use_index_=true;
    get_next_row_count_=0;
    main_table_id_=main_tid;
    return OB_SUCCESS;
}
int ObRpcScan::set_is_use_index_for_storing(uint64_t main_tid,common::ObRowDesc &row_desc)
{
    UNUSED(row_desc);
    int ret=OB_SUCCESS;
    is_use_index_for_storing_=true;
    //get_next_row_count_=0;
    main_table_id_=main_tid;

    //del liumz, [optimize group_order by index]20170419:b
    //uint64_t table_id=OB_INVALID_ID;
    //uint64_t column_id=OB_INVALID_ID;
    //del:e
    cur_row_desc_for_storing.reset();
    /* del liumz, [optimize group_order by index]20170419
    for (int64_t i = 0; OB_SUCCESS == ret && i < row_desc.get_column_num(); i ++)
    {
        if (OB_SUCCESS != (ret = row_desc.get_tid_cid(i, table_id, column_id)))
        {
            TBSYS_LOG(WARN, "fail to get tid cid:ret[%d]", ret);
        }
        if (OB_SUCCESS == ret && OB_ACTION_FLAG_COLUMN_ID != column_id)
        {
            if (OB_SUCCESS != (ret = cur_row_desc_for_storing.add_column_desc(table_id, column_id)))
            {
                TBSYS_LOG(WARN, "fail to add column desc:ret[%d]", ret);
            }
        }
    }*/
    return ret;
}
int ObRpcScan::set_main_rowkey_info(common::ObRowkeyInfo RI)
{
    main_rowkey_info_=RI;
    return OB_SUCCESS;
}

int ObRpcScan::set_second_rowdesc(common::ObRowDesc *row_desc)
{
    int ret=OB_SUCCESS;
    uint64_t table_id=OB_INVALID_ID;
    uint64_t column_id=OB_INVALID_ID;
    second_row_desc_.reset();
    for (int64_t i = 0; OB_SUCCESS == ret && i < row_desc->get_column_num(); i ++)
    {
        if (OB_SUCCESS != (ret = row_desc->get_tid_cid(i, table_id, column_id)))
        {
            TBSYS_LOG(WARN, "fail to get tid cid:ret[%d]", ret);
        }
        if (OB_SUCCESS == ret && OB_ACTION_FLAG_COLUMN_ID != column_id)
        {
            if (OB_SUCCESS != (ret = second_row_desc_.add_column_desc(table_id, column_id)))
            {
                TBSYS_LOG(WARN, "fail to add column desc:ret[%d]", ret);
            }
        }
    }
    return ret;
}
//add:e

int ObRpcScan::cast_range(ObNewRange &range)
{
    int ret = OB_SUCCESS;
    bool need_buf = false;
    int64_t used_buf_len = 0;
    if (OB_SUCCESS != (ret = ob_cast_rowkey_need_buf(rowkey_info_, range.start_key_, need_buf)))
    {
        TBSYS_LOG(WARN, "err=%d", ret);
    }
    else if (need_buf)
    {
        if (NULL == start_key_buf_)
        {
            start_key_buf_ = (char*)ob_malloc(OB_MAX_ROW_LENGTH, ObModIds::OB_SQL_RPC_SCAN);
        }
        if (NULL == start_key_buf_)
        {
            TBSYS_LOG(ERROR, "no memory");
            ret = OB_ALLOCATE_MEMORY_FAILED;
        }
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        if (OB_SUCCESS != (ret = ob_cast_rowkey(rowkey_info_, range.start_key_,
                                                start_key_buf_, OB_MAX_ROW_LENGTH, used_buf_len)))
        {
            TBSYS_LOG(WARN, "failed to cast rowkey, err=%d", ret);
        }
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        if (OB_SUCCESS != (ret = ob_cast_rowkey_need_buf(rowkey_info_, range.end_key_, need_buf)))
        {
            TBSYS_LOG(WARN, "err=%d", ret);
        }
        else if (need_buf)
        {
            if (NULL == end_key_buf_)
            {
                end_key_buf_ = (char*)ob_malloc(OB_MAX_ROW_LENGTH, ObModIds::OB_SQL_RPC_SCAN);
            }
            if (NULL == end_key_buf_)
            {
                TBSYS_LOG(ERROR, "no memory");
                ret = OB_ALLOCATE_MEMORY_FAILED;
            }
        }
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            if (OB_SUCCESS != (ret = ob_cast_rowkey(rowkey_info_, range.end_key_,
                                                    end_key_buf_, OB_MAX_ROW_LENGTH, used_buf_len)))
            {
                TBSYS_LOG(WARN, "failed to cast rowkey, err=%d", ret);
            }
        }
    }
    return ret;
}

int ObRpcScan::create_scan_param(ObSqlScanParam &scan_param)
{
    int ret = OB_SUCCESS;
    ObNewRange range;
    // until all columns and filters are set, we could know the exact range
    //modify by fanqiushi_index
    if(is_use_index_)
    {
        ret=fill_read_param_for_first_scan(scan_param);   //回表情况下，第一次scan原表的时候把对表别名的处理
    }
    else
    {
        ret = fill_read_param(scan_param);
    }

    if(OB_SUCCESS == ret)
    {
        if (OB_SUCCESS != (ret = cons_scan_range(range)))
        {
            TBSYS_LOG(WARN, "fail to construct scan range. ret=%d", ret);
        }
        // TODO: lide.wd 将range 深拷贝到ObSqlScanParam内部的buffer_pool_中
        else if (OB_SUCCESS != (ret = scan_param.set_range(range)))
        {
            TBSYS_LOG(WARN, "fail to set range to scan param. ret=%d", ret);
        }
        //add zhujun [transaction read uncommit]2016/3/24
        else if (OB_SUCCESS != (ret = scan_param.set_trans_id(trans_id_)))
        {
            TBSYS_LOG(WARN, "fail to set trans_id to scan param. ret=%d", ret);
        }
        //add:e
    }
    //modify:e
    TBSYS_LOG(DEBUG, "scan_param=%s", to_cstring(scan_param));
    TBSYS_LOG(TRACE, "dump scan range: %s", to_cstring(range));
    return ret;
}
//add  by fyd IN_EXPR  [PrefixKeyQuery_for_INstmt] 2014.4.1:b
/***
 *  当上一个range 完成查询之后，将会调用这个函数，重新设置 查询参数，range 等信息
 *
 */
int ObRpcScan::init_next_scan_param(ObNewRange &range)
{
    int ret = OB_SUCCESS;
    //ObNewRange range;
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
    range.table_id_ = base_table_id_;
    OB_ASSERT(rowkey_info_.get_size() <= OB_MAX_ROWKEY_COLUMN_NUMBER);
    // until all columns and filters are set, we could know the exact range
    // modify by fanqiushi_index
    if(is_use_index_)
    {
        ret=fill_read_param_for_first_scan(*scan_param_);
    }
    else
    {
        ret = fill_read_param(*scan_param_);
    }
    if(OB_SUCCESS != ret)
    {
        TBSYS_LOG(WARN, "fail to fill read param to scan param. ret=%d", ret);
    }
    //modify:e
    else if (OB_SUCCESS != (ret = scan_param_->set_range(range)))
    {
        TBSYS_LOG(WARN, "fail to set range to scan param. ret=%d", ret);
    }
    else if(OB_SUCCESS != (ret = sql_scan_request_->set_request_param_only(*scan_param_)))
    {
        TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",hint_.max_parallel_count, ret);
    }
    else
    {
        TBSYS_LOG(DEBUG, "next scan_param=%s", to_cstring(*scan_param_));
    }
    TBSYS_LOG(TRACE, "dump scan range: %s", to_cstring(range));
    return ret;
}
//add:e

int ObRpcScan::create_get_param(ObSqlGetParam &get_param)
{
    int ret = OB_SUCCESS;

    if (OB_SUCCESS != (ret = fill_read_param(get_param)))
    {
        TBSYS_LOG(WARN, "fail to fill read param to scan param. ret=%d", ret);
    }

    if (OB_SUCCESS == ret)
    {
        if (OB_SUCCESS != (ret = cons_get_rows(get_param)))
        {
            TBSYS_LOG(WARN, "fail to construct scan range. ret=%d", ret);
        }
    }
    //add zhujun [transaction read uncommit]2016/3/24
    if (OB_SUCCESS != (ret = get_param.set_trans_id(trans_id_)))
    {
        TBSYS_LOG(WARN, "fail to set trans_id to get param. ret=%d", ret);
    }
    //add:e

    TBSYS_LOG(DEBUG, "get_param=%s", to_cstring(get_param));
    return ret;
}

//add fanqiushi_index
int ObRpcScan::reset_read_param_for_index()  //重新设置一下read_param。因为回表情况下，第一次scan索引表的时候以及设置了一个read_param。在第二次get原表的时候，要把这个read_param清空，再重新赋值。
{
    int ret = OB_SUCCESS;
    read_param_->reset_project_and_filter();
    read_param_->set_project(main_project);
    //TBSYS_LOG(ERROR, "test::fanqs,,main_project4444=%s", to_cstring(main_project));
    read_param_->set_filter(main_filter_);
    //TBSYS_LOG(ERROR, "test::fanqs,,read_param_15662=%s", to_cstring(main_filter_));
    return ret;
}

int ObRpcScan::create_get_param_for_index(ObSqlGetParam &get_param) //生成第二次get原表时的get_param
{
  int ret = OB_SUCCESS;
  // TBSYS_LOG(ERROR, "test::fanqs,,enter create_get_param_for_index,,read_param_->has_filter()=%d,,read_param_=%s",read_param_->has_filter(),to_cstring(*read_param_));
  if (OB_SUCCESS != (ret = cons_get_rows_for_index(get_param)))  //构造主键的范围
  {
    if (OB_ITER_END != ret)//add liumz, [-8 log too much]20161230
    {
      TBSYS_LOG(WARN, "fail to construct scan range. ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    //TBSYS_LOG(ERROR, "test::fanqs,,,scan_param=%s", to_cstring(*scan_param_));

    read_param_->reset();
    get_param.set_phy_plan(get_phy_plan());
    //read_param_ = get_param_;

    if (OB_SUCCESS != (ret = fill_read_param_for_index(get_param)))
    {
      TBSYS_LOG(WARN, "fail to fill read param to scan param. ret=%d", ret);
    }
    //add zhujun [transaction read uncommit]2016/3/28
    else if (OB_SUCCESS != (ret = get_param.set_trans_id(trans_id_)))
    {
      TBSYS_LOG(WARN, "fail to set trans_id to get param. ret=%d", ret);
    }
    //add:e
    else
    {
      read_param_ = &get_param;
    }
  }
  //mod dragon [Bugfix_dg LMZ] 2016-9-8 10:40:28
  //new
  if(OB_SUCCESS == ret)
  {
    reset_read_param_for_index();
  }
  /*---old---
    reset_read_param_for_index();
    ---old---*/
  //mod 2016-9-8e
  return ret;
}

int ObRpcScan::fill_read_param_for_index(ObSqlReadParam &dest_param)
{
    int ret = OB_SUCCESS;
    ObObj val;
    OB_ASSERT(NULL != session_info_);
    if (OB_SUCCESS == ret)
    {
        dest_param.set_is_result_cached(false);
        if (OB_SUCCESS != (ret = dest_param.set_table_id(table_id_, main_table_id_)))
        {
            TBSYS_LOG(WARN, "fail to set table id and scan range. ret=%d", ret);
        }
        //add fanqiushi_index
        //TBSYS_LOG(ERROR, "test::fanqs,,table_id_=%ld,,base_table_id_=%ld,main_table_id_=%ld", table_id_,base_table_id_,main_table_id_);
        //add:e
    }

    return ret;
}


int ObRpcScan::fill_read_param_for_first_scan(ObSqlReadParam &dest_param)
{
    int ret = OB_SUCCESS;
    //ObObj val;
    OB_ASSERT(NULL != session_info_);
    if (OB_SUCCESS == ret)
    {
        dest_param.set_is_result_cached(false);
        if (OB_SUCCESS != (ret = dest_param.set_table_id(base_table_id_, base_table_id_)))
        {
            TBSYS_LOG(WARN, "fail to set table id and scan range. ret=%d", ret);
        }
        //add fanqiushi_index
        //TBSYS_LOG(ERROR, "test::fanqs,,table_id_=%ld,,base_table_id_=%ld", table_id_,base_table_id_);
        //add:e
    }

    return ret;
}
//add:e
int ObRpcScan::fill_read_param(ObSqlReadParam &dest_param)
{
    int ret = OB_SUCCESS;
    ObObj val;
    OB_ASSERT(NULL != session_info_);
    if (OB_SUCCESS == ret)
    {
        dest_param.set_is_result_cached(false);
        if (OB_SUCCESS != (ret = dest_param.set_table_id(table_id_, base_table_id_)))
        {
            TBSYS_LOG(WARN, "fail to set table id and scan range. ret=%d", ret);
        }
    }

    return ret;
}

namespace oceanbase{
namespace sql{
REGISTER_PHY_OPERATOR(ObRpcScan, PHY_RPC_SCAN);
}
}

int64_t ObRpcScan::to_string(char* buf, const int64_t buf_len) const
{
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "RpcScan(");
    pos += hint_.to_string(buf+pos, buf_len-pos);
    databuff_printf(buf, buf_len, pos, ", ");
    pos += read_param_->to_string(buf+pos, buf_len-pos);
    databuff_printf(buf, buf_len, pos, ")");
    //databuff_printf(buf, buf_len, pos, "RpcScan(row_desc=");
    //pos += cur_row_desc_.to_string(buf+pos, buf_len-pos);
    //databuff_printf(buf, buf_len, pos, ", ");
    //pos += sql_read_strategy_.to_string(buf+pos, buf_len-pos);
    //databuff_printf(buf, buf_len, pos, ", read_param=");
    //pos += hint_.to_string(buf+pos, buf_len-pos);
    //databuff_printf(buf, buf_len, pos, ", ");
    //pos += read_param_->to_string(buf+pos, buf_len-pos);
    //databuff_printf(buf, buf_len, pos, ")");
    //add zhujun [transaction read uncommit] 2016/4/5
    pos += trans_id_.to_string(buf+pos, buf_len-pos);
    databuff_printf(buf, buf_len, pos, ")");
    //add:e
    return pos;
}

void ObRpcScan::set_hint(const common::ObRpcScanHint &hint)
{
    hint_ = hint;
    // max_parallel_count
    if (hint_.max_parallel_count <= 0)
    {
        hint_.max_parallel_count = OB_DEFAULT_MAX_PARALLEL_COUNT;
    }
    // max_memory_limit
    if (hint_.max_memory_limit < 1024 * 1024 * 2)
    {
        hint_.max_memory_limit = 1024 * 1024 * 2;
    }
}
int ObRpcScan::open()
{
    int ret = OB_SUCCESS;
    if (NULL == (sql_scan_request_ = ObMergeServerMain::get_instance()->get_merge_server().get_scan_req_pool().alloc()))
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "alloc scan request from scan request pool failed, ret=%d", ret);
    }
    else if (NULL == (sql_get_request_ = ObMergeServerMain::get_instance()->get_merge_server().get_get_req_pool().alloc()))
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "alloc get request from get request pool failed, ret=%d", ret);
    }
    // del by maosy [Delete_Update_Function_isolation_RC] 20161218
    //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//    else if (is_data_mark_param_valid_
//             && !is_data_mark_cids_added_
//             && OB_SUCCESS != (ret = add_data_mark_cids_()))
//    {
//        TBSYS_LOG(WARN,"fail to add data mark cids!ret=%d",ret);
//    }
    //add duyr 20160531:e
      // del by maosy
    else
    {
        // del by maosy [Delete_Update_Function_isolation_RC] 20161218
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//        if (is_data_mark_param_valid_)
//        {
//            is_data_mark_cids_added_ = true;
//        }
        //add duyr 20160531:e
        // del by maosy
        sql_scan_request_->set_tablet_location_cache_proxy(cache_proxy_);
        sql_get_request_->set_tablet_location_cache_proxy(cache_proxy_);
        sql_scan_request_->set_merger_async_rpc_stub(async_rpc_);
        sql_get_request_->set_merger_async_rpc_stub(async_rpc_);
        if (NULL != session_info_)
        {
            ///  query timeout for sql level
            ObObj val;
            int64_t query_timeout = 0;
            int64_t parallel_count = 0;
            if (OB_SUCCESS == session_info_->get_sys_variable_value(ObString::make_string(OB_QUERY_TIMEOUT_PARAM), val))
            {
                if (OB_SUCCESS != val.get_int(query_timeout))
                {
                    TBSYS_LOG(WARN, "fail to get query timeout from session, ret=%d", ret);
                    query_timeout = 0; // use default
                }
            }
            if (OB_SUCCESS == session_info_->get_sys_variable_value(ObString::make_string(OB_MAX_REQUEST_PARALLEL_COUNT), val))
            {
                if (OB_SUCCESS != val.get_int(parallel_count))
                {
                    TBSYS_LOG(WARN, "fail to get parallel count from session, ret=%d", ret);
                    parallel_count = 0; // use default
                }
            }


            if (OB_APP_MIN_TABLE_ID > base_table_id_)
            {
                // internal table
                // BUGFIX: this timeout value should not be larger than the plan timeout
                // (see ob_transformer.cpp/int ObResultSet::open())
                // bug ref: http://bugfree.corp.taobao.com/bug/252871
                hint_.timeout_us = std::max(query_timeout, OB_DEFAULT_STMT_TIMEOUT); // prevent bad config, min timeout=3sec
            }
            else
            {
                // app table
                if (query_timeout > 0)
                {
                    hint_.timeout_us = query_timeout;
                }
                else
                {
                    // use default hint_.timeout_us, usually 10 sec
                }
                //add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140713 :b
                if(my_phy_plan_->is_need_extend_time() && hint_.timeout_us < OB_DEFAULT_STMT_TIMEOUT*60L)
                {
                    hint_.timeout_us = OB_DEFAULT_STMT_TIMEOUT*60L;
                }
                //add :e
            }
            // set parallel
            if (parallel_count > 0)
            {
                hint_.max_parallel_count = parallel_count;
                hint_.max_parallel_count = 500;
            }
            else
            {
                // use default hint_.max_parallel_count
            }
            TBSYS_LOG(DEBUG, "query timeout is %ld, parallel is %ld", hint_.timeout_us, hint_.max_parallel_count);
        }

        timeout_us_ = hint_.timeout_us;
        OB_ASSERT(my_phy_plan_);
        FILL_TRACE_LOG(" hint.read_consistency=%s ", get_consistency_level_str(hint_.read_consistency_));
        if (hint_.read_consistency_ == common::FROZEN)
        {
            ObVersion frozen_version = my_phy_plan_->get_curr_frozen_version();
            read_param_->set_data_version(frozen_version);
            FILL_TRACE_LOG("static_data_version = %s", to_cstring(frozen_version));
        }
        read_param_->set_is_only_static_data(hint_.read_consistency_ == STATIC);
        //add zhujun [transaction read uncommit]2016/5/10
        if(trans_id_.is_valid())
        {
            read_param_->set_is_read_consistency(true);
        }
        else
        {
            read_param_->set_is_read_consistency(hint_.read_consistency_ == STRONG);
        }
        //add:e
        FILL_TRACE_LOG("only_static = %c", read_param_->get_is_only_static_data() ? 'Y' : 'N');

        if (NULL == cache_proxy_ || NULL == async_rpc_)
        {
            ret = OB_NOT_INIT;
        }

        if (OB_SUCCESS == ret)
        {
            TBSYS_LOG(DEBUG, "read_method_ [%s]", hint_.read_method_ == ObSqlReadStrategy::USE_SCAN ? "SCAN" : "GET");
            // common initialization
            cur_row_.set_row_desc(cur_row_desc_);
            //add fanqiushi_index
            if(is_use_index_for_storing_)
            {
                cur_row_for_storing_.set_row_desc(cur_row_desc_for_storing);
                //TBSYS_LOG(ERROR,"test::fanqs,,11,cur_row_desc_for_storing=%s",to_cstring(cur_row_desc_for_storing));
            }
            //add:e
            FILL_TRACE_LOG("open %s", to_cstring(cur_row_desc_));
        }
        // update语句的need_cache_frozen_data_ 才会被设置为true
        ObFrozenDataCache & frozen_data_cache = ObMergeServerMain::
                get_instance()->get_merge_server().get_frozen_data_cache();
        if (frozen_data_cache.get_in_use() && need_cache_frozen_data_ && (hint_.read_consistency_ == FROZEN))
        {
            int64_t size = 0;
            if (OB_SUCCESS != (ret = read_param_->set_table_id(table_id_, base_table_id_)))
            {
                TBSYS_LOG(ERROR, "set_table_id error, ret: %d", ret);
            }
            else
            {
                size = read_param_->get_serialize_size();
            }
            if (OB_SUCCESS != ret)
            {}
            else if (OB_SUCCESS != (ret = frozen_data_key_buf_.alloc_buf(size)))
            {
                TBSYS_LOG(ERROR, "ObFrozenDataKeyBuf alloc_buf error, ret: %d", ret);
            }
            else if (OB_SUCCESS != (ret = read_param_->serialize(frozen_data_key_buf_.buf,
                                                                 frozen_data_key_buf_.buf_len, frozen_data_key_buf_.pos)))
            {
                TBSYS_LOG(ERROR, "ObSqlReadParam serialize error, ret: %d", ret);
            }
            else
            {
                frozen_data_key_.frozen_version = my_phy_plan_->get_curr_frozen_version();
                frozen_data_key_.param_buf = frozen_data_key_buf_.buf;
                frozen_data_key_.len = frozen_data_key_buf_.pos;
                ret = frozen_data_cache.get(frozen_data_key_, cached_frozen_data_);
                if (OB_SUCCESS != ret)
                {
                    TBSYS_LOG(ERROR, "ObFrozenDataCache get_frozen_data error, ret: %d", ret);
                }
                else if (cached_frozen_data_.has_data())
                {
                    OB_STAT_INC(SQL, SQL_UPDATE_CACHE_HIT);
                    cached_frozen_data_.set_row_desc(cur_row_desc_);
                }
                else
                {
                    OB_STAT_INC(SQL, SQL_UPDATE_CACHE_MISS);
                }
            }
        }

        if (!cached_frozen_data_.has_data())
        {
            // Scan
            if (OB_SUCCESS == ret && hint_.read_method_ == ObSqlReadStrategy::USE_SCAN)
            {
                is_scan_ = true;
                if (OB_SUCCESS != (ret = sql_scan_request_->initialize()))
                {
                    TBSYS_LOG(WARN, "initialize sql_scan_request failed, ret=%d", ret);
                }
                else
                {
                    sql_scan_request_->alloc_request_id();
                    //  modified by fyd  PrefixKeyQuery_for_INstmt  2014.4.1:b
                    //del:b
                    //          if (OB_SUCCESS != (ret = sql_scan_request_->init(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_SCAN)))
                    //          {
                    //            TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
                    //          }
                    //del:e
                    if (OB_SUCCESS != (ret = sql_scan_request_->init_ex(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_SCAN, this)))
                    {
                        TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
                    }
                    //mod:e
                    else if (OB_SUCCESS != (ret = create_scan_param(*scan_param_)))
                    {
                        TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
                    }
                }

                if (OB_SUCCESS == ret)
                {
                    sql_scan_request_->set_timeout_percent((int32_t)merge_service_->get_config().timeout_percent);
                    if(OB_SUCCESS != (ret = sql_scan_request_->set_request_param(*scan_param_, hint_)))
                    {
                        TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
                                  hint_.max_parallel_count, ret);
                    }
                }
            }
            // Get
            if (OB_SUCCESS == ret && hint_.read_method_ == ObSqlReadStrategy::USE_GET)
            {
                // insert and select
                is_get_ = true;
                get_row_desc_.reset();
                sql_get_request_->alloc_request_id();
                if (OB_SUCCESS != (ret = sql_get_request_->init(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_GET)))
                {
                    TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
                }
                else if (OB_SUCCESS != (ret = create_get_param(*get_param_)))
                {
                    TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
                }
                else
                {
                    // 暂时只处理单行
                    // 只有insert语句的cache_bloom_filter_才会设置为true
                    ObInsertCache & insert_cache = ObMergeServerMain::get_instance()->get_merge_server().get_insert_cache();
                    if (insert_cache.get_in_use() && cache_bloom_filter_ && get_param_->get_row_size() == 1)
                    {
                        int err = OB_SUCCESS;
                        ObTabletLocationList loc_list;
                        loc_list.set_buffer(tablet_location_list_buf_);
                        const ObRowkey *rowkey = (*get_param_)[0];
                        if (OB_SUCCESS == (err = cache_proxy_->get_tablet_location(get_param_->get_table_id(), *rowkey, loc_list)))
                        {
                            const ObNewRange &tablet_range = loc_list.get_tablet_range();
                            InsertCacheKey key;
                            key.range_ = tablet_range;
                            int64_t version = my_phy_plan_->get_curr_frozen_version();
                            key.tablet_version_ = (reinterpret_cast<ObVersion*>(&version))->major_;
                            if (OB_SUCCESS == (err = insert_cache.get(key, value_)))
                            {
                                if (!value_.bf_->may_contain(*rowkey))
                                {
                                    OB_STAT_INC(SQL,SQL_INSERT_CACHE_HIT);
                                    // rowkey not exists
                                    rowkey_not_exists_ = true;
                                }
                                else
                                {
                                    OB_STAT_INC(SQL,SQL_INSERT_CACHE_MISS);
                                }
                                insert_cache_need_revert_ = true;
                            }
                            else if (OB_ENTRY_NOT_EXIST == err)
                            {
                                OB_STAT_INC(SQL,SQL_INSERT_CACHE_MISS);
                                // go on
                                char *buff = reinterpret_cast<char*>(ob_tc_malloc(
                                                                         sizeof(ObBloomFilterTask) + rowkey->get_deep_copy_size(),
                                                                         ObModIds::OB_MS_UPDATE_BLOOM_FILTER));
                                if (buff == NULL)
                                {
                                    err = OB_ALLOCATE_MEMORY_FAILED;
                                    TBSYS_LOG(ERROR, "malloc failed, ret=%d", err);
                                }
                                else
                                {
                                    ObBloomFilterTask *task = new (buff) ObBloomFilterTask();
                                    task->table_id = get_param_->get_table_id();
                                    char *obj_buf = buff + sizeof(ObBloomFilterTask);
                                    // deep copy task->key
                                    common::AdapterAllocator alloc;
                                    alloc.init(obj_buf);
                                    if (OB_SUCCESS != (err = rowkey->deep_copy(task->rowkey, alloc)))
                                    {
                                        TBSYS_LOG(ERROR, "deep copy rowkey failed, err=%d", err);
                                    }
                                    else
                                    {
                                        err = ObMergeServerMain::get_instance()->get_merge_server().get_bloom_filter_task_queue_thread().push(task);
                                        if (OB_SUCCESS != err && OB_TOO_MANY_BLOOM_FILTER_TASK != err)
                                        {
                                            TBSYS_LOG(ERROR, "push task to bloom_filter_task_queue failed,ret=%d", err);
                                        }
                                        else if (OB_TOO_MANY_BLOOM_FILTER_TASK == err)
                                        {
                                            TBSYS_LOG(DEBUG, "push task to bloom_filter_task_queue failed, ret=%d", err);
                                        }
                                        else
                                        {
                                            TBSYS_LOG(DEBUG, "PUSH TASK SUCCESS");
                                        }
                                    }
                                    if (OB_SUCCESS != err)
                                    {
                                        task->~ObBloomFilterTask();
                                        ob_tc_free(reinterpret_cast<void*>(task));
                                    }
                                }
                            }// OB_ENTRY_NOT_EXIST == err
                            else
                            {
                                //go on
                                TBSYS_LOG(ERROR, "get from insert cache failed, err=%d", err);
                            }
                        }
                        else
                        {
                            //go on
                            TBSYS_LOG(ERROR, "get tablet location failed, table_id=%lu,rowkey=%s, err=%d", get_param_->get_table_id(), to_cstring(*rowkey), err);
                        }
                    }
                }
                if (!rowkey_not_exists_)
                {
                    if (OB_SUCCESS != (ret = cons_row_desc(*get_param_, get_row_desc_)))
                    {
                        TBSYS_LOG(WARN, "fail to get row desc:ret[%d]", ret);
                    }
                    else if (OB_SUCCESS != (ret = sql_get_request_->set_row_desc(get_row_desc_)))
                    {
                        TBSYS_LOG(WARN, "fail to set row desc:ret[%d]", ret);
                    }
                    else if(OB_SUCCESS != (ret = sql_get_request_->set_request_param(*get_param_, timeout_us_, hint_.max_parallel_count)))
                    {
                        TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
                                  hint_.max_parallel_count, ret);
                    }
                    if (OB_SUCCESS == ret)
                    {
                        sql_get_request_->set_timeout_percent((int32_t)merge_service_->get_config().timeout_percent);
                        if (OB_SUCCESS != (ret = sql_get_request_->open()))
                        {
                            TBSYS_LOG(WARN, "fail to open get request. ret=%d", ret);
                        }
                    }
                }
            }//Get
        }
        if (OB_SUCCESS != ret)
        {
            is_rpc_failed_ = true;
        }
    }
    //add wanglei [semi join update] 20160405:b
    if(ret == OB_SUCCESS && is_use_semi_join_)
    {
      //add dragon [Bugfix 1224] 2016-9-5 13:11:39
      TBSYS_LOG(DEBUG, "be4 we change, right_table_id = %ld", right_table_id_);
      if(is_use_index_for_storing_) //not back to data table
      {
        if(right_table_id_ != (int64_t)table_id_) right_table_id_ = table_id_;
      }
      else if(is_use_index_) //back
      {
        right_table_id_ = (int64_t)base_table_id_;
      }
      //add 2016-9-6e

      TBSYS_LOG(DEBUG,"wanglei::table_id_ = %ld",table_id_);
      TBSYS_LOG(DEBUG,"wanglei::base_table_id_ = %ld",base_table_id_);
      TBSYS_LOG(DEBUG,"wanglei::main_table_id_ = %ld",main_table_id_);
      TBSYS_LOG(DEBUG,"wanglei::right_table_id = %ld",right_table_id_);
      TBSYS_LOG(DEBUG,"wanglei::main_filter_ = %s",to_cstring(main_filter_));
      TBSYS_LOG(DEBUG,"wanglei::index_filter_ = %s",to_cstring(index_filter_));
      //for memory leak check:b
      //int64_t usage = 0;
      //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION);
      //TBSYS_LOG(INFO,"wanglei::OB_SEMI_JOIN_SQL_EXPRESSION before malloc usage = %ld",usage);
      //for memory leak check:e
      hint_.max_parallel_count = 500;
      const common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > &columns = read_param_->get_project ().get_output_columns();
      for (int64_t i = 0; OB_SUCCESS == ret && i < columns.count(); i ++)
      {
        const ObSqlExpression &expr = columns.at(i);
        if (OB_SUCCESS != (ret = project_raw.add_output_column (expr)))
        {
          TBSYS_LOG(WARN, "fail to add column desc:ret[%d]", ret);
        }
      }
    }
    //add wanglei [semi join update] 20160405:e
    return ret;
}

//add zhujun [transaction read uncommit]2016/3/24
int ObRpcScan::set_trans_id(const common::ObTransID &trans_id) {
    trans_id_ = trans_id;
    return OB_SUCCESS;

}
//add:e

void ObRpcScan::destroy()
{
    sql_read_strategy_.destroy();
    if (NULL != start_key_buf_)
    {
        ob_free(start_key_buf_, ObModIds::OB_SQL_RPC_SCAN);
        start_key_buf_ = NULL;
    }
    if (NULL != end_key_buf_)
    {
        ob_free(end_key_buf_, ObModIds::OB_SQL_RPC_SCAN);
        end_key_buf_ = NULL;
    }
    if (NULL != get_param_)
    {
        get_param_->~ObSqlGetParam();
        ob_free(get_param_);
        get_param_ = NULL;
    }
    if (NULL != scan_param_)
    {
        scan_param_->~ObSqlScanParam();
        ob_free(scan_param_);
        scan_param_ = NULL;
    }

    cleanup_request();
}

int ObRpcScan::close()
{
    int ret = OB_SUCCESS;
    ObFrozenDataCache & frozen_data_cache = ObMergeServerMain::
            get_instance()->get_merge_server().get_frozen_data_cache();
    if (cached_frozen_data_.has_data())
    {
        ret = frozen_data_cache.revert(cached_frozen_data_);
        if (OB_SUCCESS != ret)
        {
            TBSYS_LOG(ERROR, "ObFrozenDataCache revert error, ret: %d", ret);
        }
    }
    else if (frozen_data_cache.get_in_use() && need_cache_frozen_data_ && (hint_.read_consistency_ == FROZEN))
    {
        if (!is_rpc_failed_)
        {
            int err = frozen_data_cache.put(frozen_data_key_, frozen_data_);
            if (OB_SUCCESS != err)
            {
                TBSYS_LOG(ERROR, "ObFrozenDataCache put error, err: %d", err);
            }
        }
        frozen_data_.reuse();
    }
    if (is_scan_ || is_use_semi_join_) //add wanglei [semi join update] 20160510
    {
        sql_scan_request_->close();
        sql_scan_request_->reset();
        if (NULL != scan_param_)
        {
            scan_param_->reset_local();
        }
        is_scan_ = false;
    }
    if (is_get_||is_use_index_)  //modify by fanqiushi_index
    {
        //因为当is_use_index_为true的时候，即使is_get_为false，我也会使用到sql_get_request_。所以这里要把它释放掉
        sql_get_request_->close();
        sql_get_request_->reset();
        if (NULL != get_param_)
        {
            get_param_->reset_local();
        }
        is_get_ = false;
        //add fanqiushi_index
        is_use_index_=false;
        //add:e
        tablet_location_list_buf_.reuse();
        rowkey_not_exists_ = false;
        if (insert_cache_need_revert_)
        {
            if (OB_SUCCESS == (ret = ObMergeServerMain::get_instance()->get_merge_server().get_insert_cache().revert(value_)))
            {
                value_.reset();
            }
            else
            {
                TBSYS_LOG(WARN, "revert bloom filter failed, ret=%d", ret);
            }
            insert_cache_need_revert_ = false;
        }
        //rowkeys_exists_.clear();
    }
    insert_cache_iter_counter_ = 0;
    is_confilct_.clear ();//add wanglei [semi join update] 20160511
    //add wanglei [semi join update] 20160601:b
//    if(is_right_table_)
//    {
//        if(project_raw != NULL)
//        {
//            project_raw->~ObProject ();
//            ob_free (project_raw);
//            project_raw = NULL;
//        }
//        if(filter_raw != NULL)
//        {
//            filter_raw->~ObFilter ();
//            ob_free (filter_raw);
//            filter_raw = NULL;
//        }
//    }
    //add wanglei [semi join update] 20160601:e
    //for memory leak check:b
    //int64_t usage = 0;
    //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION);
    //TBSYS_LOG(INFO,"wanglei::CLOSE OB_SEMI_JOIN_SQL_EXPRESSION clear malloc usage = %ld",usage);
    //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
    //TBSYS_LOG(INFO,"wanglei::CLOSE OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER free malloc usage = %ld",usage);
    //for memory leak check:e
    cleanup_request();
    return ret;
}
void ObRpcScan::cleanup_request()
{
    int tmp_ret = OB_SUCCESS;
    if (sql_scan_request_ != NULL)
    {
        if (OB_SUCCESS != (tmp_ret = ObMergeServerMain::get_instance()->get_merge_server().get_scan_req_pool().free(sql_scan_request_)))
        {
            TBSYS_LOG(WARN, "free scan request back to scan req pool failed, ret=%d", tmp_ret);
        }
        sql_scan_request_ = NULL;
    }
    if (sql_get_request_ != NULL)
    {
        if (OB_SUCCESS != (tmp_ret = ObMergeServerMain::get_instance()->get_merge_server().get_get_req_pool().free(sql_get_request_)))
        {
            TBSYS_LOG(WARN, "free get request back to get req pool failed, ret=%d", tmp_ret);
        }
        sql_get_request_ = NULL;
    }
}

bool ObRpcScan::get_is_use_index() const
{
  return is_use_index_;
}

int ObRpcScan::get_row_desc(const common::ObRowDesc *&row_desc) const
{
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(base_table_id_ <= 0 || 0 >= cur_row_desc_.get_column_num()))
    {
        TBSYS_LOG(ERROR, "not init, tid=%lu, column_num=%ld", base_table_id_, cur_row_desc_.get_column_num());
        ret = OB_NOT_INIT;
    }
    //modify by fanqiushi_index
    /*else
  {
    row_desc = &cur_row_desc_;
  }*/
    else if(is_use_index_)
    {
        row_desc = &second_row_desc_;
    }
    else if(is_use_index_for_storing_)
    {
        row_desc = &cur_row_desc_for_storing;
        //TBSYS_LOG(ERROR,"test::fanqs,,row_desc=%s",to_cstring(*row_desc));
    }
    else
    {
        row_desc = &cur_row_desc_;
    }
    //modify:e
    return ret;
}
//add fanqiushi_index
int ObRpcScan::get_other_row_desc(const common::ObRowDesc *&row_desc) //获得第二次get时的行描述
{
    row_desc = &get_row_desc_;
    return OB_SUCCESS;
}
//add:e
int ObRpcScan::get_next_row(const common::ObRow *&row)
{
    int ret = OB_SUCCESS;
    if (rowkey_not_exists_)
    {
        if (insert_cache_iter_counter_ == 0)
        {
            row_not_exists_.set_row_desc(cur_row_desc_);
            int err = OB_SUCCESS;
            if (OB_SUCCESS != (err = row_not_exists_.set_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, obj_row_not_exists_)))
            {
                TBSYS_LOG(ERROR, "set cell failed, err=%d", err);
            }
            else
            {
                row = &row_not_exists_;
                insert_cache_iter_counter_ ++;
                ret = OB_SUCCESS;
            }
        }
        else if (insert_cache_iter_counter_ == 1)
        {
            ret = OB_ITER_END;
        }
    }
    else
    {
        if (cached_frozen_data_.has_data())
        {
            ret = cached_frozen_data_.get_next_row(row);
            if (OB_SUCCESS != ret && OB_ITER_END != ret)
            {
                TBSYS_LOG(ERROR, "ObCachedFrozenData get_next_row error, ret: %d", ret);
            }
        }
        else
        {
            if (ObSqlReadStrategy::USE_SCAN == hint_.read_method_)
            {
                ret = get_next_compact_row(row); // 可能需要等待CS返回
            }
            else if (ObSqlReadStrategy::USE_GET == hint_.read_method_)
            {
                ret = sql_get_request_->get_next_row(cur_row_);
                //        if(ret == OB_SUCCESS)
                //            TBSYS_LOG(ERROR,"wanglei::sql_get_request_ get_next_row= %s",to_cstring(cur_row_));
            }
            else
            {
                TBSYS_LOG(WARN, "not init. read_method_=%d", hint_.read_method_);
                ret = OB_NOT_INIT;
            }
            //modify fanqiushi_index
            //row = &cur_row_;    //old code
            if(!is_use_index_for_storing_)
            {
                row = &cur_row_;
            }
            //modify:e
            if (ObMergeServerMain::get_instance()->get_merge_server().get_frozen_data_cache().get_in_use()
                    && need_cache_frozen_data_ && (hint_.read_consistency_ == FROZEN) && OB_SUCCESS == ret)
            {
                int64_t cur_size_counter;
                ret = frozen_data_.add_row(*row, cur_size_counter);
                if (OB_SUCCESS != ret)
                {
                    TBSYS_LOG(ERROR, "ObRowStore add_row error, ret: %d", ret);
                }
            }
        }
    }
    if (OB_SUCCESS != ret && OB_ITER_END != ret)
    {
        is_rpc_failed_ = true;
    }
    // del by maosy [Delete_Update_Function_isolation_RC] 20161218
    //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//    if (NULL != row && is_data_mark_param_valid_)
//        TBSYS_LOG(DEBUG,"mul_del::debug,final_rpc_row=[%s],ret=%d",
//                  to_cstring(*row),ret);
    //add duyr 20160531:e
      // del by maosy

    return ret;
}


/**
 * 函数功能： 从scan_event中获取一行数据
 * 说明：
 * wait的功能：从finish_queue中阻塞地pop出一个事件（如果没有事件则阻塞）， 然后调用process_result()处理事件
 */
//add fanqiushi_index
int ObRpcScan::get_next_compact_row_for_index(const common::ObRow *&row)  //第一次scan索引表，返回索引表的数据
{
    //add fanqiushi_index
    //const common::ObRowDesc *tmp_desc =cur_row_.get_row_desc();
    // char buf[1000];
    //tmp_desc->to_string(buf,1000);
    //TBSYS_LOG(ERROR, "test::fanqs,,enter this,,tmp_desc=%s,,cur_row_desc=%s",buf,to_cstring(cur_row_desc_));
    cur_row_.set_row_desc(cur_row_desc_);
    //add:e
    int ret = OB_SUCCESS;
    if(is_use_semi_join_ && is_right_table_) //add wanglei [semi join update] 20160509
    {
        ret = get_table_row_with_more_scan (row);
    }
    else
    {
        bool can_break = false;
        int64_t remain_us = 0;
        row = NULL;
        do
        {
            if (OB_UNLIKELY(my_phy_plan_->is_timeout(&remain_us)))
            {
                can_break = true;
                ret = OB_PROCESS_TIMEOUT;
            }
            else if (OB_UNLIKELY(NULL != my_phy_plan_ && my_phy_plan_->is_terminate(ret)))
            {
                can_break = true;
                TBSYS_LOG(WARN, "execution was terminated ret is %d", ret);
            }
            else if (OB_LIKELY(OB_SUCCESS == (ret = sql_scan_request_->get_next_row(cur_row_))))
            {
                // got a row without block,
                // no need to check timeout, leave this work to upper layer
                //add fanqiushi_index
                //TBSYS_LOG(ERROR, "test::fanqs,,cur_row_= %s,,", to_cstring(cur_row_));
                //add:e
                can_break = true;
            }
            else if (OB_ITER_END == ret && sql_scan_request_->is_finish())
            {
                // finish all data
                // can break;
                can_break = true;
            }
            else if (OB_ITER_END == ret)
            {
                // need to wait for incomming data
                //add fanqiushi_index
                //TBSYS_LOG(ERROR, "test::fanqs,,hello,,,timeout_us_=%ld,,remain_us=%ld",timeout_us_,remain_us);
                //add:e
                can_break = false;
                timeout_us_ = std::min(timeout_us_, remain_us);
                if( OB_SUCCESS != (ret = sql_scan_request_->wait_single_event(timeout_us_)))
                {
                    if (timeout_us_ <= 0)
                    {
                        TBSYS_LOG(WARN, "wait timeout. timeout_us_=%ld", timeout_us_);
                    }
                    //add wanglei [semi join] for test
                    else
                    {
                        can_break = true;
                        /*if(OB_INVALID_ARGUMENT == ret)
              {
                  ret = OB_ITER_END;
                  TBSYS_LOG(WARN, "wanglei::test ret=%d,wait_single_event faild",ret);
              }*/
                    }
                    //add e
                }
                else
                {
                    TBSYS_LOG(DEBUG, "got a scan event. timeout_us_=%ld", timeout_us_);
                }
            }
            else
            {
                // encounter an unexpected error or
                TBSYS_LOG(WARN, "Unexprected error. ret=%d, cur_row_desc[%s], read_method_[%d]", ret, to_cstring(cur_row_desc_), hint_.read_method_);
                can_break = true;
            }
        }while(false == can_break);
        if (OB_SUCCESS == ret)
        {
            row = &cur_row_;
            //add fanqiushi_index
            //TBSYS_LOG(ERROR, "test::fanqs,,row=%s",to_cstring(cur_row_) );
            //add:e
        }
        //add fanqiushi_index
        //TBSYS_LOG(ERROR, "test::fanqs ret=%d",ret );
        //add:e

    }
    return ret;
}

//add:e

/**
 * 函数功能： 从scan_event中获取一行数据
 * 说明：
 * wait的功能：从finish_queue中阻塞地pop出一个事件（如果没有事件则阻塞）， 然后调用process_result()处理事件
 */
int ObRpcScan::get_next_compact_row(const common::ObRow *&row)
{
    int ret = OB_SUCCESS;
    //modify by fanqiushi_index
    //TBSYS_LOG(ERROR,"test::fanqs,,is_use_index_=%d",is_use_index_);
    if(!is_use_index_)   //如果不使用回表的索引，则按照原来的实现走
    {
        //add wanglei [semi join update] 20160509:b
        if(is_use_semi_join_ && is_right_table_)
        {
            ret = get_table_row_with_more_scan (row);
        }
        else
        {
            bool can_break = false;
            int64_t remain_us = 0;
            row = NULL;
            do
            {
                if (OB_UNLIKELY(my_phy_plan_->is_timeout(&remain_us)))
                {
                    can_break = true;
                    ret = OB_PROCESS_TIMEOUT;
                }
                else if (OB_UNLIKELY(NULL != my_phy_plan_ && my_phy_plan_->is_terminate(ret)))
                {
                    can_break = true;
                    TBSYS_LOG(WARN, "execution was terminated ret is %d", ret);
                }
                else if (OB_LIKELY(OB_SUCCESS == (ret = sql_scan_request_->get_next_row(cur_row_))))
                {
                    // got a row without block,
                    // no need to check timeout, leave this work to upper layer
                    can_break = true;
                }
                else if (OB_ITER_END == ret && sql_scan_request_->is_finish())
                {
                    // finish all data
                    // can break;
                    can_break = true;
                }
                else if (OB_ITER_END == ret)
                {
                    // need to wait for incomming data
                    can_break = false;
                    timeout_us_ = std::min(timeout_us_, remain_us);
                    if( OB_SUCCESS != (ret = sql_scan_request_->wait_single_event(timeout_us_)))
                    {
                        if (timeout_us_ <= 0)
                        {
                            TBSYS_LOG(WARN, "wait timeout. timeout_us_=%ld", timeout_us_);
                        }
                        can_break = true;
                    }
                    else
                    {
                        TBSYS_LOG(DEBUG, "got a scan event. timeout_us_=%ld", timeout_us_);
                    }
                }
                else
                {
                    // encounter an unexpected error or
                    TBSYS_LOG(WARN, "Unexprected error. ret=%d, cur_row_desc[%s], read_method_[%d]", ret, to_cstring(cur_row_desc_), hint_.read_method_);
                    can_break = true;
                }
            }while(false == can_break);
        }
    }
    else
    {
        /* back to base table using secondary index */
        // TBSYS_LOG(ERROR,"test::fanqs,,is_use_index_=%d,,get_next_row_count_=%ld",is_use_index_,get_next_row_count_);
        if(get_next_row_count_==0)   //只有在第一次get_next_row的时候
        {
            if (NULL != get_param_)   //重置下get_param_
            {
                get_param_->~ObSqlGetParam();
                ob_free(get_param_);
                get_param_ = NULL;
            }
            OB_ASSERT(NULL == get_param_);
            get_param_ = OB_NEW(ObSqlGetParam, ObModIds::OB_SQL_GET_PARAM);
            if (NULL == get_param_)
            {
                TBSYS_LOG(WARN, "no memory");
                ret = OB_ALLOCATE_MEMORY_FAILED;
            }            
            get_row_desc_.reset();
            sql_get_request_->alloc_request_id();  //为远程调用申请一个连接
            if (OB_SUCCESS != (ret = sql_get_request_->init(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_GET)))
            {
                TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
            }
            else if (OB_SUCCESS != (ret = create_get_param_for_index(*get_param_)))
            {
                if (OB_ITER_END != ret)//add liumz, [-8 log too much]20161230
                {
                  TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
                }
            }
            else if (OB_SUCCESS != (ret = cons_index_row_desc(*get_param_, get_row_desc_)))
            {
                TBSYS_LOG(WARN, "fail to get row desc:ret[%d]", ret);
            }
            else if (OB_SUCCESS != (ret = sql_get_request_->set_row_desc(get_row_desc_)))
            {
                TBSYS_LOG(WARN, "fail to set row desc:ret[%d]", ret);
            }
            else if(OB_SUCCESS != (ret = sql_get_request_->set_request_param(*get_param_, timeout_us_, hint_.max_parallel_count)))
            {
                TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
                          hint_.max_parallel_count, ret);
            }
            if (OB_SUCCESS == ret)
            {
                sql_get_request_->set_timeout_percent((int32_t)merge_service_->get_config().timeout_percent);
                if (OB_SUCCESS != (ret = sql_get_request_->open()))
                {
                    TBSYS_LOG(WARN, "fail to open get request. ret=%d", ret);
                }
                else
                {
                    ret = sql_get_request_->get_next_row(cur_row_);
                    if(OB_SUCCESS == ret)
                    {
                        get_next_row_count_++;
                    }
                    //add fanqiushi_index
                    //TBSYS_LOG(ERROR,"test::fanqs cur_row_=%s",to_cstring(cur_row_));
                    //add:e
                }
            }
        }
        //else if(get_next_row_count_% 5375 == 0)
        else if(get_next_row_count_% 5120 == 0) //mod liumz, MAX_SUB_GET_REQUEST_NUM * DEFAULT_MAX_GET_ROWS_PER_SUBREQ
        {
            //add wanglei [semi join] 20160111
            //重置scan param
            read_param_->reset();
            scan_param_->set_is_result_cached(false);
            scan_param_->set_table_id (base_table_id_,base_table_id_);
            read_param_ = scan_param_;
            read_param_->reset_project_and_filter ();
            read_param_->set_project(index_project_);
            read_param_->set_filter(index_filter_);
            //add:e
            if (NULL != get_param_)   //重置下get_param_
            {
                get_param_->~ObSqlGetParam();
                ob_free(get_param_);
                get_param_ = NULL;
            }
            OB_ASSERT(NULL == get_param_);
            get_param_ = OB_NEW(ObSqlGetParam, ObModIds::OB_SQL_GET_PARAM);
            if (NULL == get_param_)
            {
                TBSYS_LOG(WARN, "no memory");
                ret = OB_ALLOCATE_MEMORY_FAILED;
            }
            get_row_desc_.reset();
            sql_get_request_->close();
            sql_get_request_->reset();
            sql_get_request_->set_tablet_location_cache_proxy(cache_proxy_);
            sql_get_request_->set_merger_async_rpc_stub(async_rpc_);
            sql_get_request_->alloc_request_id();  //为远程调用申请一个连接
            if (OB_SUCCESS != (ret = sql_get_request_->init(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_GET)))
            {
                TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
            }
            else if (OB_SUCCESS != (ret = create_get_param_for_index(*get_param_)))
            {
                if (OB_ITER_END != ret)//add liumz, [-8 log too much]20161230
                {
                  TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
                }
            }
            else if (OB_SUCCESS != (ret = cons_index_row_desc(*get_param_, get_row_desc_)))
            {
                TBSYS_LOG(WARN, "fail to get row desc:ret[%d]", ret);
            }
            else if (OB_SUCCESS != (ret = sql_get_request_->set_row_desc(get_row_desc_)))
            {
                TBSYS_LOG(WARN, "fail to set row desc:ret[%d]", ret);
            }
            else if(OB_SUCCESS != (ret = sql_get_request_->set_request_param(*get_param_, timeout_us_, hint_.max_parallel_count)))
            {
                TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
                          hint_.max_parallel_count, ret);
            }
            if (OB_SUCCESS == ret)
            {
                sql_get_request_->set_timeout_percent((int32_t)merge_service_->get_config().timeout_percent);
                if (OB_SUCCESS != (ret = sql_get_request_->open()))
                {
                    TBSYS_LOG(WARN, "fail to open get request. ret=%d", ret);
                }
                else
                {
                    ret = sql_get_request_->get_next_row(cur_row_);
                    if(OB_SUCCESS == ret)
                    {
                        get_next_row_count_++;
                    }
                    //add fanqiushi_index
                    //TBSYS_LOG(ERROR,"test::fanqs cur_row_=%s",to_cstring(cur_row_));
                    //add:e
                }
            }
        }
        else
        {
            ret = sql_get_request_->get_next_row(cur_row_);
            if(OB_SUCCESS == ret)
            {
                get_next_row_count_++;
            }
            //add fanqiushi_index
            //TBSYS_LOG(ERROR,"test::fanqs get_next_row_count_=%ld",get_next_row_count_);
            //TBSYS_LOG(ERROR,"test::fanqs cur_row_=%s",to_cstring(cur_row_));
            //add:e
        }
    }

    if (OB_SUCCESS == ret)
    {
        //TBSYS_LOG(ERROR,"test::fanqs,1110,cur_row_=%s,cur_row_desc_=%s",to_cstring(cur_row_),to_cstring(cur_row_desc_));
        if(is_use_index_for_storing_)
        {
            uint64_t tid=OB_INVALID_ID;
            uint64_t cid=OB_INVALID_ID;
            const ObObj *obj_tmp=NULL;
            // TBSYS_LOG(ERROR,"test::fanqs,,cur_row_=%s,cur_row_desc_=%s",to_cstring(cur_row_),to_cstring(cur_row_desc_));
            for(int64_t i=0;i<cur_row_.get_column_num();i++)   //根据索引表的一行构造原表的一行
            {
                cur_row_desc_.get_tid_cid(i,tid,cid);
                //TBSYS_LOG(ERROR,"test::fanqs,,tid=%ld,,main_table_id_=%ld,base_table_id_=%ld",tid,main_table_id_,base_table_id_);
                if(tid == base_table_id_)
                {
                    cur_row_.raw_get_cell(i,obj_tmp);
                    cur_row_for_storing_.set_cell(main_table_id_,cid,*obj_tmp);
                }
                else
                {
                    cur_row_.raw_get_cell(i,obj_tmp);
                    cur_row_for_storing_.set_cell(tid,cid,*obj_tmp);
                }
            }
            row = &cur_row_for_storing_;

            // const  ObRowDesc *desc=cur_row_for_storing_.get_row_desc();
            //TBSYS_LOG(ERROR, "test::fanqs,,cur_row_for_storing_=%s,,desc=%s,,cur_row_desc_for_storing=%s",to_cstring(cur_row_for_storing_),to_cstring(*desc),to_cstring(cur_row_desc_for_storing) );
        }
        else
            row = &cur_row_;
        //add fanqiushi_index
        //TBSYS_LOG(ERROR, "test::fanqs,,row=%s",to_cstring(*row) );
        //add:e
    }
    //modify:e
    return ret;
}


int ObRpcScan::add_output_column(const ObSqlExpression& expr, bool change_tid_for_storing)
{
    int ret = OB_SUCCESS;
    bool is_cid = false;
    //if (table_id_ <= 0 || table_id_ != expr.get_table_id())
    if (base_table_id_ <= 0)
    {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN, "must call set_table() first. base_table_id_=%lu",
                  base_table_id_);
    }
    else if ((OB_SUCCESS == (ret = expr.is_column_index_expr(is_cid)))  && (true == is_cid))
    {
        // 添加基本列
        if (OB_SUCCESS != (ret = read_param_->add_output_column(expr)))
        {
            TBSYS_LOG(WARN, "fail to add output column ret=%d", ret);
        }
    }
    else
    {
        // 添加复合列
        if (OB_SUCCESS != (ret = read_param_->add_output_column(expr)))
        {
            TBSYS_LOG(WARN, "fail to add output column ret=%d", ret);
        }
    }
    // cons row desc
    if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = cur_row_desc_.add_column_desc(expr.get_table_id(), expr.get_column_id()))))
    {
        TBSYS_LOG(WARN, "fail to add column to scan param. ret=%d, tid_=%lu, cid=%lu", ret, expr.get_table_id(), expr.get_column_id());
    }
	//add liumz, [optimize group_order by index]20170419:b
    if ((OB_SUCCESS == ret) && is_use_index_for_storing_
        && (OB_SUCCESS != (ret = cur_row_desc_for_storing.add_column_desc(
                             (change_tid_for_storing&&expr.get_table_id()!=OB_INVALID_ID)?main_table_id_:expr.get_table_id(),
                             expr.get_column_id()))))
    {
        TBSYS_LOG(WARN, "fail to add column to scan param. ret=%d, tid_=%lu, cid=%lu", ret, expr.get_table_id(), expr.get_column_id());
    }
	//add:e
    return ret;
}



int ObRpcScan::set_table(const uint64_t table_id, const uint64_t base_table_id)
{
    int ret = OB_SUCCESS;
    if (0 >= base_table_id)
    {
        TBSYS_LOG(WARN, "invalid table id: %lu", base_table_id);
        ret = OB_INVALID_ARGUMENT;
    }
    else
    {
        table_id_ = table_id;
        base_table_id_ = base_table_id;
    }
    return ret;
}

int ObRpcScan::cons_get_rows(ObSqlGetParam &get_param)
{
    int ret = OB_SUCCESS;
    int64_t idx = 0;
    get_rowkey_array_.clear();
    // TODO lide.wd: rowkey obj storage needed. varchar use orginal buffer, will be copied later
    PageArena<ObObj,ModulePageAllocator> rowkey_objs_allocator(
                PageArena<ObObj, ModulePageAllocator>::DEFAULT_PAGE_SIZE,ModulePageAllocator(ObModIds::OB_SQL_RPC_SCAN2));
    // try  'where (k1,k2,kn) in ((a,b,c), (e,f,g))'
    if (OB_SUCCESS != (ret = sql_read_strategy_.find_rowkeys_from_in_expr(true, get_rowkey_array_, rowkey_objs_allocator)))
    {
        TBSYS_LOG(WARN, "fail to find rowkeys in IN operator. ret=%d", ret);
    }
    else if (get_rowkey_array_.count() > 0)
    {
        ObDuplicateIndicator indicator;
        bool is_dup = false;
        if (get_rowkey_array_.count() > 1)
        {
            if ((ret = indicator.init(get_rowkey_array_.count())) != OB_SUCCESS)
            {
                TBSYS_LOG(WARN, "Init ObDuplicateIndicator failed:ret[%d]", ret);
            }
        }
        for (idx = 0; idx < get_rowkey_array_.count(); idx++)
        {
            if (OB_UNLIKELY(get_rowkey_array_.count() > 1))
            {
                if (OB_SUCCESS != (ret = indicator.have_seen(get_rowkey_array_.at(idx), is_dup)))
                {
                    TBSYS_LOG(WARN, "Check duplication failed, err=%d", ret);
                    break;
                }
                else if (is_dup)
                {
                    continue;
                }
            }
            //深拷贝，从rowkey_objs_allocator 拷贝到了allocator_中
            if (OB_SUCCESS != (ret = get_param.add_rowkey(get_rowkey_array_.at(idx), true)))
            {
                TBSYS_LOG(WARN, "fail to add rowkey to get param. ret=%d", ret);
                break;
            }
        }
    }
    // try  'where k1=a and k2=b and kn=n', only one rowkey
    else if (OB_SUCCESS != (ret = sql_read_strategy_.find_rowkeys_from_equal_expr(true, get_rowkey_array_, rowkey_objs_allocator)))
    {
        TBSYS_LOG(WARN, "fail to find rowkeys from where equal condition, ret=%d", ret);
    }
    else if (get_rowkey_array_.count() > 0)
    {
        for (idx = 0; idx < get_rowkey_array_.count(); idx++)
        {
            if (OB_SUCCESS != (ret = get_param.add_rowkey(get_rowkey_array_.at(idx), true)))
            {
                TBSYS_LOG(WARN, "fail to add rowkey to get param. ret=%d", ret);
                break;
            }
        }
        OB_ASSERT(idx == 1);
    }
    rowkey_objs_allocator.free();
    return ret;
}

//add fanqiushi_index

int ObRpcScan::cons_get_rows_for_index(ObSqlGetParam &get_param)  //根据第一次scan索引表返回的数据，构造第二次get原表的主键的范围
{
    int ret = OB_SUCCESS;
    //int64_t idx = 0;
    //TBSYS_LOG(ERROR,"test::fanqs,,");
    get_rowkey_array_.clear();
    // TODO lide.wd: rowkey obj storage needed. varchar use orginal buffer, will be copied later
    PageArena<ObObj,ModulePageAllocator> rowkey_objs_allocator(
                PageArena<ObObj, ModulePageAllocator>::DEFAULT_PAGE_SIZE,ModulePageAllocator(ObModIds::OB_SQL_RPC_SCAN2));
    const ObRow *index_row_;
    ObObj *rowkey_objs = NULL;
    int32_t key_count = 0;
    while(OB_SUCCESS==(ret=get_next_compact_row_for_index(index_row_)))  //不停的获得第一次scan索引表的数据
    {
        //TBSYS_LOG(ERROR,"test::fanqs,,enter while,,index_row_=%s,,key_count=%d",to_cstring(*index_row_),key_count);
        const ObRowDesc * index_row_desc=index_row_->get_row_desc();
        ObRowkey rowkey;
        if (NULL != (rowkey_objs = rowkey_objs_allocator.alloc(main_rowkey_info_.get_size() * sizeof(ObObj))))
        {
            //TBSYS_LOG(ERROR,"test::fanqs,,enter if");
            int64_t array_index=0;
            for(int64_t i=0;i<main_rowkey_info_.get_size();i++)
            {
                uint64_t main_cid=OB_INVALID_ID;
                uint64_t index_cid=OB_INVALID_ID;
                uint64_t index_tid=OB_INVALID_ID;

                const ObObj *obj_tmp=NULL;
                main_rowkey_info_.get_column_id(i,main_cid);
                for(int64_t j=0;j<index_row_->get_column_num();j++)
                {
                    index_row_desc->get_tid_cid(j,index_tid,index_cid);
                    if(index_cid==main_cid)
                    {
                        index_row_->raw_get_cell(j,obj_tmp);
                        rowkey_objs[array_index]=*obj_tmp;
                        array_index++;
                    }
                }
            }
            rowkey.assign(rowkey_objs,main_rowkey_info_.get_size());
            if (OB_SUCCESS != (ret = get_param.add_rowkey(rowkey, true)))   //把原表的主键数组存到get_param里面
            {
                TBSYS_LOG(WARN, "fail to add rowkey to get param. ret=%d", ret);
                break;
            }
            //get_rowkey_array_.push_back(rowkey);  //构造原表的主键，存到get_rowkey_array_数组里
        }
        key_count++;
        //if(key_count == 5375)
        if(key_count == 5120) //mod liumz, MAX_SUB_GET_REQUEST_NUM * DEFAULT_MAX_GET_ROWS_PER_SUBREQ
        {
            ret = OB_ITER_END;
            //TBSYS_LOG(ERROR,"test::fanqs,,key_count=%d",key_count);
            break;
        }
        //index_row_desc->
        //TBSYS_LOG(ERROR, "test::fanqs,,index_row_=%s,,index_row_desc=%s,,main_rowkey_info=%s,main_rowkey_info_.get_size()=%ld,ret=%d",to_cstring(*index_row_),to_cstring(*index_row_desc),to_cstring(main_rowkey_info_),main_rowkey_info_.get_size(),ret);
    }
    //TBSYS_LOG(ERROR,"test::fanqs,,ret=%d",ret);
    if(ret!=OB_ITER_END)
    {
        TBSYS_LOG(WARN,"faild to get index row to construct roweky,ret=%d",ret);
    }
    else if(get_param.get_row_size()>0 && ret == OB_ITER_END)
    {
        ret =OB_SUCCESS;
    }
    /*else
  {
      if (get_rowkey_array_.count() > 0)
      {
         // TBSYS_LOG(ERROR,"test::fanqs,,get_rowkey_array_.count()=%ld",get_rowkey_array_.count());
        for (idx = 0; idx < get_rowkey_array_.count(); idx++)
        {
          //深拷贝，从rowkey_objs_allocator 拷贝到了allocator_中
            //TBSYS_LOG(ERROR,"test::fanqs,,get_rowkey_array_.at(idx)=%s,,idx=%ld",to_cstring(get_rowkey_array_.at(idx)),idx);
          if (OB_SUCCESS != (ret = get_param.add_rowkey(get_rowkey_array_.at(idx), true)))   //把原表的主键数组存到get_param里面
          {
            TBSYS_LOG(WARN, "fail to add rowkey to get param. ret=%d", ret);
            break;
          }
        }
    //	get_rowkey_array_.clear();
      }
  }
  */
    rowkey_objs_allocator.free();
    return ret;
}
int ObRpcScan::add_main_output_column(const ObSqlExpression& expr)
{
    int ret=OB_SUCCESS;
    ret=main_project.add_output_column(expr);
    return ret;
}

int ObRpcScan::add_main_filter(ObSqlExpression* expr)
{
    int ret=OB_SUCCESS;
    expr->set_owner_op(this);
    ret=main_filter_.add_filter(expr);
    return ret;
}
//add:e
//add wanlgei [semi join] 20160108:b
int ObRpcScan::add_index_filter_ll(ObSqlExpression* expr)
{
    int ret=OB_SUCCESS;
    ObSqlExpression* tmp_expr = ObSqlExpression::alloc ();
    if(tmp_expr == NULL)
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR,"[semi join] allocate memory failed! no memory!");
    }
    else
    {
        tmp_expr->copy (expr);
        tmp_expr->set_owner_op(this);
        ret = index_filter_.add_filter(tmp_expr);
        if(ret != OB_SUCCESS)
            ObSqlExpression::free(tmp_expr);
    }
    return ret;
}
int ObRpcScan::add_index_output_column_ll(const ObSqlExpression& expr)
{
     int ret=OB_SUCCESS;
    ret = index_project_.add_output_column(expr);
    return ret;
}
//add:e
int ObRpcScan::cons_scan_range(ObNewRange &range)
{
    int ret = OB_SUCCESS;
    bool found = false;
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
    range.table_id_ = base_table_id_;
    OB_ASSERT(rowkey_info_.get_size() <= OB_MAX_ROWKEY_COLUMN_NUMBER);
    // range 指向sql_read_strategy_的空间

    //mod fyd  IN_EXPR  [PrefixKeyQuery_for_INstmt] 2014.3.12:b
    bool has_next = false; //add by fyd
    //del:b
    //  if (OB_SUCCESS != (ret = sql_read_strategy_.find_scan_range(range, found, false)))
    //  {
    //    TBSYS_LOG(WARN, "fail to find range %lu", base_table_id_);
    //  }
    //del:e
    if (OB_SUCCESS != (ret = sql_read_strategy_.find_scan_range(found,false)))//single_row_only ==false
    {
        TBSYS_LOG(WARN, "construct  range %lu failed", base_table_id_);
    }
    else if ( OB_SUCCESS != (ret = sql_read_strategy_.get_next_scan_range(range,has_next)))
    {
        TBSYS_LOG(WARN, "fail to find   range %lu ", base_table_id_);
    }/*else if ( has_next )
  {
       if ( OB_SUCCESS != (ret = sql_read_strategy_.get_next_scan_range(range,has_next)))
       {
           TBSYS_LOG(WARN, "fail to find   range %lu ", base_table_id_);
       }
  }*/
    //add tianz [EXPORT_TOOL] 20141120:b
    else if (my_phy_plan_->get_has_range())
    {
        int64_t length = range.start_key_.length();
        ObRowkey start_key(my_phy_plan_->get_start_range(),length);
        ObRowkey end_key(my_phy_plan_->get_end_range(),length);
        TBSYS_LOG(INFO, "DEBUG:HINT, start_key: %s, end_key: %s",to_cstring(start_key),to_cstring(end_key));
        if (!my_phy_plan_->start_is_min() && range.start_key_ <= start_key)
        {
            range.start_key_ = start_key;
            range.border_flag_.unset_inclusive_start();
        }
        if (!my_phy_plan_->end_is_max() && range.end_key_ > end_key)
        {
            range.end_key_ = end_key;
        }
        if (range.start_key_ > range.end_key_)
        {
            range.border_flag_.set_min_value();
            range.start_key_.set_min_row();
            range.end_key_.set_min_row();
        }
    }
    //add 20141120:e
    TBSYS_LOG(DEBUG, "get range %lu ,has_next=%d", base_table_id_,has_next);
    //  sql_read_strategy_.has_next_range(has_next);
    // TBSYS_LOG(INFO, "get range %lu ,has_next=%d", base_table_id_,has_next);
    //mod: end
    return ret;
}

int ObRpcScan::get_min_max_rowkey(const ObArray<ObRowkey> &rowkey_array, ObObj *start_key_objs_, ObObj *end_key_objs_, int64_t rowkey_size)
{
    int ret = OB_SUCCESS;
    int64_t i = 0;
    if (1 == rowkey_array.count())
    {
        const ObRowkey &rowkey = rowkey_array.at(0);
        for (i = 0; i < rowkey_size && i < rowkey.get_obj_cnt(); i++)
        {
            start_key_objs_[i] = rowkey.ptr()[i];
            end_key_objs_[i] = rowkey.ptr()[i];
        }
        for ( ; i < rowkey_size; i++)
        {
            start_key_objs_[i] = ObRowkey::MIN_OBJECT;
            end_key_objs_[i] = ObRowkey::MAX_OBJECT;
        }
    }
    else
    {
        TBSYS_LOG(WARN, "only support single insert row for scan optimization. rowkey_array.count=%ld", rowkey_array.count());
        ret = OB_NOT_SUPPORTED;
    }
    return ret;
}

int ObRpcScan::add_filter(ObSqlExpression* expr)
{
    int ret = OB_SUCCESS;
    expr->set_owner_op(this);
    if (OB_SUCCESS != (ret = sql_read_strategy_.add_filter(*expr)))
    {
        TBSYS_LOG(WARN, "fail to add filter to sql read strategy:ret[%d]", ret);
    }
    if (OB_SUCCESS == ret && OB_SUCCESS != (ret = read_param_->add_filter(expr)))
    {
        TBSYS_LOG(WARN, "fail to add composite column to scan param. ret=%d", ret);
    }
    return ret;
}

int ObRpcScan::add_group_column(const uint64_t tid, const uint64_t cid)
{
    return read_param_->add_group_column(tid, cid);
}

int ObRpcScan::add_aggr_column(const ObSqlExpression& expr)
{
    int ret = OB_SUCCESS;
    if (cur_row_desc_.get_column_num() <= 0)
    {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN, "Output column(s) of ObRpcScan must be set first, ret=%d", ret);
    }
    else if ((ret = cur_row_desc_.add_column_desc(
                  expr.get_table_id(),
                  expr.get_column_id())) != OB_SUCCESS)
    {
        TBSYS_LOG(WARN, "Failed to add column desc, err=%d", ret);
    }
	//add liumz, [optimize group_order by index]20170419:b
    else if (is_use_index_for_storing_
             && (ret = cur_row_desc_for_storing.add_column_desc(
                  expr.get_table_id()==OB_INVALID_ID?expr.get_table_id():main_table_id_,
                  expr.get_column_id())) != OB_SUCCESS)
    {
        TBSYS_LOG(WARN, "Failed to add column desc, err=%d", ret);
    }
	//add:e
    else if ((ret = read_param_->add_aggr_column(expr)) != OB_SUCCESS)
    {
        TBSYS_LOG(WARN, "Failed to add aggregate column desc, err=%d", ret);
    }
    return ret;
}

int ObRpcScan::set_limit(const ObSqlExpression& limit, const ObSqlExpression& offset)
{
    return read_param_->set_limit(limit, offset);
}

int ObRpcScan::cons_row_desc(const ObSqlGetParam &sql_get_param, ObRowDesc &row_desc)
{
    int ret = OB_SUCCESS;
    if ( !sql_get_param.has_project() )
    {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "should has project");
    }

    if (OB_SUCCESS == ret)
    {
        const common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > &columns = sql_get_param.get_project().get_output_columns();
        for (int64_t i = 0; OB_SUCCESS == ret && i < columns.count(); i ++)
        {
            const ObSqlExpression &expr = columns.at(i);
            if (OB_SUCCESS != (ret = row_desc.add_column_desc(expr.get_table_id(), expr.get_column_id())))
            {
                TBSYS_LOG(WARN, "fail to add column desc:ret[%d]", ret);
            }
        }
    }

    if (OB_SUCCESS == ret)
    {
        if ( sql_get_param.get_row_size() <= 0 )
        {
            ret = OB_ERR_LACK_OF_ROWKEY_COL;
            TBSYS_LOG(WARN, "should has a least one row");
        }
        else
        {
            row_desc.set_rowkey_cell_count(sql_get_param[0]->length());
        }
    }

    return ret;
}


//add fanqiushi_index
int ObRpcScan::cons_index_row_desc(const ObSqlGetParam &sql_get_param, ObRowDesc &row_desc)  //生成第二次get原表时用到的行描述
{
    int ret = OB_SUCCESS;

    if (OB_SUCCESS == ret)
    {
        const common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > &columns = main_project.get_output_columns();
        for (int64_t i = 0; OB_SUCCESS == ret && i < columns.count(); i ++)
        {
            const ObSqlExpression &expr = columns.at(i);
            if (OB_SUCCESS != (ret = row_desc.add_column_desc(expr.get_table_id(), expr.get_column_id())))
            {
                TBSYS_LOG(WARN, "fail to add column desc:ret[%d]", ret);
            }
        }
    }

    if (OB_SUCCESS == ret)
    {
        if ( sql_get_param.get_row_size() <= 0 )
        {
            ret = OB_ERR_LACK_OF_ROWKEY_COL;
            TBSYS_LOG(WARN, "should has a least one row");
        }
        else
        {
            row_desc.set_rowkey_cell_count(sql_get_param[0]->length());
        }
    }

    return ret;
}
//add:e

//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140409:b

/*Exp:remove expression which has sub_query but not use bloomfilter 
* make stmt pass the first check 
*/
int ObRpcScan::remove_or_sub_query_expr()
{
    int ret = OB_SUCCESS;
    if(OB_SUCCESS != (ret = read_param_->remove_or_sub_query_expr()))
    {
        TBSYS_LOG(WARN, "fail to remove sub query expr from readparam. ret=%d", ret);
    }
    return ret;
}

/*Exp:update sub_query num
* if direct bind sub_query result to main query,
* do not treat it as a sub_query any more
*/
void ObRpcScan::update_sub_query_num()
{
    read_param_->update_sub_query_num();
}

//add 20140409:e
PHY_OPERATOR_ASSIGN(ObRpcScan)
{
    int ret = OB_SUCCESS;
    CAST_TO_INHERITANCE(ObRpcScan);
    reset();
    rowkey_info_ = o_ptr->rowkey_info_;
    table_id_ = o_ptr->table_id_;
    base_table_id_ = o_ptr->base_table_id_;
    hint_ = o_ptr->hint_;
    need_cache_frozen_data_ = o_ptr->need_cache_frozen_data_;
    cache_bloom_filter_ = o_ptr->cache_bloom_filter_;
    if ((ret = cur_row_desc_.assign(o_ptr->cur_row_desc_)) != OB_SUCCESS)
    {
        TBSYS_LOG(WARN, "Assign ObRowDesc failed, ret=%d", ret);
    }
    else if ((ret = sql_read_strategy_.assign(&o_ptr->sql_read_strategy_, this)) != OB_SUCCESS)
    {
        TBSYS_LOG(WARN, "Assign ObSqlReadStrategy failed, ret=%d", ret);
    }
    else if (o_ptr->scan_param_)
    {
        scan_param_ = OB_NEW(ObSqlScanParam, ObModIds::OB_SQL_SCAN_PARAM);
        if (NULL == scan_param_)
        {
            TBSYS_LOG(WARN, "no memory");
            ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
            scan_param_->set_phy_plan(get_phy_plan());
            read_param_ = scan_param_;
            if ((ret = scan_param_->assign(o_ptr->scan_param_)) != OB_SUCCESS)
            {
                TBSYS_LOG(WARN, "Assign Scan param failed, ret=%d", ret);
            }
        }
    }
    else if (o_ptr->get_param_)
    {
        get_param_ = OB_NEW(ObSqlGetParam, ObModIds::OB_SQL_GET_PARAM);
        if (NULL == get_param_)
        {
            TBSYS_LOG(WARN, "no memory");
            ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
            get_param_->set_phy_plan(get_phy_plan());
            read_param_ = get_param_;
            if ((ret = get_param_->assign(o_ptr->get_param_)) != OB_SUCCESS)
            {
                TBSYS_LOG(WARN, "Assign Get param failed, ret=%d", ret);
            }
        }
    }
    sql_read_strategy_.set_rowkey_info(rowkey_info_);
    return ret;
}

//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140911:b
/*Exp:reset*/
void ObRpcScan::reset_stuff()
{      
    cur_row_desc_.reset();
    read_param_->reset_stuff();
    sql_read_strategy_.reset_stuff();
}
//add 20140911:e
//add gaojt [Delete_Update_Function] [JHOBv0.1] 20151206:b
/*Exp:reset*/
void ObRpcScan::reset_stuff_for_ud()
{
    read_param_->reset_stuff_for_ud();
    sql_read_strategy_.reset_stuff();
}
//add 20151206:e
//add wanglei [semi join update] 20160415:b
int ObRpcScan::set_semi_join_left_table_row(common::ObRowStore *semi_join_left_table_row)
{
    int ret = OB_SUCCESS;
    semi_join_left_table_row_ = semi_join_left_table_row;
    return ret;
}
int ObRpcScan::set_is_use_semi_join(bool is_use_semi_join)
{
    int ret = OB_SUCCESS;
    is_use_semi_join_ =is_use_semi_join;
    return ret;
}
int ObRpcScan::set_left_table_id_column_id(int64_t table_id,int64_t column_id,int64_t right_column_id,int64_t right_table_id)
{
    int ret = OB_SUCCESS;
    left_table_id_ = table_id;
    left_column_id_ = column_id;
    right_column_id_ = right_column_id;
    right_table_id_ = right_table_id;
    return ret;
}
int ObRpcScan::set_is_right_table(bool flag)
{
    is_right_table_ = flag;
    return OB_SUCCESS;
}
void ObRpcScan::set_use_in(bool value)
{
    use_in_expr_ = value;
}
void ObRpcScan::set_use_btw(bool value)
{
    use_btw_expr_ = value;
}
int ObRpcScan::get_data_for_distinct(const common::ObObj *cell,
                                     ObObjType type,
                                     char *buff,
                                     int buff_size)
{
    int ret = OB_SUCCESS;
    if(cell == NULL)
    {
        ret = OB_ERR_POINTER_IS_NULL;
    }
    else
    {
        memset(buff,'\0',buff_size);
        int64_t int_val = 0;
        int32_t int_val_32 = 0;
        float float_val = 0.0;
        double double_val = 0.0;
        bool is_add = false;
        ObString str_val;
        switch (type) {
        case ObNullType:
            break;
        case ObInt32Type:
            cell->get_int32(int_val_32,is_add);
            sprintf(buff,"%d",int_val_32);
            break;
        case ObIntType:
            cell->get_int(int_val,is_add);
            sprintf(buff,"%ld",int_val);
            break;
        case ObVarcharType:
            cell->get_varchar(str_val);
            snprintf(buff, buff_size, "%.*s", str_val.length(), str_val.ptr());
            break;
        case ObFloatType:
            cell->get_float(float_val,is_add);
            sprintf(buff,"%f",float_val);
            break;
        case ObDoubleType:
            cell->get_double(double_val,is_add);
            sprintf(buff,"%.12lf",double_val);
            break;
        case ObDateTimeType:
            cell->get_datetime(int_val,is_add);
            sprintf(buff,"%ld",int_val);
            break;
        case ObPreciseDateTimeType:
            cell->get_precise_datetime(int_val,is_add);
            sprintf(buff,"%ld",int_val);
            break;
        case ObSeqType:
            break;
        case ObCreateTimeType:
            cell->get_createtime(int_val);
            sprintf(buff,"%ld",int_val);
            break;
        case ObModifyTimeType:
            cell->get_modifytime(int_val);
            sprintf(buff,"%ld",int_val);
            break;
        case ObExtendType:
            cell->get_ext(int_val);
            if (common::ObObj::MIN_OBJECT_VALUE == int_val)
            {
            }
            else if (common::ObObj::MAX_OBJECT_VALUE == int_val)
            {
            }
            else
            {
            }
            break;
        case ObBoolType:
            break;
        case ObDecimalType:
        {
            ObString num;
            cell->get_decimal(num);
            ObDecimal od;
            char res[MAX_PRINTABLE_SIZE];
            memset(res,0,MAX_PRINTABLE_SIZE);
            if(OB_SUCCESS != od.from(num.ptr(),num.length())){
                TBSYS_LOG(WARN,"failed to convert decimal from buf");
            }
            else if (OB_SUCCESS!=od.modify_value(cell->get_precision(),cell->get_scale())){
                TBSYS_LOG(WARN,"failed to modify decimal");
            }
            else{
                od.to_string(res,MAX_PRINTABLE_SIZE);
            }
            snprintf(buff, buff_size, "%.*s", num.length(), num.ptr());
            break;
        }
        default:
            break;
        }
    }
    return ret;
}
int ObRpcScan::set_semi_join_left_row_desc(const ObRowDesc *row_desc)
{
    int ret = OB_SUCCESS;
    left_row_desc_ = row_desc;
    return ret;
}
int ObRpcScan::cons_filter_for_right_table(ObSqlExpression *&table_filter_expr_,ObSqlExpression *&src_table_filter_expr_)
{
    UNUSED(src_table_filter_expr_);
    int ret = OB_SUCCESS;
    if(use_btw_expr_)
    {
        ExprItem dem1,dem2;
        dem1.type_ = T_REF_COLUMN;
        //后缀表达式组建
        table_filter_expr_->set_tid_cid(right_table_id_,right_column_id_);
        dem1.value_.cell_.tid = right_table_id_;
        dem1.value_.cell_.cid = right_column_id_;
        table_filter_expr_->add_expr_item(dem1);
        ObRow last_row;
        last_row.set_row_desc (*left_row_desc_);
        // add dragon [bugfix] 2017-5-2 b
        // skip NULL row
        // between表达式的最小值
        ObRow null_row;
        int64_t null_row_counter = 0;
        null_row.set_row_desc (*left_row_desc_);
        while(OB_SUCCESS == (ret = semi_join_left_table_row_->get_next_row(null_row)))
        {
          const common::ObObj *cell_temp=NULL;
          if(OB_SUCCESS!=(ret=null_row.get_cell(left_table_id_,left_column_id_,cell_temp)))
          {
            TBSYS_LOG(WARN, "get cell failure , ret=[%d]", ret);
            break;
          }
          else
          {
            char buff[65536];
            if(OB_SUCCESS!=(ret=get_data_for_distinct(cell_temp,
                                                      cell_temp->get_type(),
                                                      buff,
                                                      sizeof(buff))))
            {
              TBSYS_LOG(WARN, "get data faliure , ret=[%d]", ret);
              break;
            }
            else
            {
              if(strcmp (buff,"")==0)
              {
                // 如果为NULL,则跳过
                null_row_counter++;
                continue;
              }
              else
              {
                last_row = null_row;
                TBSYS_LOG(INFO,"[semi join]::first row = %s, skip %ld null row",to_cstring(null_row), null_row_counter);
                const common::ObObj *cell_temp=NULL;
                if(OB_SUCCESS!=(ret=null_row.get_cell(left_table_id_,left_column_id_,cell_temp)))
                {
                  TBSYS_LOG(WARN, "get cell failure , ret=[%d]", ret);
                }
                else
                {
                  ObConstRawExpr col_val;
                  if (OB_SUCCESS != (ret = col_val.set_value_and_type(*cell_temp)))
                  {
                    TBSYS_LOG(ERROR, "failed to set column value, err=%d", ret);
                  }
                  else
                  {
                    if ((ret = col_val.fill_sql_expression(
                           *table_filter_expr_)) != OB_SUCCESS)
                    {
                      TBSYS_LOG(ERROR,"Add cell value failed");
                    }
                    else
                    {
                      // 获取到第一行就break
                      break;
                    }
                  }
                }
              }
              //string str_temp(buff);
              // TBSYS_LOG(INFO, "[semi join]::str_temp =%s", buff);
            }
          }
        }
        //add dragon [bugfix] 2017-5-2 e

        //between表达式的最小值
        //ObRow row_temp;
        //row_temp.set_row_desc (*left_row_desc_);
        //ret = semi_join_left_table_row_->get_next_row(row_temp);
        if(ret == OB_SUCCESS)
        {
            //del dragon 2017-5-2 b
            /*
            if(ret == OB_SUCCESS)
            {
                last_row = row_temp;
                TBSYS_LOG(DEBUG,"[semi join]::first row = %s",to_cstring(row_temp));
                const common::ObObj *cell_temp=NULL;
                if(OB_SUCCESS!=(ret=row_temp.get_cell(left_table_id_,left_column_id_,cell_temp)))
                {
                    TBSYS_LOG(WARN, "get cell failure , ret=[%d]", ret);
                }
                else
                {
                    ObConstRawExpr     col_val;
                    if (OB_SUCCESS != (ret = col_val.set_value_and_type(*cell_temp)))
                    {
                        TBSYS_LOG(ERROR, "failed to set column value, err=%d", ret);
                    }
                    else
                    {
                        if ((ret = col_val.fill_sql_expression(
                                 *table_filter_expr_)) != OB_SUCCESS)
                        {
                            TBSYS_LOG(ERROR,"Add cell value failed");
                        }
                    }
                }
            }
            */
            //del dragon 2017-5-2 e
            //between表达式的最大值
            ObRow current_row;
            current_row.set_row_desc (*left_row_desc_);
            while(OB_SUCCESS == (ret = semi_join_left_table_row_->get_next_row(current_row)))
            {
                const common::ObObj *cell_temp=NULL;
                if(OB_SUCCESS!=(ret=current_row.get_cell(left_table_id_,left_column_id_,cell_temp)))
                {
                    TBSYS_LOG(WARN, "get cell failure , ret=[%d]", ret);
                    break;
                }
                else
                {
                    char buff[65536];
                    if(OB_SUCCESS!=(ret=get_data_for_distinct(cell_temp,
                                                              cell_temp->get_type(),
                                                              buff,
                                                              sizeof(buff))))
                    {
                        TBSYS_LOG(WARN, "get data faliure , ret=[%d]", ret);
                        break;
                    }
                    else
                    {
                        if(strcmp (buff,"")==0)
                        {
                            ret = OB_ITER_END;
                            break;
                        }
                        else
                        {
                            last_row = current_row;
                        }
                        //string str_temp(buff);
                        TBSYS_LOG(DEBUG, "[semi join]::str_temp  =%s", buff);
                    }
                }

            }
            if(ret == OB_ITER_END)
            {
                TBSYS_LOG(INFO,"[semi join]::last_row = %s",to_cstring(last_row));
                ret = OB_SUCCESS;
                const common::ObObj *cell_temp=NULL;
                if(OB_SUCCESS!=(ret=last_row.get_cell(left_table_id_,left_column_id_,cell_temp)))
                {
                    TBSYS_LOG(WARN, "get cell failure , ret=[%d]", ret);
                }
                else
                {
                    ObConstRawExpr     col_val;
                    if (OB_SUCCESS != (ret = col_val.set_value_and_type(*cell_temp)))
                    {
                        TBSYS_LOG(ERROR, "failed to set column value, err=%d", ret);
                    }
                    else
                    {
                        if ((ret = col_val.fill_sql_expression(
                                 *table_filter_expr_)) != OB_SUCCESS)
                        {
                            TBSYS_LOG(ERROR,"Add cell value failed");
                        }
                    }
                }
            }
            if(ret == OB_SUCCESS)
            {
                dem2.type_ = T_OP_BTW;
                dem2.data_type_ = ObMinType;
                dem2.value_.int_ = 3;
                table_filter_expr_->add_expr_item(dem2);
                table_filter_expr_->add_expr_item_end();
                TBSYS_LOG(DEBUG,"[semi join]::table_filter_expr_ = %s",to_cstring(*table_filter_expr_));
                is_in_expr_empty_ = false;
                is_left_row_cache_complete_ = true;
            }
        }
        else
        {
            if(ret == OB_ITER_END)
            {
                is_in_expr_empty_ = true;
                is_left_row_cache_complete_ = true;
            }
        }
    }
    else if(use_in_expr_)
    {
        int limit_num = 50;
        int index_param_count = 0;
        if(OB_SUCCESS == ret)
        {
            ExprItem dem1,dem2,dem3,dem5,dem6,dem7;
            dem1.type_ = T_REF_COLUMN;
            //后缀表达式组建
            table_filter_expr_->set_tid_cid(right_table_id_,right_column_id_);
            dem1.value_.cell_.tid = right_table_id_;
            dem1.value_.cell_.cid = right_column_id_;
            table_filter_expr_->add_expr_item(dem1);
            dem2.type_ = T_OP_ROW;
            dem2.data_type_ = ObMinType;
            dem2.value_.int_ = 1;
            table_filter_expr_->add_expr_item(dem2);
            dem3.type_ = T_OP_LEFT_PARAM_END;
            dem3.data_type_ = ObMinType;
            dem3.value_.int_ = 2;
            table_filter_expr_->add_expr_item(dem3);
            ObRow row_temp;
            row_temp.set_row_desc (*left_row_desc_);
            while(OB_SUCCESS == (ret = semi_join_left_table_row_->get_next_row(row_temp)))
            {
                if(OB_SUCCESS!=ret)
                    TBSYS_LOG(WARN, "no more rows , sret=[%d]", ret);
                else
                {
                    TBSYS_LOG(DEBUG,"[semi join]::row_temp = %s",to_cstring(row_temp));
                    const common::ObObj *cell_temp=NULL;
                    if(OB_SUCCESS!=(ret=row_temp.get_cell(left_table_id_,left_column_id_,cell_temp)))
                    {
                        TBSYS_LOG(WARN, "get cell failure , ret=[%d]", ret);
                        break;
                    }
                    else
                    {
                          char buff[300];
                          if(OB_SUCCESS!=(ret=get_data_for_distinct(cell_temp,
                                                                    cell_temp->get_type(),
                                                                    buff,
                                                                    sizeof(buff))))
                          {
                              TBSYS_LOG(WARN, "get data faliure , ret=[%d]", ret);
                              break;
                          }
                          else
                          {
                              string str_temp(buff);
                              bool tmp = true;
                              if(is_confilct_.find(str_temp) == is_confilct_.end())
                              {
                                  //这段代码是使用ob的hash map做数据去重，目前存在问题:b
                                  //is_confilct.at(curr_buket-1)->set(tmp_str,tmp);
                                  //e
                                  is_confilct_.insert(make_pair<string,bool>(str_temp,tmp));
//                                  ExprItem item;
//                                  switch(cell_temp->get_type())
//                                  {
//                                    case ObNullType:   // 空类型
//                                      item.type_ = T_NULL;
//                                      item.data_type_ =ObNullType;
//                                      break;
//                                    case ObIntType:
//                                      item.type_ = T_INT;
//                                      item.data_type_ =ObIntType;
//                                      break;
//                                    case ObInt32Type:
//                                      item.type_ = T_INT32;
//                                      item.data_type_ =ObInt32Type;
//                                      break;
//                                    case ObFloatType:
//                                      item.type_ = T_FLOAT;
//                                      item.data_type_ =ObFloatType;
//                                      break;
//                                    case ObDoubleType:
//                                      item.type_ = T_DOUBLE;
//                                      item.data_type_ =ObDoubleType;
//                                      break;
//                                    case ObDateType:
//                                      item.type_ = T_DATE_NEW;
//                                      item.data_type_ =ObDateType;
//                                      break;
//                                    case ObTimeType:
//                                      item.type_ = T_TIME;
//                                      item.data_type_ =ObTimeType;
//                                      break;
//                                    case ObIntervalType:
//                                      item.type_ = T_DATE;
//                                      item.data_type_ =ObIntervalType;
//                                      break;
//                                    case ObPreciseDateTimeType:
//                                    case ObCreateTimeType:
//                                    case ObModifyTimeType:
//                                      item.type_ = T_DATE;
//                                      item.data_type_ =ObPreciseDateTimeType;
//                                      break;
//                                    case ObVarcharType:
//                                      item.type_ = T_STRING;
//                                      item.data_type_ =ObVarcharType;
//                                      break;
//                                    case ObBoolType:
//                                      item.type_ = T_BOOL;
//                                      item.data_type_ =ObBoolType;
//                                      break;
//                                    case ObDecimalType:
//                                      item.type_ = T_DECIMAL;
//                                      item.data_type_ =ObDecimalType;
//                                      break;
//                                    default:
//                                      ret = OB_NOT_SUPPORTED;
//                                      TBSYS_LOG(WARN, "obj type not support, type=%d", cell_temp->get_type());
//                                      break;
//                                  }
//                                  if (OB_LIKELY(OB_SUCCESS == ret))
//                                  {
//                                    if (ObExtendType != cell_temp->get_type())
//                                    {
//                                        float f = 0.0f;
//                                        double d = 0.0;
//                                        switch (item.type_)
//                                        {
//                                          case T_STRING:
//                                          case T_BINARY:
//                                            ret = cell_temp->get_varchar(item.string_);
//                                            break;
//                                          case T_FLOAT:
//                                            ret = cell_temp->get_float(f);
//                                            item.value_.float_ = f;
//                                            break;
//                                          case T_DOUBLE:
//                                            ret = cell_temp->get_double(d);
//                                            item.value_.double_ = d;
//                                            break;
//                                          case T_DECIMAL:
//                                            ret = cell_temp->get_decimal(item.string_);
//                                            //modify:e
//                                            break;
//                                          case T_INT:
//                                            ret = cell_temp->get_int(item.value_.int_);
//                                            break;
//                                          case T_INT32:
//                                            ret = cell_temp->get_int32(item.value_.int32_);
//                                            break;
//                                          case T_BOOL:
//                                            ret = cell_temp->get_bool(item.value_.bool_);
//                                            break;
//                                          case T_DATE:
//                                            ret = cell_temp->get_precise_datetime(item.value_.datetime_);
//                                            break;
//                                          case T_DATE_NEW:
//                                            ret = cell_temp->get_date(item.value_.datetime_);
//                                            break;
//                                          case T_TIME:
//                                            ret = cell_temp->get_time(item.value_.datetime_);
//                                            break;
//                                            break;
//                                          case T_SYSTEM_VARIABLE:
//                                          case T_TEMP_VARIABLE:
//                                            ret = cell_temp->get_varchar(item.string_);
//                                            break;
//                                          case T_NULL:
//                                            break;
//                                          default:
//                                            TBSYS_LOG(WARN, "unexpected expression type %d", item.type_);
//                                            ret = OB_ERR_EXPR_UNKNOWN;
//                                            break;
//                                        }
//                                        if(ret == OB_SUCCESS)
//                                            ret = table_filter_expr_->add_expr_item(item);
//                                    }
//                                  }
                                  ObConstRawExpr     col_val;
                                  if (OB_SUCCESS != (ret = col_val.set_value_and_type(*cell_temp)))
                                  {
                                      TBSYS_LOG(ERROR, "failed to set column value, err=%d", ret);
                                      break;
                                  }
                                  else
                                  {
                                      if ((ret = col_val.fill_sql_expression(
                                               *table_filter_expr_)) != OB_SUCCESS)
                                      {
                                          TBSYS_LOG(ERROR,"Add cell value failed");
                                          break;
                                      }
                                  }
                                  dem5.type_ = T_OP_ROW;
                                  dem5.data_type_ = ObMinType;
                                  dem5.value_.int_ =1;
                                  table_filter_expr_->add_expr_item(dem5);
                                  index_param_count++;
                              }
                              else
                              {
                                  continue;
                              }
                          }
                    }
                }
                if(index_param_count >= limit_num)
                {
                    break;
                }
            }
            if(OB_ITER_END == ret)
            {
                is_left_row_cache_complete_ =  true;
                ret =OB_SUCCESS;
            }
            if(OB_SUCCESS == ret)
            {
                dem6.type_ = T_OP_ROW;
                dem6.data_type_ = ObMinType;
                dem6.value_.int_ = index_param_count;
                table_filter_expr_->add_expr_item(dem6);
                dem7.type_ = T_OP_IN;
                dem7.data_type_ = ObMinType;
                dem7.value_.int_ = 2;
                table_filter_expr_->add_expr_item(dem7);
                table_filter_expr_->add_expr_item_end();
            }
        }
        if(index_param_count == 0)
        {
            is_in_expr_empty_ = true;
            is_left_row_cache_complete_ = true;
        }
    }
    return ret;
}
int ObRpcScan::get_table_row_with_more_scan(const common::ObRow *&row)
{
//    UNUSED(row);
//    int ret = OB_ITER_END;
//    filter_expr_ = (ObSqlExpression*)ob_malloc(sizeof(ObSqlExpression),ObModIds::OB_SQL_TRANSFORMER);
//    if (filter_expr_ == NULL)
//    {
//        ret = OB_ERR_PARSER_MALLOC_FAILED;
//    }
//    else
//    {
//        filter_expr_ = new(filter_expr_) ObSqlExpression();
//    }
//    cons_filter_for_right_table(filter_expr_,src_filter_expr_);
//    ob_free(filter_expr_);
//    ret = OB_ITER_END;
//    return ret;
    UNUSED(row);
    int ret = OB_SUCCESS;
    //for memory leak check:b
    //int64_t usage = 0;
    //for memory leak check:e
    if(is_first_cons_scan_param_)
    {
        is_first_cons_scan_param_ = false;
        //filter_expr_ = ObSqlExpression::alloc ();
        //for memory leak check:b
        //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
        //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER before malloc usage = %ld",usage);
        //for memory leak check:e
        filter_expr_ = ObSqlExpression::alloc ();//(ObSqlExpression*)ob_malloc(sizeof(ObSqlExpression),ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
        if(filter_expr_ == NULL)
        {
            TBSYS_LOG(WARN, "no memory");
            ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
            //filter_expr_ = new(filter_expr_) ObSqlExpression();
            //for memory leak check:b
            //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
            //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER after malloc usage = %ld",usage);
            //for memory leak check:e
            ret = cons_filter_for_right_table(filter_expr_,src_filter_expr_);
            if(!is_in_expr_empty_ && ret == OB_SUCCESS)
            {
                sql_read_strategy_.reset_for_semi_join ();
                sql_read_strategy_.set_rowkey_info (rowkey_info_);
                //mod dragon [type_err] 2016-12-6 b
                ObSqlExpression* expr_clone = ObSqlExpression::alloc();
                if (NULL == expr_clone)
                {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  TBSYS_LOG(WARN, "no memory");
                }
                else
                {
                  *expr_clone = *filter_expr_;
                }
                if(ret == OB_SUCCESS)
                {
                  if(OB_SUCCESS != (ret  = filter_raw.add_filter (expr_clone)))
                  {
                    ObSqlExpression::free(expr_clone);
                    TBSYS_LOG(WARN, "add filter to filter_raw error, ret =%d", ret);
                  }
                }
                //add dragon 2016-12-21 b
                //多注释了一行代码，恢复并添加返回值检查
                if(ret == OB_SUCCESS)
                {
                  if(OB_SUCCESS != (ret = sql_read_strategy_.add_filter (*filter_expr_)))
                  {
                    TBSYS_LOG(WARN, "push filter into sql_read_strategy_ failed.ret[%d]", ret);
                  }
                }
                //add 2016-12-21 e
//                ret = sql_read_strategy_.add_filter (*filter_expr_);
//                if(ret == OB_SUCCESS)
//                {
//                    ret  = filter_raw.add_filter (filter_expr_);
//                }
                //mod e
                if(ret == OB_SUCCESS)
                {
                    TBSYS_LOG(DEBUG,"[semi join]::first>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                    TBSYS_LOG(DEBUG,"[semi join]::filter_expr_ = %s",to_cstring(*filter_expr_));
                    TBSYS_LOG(DEBUG,"[semi join]::filter_raw = %s",to_cstring(filter_raw));
                    TBSYS_LOG(DEBUG,"[semi join]::sql_read_strategy_ = %s",to_cstring(sql_read_strategy_));
                    TBSYS_LOG(DEBUG,"[semi join]::first<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
                    if (NULL != scan_param_)   //重置下scan_param_
                    {
                        scan_param_->~ObSqlScanParam();
                        ob_free(scan_param_);
                        scan_param_ = NULL;
                    }
                    OB_ASSERT(NULL == scan_param_);
                    scan_param_ = OB_NEW(ObSqlScanParam, ObModIds::OB_SQL_SCAN_PARAM);
                    if (NULL == scan_param_)
                    {
                        TBSYS_LOG(WARN, "no memory");
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                    }
                    else
                    {
                        sql_scan_request_->close();
                        sql_scan_request_->reset();
                        sql_scan_request_->set_tablet_location_cache_proxy(cache_proxy_);
                        sql_scan_request_->set_merger_async_rpc_stub(async_rpc_);
                        if (OB_SUCCESS != (ret = sql_scan_request_->initialize()))
                        {
                            TBSYS_LOG(WARN, "initialize sql_scan_request failed, ret=%d", ret);
                        }
                        else
                        {
                            sql_scan_request_->alloc_request_id();  //为远程调用申请一个连接
                            if (OB_SUCCESS != (ret = sql_scan_request_->init_ex(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_SCAN, this)))
                            {
                                TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
                            }
                            if (OB_SUCCESS != (ret = create_scan_param(*scan_param_)))
                            {
                                TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
                            }
                            //重置read param
                            if(ret == OB_SUCCESS)
                            {
                                read_param_ = scan_param_;
                                read_param_->reset_project_and_filter ();
                                read_param_->set_project(project_raw);
                                read_param_->set_filter(filter_raw);
                            }
                            if (ret == OB_SUCCESS)
                            {
                                sql_scan_request_->set_timeout_percent((int32_t)merge_service_->get_config().timeout_percent);
                                if(OB_SUCCESS != (ret = sql_scan_request_->set_request_param(*scan_param_, hint_)))
                                {
                                    TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
                                              hint_.max_parallel_count, ret);
                                }
                            }
                        }
                    }
                }
            }
            if(is_in_expr_empty_)
            {
                ret = OB_ITER_END;
                if(filter_expr_ != NULL)
                {
//                    filter_expr_->~ObSqlExpression();
//                    ob_free (filter_expr_);
//                    filter_expr_ = NULL;
                    ObSqlExpression::free (filter_expr_);
                    filter_expr_ = NULL;
                    //for memory leak check:b
                    //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                    //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER free malloc usage = %ld",usage);
                    //for memory leak check:e
                }
            }
            //防止内存泄露
            if(ret != OB_SUCCESS && filter_expr_ != NULL)
            {
//                filter_expr_->~ObSqlExpression();
//                ob_free (filter_expr_);
//                filter_expr_ = NULL;
                ObSqlExpression::free (filter_expr_);
                filter_expr_ = NULL;
                //for memory leak check:b
                //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER free malloc usage = %ld",usage);
                //for memory leak check:e
            }
        }
    }
    if(ret == OB_SUCCESS)
    {
        bool can_break = false;
        int64_t remain_us = 0;
        row = NULL;
        if (OB_UNLIKELY(my_phy_plan_->is_timeout(&remain_us)))
        {
            can_break = true;
            ret = OB_PROCESS_TIMEOUT;
        }
        else if (OB_UNLIKELY(NULL != my_phy_plan_ && my_phy_plan_->is_terminate(ret)))
        {
            can_break = true;
            TBSYS_LOG(WARN, "execution was terminated ret is %d", ret);
        }
        else
        {
            do
            {
                if (OB_LIKELY(OB_SUCCESS == (ret = sql_scan_request_->get_next_row(cur_row_))))
                {
                    row = &cur_row_;
                    can_break = true;
                }
                else if (OB_ITER_END == ret && sql_scan_request_->is_finish())
                {
                  //mod dragon [type_err] 2016-12-6 b
                  if(OB_SUCCESS != sql_read_strategy_.remove_last_inexpr ())
                  {
                    TBSYS_LOG(WARN, "sql_read_strategy_.remove_last_inexpr failed. ret=%d", ret);
                  }
                  if(OB_SUCCESS != filter_raw.remove_last_filter ())
                  {
                    TBSYS_LOG(WARN, "remove last filter failed. ret=%d", ret);
                  }
//                  sql_read_strategy_.remove_last_inexpr ();
//                  filter_raw.remove_last_filter ();
                  //mod e
                    if(filter_expr_ != NULL)
                    {
//                        filter_expr_->~ObSqlExpression ();
//                        ob_free (filter_expr_);
//                        filter_expr_ = NULL;
                        ObSqlExpression::free (filter_expr_);
                        filter_expr_ = NULL;
                        //for memory leak check:b
                        //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                        //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER free malloc usage = %ld",usage);
                        //for memory leak check:e
                    }
                    if(!is_left_row_cache_complete_)
                    {
                        //for memory leak check:b
                        //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                        //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER before malloc usage = %ld",usage);
                        //for memory leak check:e
                        filter_expr_ = ObSqlExpression::alloc ();//(ObSqlExpression*)ob_malloc(sizeof(ObSqlExpression),ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                        if(filter_expr_ == NULL)
                        {
                            TBSYS_LOG(WARN, "no memory");
                            ret = OB_ALLOCATE_MEMORY_FAILED;
                            can_break = true;
                        }
                        else
                        {
                            //filter_expr_ = new(filter_expr_) ObSqlExpression();
                            //for memory leak check:b
                            //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                            //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER after malloc usage = %ld",usage);
                            //for memory leak check:e
                            ret = cons_filter_for_right_table(filter_expr_,src_filter_expr_);
                            if(!is_in_expr_empty_ && ret == OB_SUCCESS)
                            {
                                sql_read_strategy_.reset_for_semi_join ();
                                sql_read_strategy_.set_rowkey_info (rowkey_info_);
                                //add dragon [type_err] 2016-12-6 b
                                ObSqlExpression* expr_clone = ObSqlExpression::alloc();
                                if (NULL == expr_clone)
                                {
                                  ret = OB_ALLOCATE_MEMORY_FAILED;
                                  TBSYS_LOG(WARN, "no memory");
                                }
                                else
                                {
                                  *expr_clone = *filter_expr_;
                                }
                                //add e
                                if(OB_SUCCESS != (ret = sql_read_strategy_.add_filter (*filter_expr_)))
                                {
                                    can_break = true;
                                }
                                else if(OB_SUCCESS !=( ret  = filter_raw.add_filter (expr_clone)))
                                {
                                  //add dragon [type_err] 2016-12-6 b
                                  ObSqlExpression::free(expr_clone);
                                  TBSYS_LOG(WARN, "add filter to filter_raw error, ret =%d", ret);
                                  //add e
                                  can_break = true;
                                }
                                else
                                {
                                    TBSYS_LOG(DEBUG,"[semi join]::second>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                                    TBSYS_LOG(DEBUG,"[semi join]::filter_expr_ = %s",to_cstring(*filter_expr_));
                                    TBSYS_LOG(DEBUG,"[semi join]::filter_raw = %s",to_cstring(filter_raw));
                                    TBSYS_LOG(DEBUG,"[semi join]::sql_read_strategy_ = %s",to_cstring(sql_read_strategy_));
                                    TBSYS_LOG(DEBUG,"[semi join]::second<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
                                    if (NULL != scan_param_)   //重置下scan_param_
                                    {
                                        scan_param_->~ObSqlScanParam();
                                        ob_free(scan_param_);
                                        scan_param_ = NULL;
                                    }
                                    OB_ASSERT(NULL == scan_param_);
                                    scan_param_ = OB_NEW(ObSqlScanParam, ObModIds::OB_SQL_SCAN_PARAM);
                                    if (NULL == scan_param_)
                                    {
                                        TBSYS_LOG(WARN, "no memory");
                                        ret = OB_ALLOCATE_MEMORY_FAILED;
                                        can_break = true;
                                    }
                                    else
                                    {
                                        sql_scan_request_->close();
                                        sql_scan_request_->reset();
                                        sql_scan_request_->set_tablet_location_cache_proxy(cache_proxy_);
                                        sql_scan_request_->set_merger_async_rpc_stub(async_rpc_);
                                        if (OB_SUCCESS != (ret = sql_scan_request_->initialize()))
                                        {
                                            can_break = true;
                                            TBSYS_LOG(WARN, "initialize sql_scan_request failed, ret=%d", ret);
                                        }
                                        else
                                        {
                                            sql_scan_request_->alloc_request_id();  //为远程调用申请一个连接
                                            if (OB_SUCCESS != (ret = sql_scan_request_->init_ex(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_SCAN, this)))
                                            {
                                                TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
                                                can_break = true;
                                            }
                                            if (OB_SUCCESS != (ret = create_scan_param(*scan_param_)))
                                            {
                                                TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
                                                can_break = true;
                                            }
                                            //重置read param
                                            if(ret == OB_SUCCESS)
                                            {
                                                read_param_ = scan_param_;
                                                read_param_->reset_project_and_filter ();
                                                read_param_->set_project(project_raw);
                                                read_param_->set_filter(filter_raw);
                                            }
                                            if (OB_SUCCESS == ret)
                                            {
                                                sql_scan_request_->set_timeout_percent((int32_t)merge_service_->get_config().timeout_percent);
                                                if(OB_SUCCESS != (ret = sql_scan_request_->set_request_param(*scan_param_, hint_)))
                                                {
                                                    TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
                                                              hint_.max_parallel_count, ret);
                                                    can_break = true;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            if(is_in_expr_empty_)
                            {
                                can_break = true;
                                ret = OB_ITER_END;
                                if(filter_expr_ != NULL)
                                {
//                                    filter_expr_->~ObSqlExpression();
//                                    ob_free (filter_expr_);
//                                    filter_expr_ = NULL;
                                    ObSqlExpression::free (filter_expr_);
                                    filter_expr_ = NULL;
                                    //for memory leak check:b
                                    //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                                    //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER free malloc usage = %ld",usage);
                                    //for memory leak check:e
                                }
                            }
                            //防止内存泄露
                            if(ret != OB_SUCCESS && filter_expr_ != NULL)
                            {
//                                filter_expr_->~ObSqlExpression();
//                                ob_free (filter_expr_);
//                                filter_expr_ = NULL;
                                ObSqlExpression::free (filter_expr_);
                                filter_expr_ = NULL;
                                can_break = true;
                                //for memory leak check:b
                                //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                                //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER free malloc usage = %ld",usage);
                                //for memory leak check:e
                            }
                        }
                    }
                    else
                    {
                        can_break = true;
                    }
                }
                else if (OB_ITER_END == ret)
                {
                    // need to wait for incomming data
                    can_break = false;
                    timeout_us_ = std::min(timeout_us_, remain_us);
                    if( OB_SUCCESS != (ret = sql_scan_request_->wait_single_event(timeout_us_)))
                    {
                        if (timeout_us_ <= 0)
                        {
                            TBSYS_LOG(WARN, "wait timeout. timeout_us_=%ld", timeout_us_);
                        }
                        can_break = true;
                    }
                    else
                    {
                        TBSYS_LOG(DEBUG, "got a scan event. timeout_us_=%ld", timeout_us_);
                    }
                }
                else
                {
                    // encounter an unexpected error or
                    TBSYS_LOG(WARN, "Unexprected error. ret=%d, cur_row_desc[%s], read_method_[%d]", ret, to_cstring(cur_row_desc_), hint_.read_method_);
                    can_break = true;
                }
            }while(false == can_break);
            //防止内存泄露
            if(ret != OB_SUCCESS && filter_expr_ != NULL)
            {
                ObSqlExpression::free (filter_expr_);
                filter_expr_ = NULL;
                //for memory leak check:b
                //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER free malloc usage = %ld",usage);
                //for memory leak check:e
            }
        }
    }
    return ret;
}
//以下代码目前先弃用，以下代码为多线程模式，在以后的设计中可能会用到
//void * ObRpcScan::process_one_rpc(void * args)
//{
//    ObRpcScan * rpc_scan = (ObRpcScan *)(args);
//    UNUSED(args);
//    int ret = OB_SUCCESS;
//    TBSYS_LOG(ERROR,"wanglei::thread start=================================");
//    while(true)
//    {
//        if(rpc_scan->get_is_first_cons_scan_param ())
//        {
//            rpc_scan->get_is_first_cons_scan_param () = false;
//            rpc_scan->get_filter_expr ()= ObSqlExpression::alloc();
//            ret = rpc_scan->cons_filter_for_right_table(rpc_scan->get_filter_expr ());
//            if(!rpc_scan->get_is_in_expr_empty() && ret == OB_SUCCESS)
//            {
//                if(rpc_scan->get_is_first_set_read_stra ())
//                {
//                    rpc_scan->get_sql_read_strategy().add_filter (*rpc_scan->get_filter_expr ());
//                    rpc_scan->get_is_first_set_read_stra () = false;
//                }
//                else
//                {
//                    rpc_scan->get_sql_read_strategy().remove_last_inexpr ();
//                    rpc_scan->get_sql_read_strategy().add_filter (*rpc_scan->get_filter_expr ());
//                }
//                ObFilter filter_temp;
//                if(ret == OB_SUCCESS)
//                {
//                    //重置一下filter temp
//                    common::DList &filters =rpc_scan->get_filter_raw().get_filter ();
//                    dlist_for_each(ObSqlExpression, p, filters)
//                    {
//                        ObSqlExpression *filter = ObSqlExpression::alloc();
//                        filter->copy(p);
//                        ret = filter_temp.add_filter(filter);
//                        if(ret != OB_SUCCESS)
//                            break;
//                    }
//                    ret = filter_temp.add_filter (rpc_scan->get_filter_expr ());
//                }
//                if(ret == OB_SUCCESS)
//                {
//                    if (NULL != rpc_scan->get_scan_param())   //重置下scan_param_
//                    {
//                        rpc_scan->get_scan_param()->~ObSqlScanParam();
//                        ob_free(rpc_scan->get_scan_param());
//                        rpc_scan->get_scan_param() = NULL;
//                    }
//                    OB_ASSERT(NULL == rpc_scan->get_scan_param());
//                    rpc_scan->get_scan_param() = OB_NEW(ObSqlScanParam, ObModIds::OB_SQL_SCAN_PARAM);
//                    if (NULL == rpc_scan->get_scan_param())
//                    {
//                        TBSYS_LOG(WARN, "no memory");
//                        ret = OB_ALLOCATE_MEMORY_FAILED;
//                    }
//                    rpc_scan->get_sql_scan_request()->close();
//                    rpc_scan->get_sql_scan_request()->reset();
//                    rpc_scan->get_sql_scan_request()->set_tablet_location_cache_proxy(rpc_scan->get_cache_proxy());
//                    rpc_scan->get_sql_scan_request()->set_merger_async_rpc_stub(rpc_scan->get_async_rpc ());
//                    if (OB_SUCCESS != (ret = rpc_scan->get_sql_scan_request()->initialize()))
//                    {
//                        TBSYS_LOG(WARN, "initialize sql_scan_request failed, ret=%d", ret);
//                    }
//                    else
//                    {
//                        rpc_scan->get_sql_scan_request()->alloc_request_id();  //为远程调用申请一个连接
//                        if (OB_SUCCESS != (ret = rpc_scan->get_sql_scan_request()->init_ex(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_SCAN, rpc_scan)))
//                        {
//                            TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
//                        }
//                        if (OB_SUCCESS != (ret = rpc_scan->create_scan_param(*rpc_scan->get_scan_param())))
//                        {
//                            TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
//                        }
//                        //重置read param
//                        if(ret == OB_SUCCESS)
//                        {
//                            rpc_scan->get_read_param() = rpc_scan->get_scan_param();
//                            rpc_scan->get_read_param()->reset_project_and_filter ();
//                            rpc_scan->get_read_param()->set_project(rpc_scan->get_project_raw());
//                            rpc_scan->get_read_param()->set_filter(filter_temp);
//                        }
//                        if (OB_SUCCESS == ret)
//                        {
//                            rpc_scan->get_sql_scan_request()->set_timeout_percent((int32_t)rpc_scan->get_merge_service()->get_config().timeout_percent);
//                            if(OB_SUCCESS != (ret = rpc_scan->get_sql_scan_request()->set_request_param(*rpc_scan->get_scan_param(), rpc_scan->get_hint ())))
//                            {
//                                TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
//                                          rpc_scan->get_hint ().max_parallel_count, ret);
//                            }
//                        }
//                    }
//                }
//            }
//        }
//        if(ret == OB_SUCCESS)
//        {
//            bool can_break = false;
//            int64_t remain_us = 0;
//            if (OB_UNLIKELY(rpc_scan->my_phy_plan_->is_timeout(&remain_us)))
//            {
//                can_break = true;
//                ret = OB_PROCESS_TIMEOUT;
//            }
//            else if (OB_UNLIKELY(NULL != rpc_scan->my_phy_plan_ && rpc_scan->my_phy_plan_->is_terminate(ret)))
//            {
//                can_break = true;
//                TBSYS_LOG(WARN, "execution was terminated ret is %d", ret);
//            }
//            else
//            {
//                do
//                {
//                    if (OB_LIKELY(OB_SUCCESS == (ret = rpc_scan->get_sql_scan_request()->get_next_row(rpc_scan->get_cur_row_ ()))))
//                    {
//                        // got a row without block,
//                        // no need to check timeout, leave this work to upper layer
//                        //TBSYS_LOG(ERROR,"wanglei::thread row = %s",to_cstring(rpc_scan->get_cur_row_ ()));
//                        const ObRowStore::StoredRow *stored_row = NULL;
//                        rpc_scan->get_one_cache().add_row(rpc_scan->get_cur_row_ (), stored_row);
//                        bool is_empty = rpc_scan->get_one_cache().is_empty ();
//                        if(is_empty)
//                             TBSYS_LOG(ERROR,"wanglei:one_cache is empty");
//                        can_break = true;
//                    }
//                    else if (OB_ITER_END == ret && rpc_scan->get_sql_scan_request()->is_finish())
//                    {
//                        //can_break = true;
//                        // finish all data
//                        // can break;
//                        if(!rpc_scan->get_is_left_row_cache_complete())
//                        {
//                            rpc_scan->get_filter_expr ()= ObSqlExpression::alloc();
//                            ret = rpc_scan->cons_filter_for_right_table(rpc_scan->get_filter_expr ());
//                            if(!rpc_scan->get_is_in_expr_empty() && ret == OB_SUCCESS)
//                            {
//                                if(rpc_scan->get_is_first_set_read_stra ())
//                                {
//                                    rpc_scan->get_sql_read_strategy().add_filter (*rpc_scan->get_filter_expr ());
//                                    rpc_scan->get_is_first_set_read_stra () = false;
//                                }
//                                else
//                                {
//                                    rpc_scan->get_sql_read_strategy().remove_last_inexpr ();
//                                    rpc_scan->get_sql_read_strategy().add_filter (*rpc_scan->get_filter_expr ());
//                                }
//                                ObFilter filter_temp;
//                                if(ret == OB_SUCCESS)
//                                {
//                                    //重置一下filter temp
//                                    common::DList &filters =rpc_scan->get_filter_raw().get_filter ();
//                                    dlist_for_each(ObSqlExpression, p, filters)
//                                    {
//                                        ObSqlExpression *filter = ObSqlExpression::alloc();
//                                        filter->copy(p);
//                                        ret = filter_temp.add_filter(filter);
//                                        if(ret != OB_SUCCESS)
//                                            break;
//                                    }
//                                    ret = filter_temp.add_filter (rpc_scan->get_filter_expr ());
//                                }
//                                if(ret == OB_SUCCESS)
//                                {
//                                    if (NULL != rpc_scan->get_scan_param())   //重置下scan_param_
//                                    {
//                                        rpc_scan->get_scan_param()->~ObSqlScanParam();
//                                        ob_free(rpc_scan->get_scan_param());
//                                        rpc_scan->get_scan_param() = NULL;
//                                    }
//                                    OB_ASSERT(NULL == rpc_scan->get_scan_param());
//                                    rpc_scan->get_scan_param() = OB_NEW(ObSqlScanParam, ObModIds::OB_SQL_SCAN_PARAM);
//                                    if (NULL == rpc_scan->get_scan_param())
//                                    {
//                                        TBSYS_LOG(WARN, "no memory");
//                                        ret = OB_ALLOCATE_MEMORY_FAILED;
//                                    }
//                                    rpc_scan->get_sql_scan_request()->close();
//                                    rpc_scan->get_sql_scan_request()->reset();
//                                    rpc_scan->get_sql_scan_request()->set_tablet_location_cache_proxy(rpc_scan->get_cache_proxy());
//                                    rpc_scan->get_sql_scan_request()->set_merger_async_rpc_stub(rpc_scan->get_async_rpc ());
//                                    if (OB_SUCCESS != (ret = rpc_scan->get_sql_scan_request()->initialize()))
//                                    {
//                                        TBSYS_LOG(WARN, "initialize sql_scan_request failed, ret=%d", ret);
//                                    }
//                                    else
//                                    {
//                                        rpc_scan->get_sql_scan_request()->alloc_request_id();  //为远程调用申请一个连接
//                                        if (OB_SUCCESS != (ret = rpc_scan->get_sql_scan_request()->init_ex(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_SCAN, rpc_scan)))
//                                        {
//                                            TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
//                                        }
//                                        if (OB_SUCCESS != (ret = rpc_scan->create_scan_param(*rpc_scan->get_scan_param())))
//                                        {
//                                            TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
//                                        }
//                                        //重置read param
//                                        if(ret == OB_SUCCESS)
//                                        {
//                                            rpc_scan->get_read_param() = rpc_scan->get_scan_param();
//                                            rpc_scan->get_read_param()->reset_project_and_filter ();
//                                            rpc_scan->get_read_param()->set_project(rpc_scan->get_project_raw());
//                                            rpc_scan->get_read_param()->set_filter(filter_temp);
//                                        }
//                                        if (OB_SUCCESS == ret)
//                                        {
//                                            rpc_scan->get_sql_scan_request()->set_timeout_percent((int32_t)rpc_scan->get_merge_service()->get_config().timeout_percent);
//                                            if(OB_SUCCESS != (ret = rpc_scan->get_sql_scan_request()->set_request_param(*rpc_scan->get_scan_param(), rpc_scan->get_hint ())))
//                                            {
//                                                TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
//                                                          rpc_scan->get_hint ().max_parallel_count, ret);
//                                            }
//                                        }
//                                    }
//                                }
//                            }
//                        }
//                        else
//                        {
//                            can_break = true;
//                        }
//                    }
//                    else if (OB_ITER_END == ret)
//                    {
//                        // need to wait for incomming data
//                        can_break = false;
//                        rpc_scan->get_timeout_us() = std::min(rpc_scan->get_timeout_us(), remain_us);
//                        if( OB_SUCCESS != (ret = rpc_scan->get_sql_scan_request()->wait_single_event(rpc_scan->get_timeout_us())))
//                        {
//                            if (rpc_scan->get_timeout_us() <= 0)
//                            {
//                                TBSYS_LOG(WARN, "wait timeout. timeout_us_=%ld", rpc_scan->get_timeout_us());
//                            }
//                            can_break = true;
//                        }
//                        else
//                        {
//                            TBSYS_LOG(DEBUG, "got a scan event. timeout_us_=%ld", rpc_scan->get_timeout_us());
//                        }
//                    }
//                    else
//                    {
//                        // encounter an unexpected error or
//                        can_break = true;
//                    }
//                }while(false == can_break);
//            }
//        }
//        if(ret == OB_ITER_END)
//        {
//            rpc_scan->get_thread_complete() = true;
//            break;
//        }
//    }
//    bool is_empty = rpc_scan->get_one_cache().is_empty ();
//    if(is_empty)
//         TBSYS_LOG(ERROR,"wanglei:one_cache is empty");
//     TBSYS_LOG(ERROR,"wanglei::thread stop=================================");
//    return 0;
//}
int ObRpcScan::init_next_scan_param_for_one_scan_request(ObNewRange &range,
                                                         ObSqlScanParam * scan_param,
                                                         mergeserver::ObMsSqlScanRequest * scan_request)
{
    UNUSED(range);
    UNUSED(scan_param);
    UNUSED(scan_request);
    int ret = OB_SUCCESS;
    //    range.border_flag_.set_inclusive_start();
    //    range.border_flag_.set_inclusive_end();
    //    range.table_id_ = base_table_id_;
    //    OB_ASSERT(rowkey_info_.get_size() <= OB_MAX_ROWKEY_COLUMN_NUMBER);
    //    if(is_use_index_)
    //    {
    //        ret = fill_read_param_for_first_scan(*scan_param);
    //    }
    //    else
    //    {
    //        ret = fill_read_param(*scan_param);
    //    }
    //    if(OB_SUCCESS != ret)
    //    {
    //        TBSYS_LOG(WARN, "fail to fill read param to scan param. ret=%d", ret);
    //    }
    //    else if (OB_SUCCESS != (ret = scan_param->set_range(range)))
    //    {
    //        TBSYS_LOG(WARN, "fail to set range to scan param. ret=%d", ret);
    //    }
    //    else if(OB_SUCCESS != (ret = scan_request->set_request_param_only(*scan_param)))
    //    {
    //        TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",hint_.max_parallel_count, ret);
    //    }
    //    else
    //    {
    //        TBSYS_LOG(DEBUG, "next scan_param=%s", to_cstring(*scan_param));
    //    }
    //    TBSYS_LOG(TRACE, "dump scan range: %s", to_cstring(range));
    return ret;
}
//int ObRpcScan::get_table_row_with_more_scan_thread(const common::ObRow *&row)
//{
//    UNUSED(row);
//    int ret = OB_SUCCESS;
//    if(is_first_use_thread_)
//    {
//        if(pthread_mutex_init(&mutex,NULL)!=0)
//        {
//            ret = OB_INVALID;
//        }
//        if(pthread_mutex_init(&mutex_1,NULL)!=0)
//        {
//            ret = OB_INVALID;
//        }
//        if(ret == OB_SUCCESS)
//        {
//            pthread_t tid = 0;
//            while(!is_left_row_cache_complete_)
//            {
//                ObSqlExpression *filter = ObSqlExpression::alloc ();
//                ret = cons_filter_for_right_table(filter);
//                if(ret == OB_SUCCESS)
//                {
//                    THREAD_PARAM * param = (THREAD_PARAM *)ob_malloc(sizeof(THREAD_PARAM), ObModIds::OB_GET_PARAM);
//                    param->rpcscan = this;
//                    param->filter_expr = filter;
//                    param->mutex = mutex;
//                    param->mutex_1 = mutex_1;
//                    if(!is_in_expr_empty_)
//                    {
//                        pthread_create(&(tid),NULL,process_one_scan_param,(void*)param);
//                        thread_id.push_back (tid);
//                        tid++;
//                    }
//                }
//            }
//        }
//        is_first_use_thread_ =false;
////        for(int i=0;i<thread_id.count ();i++)
////        {
////            pthread_join(thread_id.at (i),NULL);
////        }
//    }
//    sleep(2);
//    ret = OB_ITER_END;
//    return ret;
//}
//void * ObRpcScan::process_one_scan_param(void * args)
//{
//    UNUSED(args);
//    int ret =  OB_SUCCESS;
//    int val;
//    pthread_mutex_t mutex,mutex_1;
//    THREAD_PARAM *param = (THREAD_PARAM *)(args); //手动释放
//    ObRpcScan * rpc_scan = param->rpcscan; //手动释放
//    //rpc 使用参数部分：b
//    ObSqlExpression *filter_expr = param->filter_expr;//手动释放
//    mutex = param->mutex;
//    mutex_1 = param->mutex_1;
//    ObSqlReadStrategy temp_read_strategy;// = (ObSqlReadStrategy*)ob_malloc(sizeof(ObSqlReadStrategy), ObModIds::OB_SCAN_PARAM);
//    ObSqlScanParam scan_param ;
//    mergeserver::ObMsSqlScanRequest * sql_scan_request = NULL;
//    ObSqlReadParam  *read_param_temp =NULL;
//    //rpc 使用参数部分：e
//    //获取rpc scan的公用参数:只读部分：b
//    val = pthread_mutex_lock(&mutex);
//    ObSqlReadStrategy & read_strategy = rpc_scan->get_sql_read_strategy();
//    common::ObRowkeyInfo &rowkey_info = rpc_scan->rowkey_info ();
//    ObFilter &filter_raw_temp = rpc_scan->get_filter_raw();
//    common::ObTabletLocationCacheProxy *& cache_proxy_temp = rpc_scan->get_cache_proxy ();
//    mergeserver::ObMergerAsyncRpcStub *& async_rpc_temp = rpc_scan->get_async_rpc ();
//    ObProject & project_raw_temp = rpc_scan->get_project_raw ();
//    pthread_mutex_unlock(&mutex);
//    //获取rpc scan的公用参数：e
//    val = pthread_mutex_lock(&mutex);
//    if(ret == OB_SUCCESS)
//    {
////        temp_read_strategy = new(std::nothrow)ObSqlReadStrategy();
//        temp_read_strategy.assign (&read_strategy,rpc_scan);
//        temp_read_strategy.set_rowkey_info (rowkey_info);
//        temp_read_strategy.add_filter(*filter_expr);
//        ObFilter filter_temp;
//        if(ret == OB_SUCCESS)
//        {
//            //重置一下filter temp
//            common::DList &filters =filter_raw_temp.get_filter ();
//            dlist_for_each(ObSqlExpression, p, filters)
//            {
//                ObSqlExpression *filter = ObSqlExpression::alloc();
//                filter->copy(p);
//                ret = filter_temp.add_filter(filter);
//                if(ret != OB_SUCCESS)
//                    break;
//            }
//            ret = filter_temp.add_filter (filter_expr);
//        }
//        if(ret == OB_SUCCESS)
//        {
//            //scan_param = OB_NEW(ObSqlScanParam, ObModIds::OB_SQL_SCAN_PARAM);
////            if (NULL == scan_param)
////            {
////                TBSYS_LOG(WARN, "no memory");
////                ret = OB_ALLOCATE_MEMORY_FAILED;
////            }
//            if (NULL == (sql_scan_request = ObMergeServerMain::get_instance()->get_merge_server().get_scan_req_pool().alloc()))
//            {
//               ret = OB_ALLOCATE_MEMORY_FAILED;
//               TBSYS_LOG(WARN, "alloc scan request from scan request pool failed, ret=%d", ret);
//            }
//            sql_scan_request->set_tablet_location_cache_proxy(cache_proxy_temp);
//            sql_scan_request->set_merger_async_rpc_stub(async_rpc_temp);
//            if (OB_SUCCESS != (ret = sql_scan_request->initialize()))
//            {
//                TBSYS_LOG(WARN, "initialize sql_scan_request failed, ret=%d", ret);
//            }
//            else
//            {
//                sql_scan_request->alloc_request_id();  //为远程调用申请一个连接
//                if (OB_SUCCESS != (ret = sql_scan_request->init_ex(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_SCAN, rpc_scan)))
//                {
//                    TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
//                }
//                if (OB_SUCCESS != (ret = rpc_scan->create_scan_param_for_one_scan_request(scan_param,temp_read_strategy)))
//                {
//                    TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
//                }
//                //重置read param
//                if(ret == OB_SUCCESS)
//                {/*
//                    if (NULL == read_param_temp)
//                    {
//                        TBSYS_LOG(WARN, "no memory");
//                        ret = OB_ALLOCATE_MEMORY_FAILED;
//                    }
//                    else
//                    {
//                        read_param_temp->assign (rpc_scan->get_read_param ());
//                    }*/
//                    read_param_temp = &scan_param;
//                    read_param_temp->reset_project_and_filter ();
//                    read_param_temp->set_project(project_raw_temp);
//                    read_param_temp->set_filter(filter_temp);
//                }
//                if (OB_SUCCESS == ret)
//                {
//                    sql_scan_request->set_timeout_percent((int32_t)rpc_scan->get_merge_service ()->get_config().timeout_percent);
//                    if(OB_SUCCESS != (ret = sql_scan_request->set_request_param(scan_param, rpc_scan->get_hint ())))
//                    {
//                        TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
//                                  rpc_scan->get_hint ().max_parallel_count, ret);
//                    }
//                }
//            }
//        }
//    }
//    pthread_mutex_unlock(&mutex);
//    bool is_finish =false;
//    while(!is_finish)
//    {
//        bool can_break = false;
//        if(ret == OB_SUCCESS)
//        {
//            do
//            {
//                val = pthread_mutex_lock(&mutex);
//                int64_t remain_us = 0;
//                if (OB_UNLIKELY(rpc_scan->my_phy_plan_->is_timeout(&remain_us)))
//                {
//                    ret = OB_PROCESS_TIMEOUT;
//                }
//                else if (OB_UNLIKELY(NULL != rpc_scan->my_phy_plan_ && rpc_scan->my_phy_plan_->is_terminate(ret)))
//                {
//                    TBSYS_LOG(WARN, "execution was terminated ret is %d", ret);
//                }
//                common::ObRow cur_row;
//                cur_row.set_row_desc (rpc_scan->get_cur_row_desc());
//                if (OB_LIKELY(OB_SUCCESS == (ret = sql_scan_request->get_next_row_for_one_scan_param(cur_row,&temp_read_strategy,&scan_param,sql_scan_request))))
//                {
//                    // got a row without block,
//                    // no need to check timeout, leave this work to upper layer
//                    TBSYS_LOG(ERROR,"wanglei::thread row = %s",to_cstring(cur_row));
//                   // const ObRowStore::StoredRow *stored_row = NULL;
//                    //rpc_scan->get_one_cache().add_row(rpc_scan->get_cur_row_ (), stored_row);
//                    can_break = true;
//                }
//                else if (OB_ITER_END == ret && sql_scan_request->is_finish())
//                {
//                    can_break = true;
//                    is_finish = true;
//                }
//                else if (OB_ITER_END == ret)
//                {
//                    // need to wait for incomming data
//                    can_break = false;
//                    rpc_scan->get_timeout_us() = std::min(rpc_scan->get_timeout_us(), remain_us);
//                    if( OB_SUCCESS != (ret = sql_scan_request->wait_single_event(rpc_scan->get_timeout_us())))
//                    {
//                        if (rpc_scan->get_timeout_us() <= 0)
//                        {
//                            TBSYS_LOG(WARN, "wait timeout. timeout_us_=%ld", rpc_scan->get_timeout_us());
//                        }
//                        can_break = true;
//                    }
//                    else
//                    {
//                        TBSYS_LOG(DEBUG, "got a scan event. timeout_us_=%ld", rpc_scan->get_timeout_us());
//                    }
//                }
//                else
//                {
//                    // encounter an unexpected error or
//                    can_break = true;
//                }
//                pthread_mutex_unlock(&mutex);
//            }while(false == can_break);
//        }

//    }
//    if(ret == OB_ITER_END)
//    {
//       // 清理内存
//        val = pthread_mutex_lock(&mutex_1);
//        temp_read_strategy.destroy ();
////        val = pthread_mutex_lock(&mutex_1);
////        if (NULL != scan_param)
////        {
////            scan_param->~ObSqlScanParam();
////            ob_free(scan_param);
////            scan_param = NULL;
////        }
//        val = pthread_mutex_lock(&mutex_1);
//        if (sql_scan_request != NULL)
//        {
//            if (OB_SUCCESS != (ret = ObMergeServerMain::get_instance()->get_merge_server().get_scan_req_pool().free(sql_scan_request)))
//            {
//                TBSYS_LOG(WARN, "free scan request back to scan req pool failed, ret=%d", ret);
//            }
//            sql_scan_request = NULL;
//        }
//        val = pthread_mutex_lock(&mutex_1);
//        if(filter_expr!= NULL)
//        {
//            ObSqlExpression::free (filter_expr);
//        }
//        val = pthread_mutex_lock(&mutex_1);
//        if(param != NULL)
//        {
//            ob_free (param);
//        }

//    }
//    pthread_mutex_unlock(&mutex_1);
//    pthread_mutex_unlock(&mutex_1);
//    pthread_mutex_unlock(&mutex_1);
//    pthread_mutex_unlock(&mutex_1);
//    return 0;
//}
int ObRpcScan::create_scan_param_for_one_scan_request(ObSqlScanParam &scan_param,
                                                      ObSqlReadStrategy &sql_read_strategy)
{
    UNUSED(scan_param);
    UNUSED(sql_read_strategy);
    int ret = OB_SUCCESS;
    //    ObNewRange range;
    //    // until all columns and filters are set, we could know the exact range
    //    //modify by fanqiushi_index
    //    if(is_use_index_)
    //    {
    //        ret=fill_read_param_for_first_scan(scan_param);   //回表情况下，第一次scan原表的时候把对表别名的处理
    //    }
    //    else
    //    {
    //        ret = fill_read_param(scan_param);
    //    }

    //    if(OB_SUCCESS == ret)
    //    {
    //        if (OB_SUCCESS != (ret = cons_scan_range_for_one_scan_request(range,sql_read_strategy)))
    //        {
    //            TBSYS_LOG(WARN, "fail to construct scan range. ret=%d", ret);
    //        }
    //        // TODO: lide.wd 将range 深拷贝到ObSqlScanParam内部的buffer_pool_中
    //        else if (OB_SUCCESS != (ret = scan_param.set_range(range)))
    //        {
    //            TBSYS_LOG(WARN, "fail to set range to scan param. ret=%d", ret);
    //        }
    //    }
    //    //modify:e
    //    TBSYS_LOG(DEBUG, "scan_param=%s", to_cstring(scan_param));
    //    TBSYS_LOG(TRACE, "dump scan range: %s", to_cstring(range));
    return ret;
}
int ObRpcScan::cons_scan_range_for_one_scan_request(ObNewRange &range,
                                                    ObSqlReadStrategy &sql_read_strategy)
{
    UNUSED(range);
    UNUSED(sql_read_strategy);
    int ret = OB_SUCCESS;
    //    bool found = false;
    //    range.border_flag_.set_inclusive_start();
    //    range.border_flag_.set_inclusive_end();
    //    range.table_id_ = base_table_id_;
    //    OB_ASSERT(rowkey_info_.get_size() <= OB_MAX_ROWKEY_COLUMN_NUMBER);
    //    bool has_next = false; //add by fyd
    //    if (OB_SUCCESS != (ret = sql_read_strategy.find_scan_range(found,false)))//single_row_only ==false
    //    {
    //        TBSYS_LOG(WARN, "construct  range %lu failed", base_table_id_);
    //    }
    //    else if ( OB_SUCCESS != (ret = sql_read_strategy.get_next_scan_range(range,has_next)))
    //    {
    //        TBSYS_LOG(WARN, "fail to find   range %lu ", base_table_id_);
    //    }
    //    else if (my_phy_plan_->get_has_range())
    //    {
    //        int64_t length = range.start_key_.length();
    //        ObRowkey start_key(my_phy_plan_->get_start_range(),length);
    //        ObRowkey end_key(my_phy_plan_->get_end_range(),length);
    //        TBSYS_LOG(INFO, "DEBUG:HINT, start_key: %s, end_key: %s",to_cstring(start_key),to_cstring(end_key));
    //        if (!my_phy_plan_->start_is_min() && range.start_key_ <= start_key)
    //        {
    //            range.start_key_ = start_key;
    //            range.border_flag_.unset_inclusive_start();
    //        }
    //        if (!my_phy_plan_->end_is_max() && range.end_key_ > end_key)
    //        {
    //            range.end_key_ = end_key;
    //        }
    //        if (range.start_key_ > range.end_key_)
    //        {
    //            range.border_flag_.set_min_value();
    //            range.start_key_.set_min_row();
    //            range.end_key_.set_min_row();
    //        }
    //    }
    return ret;
}
//add wanglei fix now() bug
int64_t ObRpcScan::get_type_num(int64_t idx,int64_t type,ObSEArray<ObObj, 64> &expr_)
{
    int64_t num = 0;
    int ret = OB_SUCCESS;
    if(type == ObPostfixExpression::BEGIN_TYPE)
    {
        num = 1;
    }
    else if (type == ObPostfixExpression::OP)
    {
        num = 3;
        int64_t op_type = 0;
        if (OB_SUCCESS != (ret = expr_[idx+1].get_int(op_type)))
        {
            TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
        }
        else if (T_FUN_SYS == op_type)
        {
            ++num;
        }
    }
    else if (type == ObPostfixExpression::COLUMN_IDX || type == T_OP_ROW)
    {
        num = 3;
    }
    else if (type == ObPostfixExpression::CONST_OBJ ||type == ObPostfixExpression::QUERY_ID||type == ObPostfixExpression::PARAM_IDX||type==ObPostfixExpression::TEMP_VAR)//add wanglei TEMP_VAR [second index fix] 20160513
    {
        num = 2;
    }
    else if (type == ObPostfixExpression::END || type == ObPostfixExpression::UPS_TIME_OP||ObPostfixExpression::CUR_TIME_OP
             ||ObPostfixExpression::CUR_TIME_HMS_OP
             ||ObPostfixExpression::CUR_DATE_OP
             ||ObPostfixExpression::UPS_TIME_OP)
    {
        num = 1;
    }
    else
    {
        TBSYS_LOG(WARN, "Unkown type %ld", type);
        return -1;
    }
    return num;
}
//add wanglei [semi join update] 20160415:e
// del by maosy [Delete_Update_Function_isolation_RC] 20161218
//add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//int ObRpcScan::set_data_mark_param(const ObDataMarkParam &param)
//{
//    int ret = OB_SUCCESS;
//    if (NULL == read_param_)
//    {
//        ret = OB_NOT_INIT;
//        TBSYS_LOG(ERROR,"read param is NULL!ret=%d",ret);
//    }
//    else if (!param.is_valid())
//    {
//        ret = OB_INVALID_ARGUMENT;
//        TBSYS_LOG(ERROR,"invalid data mark param[%s]!ret=%d",
//                  to_cstring(param),ret);
//    }
//    //mod gaojt [Delete_Update_Function_isolation] [JHOBv0.1] 20161214:b
////    else if (param.table_id_ != base_table_id_)
//    else if (!is_use_index_ && !is_use_index_for_storing_ && param.table_id_ != base_table_id_)
//    {
//        ret = OB_INVALID_ARGUMENT;
//        TBSYS_LOG(ERROR,"orig_tid[%ld] != param_tid[%ld]!ret=%d",
//                  base_table_id_,param.table_id_,ret);
//    }
//    //mod gaojt 20161214:e
//    else
//    {
//        read_param_->set_data_mark_param(param);
//        is_data_mark_param_valid_ = read_param_->get_data_mark_param().is_valid();
//        //add gaojt [Delete_Update_Function_isolation] [JHOBv0.1] 20161214:b
//        data_mark_ = param;
//        TBSYS_LOG(DEBUG,"gjt-ud:data_mark_[%s]",to_cstring(data_mark_));
//        //add 20161214:e
//    }
//    return ret;
//}
//int ObRpcScan::get_data_mark_param(const ObDataMarkParam *&param)
//{
//    int ret = OB_SUCCESS;
//    param   = NULL;
//    if (NULL == read_param_)
//    {
//        ret = OB_NOT_INIT;
//        TBSYS_LOG(ERROR,"read param is NULL!ret=%d",ret);
//    }
//    else
//    {
//        param = &(read_param_->get_data_mark_param());
//    }

//    if (OB_SUCCESS == ret && NULL == param)
//    {
//        ret = OB_ERR_UNEXPECTED;
//        TBSYS_LOG(ERROR,"shouldn't get null param!ret=%d",ret);
//    }
//    return ret;
//}
//int ObRpcScan::add_data_mark_cids_()
//{
//    int ret = OB_SUCCESS;
//    const ObDataMarkParam *param = NULL;
//    if (OB_SUCCESS != (ret = get_data_mark_param(param)))
//    {
//        TBSYS_LOG(WARN,"fail to get data mark param!ret=%d",ret);
//    }
//    else if (NULL == param)
//    {
//        ret = OB_ERR_UNEXPECTED;
//        TBSYS_LOG(ERROR,"data mark param is NULL!ret=%d",ret);
//    }
//    else if (!param->is_valid())
//    {
//        ret = OB_INVALID_ARGUMENT;
//        TBSYS_LOG(ERROR,"invalid data mark param[%s]!ret=%d",
//                  to_cstring(*param),ret);
//    }
//    else if (!is_data_mark_param_valid_)
//    {
//        ret = OB_ERR_UNEXPECTED;
//        TBSYS_LOG(ERROR,"param should be valid!ret=%d",ret);
//    }
//    else if (param->need_modify_time_
//             && OB_SUCCESS != (ret = add_special_output_column_(param->table_id_,
//                                                                param->modify_time_cid_)))
//    {
//        TBSYS_LOG(WARN,"fail to add modify time cid!ret=%d",ret);
//    }
//    else if (param->need_major_version_
//             && OB_SUCCESS != (ret = add_special_output_column_(param->table_id_,
//                                                                param->major_version_cid_)))
//    {
//        TBSYS_LOG(WARN,"fail to add major_ver cid!ret=%d",ret);
//    }
//    else if (param->need_minor_version_
//             && OB_SUCCESS != (ret = add_special_output_column_(param->table_id_,
//                                                                param->minor_ver_start_cid_)))
//    {
//        TBSYS_LOG(WARN,"fail to add minor_ver_start_cid!ret=%d",ret);
//    }
//    else if (param->need_minor_version_
//             && OB_SUCCESS != (ret = add_special_output_column_(param->table_id_,
//                                                                param->minor_ver_end_cid_)))
//    {
//        TBSYS_LOG(WARN,"fail to add minor_ver_end_cid_!ret=%d",ret);
//    }
//    else if (param->need_data_store_type_
//             && OB_SUCCESS != (ret = add_special_output_column_(param->table_id_,
//                                                                param->data_store_type_cid_)))
//    {
//        TBSYS_LOG(WARN,"fail to add data store type cid!ret=%d",ret);
//    }
//    return ret;
//}

//int ObRpcScan::add_special_output_column_(const uint64_t tid, const uint64_t cid)
//{
//    int ret = OB_SUCCESS;
//    if (OB_SUCCESS == ret)
//    {
//      ObSqlExpression special_column;
//      special_column.set_tid_cid(tid, cid);
//      if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_column_expr(tid, cid, special_column)))
//      {
//        TBSYS_LOG(WARN, "fail to create column expression. ret=%d,tid=%ld,cid=%ld",
//                  ret,tid,cid);
//      }
//      else if (OB_SUCCESS != (ret = this->add_output_column(special_column)))
//      {
//        TBSYS_LOG(WARN, "fail to add special output column. ret=%d,tid=%ld,cid=%ld",
//                  ret,tid,cid);
//      }
//    }

//    return ret;
//}

//add duyr 20160531:e
    // del by maosy
