/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_table_rpc_scan.cpp
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#include "ob_table_rpc_scan.h"
#include "common/utility.h"
#include "ob_sql_read_strategy.h"
#include "mergeserver/ob_merge_server_main.h"
#define CREATE_PHY_OPERRATOR_NEW(op, type_name, physical_plan, err)    \
  ({err = OB_SUCCESS;                                                   \
    op = OB_NEW(type_name, ObModIds::OB_SQL_TABLE_RPC_SCAN);            \
   if (op == NULL) \
   { \
     err = OB_ERR_PARSER_MALLOC_FAILED; \
     TBSYS_LOG(WARN, "Can not malloc space for %s", #type_name);  \
   } \
   else\
   {\
     op->set_phy_plan(physical_plan);              \
     ob_inc_phy_operator_stat(op->get_type());\
   } \
   op;})


namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObTableRpcScan, PHY_TABLE_RPC_SCAN);
  }
}

namespace oceanbase
{
  namespace sql
  {
    ObTableRpcScan::ObTableRpcScan() :
      rpc_scan_(), scalar_agg_(NULL), group_(NULL), group_columns_sort_(), limit_(),
      has_rpc_(false), has_scalar_agg_(false), has_group_(false),
      has_group_columns_sort_(false), is_indexed_group_(false), has_limit_(false), is_skip_empty_row_(true),//add liumz, [optimize group_order by index]20170419
      //add fanqiushi_index
      is_use_index_rpc_scan_(false),is_use_index_for_storing(false),is_use_index_for_storing_for_tostring_(false),
      index_tid_for_storing_for_tostring_(OB_INVALID_ID),is_use_index_without_storing_for_tostring_(false),
      index_tid_without_storing_for_tostring_(OB_INVALID_ID),
      //add:e
      read_method_(ObSqlReadStrategy::USE_SCAN),
      is_listagg_(false),//add gaojt [ListAgg][JHOBv0.1]20150104
      sort_for_listagg_()//add gaojt [ListAgg][JHOBv0.1]20150104
    {
        expr_clone = NULL;
    }

    ObTableRpcScan::~ObTableRpcScan()
    {
      if (NULL != group_)
      {
        ob_dec_phy_operator_stat(group_->get_type());
        OB_DELETE(ObMergeGroupBy, ObModIds::OB_SQL_TABLE_RPC_SCAN, group_);
        group_ = NULL;
      }

      if (NULL != scalar_agg_)
      {
        ob_dec_phy_operator_stat(scalar_agg_->get_type());
        OB_DELETE(ObScalarAggregate, ObModIds::OB_SQL_TABLE_RPC_SCAN, scalar_agg_);
        scalar_agg_ = NULL;
      }
    }

    void ObTableRpcScan::reset()
    {
      if (has_rpc_)
      {
        rpc_scan_.reset();
        has_rpc_ = false;
      }
      select_get_filter_.reset();
      if (has_scalar_agg_)
      {
        if (NULL != scalar_agg_)
        {
          scalar_agg_->reset();
        }
        has_scalar_agg_ = false;
      }
      if (has_group_)
      {
        if (NULL != group_)
        {
          group_->reset();
        }
        has_group_ = false;
      }
      if (has_group_columns_sort_)
      {
        group_columns_sort_.reset();
        has_group_columns_sort_ = false;
        is_indexed_group_ = false;//add liumz, [optimize group_order by index]20170419
      }
      if (has_limit_)
      {
        limit_.reset();
        has_limit_ = false;
      }
      //add gaojt [ListAgg][JHOBv0.1]20150104:b
      if(is_listagg_)
      {
          sort_for_listagg_.reset();
          is_listagg_ = false;
      }
      /* add 20150104:e */
      empty_row_filter_.reset();
      is_skip_empty_row_ = true;
      //add fanqiushi_index
      is_use_index_rpc_scan_=false;
      is_use_index_for_storing=false;
      is_use_index_for_storing_for_tostring_=false;
      index_tid_for_storing_for_tostring_=OB_INVALID_ID;
      is_use_index_without_storing_for_tostring_=false;
      index_tid_without_storing_for_tostring_=OB_INVALID_ID;
      //add liumz, [bugfix: index memory overflow]20170105:b
      main_project_.reset();
      main_filter_.reset();
      //add:e
      //add:e
      read_method_ = ObSqlReadStrategy::USE_SCAN;
      ObTableScan::reset();
    }

    void ObTableRpcScan::reuse()
    {
      if (has_rpc_)
      {
        rpc_scan_.reuse();
        has_rpc_ = false;
      }
      select_get_filter_.reuse();
      if (has_scalar_agg_)
      {
        if (NULL != scalar_agg_)
        {
          scalar_agg_->reuse();
        }
        has_scalar_agg_ = false;
      }
      if (has_group_)
      {
        if (NULL == group_)
        {
          group_->reuse();
        }
        has_group_ = false;
      }
      if (has_group_columns_sort_)
      {
        group_columns_sort_.reuse();
        has_group_columns_sort_ = false;
        is_indexed_group_ = false;//add liumz, [optimize group_order by index]20170419
      }
      if (has_limit_)
      {
        limit_.reuse();
        has_limit_ = false;
      }
      //add gaojt [ListAgg][JHOBv0.1]20150104:b
      if(is_listagg_)
      {
          sort_for_listagg_.reuse();
          is_listagg_ = false;
      }
      /* add 20150104:e */
      empty_row_filter_.reuse();
      is_skip_empty_row_ = true;
      //add fanqiushi_index
      is_use_index_rpc_scan_=false;
      is_use_index_for_storing=false;
      is_use_index_for_storing_for_tostring_=false;
      index_tid_for_storing_for_tostring_=OB_INVALID_ID;
      is_use_index_without_storing_for_tostring_=false;
      index_tid_without_storing_for_tostring_=OB_INVALID_ID;
      //add liumz, [bugfix: index memory overflow]20170105:b
      main_project_.reuse();
      main_filter_.reuse();
      //add:e
      //add:e
      read_method_ = ObSqlReadStrategy::USE_SCAN;
    }

    int ObTableRpcScan::open()
    {

      int ret = OB_SUCCESS;
      if (child_op_ == NULL)
      {
        //add zhujun [transaction read uncommit]2016/3/22
        ObSQLSessionInfo *session = NULL;
        if(my_phy_plan_ != NULL && my_phy_plan_->get_result_set() != NULL)
        {
            session = my_phy_plan_->get_result_set()->get_session();
            if(session != NULL)
                rpc_scan_.set_trans_id(session->get_trans_id());
        }
        //add:e

        // rpc_scan_ is the leaf operator
        if (OB_SUCCESS == ret && has_rpc_)
        {
          child_op_ = &rpc_scan_;
          child_op_->set_phy_plan(my_phy_plan_);
          if (ObSqlReadStrategy::USE_GET == read_method_
            && is_skip_empty_row_)
          {
            empty_row_filter_.set_child(0, *child_op_);
            select_get_filter_.set_child(0, empty_row_filter_);
            child_op_ = &select_get_filter_;
          }
          //add fanqiushi_index
          else if(ObSqlReadStrategy::USE_SCAN == read_method_ && is_use_index_rpc_scan_)
          {
              //mod liumz, [secondary_index, null_row bugfix]20170523:b
              //select_get_filter_.set_child(0, *child_op_);
              empty_row_filter_.set_child(0, *child_op_);
              select_get_filter_.set_child(0, empty_row_filter_);
              //mod:e
              child_op_ = &select_get_filter_;
          }
          //add:e
        }
        else
        {
          ret = OB_NOT_INIT;
          TBSYS_LOG(WARN, "must call init() before call open(). ret=%d", ret);
        }
        // more operation over the leaf
        // pushed-down group by or scalar aggregation
        if (OB_SUCCESS == ret && (has_group_ || has_scalar_agg_))
        {
          if (has_group_ && has_scalar_agg_)
          {
            ret = OB_ERR_GEN_PLAN;
            TBSYS_LOG(WARN, "Group operator and scalar aggregate operator can not appear in TableScan at the same time. ret=%d", ret);
          }
          else if (has_scalar_agg_)
          {
            // add scalar aggregation
            //add gaojt [ListAgg][JHOBv0.1]20150104:b
            if(is_listagg_)
            {
                if (OB_SUCCESS != (ret = sort_for_listagg_.set_child(0, *child_op_)))
                {
                  TBSYS_LOG(WARN, "Fail to set child of scalar aggregate operator. ret=%d", ret);
                }
                else if (OB_SUCCESS != (ret = scalar_agg_->set_child(0, sort_for_listagg_)))
                {
                  TBSYS_LOG(WARN, "Fail to set child of scalar aggregate operator. ret=%d", ret);
                }
                else
                {
                  child_op_ = scalar_agg_;
                  child_op_->set_phy_plan(my_phy_plan_);
                }
            }
            else
            {
            /* add 20150104:e */
                if (OB_SUCCESS != (ret = scalar_agg_->set_child(0, *child_op_)))
                {
                    TBSYS_LOG(WARN, "Fail to set child of scalar aggregate operator. ret=%d", ret);
                }
                else
                {
                    child_op_ = scalar_agg_;
                    child_op_->set_phy_plan(my_phy_plan_);
                }
            }
          }
          else if (has_group_)
          {
            // add group by
            if (!has_group_columns_sort_)
            {
              ret = OB_ERR_GEN_PLAN;
              TBSYS_LOG(WARN, "Physical plan error, group need a sort operator. ret=%d", ret);
            }
            else if (OB_SUCCESS != (ret = group_columns_sort_.set_child(0, *child_op_)))
            {
              TBSYS_LOG(WARN, "Fail to set child of sort operator. ret=%d", ret);
            }
            else if (OB_SUCCESS != (ret = group_->set_child(0, group_columns_sort_)))
            {
              TBSYS_LOG(WARN, "Fail to set child of group operator. ret=%d", ret);
            }
            else
            {
              child_op_ = group_;
              child_op_->set_phy_plan(my_phy_plan_);
            }
          }
        }
        // limit
        if (OB_SUCCESS == ret && has_limit_)
        {
          if (OB_SUCCESS != (ret = limit_.set_child(0, *child_op_)))
          {
            TBSYS_LOG(WARN, "fail to set limit child. ret=%d", ret);
          }
          else
          {
            child_op_ = &limit_;
            child_op_->set_phy_plan(my_phy_plan_);
          }
        }
      }

      // open the operation chain
      if (OB_SUCCESS == ret)
      {
        ret = child_op_->open();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to open table scan. ret=%d", ret);
        }
      }
      return ret;
    }

    int ObTableRpcScan::close()
    {
      int ret = OB_SUCCESS;
      if (NULL == child_op_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = child_op_->close();
      }

      return ret;
    }

    int ObTableRpcScan::get_next_row(const common::ObRow *&row)
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(NULL == child_op_))
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = child_op_->get_next_row(row);

      }
      return ret;
    }

    int ObTableRpcScan::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(NULL == child_op_))
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = child_op_->get_row_desc(row_desc);
      }
      return ret;
    }

    int ObTableRpcScan::init(ObSqlContext *context, const common::ObRpcScanHint *hint)
    {
      int ret = OB_SUCCESS;
      if (NULL != context)
      {
        if (hint)
        {
          read_method_ = hint->read_method_;
          is_skip_empty_row_ = hint->is_get_skip_empty_row_;
        }
        ret = rpc_scan_.init(context, hint);
        if (OB_SUCCESS == ret)
        {
          has_rpc_ = true;
        }
      }
      else
      {
        TBSYS_LOG(WARN, "fail to init table rpc scan. params(null)");
        ret = OB_INVALID_ARGUMENT;
      }
      return ret;
    }
    //add fanqiushi_index
    void ObTableRpcScan::set_main_tid(uint64_t tid)
    {
        //is_use_index_rpc_scan_=true;
        main_tid_=tid;
        rpc_scan_.set_main_tid(tid);
    }
    void ObTableRpcScan::set_is_index_for_storing(bool is_use,uint64_t tid)
    {
        is_use_index_for_storing_for_tostring_=is_use;
        index_tid_for_storing_for_tostring_=tid;
    }
    void ObTableRpcScan::set_is_index_without_storing(bool is_use,uint64_t tid)
    {
        is_use_index_without_storing_for_tostring_=is_use;
        index_tid_without_storing_for_tostring_=tid;
    }

    void ObTableRpcScan::set_is_use_index_without_storing()
    {
        is_use_index_rpc_scan_=true;
    }
    void ObTableRpcScan::set_is_use_index_for_storing(uint64_t tid,ObRowDesc &row_desc)
    {
        is_use_index_for_storing = true;
        main_tid_ = tid;//add liumz, [optimize group_order by index]20170419
        rpc_scan_.set_is_use_index_for_storing(tid,row_desc);
    }

    void ObTableRpcScan::set_main_rowkey_info(common::ObRowkeyInfo RI)
    {
        rpc_scan_.set_main_rowkey_info(RI);
    }

    int ObTableRpcScan::cons_second_row_desc(ObRowDesc &row_desc)
    {
        int ret = OB_SUCCESS;

        if (OB_SUCCESS == ret)
        {
          const common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > &columns = main_project_.get_output_columns();
          for (int64_t i = 0; OB_SUCCESS == ret && i < columns.count(); i ++)
          {
            const ObSqlExpression &expr = columns.at(i);
            if (OB_SUCCESS != (ret = row_desc.add_column_desc(expr.get_table_id(), expr.get_column_id())))
            {
              TBSYS_LOG(WARN, "fail to add column desc:ret[%d]", ret);
            }
          }
        }
        return ret;
    }
    int ObTableRpcScan::set_second_row_desc(ObRowDesc *row_desc)
    {
        int ret = OB_SUCCESS;

        ret=rpc_scan_.set_second_rowdesc(row_desc);
        return ret;
    }

    int ObTableRpcScan::add_main_output_column(const ObSqlExpression& expr)
    {
        int ret = OB_SUCCESS;
        if (OB_SUCCESS == ret)
        {
          // add output column to scan param
          ret = main_project_.add_output_column(expr);
          ret=rpc_scan_.add_main_output_column(expr);
          //add fanqiushi_index
          //if(is_use_index_rpc_scan_)
         // {
             // ret = index_rpc_scan_.add_output_column(expr);
          //}
          //add:e
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to add column to rpc scan operator. ret=%d", ret);
          }
        }
        return ret;
    }

    int ObTableRpcScan::add_main_filter(ObSqlExpression* expr)
    {
        int ret = OB_SUCCESS;
        //ret = main_filter_.add_filter(expr);
        ObSqlExpression* expr_clone = ObSqlExpression::alloc(); // @todo temporary work around
        if (NULL == expr_clone)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(WARN, "no memory");
        }
        else
        {
            *expr_clone = *expr;
            ret=rpc_scan_.add_main_filter(expr_clone);
        }
        return ret;
    }

    int ObTableRpcScan::add_index_filter(ObSqlExpression* expr)
    {
        int ret = OB_SUCCESS;
        expr_clone = ObSqlExpression::alloc(); // @todo temporary work around
        if (NULL == expr_clone)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(WARN, "no memory");
        }
        else
        {

          *expr_clone = *expr;
            //expr_clone->copy (expr);
          if (OB_SUCCESS != (ret = select_get_filter_.add_filter(expr_clone)))
          {
            TBSYS_LOG(WARN, "fail to add filter to filter for select get. ret=%d", ret);
          }
        }
        return ret;
    }
    //add:e
    //add wanglei [semi join] 20150108 :b
    int ObTableRpcScan::add_index_filter_ll(ObSqlExpression* expr)
    {
        int ret = OB_SUCCESS;
        if (OB_SUCCESS == ret)
        {
          // add output column to scan param
          ret = rpc_scan_.add_index_filter_ll (expr);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to add filter to rpc scan operator. ret=%d", ret);
          }
        }
        return ret;
    }
    int ObTableRpcScan::add_index_output_column_ll(ObSqlExpression &expr)
    {
        int ret = OB_SUCCESS;
        if (OB_SUCCESS == ret)
        {
          // add output column to scan param
          ret = rpc_scan_.add_index_output_column_ll(expr);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to add filter to rpc scan operator. ret=%d", ret);
          }
        }
        return ret;
    }
    //add:e
    //add wanglei [semi join update] 20160405:b
    ObRpcScan & ObTableRpcScan::get_rpc_scan()
    {
        return rpc_scan_;
    }
    void ObTableRpcScan::reset_select_get_filter()
    {
        select_get_filter_.reset ();
    }
    bool ObTableRpcScan::is_right_table()
    {
        return is_right_table_;
    }
    void ObTableRpcScan::set_is_right_table(bool flag)
    {
       is_right_table_ = flag ;
    }
    //add wanglei :e
    int ObTableRpcScan::add_output_column(const ObSqlExpression& expr, bool change_tid_for_storing)//mod liumz, [optimize group_order by index]20170419
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == ret)
      {
        // add output column to scan param
        ret = rpc_scan_.add_output_column(expr, change_tid_for_storing);//mod liumz, [optimize group_order by index]20170419
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to add column to rpc scan operator. ret=%d", ret);
        }
      }
      return ret;
    }

    int ObTableRpcScan::add_group_column(const uint64_t tid, const uint64_t cid, bool change_tid_for_storing)//mod liumz, [optimize group_order by index]20170419
    {
      int ret = OB_SUCCESS;
      if (has_scalar_agg_)
      {
        ret = OB_ERR_GEN_PLAN;
        TBSYS_LOG(WARN, "Can not adding group column after adding aggregate function(s). ret=%d", ret);
      }
      if (OB_SUCCESS == ret)
      {
        if (group_ == NULL)
        {
          CREATE_PHY_OPERRATOR_NEW(group_, ObMergeGroupBy, my_phy_plan_, ret);
        }
        if (OB_SUCCESS == ret)
        {
          group_->set_phy_plan(my_phy_plan_);
          if ((ret = group_columns_sort_.add_sort_column((is_use_index_for_storing&&change_tid_for_storing)?main_tid_:tid, cid, true)) != OB_SUCCESS)//mod liumz, [optimize group_order by index]20170419
          {
            TBSYS_LOG(WARN, "Add sort column of TableRpcScan sort operator failed. ret=%d", ret);
          }
          else if ((ret = group_->add_group_column((is_use_index_for_storing&&change_tid_for_storing)?main_tid_:tid, cid)) != OB_SUCCESS)//mod liumz, [optimize group_order by index]20170419
          {
            TBSYS_LOG(WARN, "Add group column of TableRpcScan group operator failed. ret=%d", ret);
          }
          else
          {
            has_group_ = true;
            has_group_columns_sort_ = true;
            ret = rpc_scan_.add_group_column(tid, cid);
          }
        }
      }
      return ret;
    }
    //add gaojt [ListAgg][JHOBv0.1]20150104:b
    int ObTableRpcScan::add_sort_column_for_listagg(const uint64_t tid, const uint64_t cid,bool is_asc)
    {
        int ret = OB_SUCCESS;

        if ((ret = sort_for_listagg_.add_sort_column(
                                tid,
                                cid,
                                is_asc
                                )) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"Add sort column to sort plan failed");
        }
        if(OB_SUCCESS == ret)
        {
           rpc_scan_.add_sort_column_for_listagg(tid,cid,is_asc);
        }
        return ret;
    }
    /* add 20150104:e */
    int ObTableRpcScan::add_aggr_column(const ObSqlExpression& expr)
    {
      int ret = OB_SUCCESS;
      ObItemType aggr_type = T_INVALID;
      bool is_distinct;
      if ((ret = expr.get_aggr_column(aggr_type, is_distinct)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Get aggregate function type failed. ret=%d", ret);
      }
      else if (is_distinct)
      {
        ret = OB_ERR_GEN_PLAN;
        TBSYS_LOG(WARN, "Distinct aggregate function can not be processed in TableRpcScan. ret=%d", ret);
      }
      else if (aggr_type == T_FUN_AVG)
      {
        // avg() = sum()/count()
        // avg() is no longer in TableRpcScan
        ret = OB_ERR_GEN_PLAN;
        TBSYS_LOG(WARN, "Avg() aggregate function can not appears in TableRpcScan. ret=%d", ret);
      }
      else
      {
        ObSqlExpression part_expr(expr);
        part_expr.set_tid_cid(expr.get_table_id(), expr.get_column_id() - 1);
        ret = rpc_scan_.add_aggr_column(part_expr);
      }

      // generate local aggregate function
      ObSqlExpression local_expr;
      if (ret == OB_SUCCESS)
      {
        ObBinaryRefRawExpr col_expr(expr.get_table_id(), expr.get_column_id() - 1, T_REF_COLUMN);
        ObAggFunRawExpr sub_agg_expr(&col_expr, is_distinct, aggr_type);
        ObSqlRawExpr col_raw_expr(common::OB_INVALID_ID, expr.get_table_id(), expr.get_column_id(), &sub_agg_expr);
        if ((ret = col_raw_expr.fill_sql_expression(local_expr)) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Generate local aggregate function of TableRpcScan failed. ret=%d", ret);
        }
        else if (aggr_type == T_FUN_COUNT)
        {
          local_expr.set_aggr_func(T_FUN_SUM, is_distinct);
        }
      }

      // add local aggregate function
      if (ret == OB_SUCCESS)
      {
        if (has_group_)
        {
          if ((ret = group_->add_aggr_column(local_expr)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "Add aggregate function to TableRpcScan group operator failed. ret=%d", ret);
          }
        }
        else
        {
          if (scalar_agg_ == NULL)
          {
            CREATE_PHY_OPERRATOR_NEW(scalar_agg_, ObScalarAggregate, my_phy_plan_, ret);
          }
          if (OB_SUCCESS == ret)
          {
            scalar_agg_->set_phy_plan(my_phy_plan_);
            has_scalar_agg_ = true;
            if ((ret = scalar_agg_->add_aggr_column(local_expr)) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN, "Add aggregate function to TableRpcScan scalar aggregate operator failed. ret=%d", ret);
            }
          }
        }
      }
      return ret;
    }

    int ObTableRpcScan::set_table(const uint64_t table_id, const uint64_t base_table_id)
    {
      int ret = OB_SUCCESS;
      // add table id to scan param
      if (OB_SUCCESS != (ret = rpc_scan_.set_table(table_id, base_table_id)))
      {
        TBSYS_LOG(WARN, "fail to add table id to rpc scan operator. table_id=%lu, ret=%d", base_table_id, ret);
      }
      return ret;
    }

    int ObTableRpcScan::add_filter(ObSqlExpression *expr)
    {
      int ret = OB_SUCCESS;
      /*del liumz, [bugfix: index memory overflow]20170105:b
      ObSqlExpression* expr_clone = ObSqlExpression::alloc(); // @todo temporary work around
      if (NULL == expr_clone)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "no memory");
      }
      else
      {
        *expr_clone = *expr;
        */ //del:e
        if (OB_SUCCESS != (ret = rpc_scan_.add_filter(expr)))
        {
          TBSYS_LOG(WARN, "fail to add filter to rpc scan operator. ret=%d", ret);
        }
        else
        {//modify by fanqiushi_index
            if(!is_use_index_rpc_scan_)   //如果回表，这里不能再把表达式存到select_get_filter_里了，因为我在函数add_index_filter里已经放进去过了。
            {
              //add liumz, [bugfix: index memory overflow]20170105:b
              ObSqlExpression* expr_clone = ObSqlExpression::alloc(); // @todo temporary work around
              if (NULL == expr_clone)
              {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(WARN, "no memory");
              }
              else
              {
                *expr_clone = *expr;
                //add:e
                if (OB_SUCCESS != (ret = select_get_filter_.add_filter(expr_clone)))
                {
                  ObSqlExpression::free(expr_clone);
                  TBSYS_LOG(WARN, "fail to add filter to filter for select get. ret=%d", ret);
                }
              }
            }
        }
            //modify:e
      //}
      return ret;
    }

    int ObTableRpcScan::set_limit(const ObSqlExpression& limit, const ObSqlExpression& offset)
    {
      int ret = OB_SUCCESS;
      if ((ret = limit_.set_limit(limit, offset)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "fail to set limit. ret=%d", ret);
      }
      else
      {
        has_limit_ = true;
        // add limit to scan param
        if (!is_use_index_rpc_scan_)
        {
          if (offset.is_empty())
          {
            ret = rpc_scan_.set_limit(limit, offset);
          }
          else if (limit.is_empty())
          {
            ObSqlExpression empty_offset;
            ret = rpc_scan_.set_limit(limit, empty_offset);
          }
          else
          {
            ObSqlExpression new_limit;
            ObSqlExpression empty_offset;
            ExprItem op;
            op.type_ = T_OP_ADD;
            op.data_type_ = ObIntType;
            op.value_.int_ = 2;
            if ((ret = new_limit.merge_expr(limit, offset, op)) != OB_SUCCESS
                || (ret = rpc_scan_.set_limit(new_limit, empty_offset)) != OB_SUCCESS)
            {
            }
          }
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "Fail to add limit/offset to rpc scan operator. ret=%d", ret);
          }
        }
      }
      return ret;
    }

    int64_t ObTableRpcScan::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      //modify zhuyanchao [secondary index] 20150615
      databuff_printf(buf, buf_len, pos, "TableRpcScan(read_method=%s, is use index for storing=%d,index tid=%ld; is use index without storing=%d,index tid=%ld;is_indexed_group_=%d; ", read_method_ == ObSqlReadStrategy::USE_SCAN ? "SCAN":"GET",is_use_index_for_storing_for_tostring_,index_tid_for_storing_for_tostring_,is_use_index_without_storing_for_tostring_,index_tid_without_storing_for_tostring_,is_indexed_group_);//add liumz, [optimize group_order by index]20170419
      //modify e
      if (has_limit_)
      {
        databuff_printf(buf, buf_len, pos, "limit=<");
        pos += limit_.to_string(buf+pos, buf_len-pos);
        //add yushengjuan [ocanbase_gui_client] 20160218
        pos = pos-2;
        //add e:20160218
        databuff_printf(buf, buf_len, pos, ">, ");
      }
      if (has_scalar_agg_)
      {
        databuff_printf(buf, buf_len, pos, "ScalarAggregate=<");
        pos += scalar_agg_->to_string(buf+pos, buf_len-pos);
        //add yushengjuan [ocanbase_gui_client] 20160218
        pos = pos-2;
        //add e:20160218
        databuff_printf(buf, buf_len, pos, ">, ");
        //add gaojt [ListAgg][JHOBv0.1]20150104:b
        if(is_listagg_)
        {
            databuff_printf(buf, buf_len, pos, "obsort=<");
            pos += sort_for_listagg_.to_string(buf+pos, buf_len-pos);
            databuff_printf(buf, buf_len, pos, ">, ");
        }
        /* add 20150104:e */
      }
      if (has_group_)
      {
        databuff_printf(buf, buf_len, pos, "GroupBy=<");
        pos += group_->to_string(buf+pos, buf_len-pos);
        databuff_printf(buf, buf_len, pos, ">, ");
      }
      if (has_group_columns_sort_)
      {
        databuff_printf(buf, buf_len, pos, "Sort=<");
        pos += group_columns_sort_.to_string(buf+pos, buf_len-pos);
        databuff_printf(buf, buf_len, pos, ">, ");
      }
      //modify yushengjuan [ocanbase_gui_client] 20160218  "\nrpc_scan=<" ==> "rpc_scan=<"
      databuff_printf(buf, buf_len, pos, "rpc_scan=<");
      //modify e:20160218
      pos += rpc_scan_.to_string(buf+pos, buf_len-pos);
      //modify yushengjuan [ocanbase_gui_client] 20160218  "(buf, buf_len, pos, ">)\n")" ==> "(buf, buf_len, pos, ">)")"
      pos = pos-2;
      databuff_printf(buf, buf_len, pos, ">)\n");
      //modify e:20160218
      if (NULL != child_op_)
      {
        pos += child_op_->to_string(buf+pos, buf_len-pos);
      }
      return pos;
    }
	//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140911:b	
	/*Exp:get filter from three places*/
    int ObTableRpcScan::reset_stuff()
	{
        int ret = OB_SUCCESS;
		rpc_scan_.reset_stuff();
        return ret;
	}
	//add 20140911:e
    //add gaojt [Delete_Update_Function] [JHOBv0.1] 20151206:b
    int ObTableRpcScan::reset_stuff_for_ud()
    {
        int ret = OB_SUCCESS;
        rpc_scan_.reset_stuff_for_ud();
        return ret;
    }
     //add 20151206:e
	//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140404:b 
	
	/*Exp:use sub_query's number in main_query to find its index in physical_plan
	* @param idx[in] sub_query's number in main_query
	* @param index[out] sub_query index in physical_plan
	*/
    int ObTableRpcScan::get_sub_query_index(int32_t idx,int32_t & index)
    {
    	return select_get_filter_.get_sub_query_index(idx,index);
    }
	
    /*Exp:remove expression which has sub_query but not use bloomfilter 
    * make stmt pass the first check 
	* it need modify two place: 
	* in rpc_scan's member read_param and in its own member select_get_filter_
    */
    int ObTableRpcScan::remove_or_sub_query_expr()
    {
      int ret = OB_SUCCESS;
	if(OB_SUCCESS != (ret = rpc_scan_.remove_or_sub_query_expr()))
	{
	  TBSYS_LOG(WARN, "fail to remove sub query expr from rpc scan. ret=%d", ret);
	}
	else if(OB_SUCCESS != (ret = select_get_filter_.remove_or_sub_query_expr()))
	{
	  TBSYS_LOG(WARN, "fail to remove sub query expr filter. ret=%d", ret);
	}
	return ret;
    }
	
	/*Exp:update sub_query num
	* if direct bind sub_query result to main query,
	* do not treat it as a sub_query any more
	* it need modify two place: 
	* in rpc_scan's member read_param and in its own member select_get_filter_
	*/
    void ObTableRpcScan::update_sub_query_num()
    {
      select_get_filter_.update_sub_query_num();
      rpc_scan_.update_sub_query_num();
    }

    /*Exp:copy filter expression to object filter
	* used for ObMultiBind second check 
	* @param object[in,out] object filter
	*/
    int ObTableRpcScan::copy_filter(ObFilter &object)
    {
      return select_get_filter_.copy_filter(object);
    }
	//add 20140404:e
    PHY_OPERATOR_ASSIGN(ObTableRpcScan)
    {
      int ret = OB_SUCCESS;
      CAST_TO_INHERITANCE(ObTableRpcScan);
      reset();
      rpc_scan_.set_phy_plan(my_phy_plan_);
      select_get_filter_.set_phy_plan(my_phy_plan_);
      group_columns_sort_.set_phy_plan(my_phy_plan_);
      sort_for_listagg_.set_phy_plan(my_phy_plan_);//add gaojt [ListAgg][JHOBv0.1]20150104
      limit_.set_phy_plan(my_phy_plan_);
      empty_row_filter_.set_phy_plan(my_phy_plan_);

      if ((ret = rpc_scan_.assign(&o_ptr->rpc_scan_)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign rpc_scan_ failed. ret=%d", ret);
      }
      else if ((ret = select_get_filter_.assign(&o_ptr->select_get_filter_)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign select_get_filter_ failed. ret=%d", ret);
      }
      else if ((ret = group_columns_sort_.assign(&o_ptr->group_columns_sort_)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign group_columns_sort_ failed. ret=%d", ret);
      }
      //add gaojt [ListAgg][JHOBv0.1]20150104:b
      else if ((ret = sort_for_listagg_.assign(&o_ptr->sort_for_listagg_)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign sort_for_listagg_ failed. ret=%d", ret);
      }
      /* add 20150104:e */
      else if ((ret = limit_.assign(&o_ptr->limit_)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign limit_ failed. ret=%d", ret);
      }
      else if ((ret = empty_row_filter_.assign(&o_ptr->empty_row_filter_)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign empty_row_filter_ failed. ret=%d", ret);
      }
      else
      {
        if (o_ptr->scalar_agg_)
        {
          if (!scalar_agg_)
          {
            CREATE_PHY_OPERRATOR_NEW(scalar_agg_, ObScalarAggregate, my_phy_plan_, ret);
            if (OB_SUCCESS == ret)
            {
              scalar_agg_->set_phy_plan(my_phy_plan_);
            }
          }
          if (ret == OB_SUCCESS
            && (ret = scalar_agg_->assign(o_ptr->scalar_agg_)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "Assign scalar_agg_ failed. ret=%d", ret);
          }
        }
        if (o_ptr->group_)
        {
          if (!group_)
          {
            CREATE_PHY_OPERRATOR_NEW(group_, ObMergeGroupBy, my_phy_plan_, ret);
            if (OB_SUCCESS == ret)
            {
              group_->set_phy_plan(my_phy_plan_);
            }
          }
          if (ret == OB_SUCCESS
            && (ret = group_->assign(o_ptr->group_)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "Assign group_ failed. ret=%d", ret);
          }
        }
        has_rpc_ = o_ptr->has_rpc_;
        has_scalar_agg_ = o_ptr->has_scalar_agg_;
        has_group_ = o_ptr->has_group_;
        has_group_columns_sort_ = o_ptr->has_group_columns_sort_;
        is_listagg_ = o_ptr->is_listagg_;//add gaojt [ListAgg][JHOBv0.1]20150104
        has_limit_ = o_ptr->has_limit_;
        is_skip_empty_row_ = o_ptr->is_skip_empty_row_;
        //add fanqiushi_index
        is_use_index_rpc_scan_=o_ptr->is_use_index_rpc_scan_;
        //add:e
        read_method_ = o_ptr->read_method_;
      }
      return ret;
    }

    DEFINE_SERIALIZE(ObTableRpcScan)
    {
      UNUSED(buf);
      UNUSED(buf_len);
      UNUSED(pos);
      return OB_NOT_IMPLEMENT;
#if 0
      int ret = OB_SUCCESS;
#define ENCODE_OP(has_op, op) \
      if (OB_SUCCESS == ret) \
      { \
        if (OB_SUCCESS != (ret = common::serialization::encode_bool(buf, buf_len, pos, has_op))) \
        { \
          TBSYS_LOG(WARN, "fail to encode " #has_op ":ret[%d]", ret); \
        } \
        else if (has_op) \
        { \
          if (OB_SUCCESS != (ret = op.serialize(buf, buf_len, pos))) \
          { \
            TBSYS_LOG(WARN, "fail to serialize " #op ":ret[%d]", ret); \
          } \
        } \
      }

      ENCODE_OP(has_scalar_agg_, scalar_agg_);
      ENCODE_OP(has_group_columns_sort_, group_columns_sort_);
      ENCODE_OP(is_listagg_, sort_for_listagg_);//add gaojt [ListAgg][JHOBv0.1]20150104
      ENCODE_OP(has_group_, group_);
      ENCODE_OP(has_limit_, limit_);

#undef ENCODE_OP
      return ret;
#endif
    }

    DEFINE_DESERIALIZE(ObTableRpcScan)
    {
      UNUSED(buf);
      UNUSED(data_len);
      UNUSED(pos);
      return OB_NOT_IMPLEMENT;
#if 0
      int ret = OB_SUCCESS;
#define DECODE_OP(has_op, op) \
      if (OB_SUCCESS == ret) \
      { \
        if (OB_SUCCESS != (ret = common::serialization::decode_bool(buf, data_len, pos, &has_op))) \
        { \
          TBSYS_LOG(WARN, "fail to decode " #has_op ":ret[%d]", ret); \
        } \
        else if (has_op) \
        { \
          if (OB_SUCCESS != (ret = op.deserialize(buf, data_len, pos))) \
          { \
            TBSYS_LOG(WARN, "fail to deserialize " #op ":ret[%d]", ret); \
          } \
        } \
      }

      scalar_agg_.reset();
      DECODE_OP(has_scalar_agg_, scalar_agg_);
      group_columns_sort_.reset();
      DECODE_OP(has_group_columns_sort_, group_columns_sort_);
      //add gaojt [ListAgg][JHOBv0.1]20150104:b
      sort_for_listagg_.reset();
      DECODE_OP(is_listagg_, sort_for_listagg_);
      /* add 20150104:e */
      group_.reset();
      DECODE_OP(has_group_, group_);
      limit_.reset();
      DECODE_OP(has_limit_, limit_);
#undef DECODE_OP
      return ret;
#endif
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTableRpcScan)
    {
      return 0;
#if 0
      int64_t size = 0;
#define GET_OP_SERIALIZE_SIZE(size, has_op, op) \
      size += common::serialization::encoded_length_bool(has_op); \
      if (has_op) \
      { \
        size += op.get_serialize_size(); \
      }

      GET_OP_SERIALIZE_SIZE(size, has_scalar_agg_, scalar_agg_);
      GET_OP_SERIALIZE_SIZE(size, has_group_columns_sort_, group_columns_sort_);
      GET_OP_SERIALIZE_SIZE(size, is_listagg_, sort_for_listagg_);//add gaojt [ListAgg][JHOBv0.1]20150104
      GET_OP_SERIALIZE_SIZE(size, has_group_, group_);
      GET_OP_SERIALIZE_SIZE(size, has_limit_, limit_);
#undef GET_OP_SERIALIZE_SIZE
      return size;
#endif
    }

    ObPhyOperatorType ObTableRpcScan::get_type() const
    {
      return PHY_TABLE_RPC_SCAN;
    }
  } // end namespace sql
} // end namespace oceanbase
