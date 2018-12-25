/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_cs_create_plan.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_cs_create_plan.h"

using namespace oceanbase;
using namespace sql;

int ObCSCreatePlan::get_basic_column_and_join_info(
  const ObProject &project,
  const ObSchemaManagerV2 &schema_mgr,
  const uint64_t table_id, 
  const uint64_t renamed_table_id,
  ObArray<uint64_t> &basic_columns,
  ObTabletJoin::TableJoinInfo &table_join_info
  // mod by maosy [Delete_Update_Function_isolation_RC] 20161218 b:
  //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//  ,const ObDataMarkParam *data_mark_param
  //add duyr 20160531:e
  //mod e
  )
{
  int ret = OB_SUCCESS;
  ObSqlExpression expr;
  uint64_t column_id = OB_INVALID_ID;
  const ObColumnSchemaV2 *column_schema = NULL;
  const ObColumnSchemaV2::ObJoinInfo *join_info = NULL;
  const ObTableSchema *table_schema = NULL;
  ObTabletJoin::JoinInfo join_info_item;
  bool is_cid = false;

  column_ids_.clear();

  //找到所有的需要join的普通列
  for(int32_t i=0;OB_SUCCESS == ret && i<project.get_output_columns().count();i++)
  {
    if(OB_SUCCESS != (ret = project.get_output_columns().at(i, expr)))
    {
      TBSYS_LOG(WARN, "get expression from out columns fail:ret[%d] i[%d]", ret, i);
    }
    if(OB_SUCCESS == ret)
    {
      column_id = expr.get_column_id();

      if(OB_INVALID_ID == expr.get_table_id()
        || table_id == expr.get_table_id()
        || renamed_table_id == expr.get_table_id())
      {
        //do nothing 复合列 or OB_ACTION_FLAG_COLUMN_ID
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "Unknown expression id, table_id[%lu], renamed_table_id[%lu], expr_table_id[%lu]", 
            table_id, renamed_table_id, expr.get_table_id());
      }
    }

     if(OB_SUCCESS == ret)
     {
      //如果是普通列, 复合列不交给下层的物理操作符
      //if( (OB_SUCCESS == (ret = expr.is_column_index_expr(is_cid))) && is_cid)
      uint64_t tid = OB_INVALID_ID;
      if ( (OB_SUCCESS == (ret = expr.get_column_index_expr(tid, column_id, is_cid))) && is_cid && common::OB_ACTION_FLAG_COLUMN_ID != column_id)
      {
        if (column_ids_.has_member((int32_t)column_id))
        {
          TBSYS_LOG(DEBUG, "duplicate column [%lu]", column_id);
          continue; //如果发现重复列，就直接跳过
        }
        else if (!column_ids_.add_member((int32_t)column_id))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fail to add member[%lu]", column_id);
        }
		// mod by maosy [Delete_Update_Function_isolation_RC] 20161218 b:
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//        else if (NULL != data_mark_param
//                 && data_mark_param->is_valid()
//                 && data_mark_param->is_data_mark_cid(column_id))
//        {
//            TBSYS_LOG(DEBUG,"mul_del::debug,data mark cid=%ld",column_id);
//            if(OB_SUCCESS != (ret = basic_columns.push_back(column_id)))
//            {
//              TBSYS_LOG(WARN, "fail to push item:ret[%d],tid[%lu], column_id[%lu]",
//                        ret,table_id, column_id);
//            }
//            else
//            {
//              continue;
//            }
//        }
        //add duyr 20160531:e
		// mod e
        else if(NULL == (column_schema = schema_mgr.get_column_schema(table_id, column_id)))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "get column schema fail:tid[%lu], table_id[%lu] column_id[%lu]", tid, table_id, column_id);
        }
        else
        {
          // erase duplicate columns;
          if(OB_SUCCESS != (ret = basic_columns.push_back(column_id)))
          {
            TBSYS_LOG(WARN, "fail to push item:ret[%d], column_id[%lu]", ret, column_id);
          }
        }

        //构造对应的join_info信息
        if(OB_SUCCESS == ret)
        {
          join_info = column_schema->get_join_info();
          if(NULL != join_info)
          {
            if(OB_INVALID_ID == table_join_info.left_table_id_)
            {
              table_join_info.left_table_id_ = table_id;
            }
            else if(table_join_info.left_table_id_ != table_id)
            {
              ret = OB_ERROR;
              TBSYS_LOG(WARN, "left table id cannot change:t1[%lu], t2[%lu]", table_join_info.left_table_id_, table_id);
            }

            if(OB_SUCCESS == ret)
            {
              if(table_join_info.join_condition_.count() == 0)
              {
                uint64_t left_rowkey_column_id = OB_INVALID_ID;
                for(uint64_t left_col_idx=0;left_col_idx<join_info->left_column_count_;left_col_idx++)
                {
                  int64_t col_idx = join_info->left_column_offset_array_[left_col_idx];
                  if(NULL == (table_schema = schema_mgr.get_table_schema(table_id)))
                  {
                    ret = OB_ERROR;
                    TBSYS_LOG(WARN, "get table schema fail:table_id[%lu]", table_id);
                  }
                  else if(OB_SUCCESS != (ret = table_schema->get_rowkey_info().get_column_id(col_idx, left_rowkey_column_id)))
                  {
                    TBSYS_LOG(WARN, "get left rowkey column id fail:ret[%d], col_idx[%ld]", ret, col_idx);
                  }
                  else if(OB_SUCCESS != (ret = table_join_info.join_condition_.push_back(left_rowkey_column_id)))
                  {
                    TBSYS_LOG(WARN, "push left rowkey column id fail:ret[%d], left_rowkey_column_id[%ld]", ret, left_rowkey_column_id);
                  }
                }
              }
              else if((uint64_t)table_join_info.join_condition_.count() != join_info->left_column_count_)
              {
                ret = OB_ERROR;
                TBSYS_LOG(WARN, "join info count change");
              }
            }

            if(OB_SUCCESS == ret)
            {
              table_join_info.right_table_id_ = join_info->join_table_;
              join_info_item.left_column_id_ = column_schema->get_id();
              join_info_item.right_column_id_ = join_info->correlated_column_;
              if(OB_SUCCESS != (ret = table_join_info.join_column_.push_back(join_info_item)))
              {
                TBSYS_LOG(WARN, "add join info item fail:ret[%d]", ret);
              }
            }
          }
        }
      }
     }
   }

  if(OB_SUCCESS == ret)
  {
    //如果存在需要join的列
    if(table_join_info.join_column_.count() > 0)
    {
      //添加join需要的列id
      if(OB_SUCCESS == ret)
      {
        uint64_t left_column_id = OB_INVALID_ID;
        for(int32_t i=0;OB_SUCCESS == ret && i<table_join_info.join_condition_.count();i++)
        {
          if(OB_SUCCESS != (ret = table_join_info.join_condition_.at(i, left_column_id)))
          {
            TBSYS_LOG(WARN, "get join cond fail:ret[%d] i[%d]", ret, i);
          }
          else if (column_ids_.has_member((int32_t)left_column_id))
          {
            //do nothing
          }
          else if (!column_ids_.add_member((int32_t)left_column_id))
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "fail to add member[%lu]", column_id);
          }
          else
          {
            // erase duplicate columns;
            if(OB_SUCCESS != (ret = basic_columns.push_back(left_column_id)))
            {
              TBSYS_LOG(WARN, "fail to push item:ret[%d], column_id[%lu]", ret, left_column_id);
            }
          }
        }
      }
    }
  }
  
  return ret;
}

