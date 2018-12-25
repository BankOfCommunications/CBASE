/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   gaojt
 * function:
 * Delete_Update_Function
 *
 */
#include "ob_fill_values.h"
#include "common/utility.h"
#include "common/ob_obj_cast.h"
#include "common/hash/ob_hashmap.h"
#include "ob_iud_loop_control.h"
#include "ob_raw_expr.h"
#include "ob_values.h"
//add lbzhong[Update rowkey] 20160112:b
#include "common/page_arena.h"
//add:e

using namespace oceanbase::sql;
using namespace oceanbase::common;
ObFillValues::ObFillValues()
  :
    // add by maosy [Delete_Update_Function]
    max_rowsize_capacity_(256),
    max_row_value_size_(MAX_UDPATE_DELETE_VALUE_SIZE),
  // add e
  is_row_num_null_(true),
    is_delete_update_(false),
    is_column_hint_index_(false),
    //add lijianqiang [sequence update] 20160319:b
    sequence_update_(NULL),
    //add 20160319:e
    is_reset_(0),
    is_multi_batch_(false),
    is_close_sub_query_(true)
  ,questionmark_num_in_assginlist_of_update_(0)
  //add lbzhong [Update rowkey] 20151221:b
  ,is_update_rowkey_(false)
  //add:e
  ,select_result_set_(NULL)
  ,is_in_prepare_(false)
  // add by maosy [Delete_Update_Function]
     , affect_row_(0)
  ,is_multi_batch_over_(false)
  // add e
  ,is_already_clear_(false)
  ,need_to_tage_empty_row_(false)// add by maosy [Delete_Update_Function_isolation_RC] 20161218
{
  //add lbzhong [Update rowkey] 20160319:b
  hint_expr_.read_method_ = ObSqlReadStrategy::USE_SCAN;
  //add:e
}

ObFillValues::~ObFillValues()
{
  //add lbzhong [Update rowkey] 20160107:b
  clear_exprs();
  //add:e
}
void ObFillValues::reset_for_prepare_multi()
{
    is_reset_ = 0;
    is_multi_batch_over_ = false;
    is_row_num_null_ = true;
    is_close_sub_query_ = true;
    affect_row_ = 0;
}
void ObFillValues::reset()
{
    is_row_num_null_ = true;
    is_column_hint_index_ = false;
    is_delete_update_ = false;
    row_desc_.reset();
    is_reset_ = 0;
    is_multi_batch_ = false;
    is_close_sub_query_ = true;
    //add lbzhong [Update rowkey] 20151221:b
    is_update_rowkey_ = false;
    clear_exprs();
    update_rowkey_columns_.clear();
    update_columns_flag_.clear();
    if(NULL != table_rpc_scan_for_update_rowkey_)
    {
      table_rpc_scan_for_update_rowkey_->reset();
    }
    //add:e
  column_items_.clear();
    select_result_set_ = NULL;
    is_in_prepare_ = false;
    questionmark_num_in_assginlist_of_update_ = 0;
    // add by maosy [Delete_Update_Function]
    affect_row_ = 0;
    is_multi_batch_over_=false;
    max_rowsize_capacity_=256;
    max_row_value_size_ = MAX_UDPATE_DELETE_VALUE_SIZE;
    // add e
    rowkey_ids_.clear();
    is_already_clear_ = false;
    need_to_tage_empty_row_ = false;// add by maosy [Delete_Update_Function_isolation_RC] 20161218
}

void ObFillValues::reuse()
{
    is_row_num_null_ = true;
    is_column_hint_index_ = false;
    is_delete_update_ = false;
    row_desc_.reset();
    reset(); //mod lbzhong [Update rowkey] 20151221:b:e
    is_reset_ = 0;
    is_multi_batch_ = false;
    is_close_sub_query_ = true;
    column_items_.clear();
    select_result_set_ = NULL;
    is_in_prepare_ = false;
    questionmark_num_in_assginlist_of_update_ = 0;
    // add by maosy [Delete_Update_Function]
    affect_row_=0;
    is_multi_batch_over_=false;
    max_rowsize_capacity_=256;
    max_row_value_size_ = MAX_UDPATE_DELETE_VALUE_SIZE;
    // add e
    is_already_clear_ = false;
    need_to_tage_empty_row_ = false;// add by maosy [Delete_Update_Function_isolation_RC] 20161218
}

int ObFillValues::set_row_desc(const common::ObRowDesc &row_desc)
{
  row_desc_ = row_desc;
  return OB_SUCCESS;
}
/*
 * first,check the valid of children operator
 * then,call the fill_values function, filling the ObExprValues
 *      and the child of ObValues operator with values fetched
 *      from the first child of ObFillValues
 * last,if there is none row to delete or update,don't open the
 *      second and third child
 */
int ObFillValues::open()
{
  int ret = OB_SUCCESS;
  //add gaojt [Delete_Update_Function_isolation] [JHOBv0.1] 20160616:b
//  bool is_start_transaction = true;
//  if (!is_multi_batch_ )
//  {
//      if (sql_context_.session_info_->get_trans_id().is_valid())
//      {
//         is_start_transaction = false;
//      }
//      else if (OB_SUCCESS != (ret = start_transaction()))
//      {
//          TBSYS_LOG(WARN,"fail to start transaction");
//      }
//      else
//      {
//          TBSYS_LOG(DEBUG,"start transaction");
//      }
//  }
  //add gaojt 20160616:e
  // add by maosy [Delete_Update_Function]
  affect_row_=0;
  // add e
//  ObTableRpcScan* table_rpc_scan_for_values = NULL;
  ObValues* tmp_value = dynamic_cast<ObValues*>(get_child(0));
  ObExprValues* expr_values = dynamic_cast<ObExprValues*>(get_child(1));
  if (OB_SUCCESS != ret)
  {

  }
  else if(NULL == tmp_value || NULL == expr_values)
  {
      ret = OB_ERROR;
      TBSYS_LOG(WARN,"tmp_value[%p] or expr_values[%p]",tmp_value,expr_values);
  }
//  else if(NULL == (table_rpc_scan_for_values = dynamic_cast<ObTableRpcScan*>(tmp_value->get_child(0))))
//  {
//      ret = OB_ERROR;
//      TBSYS_LOG(WARN,"table_rpc_scan_for_values is NULL");
//  }
  else if(1==is_reset_&&is_multi_batch_)
  {
//      table_rpc_scan_for_values->reset_stuff_for_ud();
      expr_values->reset_stuff_for_insert();
  }
  else
  {
      sql_context_.session_info_->set_read_times (FIRST_SELECT_READ);
      common::ObString stmt_name = common::ObString::make_string(prepare_name_.data());
      select_result_set_ = sql_context_.session_info_->get_plan(stmt_name);
      if(NULL == select_result_set_)
      {
          ret = OB_ERROR;
          TBSYS_LOG(WARN,"select_result_set_ is NULL");
      }
      else if (OB_SUCCESS != (ret=select_result_set_->open()))
      {
          TBSYS_LOG(ERROR,"fail to open the constucted select-result-set");
      }
      else
      {
          if(is_multi_batch_)
          {
              is_close_sub_query_ = false;
              is_reset_ = 1;
          }
          // add by maosy [Delete_Update_Function]
          int64_t cell_num  =0;
          // mod by maosy 20160919 todo
//          if(is_column_hint_index_)
//          {
              cell_num = row_desc_.get_column_num();
//          }
//          else
//          {
//              cell_num = row_desc_.get_rowkey_cell_count();
//          }
          cell_num = cell_num >=4 ? cell_num:4;
          max_rowsize_capacity_ = 20164/(cell_num+2);
          TBSYS_LOG(DEBUG,"MAX CELL ROW SIZE=%ld",max_rowsize_capacity_);
          ObUpsModifyWithDmlType *ups_modify = NULL;
          ObPhysicalPlan * phy_plan = get_phy_plan ();
          int64_t phy_count = phy_plan->get_operator_size ();
          for(int64_t i =0  ; i <phy_count ;i++)
          {
              if(NULL != (ups_modify = dynamic_cast<ObUpsModifyWithDmlType *>(phy_plan->get_phy_operator (i))))
              {
                  max_row_value_size_ -= ups_modify->get_child (0)->get_serialize_size ();
                  break;
              }
          }
          TBSYS_LOG(DEBUG,"a packet size value size = %ld",max_row_value_size_);
          // add e
          TBSYS_LOG(DEBUG,"mul_ud:debug- row_desc=%s",to_cstring(row_desc_));
      }
      //del by maosy [Delete_Update_Function]
      //is_iud = 1;
      //del e
  }
  if(OB_SUCCESS == ret)
  {
//      if(OB_SUCCESS != (ret = table_rpc_scan_for_values->reset_stuff_for_ud()))
//      {
//          TBSYS_LOG(WARN,"fail to reset_stuff in table_rpc_scan_for_values");
//      }
      if(OB_SUCCESS != (ret = expr_values->reset_stuff_for_ud()))
      {
          TBSYS_LOG(WARN,"fail to reset_stuff in expr_values");
      }
      else if(OB_SUCCESS != (ret = expr_values->open()))
      {
          TBSYS_LOG(WARN,"fail to open ob_expr_values");
      }
      else if(OB_SUCCESS != (ret = fill_values(tmp_value,
                                               rowkey_info_,
                                               expr_values,
                                               row_desc_,
                                               table_id_,
                                               is_close_sub_query_))
      //add lbzhong [Update rowkey] 20151222:b
          && OB_ERR_PRIMARY_KEY_DUPLICATE != ret
          //add:e
      )
      {
          TBSYS_LOG(WARN,"fail to fill expr values,expr_values=%p\
                    ",expr_values);
      }
  }
  if(OB_SUCCESS == ret)
  {
      // mod by maosy [Delete_Update_Function_isolation_RC] 20161218 b:
      //add by maosy [Delete_Update_Function_for_snpshot] 20161210
      //       sql_context_.session_info_->set_read_times (2);
      //add by maosy 20161210
      // mod by maosy e:
      //      if(OB_SUCCESS != (ret = tmp_value->open()))
      //     {
      //         TBSYS_LOG(WARN,"fail to open values");
      //     }
  if(!is_row_num_null_ &&OB_SUCCESS != (ret = expr_values->store_input_values()))
      {
          TBSYS_LOG(ERROR,"fail to do the function: store_input_values");
      }
  }
  //add gaojt [Delete_Update_Function_isolation] [JHOBv0.1] 20160616:b
//  if (!is_multi_batch_ )
//  {
//      if ( OB_SUCCESS == ret && is_start_transaction)
//      {
//          if (OB_SUCCESS != (ret = commit()))
//          {
//              TBSYS_LOG(WARN,"fail to commit");
//          }
//      }
//      if (OB_SUCCESS != ret && is_start_transaction )
//      {
//          if (OB_SUCCESS != (rollback()))
//          {
//              TBSYS_LOG(WARN,"fail to rollback");
//          }
//      }
//  }
  //add gaojt 20160616:e
  // mod by maosy [Delete_Update_Function_isolation_RC] 20161218 b:
//  sql_context_.session_info_->set_read_times (0);
// mod by maosy e:
  return ret;
}
/*
 * if there is none row to delete or udpate, then just close the first child which
 *    is only opened
 * else close all the children of ObFillValues
 *
 */
int ObFillValues::close()
{
    int ret = OB_SUCCESS;
    if( !is_row_num_null_ && OB_SUCCESS != (ret = ObMultiChildrenPhyOperator::close()))
    {
        TBSYS_LOG(WARN, "failed to close child_op in ob_bind_values, ret=%d",ret);
    }
    else if (is_row_num_null_)
    {
        for (int32_t index = 0; OB_SUCCESS == ret && index < ObMultiChildrenPhyOperator::get_child_num(); index++)
        {
            if( 1 == index) // If the update/delete rows is 0, then don't close the ObExprValues
            {
                continue;
            }
            else if (OB_SUCCESS != (ret = ObMultiChildrenPhyOperator::get_child(index)->close()))
            {
                TBSYS_LOG(WARN,"fail to close the [%d]th child of ObFillValues",index);
            }
        }
    }
    if(is_close_sub_query_)
    {
        if( select_result_set_ && OB_SUCCESS != (ret = select_result_set_->close()))
        {
            TBSYS_LOG(WARN, "failed to close result_set_, ret=%d",ret);
        }
        if (!is_in_prepare_)
        {
            common::ObString temp_stmt = common::ObString::make_string(prepare_name_.data());
            uint64_t stmt_id = OB_INVALID_ID;
            if (sql_context_.session_info_->plan_exists(temp_stmt, &stmt_id) == false)
            {
                ret = OB_ERR_PREPARE_STMT_UNKNOWN;
                TBSYS_LOG(WARN,"Unknown prepared statement handler (%.*s) given to DEALLOCATE PREPARE",
                          temp_stmt.length(), temp_stmt.ptr());
            }
            else if (OB_SUCCESS != (ret = sql_context_.session_info_->remove_plan(stmt_id)))
            {
                TBSYS_LOG(ERROR,"fail to remove plan, ret=%d",ret);
            }
            is_already_clear_ = true;
        }
    }
    return ret;
}

int ObFillValues::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  UNUSED(row);
  return ret;
}

int ObFillValues::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  row_desc = &row_desc_;
  return OB_SUCCESS;
}
/*
 * construct the left part of in operator
 */
//int ObFillValues::construct_in_left_part(const ObRowDesc& row_desc,
//                           const uint64_t table_id,
//                           const ObRowkeyInfo &rowkey_info,
//                           ObSqlExpression *&rows_filter,
//                           ExprItem& expr_item)
//{
//    int ret = OB_SUCCESS;
//    // construct left operand of IN operator
//    // the same order with row_desc
//    expr_item.type_ = T_REF_COLUMN;
//    expr_item.value_.cell_.tid = table_id;
//    int64_t rowkey_column_num = rowkey_info.get_size();
//    uint64_t tid = OB_INVALID_ID;
//    //TBSYS_LOG(ERROR,"TETE:DYR row_desc:%s", to_cstring(row_desc));
//    for (int i = 0; OB_SUCCESS == ret && i < row_desc.get_column_num(); ++i)
//    {
//        if (OB_UNLIKELY(OB_SUCCESS != (ret = row_desc.get_tid_cid(i, tid, expr_item.value_.cell_.cid))))
//        {
//            TBSYS_LOG(ERROR,"fail to get-tid-cid from row_desc,ret=%d",ret);
//            break;
//        }
//        else if (rowkey_info.is_rowkey_column(expr_item.value_.cell_.cid))
//        {
//            if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
//            {
//                TBSYS_LOG(WARN, "failed to add expr item, err=%d", ret);
//                break;
//            }
//        }
//    } // end for
//    if (OB_LIKELY(OB_SUCCESS == ret))
//    {
//        expr_item.type_ = T_OP_ROW;
//        expr_item.value_.int_ = rowkey_column_num;
//        if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
//        {
//            TBSYS_LOG(WARN,"Failed to add expr item, err=%d", ret);
//        }
//    }
//    if (OB_LIKELY(OB_SUCCESS == ret))
//    {
//        expr_item.type_ = T_OP_LEFT_PARAM_END;
//        // a in (a,b,c) => 1 Dim;  (a,b) in ((a,b),(c,d)) =>2 Dim; ((a,b),(c,d)) in (...) =>3 Dim
//        expr_item.value_.int_ = 2;
//        if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
//        {
//            TBSYS_LOG(WARN, "failed to add expr item, err=%d", ret);
//        }
//    }
//    return ret;
//}
///*
// * construct the right part of in operator
// */
//int ObFillValues::construct_in_right_part(const ObRowkeyInfo &rowkey_info,
//                                          const ObRow *row,
//                                          ObSqlExpression *&rows_filter,
//                                          ExprItem &expr_item,
//                                          int64_t &total_row_size)
//{
//    int ret = OB_SUCCESS;
//    int64_t rowkey_num = rowkey_info.get_size();
//    const ObObj* cell = NULL;
//    for(int64_t index = 0; ret == OB_SUCCESS && index < rowkey_num; index++)
//    {
//        ObConstRawExpr col_val;
//        if ( OB_SUCCESS != (ret = const_cast<ObRow *>(row)->raw_get_cell(index,cell)))
//        {
//            TBSYS_LOG(WARN,"fail to get the %ldth cell",index);
//        }
//        else if(NULL == cell)
//        {
//            TBSYS_LOG(WARN,"get null cell");
//            ret = OB_ERROR;
//            break;
//        }
//        else
//        {
//            total_row_size +=cell->get_serialize_size();
//        }
//        if(OB_SUCCESS != ret)
//        {

//        }
//        else if(OB_SUCCESS != (ret = col_val.set_value_and_type((*cell))))
//        {
//            TBSYS_LOG(WARN,"fail to set column value,err=%d",ret);
//            break;
//        }
//        else if ((ret = col_val.fill_sql_expression(*rows_filter)) != OB_SUCCESS)
//        {
//            TBSYS_LOG(WARN,"Add table output columns failed");
//            break;
//        }

//    }
//    if(OB_SUCCESS == ret)
//    {
//        expr_item.type_ = T_OP_ROW;
//        expr_item.value_.int_ = rowkey_info.get_size();
//        if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
//        {
//            TBSYS_LOG(WARN,"Failed to add expr item, err=%d", ret);
//        }
//    }
//    return ret;
//}
///*
// *construct the end part of in operator
// *
// */
//int ObFillValues::construct_in_end_part(ObTableRpcScan *&table_scan,
//                          ObSqlExpression *&rows_filter,
//                          ObRowkeyInfo rowkey_info,
//                          int64_t row_num,
//                          ExprItem &expr_item)
//{
//    int ret = OB_SUCCESS;
//    int64_t tmp_row_num = row_num;
//    int64_t rowkey_column_num = rowkey_info.get_size();
//    int64_t tmp_rowkey_col_num = rowkey_column_num;
//    if(0 == tmp_row_num)
//    {
//        tmp_row_num = 1;
//        while(OB_SUCCESS == ret && tmp_rowkey_col_num>0)
//        {
//            tmp_rowkey_col_num--;
//            ObConstRawExpr col_val;
//            ObObj null_obj;
//            col_val.set_value_and_type(null_obj);
//            if ((ret = col_val.fill_sql_expression(*rows_filter)) != OB_SUCCESS)
//            {
//                TBSYS_LOG(WARN,"Add table output columns failed");
//                break;
//            }
//        }
//        if(OB_SUCCESS == ret)
//        {
//            expr_item.type_ = T_OP_ROW;
//            expr_item.value_.int_ = rowkey_column_num;
//            if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
//            {
//                TBSYS_LOG(WARN,"Failed to add expr item, err=%d", ret);
//            }
//        }
//    }

//    expr_item.type_ = T_OP_ROW;
//    expr_item.value_.int_ = tmp_row_num;
//    ExprItem expr_in;
//    expr_in.type_ = T_OP_IN;
//    expr_in.value_.int_ = 2;
//    if(OB_SUCCESS != ret)
//    {

//    }
//    else if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
//    {
//        TBSYS_LOG(WARN,"Failed to add expr item, err=%d", ret);
//    }
//    else if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_in)))
//    {
//        TBSYS_LOG(WARN,"Failed to add expr item, err=%d", ret);
//    }
//    else if (OB_SUCCESS != (ret = rows_filter->add_expr_item_end()))
//    {
//        TBSYS_LOG(WARN,"Failed to add expr item end, err=%d", ret);
//    }
//    else if (OB_SUCCESS != (ret = table_scan->add_filter(rows_filter)))
//    {
//        TBSYS_LOG(WARN,"Failed to add filter, err=%d", ret);
//    }
//    return ret;
//}
/*
 * first,open the first child of ObFillValues
 * then,get the rows from first child one by one,
 *      and finish the following things with these rows:
 * if the size of rows fetched over the theshhold 2M,then report error
 * else
 *    1.call the three functions:complete_in_left_part complete_in_right_part
 *      complete_in_end_part to construct in-operator, then fill in-operator
 *      to the child of the second child of ObFillValues,which is table_rpc_scan_for_obvalues
 *    2.transform the rows to ObSqlExpression, which is filled to get_param_values as the
 *      deleting or updating values
 *
 */
int ObFillValues::fill_values(ObValues *&tmp_value,
                                    const ObRowkeyInfo &rowkey_info,
                                    ObExprValues*& get_param_values,
                                    const ObRowDesc &row_desc,
                                    const uint64_t table_id,
                                    bool &is_close_sub_query)
{

    int ret = OB_SUCCESS;
    int64_t row_num = 0;
//    ExprItem expr_item;
    //ObSqlExpression *rows_filter = ObSqlExpression::alloc();//del by maosy [Delete_Update_Function]20170505
    //add lbzhong [Update rowkey] 20160108:b
    ObArray<ObRow*> row_array; //for check duplication where update sequence
    ModuleArena row_alloc(OB_MAX_VARCHAR_LENGTH, ModulePageAllocator(ObModIds::OB_SQL_TRANSFORMER));
    std::set<int64_t> rowkey_set; //save rowkey hash value
    bool is_update_rowkey_sequence = false;
    ObSequenceUpdate * sequence_update_op = NULL;
    if (OB_SUCCESS == ret && NULL != (sequence_update_op = dynamic_cast<ObSequenceUpdate *>(sequence_update_)))
    {
      if(OB_SUCCESS != (ret = sequence_update_op->is_update_rowkey_sequence(is_update_rowkey_sequence, update_columns_flag_)))
      {
        TBSYS_LOG(WARN, "fail to get is_update_rowkey_sequence from sequence_update_op, ret=%d", ret);
      }
    }
    if(OB_SUCCESS != ret)
    {
      //do nothing!
    }
    //add:e
    //del by maosy [Delete_Update_Function]20170505
//    else if (NULL == rows_filter)
//    {
//        ret = OB_ALLOCATE_MEMORY_FAILED;
//        TBSYS_LOG(WARN, "no memory");
//    }
    //del by maosy
    else
    {
  // add by maosy [Delete_Update_Function]
        int64_t total_row_size=0;
        //int64_t total_rowkey_size = 0;
        // add e
        const ObRow* row = NULL;
//        if(OB_SUCCESS != (ret = construct_in_left_part(row_desc,table_id,rowkey_info,rows_filter,expr_item)))
//        {
//            TBSYS_LOG(ERROR,"fail to construct in left part,ret=%d",ret);
//        }
        while(OB_SUCCESS == ret)
        {
            if(is_multi_batch_)
            {
                // mod by maosy  [Delete_Update_Function]
//                if((total_rowkey_size < MAX_UDPATE_DELETE_VALUE_SIZE)&&(row_num<MAX_UDPATE_DELETE_ROW_NUM))
                if(total_row_size < max_row_value_size_ && row_num< max_rowsize_capacity_)
                {
                    ret = select_result_set_->get_next_row(row);
                }
                // mod e
            }
            else
            {
                ret = select_result_set_->get_next_row(row);
            }
            if(OB_ITER_END == ret)
            {
                // mod by maosy [Delete_Update_Function_isolation_RC] 20161218 b:
                sql_context_.session_info_->set_read_times (CLEAR_TIMESTAMP);
                // mod by maosy e
                if(is_multi_batch_)
                {
                    is_close_sub_query = true;
                    // mod by maosy [Delete_Update_Function]
                    //is_multi_batch_over = true;
                    is_multi_batch_over_ = true;
                    //mod e
                }
                if(0 == row_num)
                {
                    is_row_num_null_ = true;
                    // mod by maosy [Delete_Update_Function_isolation_RC] 20161218 b:
                    if(need_to_tage_empty_row_)
                    {
                        sql_context_.session_info_->set_read_times (EMPTY_ROW_CLEAR);
                    }
                    // mod by maosy e
                }
                ret = OB_SUCCESS;
                break;
            }
            else if(!is_multi_batch_&&(OB_SUCCESS == ret) && (NULL == row))
            {
                TBSYS_LOG(WARN, "fail to get row from select statement,err = %d", ret);
                ret = OB_ERROR;
            }
            // mod by maosy  [Delete_Update_Function]
//            else if(is_multi_batch_&&(OB_SUCCESS == ret) &&(NULL == row || total_rowkey_size >= MAX_UDPATE_DELETE_VALUE_SIZE || row_num>=MAX_UDPATE_DELETE_ROW_NUM))
//            {
            else if(is_multi_batch_&&(OB_SUCCESS == ret) &&(NULL == row || total_row_size >= max_row_value_size_|| row_num >= max_rowsize_capacity_))
            {
                // mod e
                if (NULL == row)
                {
                    TBSYS_LOG(WARN, "fail to get row from select statement,err = %d", ret);
                    ret = OB_ERROR;
                }
                break;
            }
            else if(OB_SUCCESS != ret)
            {
                TBSYS_LOG(WARN,"fail to get next row,ret = %d",ret);
            }
            //add lbzhong [Update rowkey] 20151221:b
            else if(is_update_rowkey_ && !is_update_rowkey_sequence
                    && (OB_SUCCESS != (ret = check_rowkey_duplication(table_rpc_scan_for_update_rowkey_, row, rowkey_set))))
            {
              if(OB_ERR_PRIMARY_KEY_DUPLICATE != ret)
              {
                TBSYS_LOG(ERROR,"fail to check_rowkey_duplication,ret = %d",ret);
              }
              break;
            }
            //add:e
            else
            {
                row_num++;
                // add  by maosy b:
                int64_t  num = row->get_column_num ();
                for(int64_t i = 0 ;OB_SUCCESS==ret && i <num ;i++)
                {
                    const ObObj *cell = NULL;
                    if ( OB_SUCCESS != (ret = const_cast<ObRow *>(row)->raw_get_cell(i,cell)))
                    {
                        TBSYS_LOG(WARN,"fail to get the %ldth cell",i);
                    }
                    else if(cell != NULL)
                    {
                        total_row_size += cell->get_serialize_size ();
                    }
                }
                // add e
                TBSYS_LOG(DEBUG,"select-row=%s",to_cstring(row_desc));
                if(OB_SUCCESS == ret && is_update_rowkey_sequence)
                {
                    void *tmp_row_buf = ob_malloc(sizeof(ObRow), ObModIds::OB_UPDATE_ROWKEY);
                    if (NULL == tmp_row_buf)
                    {
                        TBSYS_LOG(WARN, "no memory");
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                    }
                    else
                    {
                      ObRow* tmp_row = new (tmp_row_buf) ObRow(*row, row_alloc); // deep copy
                      if(OB_SUCCESS != (ret = row_array.push_back(tmp_row)))
                      {
                          TBSYS_LOG(WARN,"fail to push_back(row=%s) to row_array, ret = %d", to_cstring(*row), ret);
                      }
                    }
                }
                //add:e
//                if (OB_SUCCESS != ret)
//                {
//                }
//                else if(OB_SUCCESS != (ret = construct_in_right_part(rowkey_info,
//                                                                     row,
//                                                                     rows_filter,
//                                                                     expr_item,
//                                                                     total_row_size)))
//                {
//                    TBSYS_LOG(ERROR,"fail to construct in right part,row=%p,rows_filter=%p,ret=%d",row,rows_filter,ret);
//                }
                if(OB_SUCCESS != ret)
                {}
                else if (OB_SUCCESS != (ret = add_row_to_obvalues(tmp_value,const_cast<ObRow*>(row))))
                {
                    TBSYS_LOG(WARN,"fail to add row to obvalues,ret[%d]",ret);
                }
                else if(is_column_hint_index_)
                {
                    ObRow val_row;//for expr_values, temp value
                    val_row.set_row_desc(row_desc_);
                    // add by maosy [Delete_Update_Function] for fix no gard for row size 20161031 b:
//                    int64_t rowkey_num = rowkey_info.get_size();
//                    OB_ASSERT(rowkey_num==rowkey_ids_.count());
                   int64_t cell_num = row->get_column_num ();
//                    for(int64_t index = 0; ret == OB_SUCCESS && index < rowkey_num; index++)
                   for(int64_t index = 0; ret == OB_SUCCESS && index < cell_num; index++)
                    {
                        const ObObj *cell = NULL;
                        // add by maosy [Delete_Update_Function] for fix no gard for row size 20161031 b:
                        uint64_t tid = OB_INVALID_ID;
                        uint64_t column_id = OB_INVALID_ID;
//                        if ( OB_SUCCESS != (ret = const_cast<ObRow *>(row)->raw_get_cell(index,cell)))
                        if ( OB_SUCCESS != (ret = row_desc.get_tid_cid (index,tid,column_id)))
                            // add by maosy 20161031 :e
                        {
                            TBSYS_LOG(WARN,"fail to get the %ldth cell",index);
                        }
                        else
                        {
                            // add by maosy [Delete_Update_Function] for fix no gard for row size 20161031 b:
                             if(!rowkey_info.is_rowkey_column (column_id))
                            {
                                continue;
                            }
                             else if ( OB_SUCCESS != (ret = const_cast<ObRow *>(row)->raw_get_cell(index,cell)))
                             {
                                 TBSYS_LOG(WARN,"fail to get the %ldth cell",index);
                             }
                            else if(NULL == cell)
                            {
                                TBSYS_LOG(WARN,"get null cell");
                                ret = OB_ERROR;
                                break;
                            }
                            // add by maosy  20161031 :e
                              //else if((ret = val_row.set_cell(table_id,rowkey_ids_.at (index),*cell)) != OB_SUCCESS)
                            else if((ret = val_row.set_cell(table_id,column_id,*cell)) != OB_SUCCESS)
                            {
                                TBSYS_LOG(WARN, "Add value to ObRow failed");
                            }
                            // add by maosy  [Delete_Update_Function]
                            else
                            {
                                total_row_size+=cell->get_serialize_size();
                            }
                            // add e
                        }
                    }
                    //add second index column
                    int64_t column_num = row_desc.get_column_num();
                    for (int32_t i = 0; ret == OB_SUCCESS && i < column_num; i++)
                    {
                        uint64_t cid = OB_INVALID_ID;
                        uint64_t tid = OB_INVALID_ID;
                        if(OB_SUCCESS != (ret = row_desc.get_tid_cid(i, tid, cid)))
                        {
                            TBSYS_LOG(WARN, "get cid from row_desc failed,ret[%d]", ret);
                            break;
                        }
                        else if(!rowkey_info.is_rowkey_column(cid))
                        {
                            ObObj           null_obj;
                            if((ret = val_row.set_cell(tid, cid, null_obj)) != OB_SUCCESS)
                            {
                                TBSYS_LOG(WARN, "Add value to ObRow failed");
                            }
                            // add by maosy  [Delete_Update_Function]
                            else
                            {
                                total_row_size+=null_obj.get_serialize_size();
                            }
                            // add e
                        }
                    }
                    if(OB_SUCCESS == ret)
                    {
                        if (OB_SUCCESS != (ret = get_param_values->add_values(val_row)))
                        {
                            TBSYS_LOG(WARN,"Failed to add cell into get param, err=%d", ret);
                            break;
                        }
                        else
                        {
                            is_row_num_null_ = false;
                        }
                    }
                }
                else
                {
                    ObRow val_row;//for expr_values, temp value
                    val_row.set_row_desc(row_desc_);
                    // add by maosy [Delete_Update_Function] for fix no gard for row size 20161031 b:
//                   int64_t rowkey_num = rowkey_info.get_size();
                   int64_t cell_num = row->get_column_num ();
//                    for(int64_t index = 0; ret == OB_SUCCESS && index < rowkey_num; index++)
                   for(int64_t index = 0; ret == OB_SUCCESS && index < cell_num; index++)
                    {
                        const ObObj *cell = NULL;
                        // add by maosy [Delete_Update_Function] for fix no gard for row size 20161031 b:
                        uint64_t tid = OB_INVALID_ID;
                        uint64_t column_id = OB_INVALID_ID;
//                        if ( OB_SUCCESS != (ret = const_cast<ObRow *>(row)->raw_get_cell(index,cell)))
                       if ( OB_SUCCESS != (ret =row_desc.get_tid_cid (index,tid,column_id)))
                            // add by maosy 20161031 :e
                        {
                            TBSYS_LOG(WARN,"fail to get the %ldth cell",index);
                        }
                        else
                        {
                            // add by maosy [Delete_Update_Function] for fix no gard for row size 20161031 b:
                             if(!rowkey_info.is_rowkey_column (column_id))
                            {
                                continue;
                            }
                           else if ( OB_SUCCESS != (ret = const_cast<ObRow *>(row)->raw_get_cell(index,cell)))
                           {
                               TBSYS_LOG(WARN,"fail to get the %ldth cell",index);
                           }
                          else if(NULL == cell)
                          {
                              TBSYS_LOG(WARN,"get null cell");
                              ret = OB_ERROR;
                              break;
                          }
                            // add by maosy  20161031 :e
                            // else if((ret = val_row.set_cell(table_id,rowkey_ids_.at (index),*cell)) != OB_SUCCESS)
                            else if((ret = val_row.set_cell(table_id,column_id,*cell)) != OB_SUCCESS)
                            {
                                TBSYS_LOG(WARN, "Add value to ObRow failed");
                            }
                            // add by maosy  [Delete_Update_Function]
                            else
                            {
                                total_row_size+=cell->get_serialize_size();
                            }
                            // add e
                        }
                    }
                    //add lbzhong [Update rowkey] 20151222:b
                    if(is_update_rowkey_)
                    {
                        if(OB_SUCCESS == ret)
                        {
                            uint64_t rowkey_tid = OB_INVALID_ID;
                            uint64_t rowkey_cid = OB_INVALID_ID;
                            for (int64_t j = rowkey_info_.get_size(); j < row_desc_.get_column_num(); ++j)
                            {
                                if (OB_SUCCESS != (ret = row_desc_.get_tid_cid(j, rowkey_tid, rowkey_cid)))
                                {
                                    TBSYS_LOG(WARN, "Failed to get tid cid");
                                    break;
                                }
                                else if(!exist_column(rowkey_cid))
                                {
                                    ObObj           null_obj;
                                    if((ret = val_row.set_cell(rowkey_tid, rowkey_cid, null_obj)) != OB_SUCCESS)
                                    {
                                        TBSYS_LOG(WARN, "Add value to ObRow failed");
                                    }
                                    // add by maosy  [Delete_Update_Function]
                                    else
                                    {
                                        total_row_size+=null_obj.get_serialize_size();
                                    }
                                    // add e
                                }
                            }
                        }
                    }
                    //add:e
                    if(OB_SUCCESS == ret)
                    {
                        if (OB_SUCCESS != (ret = get_param_values->add_values(val_row)))
                        {
                            TBSYS_LOG(WARN,"Failed to add cell into get param, err=%d", ret);
                            break;
                        }
                        else
                        {
                            is_row_num_null_ = false;
                        }
                    }
                    //add:e
                }
            }
        }
    }
  if (OB_SUCCESS == ret && NULL != sequence_update_op)
    {
      //add lbzhong [Update rowkey] 20160111:b
      int64_t nonrowkey_count = row_desc.get_column_num() - rowkey_info.get_size();
      for(int64_t i = 0; i < update_columns_flag_.count(); i++)
      {
        if(0 == update_columns_flag_.at(i))
        {
          nonrowkey_count--;
        }
      }
      //add:e
      if(OB_SUCCESS != (ret = sequence_update_op->handle_the_set_clause_of_seuqence(row_num
                                  //add lbzhong [Update rowkey] 20160107:b
                                  , nonrowkey_count, is_update_rowkey_
                                  //add:e
                                  )))
      {
        TBSYS_LOG(WARN, "fail to handle_the_set_clause_of_seuqence, ret=%d", ret);
      }
      //add lbzhong [Update rowkey] 20151221:b
      else if(is_update_rowkey_sequence && OB_SUCCESS !=
              (ret = check_sequence_update_rowkey_duplication(table_rpc_scan_for_update_rowkey_, row_array, rowkey_set,
                                                              sequence_update_op->get_project_for_update())))
      {
        if(OB_ERR_PRIMARY_KEY_DUPLICATE != ret)
        {
          TBSYS_LOG(ERROR,"fail to check_sequence_update_rowkey_duplication,ret = %d",ret);
        }
      }
      for(int64_t row_idx = 0; row_idx < row_array.count(); row_idx++)
      {
        ObRow* tmp_row = row_array.at(row_idx); //free row memory
        tmp_row->~ObRow();
        ob_free(tmp_row);
        tmp_row = NULL;
      }
      row_array.clear();
      //add:e
    }
  // add by maosy [Delete_Update_Function]
  affect_row_+=row_num;
  // add e
  //del by maosy [Delete_Update_Function]
//    iud_affect_rows = iud_affect_rows + row_num;
//    if (NULL != rows_filter)
//    TBSYS_LOG(INFO,"iud_affect_rows = %ld,rows_filter=[%s]",
//              row_num,to_cstring(*rows_filter));
  //del e
//    if(OB_SUCCESS == ret)
//    {
//        if(OB_SUCCESS != (ret = construct_in_end_part(table_rpc_scan_for_obvalues,
//                                                      rows_filter,
//                                                      rowkey_info,
//                                                      row_num,
//                                                      expr_item)))
//        {
//            TBSYS_LOG(ERROR,"fail to construct in end part,rows_filter=%p,row_num=%ld,ret=%d",rows_filter,row_num,ret);
//        }
//    }
    // del by maosy [Delete_Update_Function]
//    TBSYS_LOG(DEBUG,"iud_affect_rows = %ld,rows_filter=[%s]",
//              iud_affect_rows,to_cstring(*rows_filter));
//    if (OB_SUCCESS != ret && NULL != rows_filter)
//    {
//      ObSqlExpression::free(rows_filter);
//    }
    //del e
    return ret;
}

int ObFillValues::add_row_to_obvalues(ObValues*& tmp_value,ObRow *row)
{
    int ret = OB_SUCCESS;

    OB_ASSERT(tmp_value && row);
    if (OB_SUCCESS != (ret = tmp_value->add_values_for_ud(row)))
    {
        TBSYS_LOG(WARN,"fail to add value for ud,ret[%d]",ret);
    }

    return ret;
}

const ColumnItem* ObFillValues::get_column_item(int32_t index) const
{
  const ColumnItem *column_item = NULL;
  if (0 <= index && index < column_items_.size())
  {
    column_item = &column_items_[index];
  }
  return column_item;
}
int ObFillValues::add_column_item(const ColumnItem& column_item)
{
    int ret = OB_SUCCESS;
    ret = column_items_.push_back(column_item);
    return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObFillValues, PHY_FILL_VALUES);
  }
}

int64_t ObFillValues::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObFillValues()\n");

  for (int32_t i = 0; i < ObMultiChildrenPhyOperator::get_child_num(); ++i)
  {
      databuff_printf(buf, buf_len, pos, "====child_%d====\n", i);
      pos += get_child(i)->to_string(buf + pos, buf_len - pos);
      if (i != ObMultiChildrenPhyOperator::get_child_num() - 1)
      {
          databuff_printf(buf, buf_len, pos, ",");
      }
  }

  return pos;
}

//add lbzhong [Update rowkey] 20151222:b
//update rowkey: check if new rowkey is existed!
int ObFillValues::check_rowkey_duplication(ObTableRpcScan*& table_rpc_scan_for_update_rowkey, const ObRow*& row,
                                           std::set<int64_t> &rowkey_set)
{
  int ret = OB_SUCCESS;
  ObRow old_row = *row;
  old_row.set_row_desc(row_desc_);
  int64_t rowkey_size = rowkey_info_.get_size();
  //replace new rowkey value
  for (int64_t rowkey_idx = 0; rowkey_idx < rowkey_size; rowkey_idx++)
  {
    uint64_t rowkey_cid = OB_INVALID_ID;
    if(OB_SUCCESS != (ret = rowkey_info_.get_column_id(rowkey_idx, rowkey_cid)))
    {
      TBSYS_LOG(ERROR, "rowkey_info.get_column_id() fail, err=%d", ret);
      break;
    }
    else // get value from update stmt
    {
      const ObObj *value = NULL;
      ObSqlExpression *update_filter = get_expr(rowkey_cid);
      if(NULL != update_filter) // get value from stmt
      {

        //add huangcc [Update rowkey] bug#1216 20161019:b
        if(OB_SUCCESS != (ret = old_row.get_cell(table_id_, rowkey_cid, value)))
        {
             TBSYS_LOG(WARN, "failed to get cell from old_row, err=%d", ret);
        }
        else if(value!=NULL && value->get_type()==ObDecimalType && value->get_scale()!=0)
        {
            old_row.set_is_update_rowkey(true);
        }
        else
        {
            value=NULL;
        }
        //add:e

        if(OB_SUCCESS != (ret = update_filter->calc(old_row, value)))
        {
          TBSYS_LOG(WARN, "failed to calculate, err=%d", ret);
          break;
        }
        else
        {
          char buf[OB_MAX_ROW_LENGTH];
          ObString cast_buffer;
          cast_buffer.assign_ptr(buf, OB_MAX_ROW_LENGTH);
          ObObj tmp_value = *value;
          if (OB_SUCCESS != (ret = obj_cast(tmp_value, rowkey_info_.get_column(rowkey_idx)->type_, cast_buffer)))
          {
            TBSYS_LOG(WARN, "failed to cast obj, value=%s, type=%d, err=%d", to_cstring(tmp_value), rowkey_info_.get_column(rowkey_idx)->type_, ret);
            break;
          }
          else if(OB_SUCCESS != (ret = old_row.set_cell(table_id_, rowkey_cid, tmp_value)))
          {
            TBSYS_LOG(WARN, "failed to set cell, err=%d", ret);
            break;
          }
        }
      }
    }
  }

  if(OB_SUCCESS == ret)
  {
    ObRowkey new_rowkey;
    const ObObj *new_rk_values = NULL;
    uint64_t new_rk_tid = OB_INVALID_ID;
    uint64_t new_rk_cid = OB_INVALID_ID;
    old_row.raw_get_cell(0, new_rk_values, new_rk_tid, new_rk_cid);
    new_rowkey.assign(const_cast<ObObj*>(new_rk_values), rowkey_size);
    int64_t new_rowkey_hash = new_rowkey.hash();

    if(rowkey_set.end() != rowkey_set.find(new_rowkey_hash)) // batch update
    {
      char rowkey_buf[512];
      memset(rowkey_buf, 0, sizeof(rowkey_buf));
      old_row.rowkey_to_string(rowkey_buf, sizeof(rowkey_buf));
      TBSYS_LOG(USER_ERROR, "Duplicate entry '%s' for key 'PRIMARY'", rowkey_buf);
      ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
    }
    else
    {
      rowkey_set.insert(new_rowkey_hash); //hashmap
      ObRowkey old_rowkey;
      const ObObj *rk_values = NULL;
      uint64_t rk_tid = OB_INVALID_ID;
      uint64_t rk_cid = OB_INVALID_ID;
      row->raw_get_cell(0, rk_values, rk_tid, rk_cid);
      old_rowkey.assign(const_cast<ObObj*>(rk_values), rowkey_size);

      if(old_rowkey != new_rowkey)
      {
        table_rpc_scan_for_update_rowkey->reuse();
        if ((ret = table_rpc_scan_for_update_rowkey->set_table(table_item_.table_id_, table_item_.ref_id_)) != OB_SUCCESS)
        {
            TBSYS_LOG(ERROR, "ObTableRpcScan set table failed");
        }
        else if (OB_SUCCESS != (ret = table_rpc_scan_for_update_rowkey->init(&sql_context_, &hint_expr_)))
        {
            TBSYS_LOG(ERROR, "ObTableRpcScan init failed");
        }
        else
        {
          table_rpc_scan_for_update_rowkey->set_rowkey_cell_count(row_desc_.get_rowkey_cell_count());
          table_rpc_scan_for_update_rowkey->set_need_cache_frozen_data(true);
          for (int64_t rowkey_idx = 0; rowkey_idx < rowkey_size; rowkey_idx++)
          {
            uint64_t rowkey_cid = OB_INVALID_ID;
            if(OB_SUCCESS != (ret = rowkey_info_.get_column_id(rowkey_idx, rowkey_cid)))
            {
              TBSYS_LOG(ERROR, "rowkey_info.get_column_id() fail, err=%d", ret);
              break;
            }
            else // get value from update stmt
            {
              ObSqlExpression *filter = ObSqlExpression::alloc();
              if (NULL == filter)
              {
                TBSYS_LOG(ERROR, "no memory");
                ret = OB_ALLOCATE_MEMORY_FAILED;
                break;
              }
              else
              {
                filter->set_tid_cid(OB_INVALID_ID, rowkey_cid);
                const ObObj *value = NULL;
                if(OB_SUCCESS != (ret = old_row.get_cell(table_id_, rowkey_cid, value)))
                {
                  ObSqlExpression::free(filter);
                  TBSYS_LOG(ERROR, "Failed to get cell from row, table_id=%ld, rowkey_cid=%ld, err=%d",
                            table_id_, rowkey_cid, ret);
                  break;
                }
                else if(OB_SUCCESS != (ret = filter->convert_to_equal_expression(table_id_, rowkey_cid, value)))
                {
                  ObSqlExpression::free(filter);
                  TBSYS_LOG(ERROR, "Failed to convert equal expression, table_id=%ld, rowkey_cid=%ld, value=%s, err=%d",
                            table_id_, rowkey_cid, to_cstring(*value), ret);
                  break;
                }
                else if(OB_SUCCESS != (ret = table_rpc_scan_for_update_rowkey->add_filter(filter)))
                {
                  ObSqlExpression::free(filter);
                  TBSYS_LOG(ERROR, "Failed to add filter, err=%d", ret);
                  break;
                }
              }
            }
          }
          if(OB_SUCCESS == ret)
          {
            //add output columns
            for(int32_t col_idx = 0; col_idx < rowkey_size; col_idx++)
            {
              const ColumnItem *col_item = get_column_item(col_idx);
              if (col_item && col_item->table_id_ == table_id_)
              {
                if(rowkey_info_.is_rowkey_column(col_item->column_id_))
                {
                  ObBinaryRefRawExpr col_expr(table_id_, col_item->column_id_, T_REF_COLUMN);
                  ObSqlRawExpr col_raw_expr(
                          common::OB_INVALID_ID,
                          table_id_,
                          col_item->column_id_,
                          &col_expr);
                  ObSqlExpression output_expr;
                  if ((ret = col_raw_expr.fill_sql_expression(output_expr)) != OB_SUCCESS
                      || (ret = table_rpc_scan_for_update_rowkey->add_output_column(output_expr)) != OB_SUCCESS)
                  {
                    TBSYS_LOG(ERROR, "Add table output columns faild");
                    break;
                  }
                }
              }
              else
              {
                TBSYS_LOG(WARN, "Cannot get column id for table_rpc_scan_for_update_rowkey");
                ret = OB_ERR_UNEXPECTED;
                break;
              }
            }
          }
          if(OB_SUCCESS == ret)
          {
            table_rpc_scan_for_update_rowkey->get_phy_plan()->set_curr_frozen_version(sql_context_.session_info_->get_frozen_version());
            table_rpc_scan_for_update_rowkey->get_phy_plan()->set_result_set(sql_context_.session_info_->get_current_result_set());

            if(OB_SUCCESS != (ret = table_rpc_scan_for_update_rowkey->open()))
            {
                TBSYS_LOG(ERROR, "Fail to open table_rpc_scan_for_update_rowkey");
            }
            else
            {
              while(OB_SUCCESS == ret)
              {
                const ObRow* new_row = NULL;
                ret = table_rpc_scan_for_update_rowkey->get_next_row(new_row);
                //mod bingo [Update Rowkey] retcode handle 20170106:b
                if(OB_SUCCESS != ret )
                {
          if(OB_ITER_END == ret)
          {
            ret = OB_SUCCESS;
            break;
          }
          else{
            TBSYS_LOG(WARN, "get_next_row failed, ret=%d", ret);
            break;
          }
        }
                else
                {
                  ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
                  TBSYS_LOG(INFO, "Duplicate entry '%s' for key 'PRIMARY'", to_cstring(*new_row));
                  break;
                }
                //mod:e
              }
            }
          }
        }
        if(OB_SUCCESS != (table_rpc_scan_for_update_rowkey->close()))
        {
          TBSYS_LOG(WARN,"Failed to close table_rpc_scan_for_update_rowkey");
        }
      }
    }
  }
  return ret;
}

int ObFillValues::check_sequence_update_rowkey_duplication(ObTableRpcScan*& table_rpc_scan_for_update_rowkey, const ObArray<ObRow*> &row_array,
            std::set<int64_t> &rowkey_set, ObProject *project)
{
  int ret = OB_SUCCESS;
  for(int64_t row_idx = 0; OB_SUCCESS == ret && row_idx < row_array.count(); row_idx++)
  {
    const ObRow* row = row_array.at(row_idx);
    ObRow old_row = *row;
    old_row.set_row_desc(row_desc_);
    //replace update sequence value
    project->fill_sequence_info();
    common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM,
        ModulePageAllocator,
        ObArrayExpressionCallBack<ObSqlExpression> > &out_put_columns = project->get_output_columns_for_update();
    for (int64_t i = rowkey_info_.get_size(); i < out_put_columns.count(); ++i)
    {
      const ObObj *value = NULL;
      ObSqlExpression &expr = out_put_columns.at(i);
      uint64_t column_id = expr.get_column_id();
      if(rowkey_info_.is_rowkey_column(column_id))
      {
        if(OB_SUCCESS != (ret = expr.calc(old_row, value)))
        {
          TBSYS_LOG(WARN, "failed to calculate, err=%d", ret);
          break;
        }
        else
        {
          char buf[OB_MAX_ROW_LENGTH];
          ObString cast_buffer;
          cast_buffer.assign_ptr(buf, OB_MAX_ROW_LENGTH);
          ObObj tmp_value = *value;
          ObRowkeyColumn column;
          int64_t rowkey_idx = 0;
          if(OB_SUCCESS != (ret = rowkey_info_.get_index(column_id, rowkey_idx, column)))
          {
            TBSYS_LOG(WARN, "failed to get_index from rowkey_info, column_id=%ld, err=%d", column_id, ret);
            break;
          }
          else if (OB_SUCCESS != (ret = obj_cast(tmp_value, column.type_, cast_buffer)))
          {
            TBSYS_LOG(WARN, "failed to cast obj, value=%s, type=%d, err=%d", to_cstring(tmp_value), column.type_, ret);
            break;
          }
          else if(OB_SUCCESS != (ret = old_row.set_cell(table_id_, column_id, tmp_value)))
          {
            TBSYS_LOG(WARN, "failed to set cell, err=%d", ret);
            break;
          }
        }
      }
    }

    if(OB_SUCCESS == ret)
    {
      int64_t rowkey_size = rowkey_info_.get_size();
      ObRowkey new_rowkey;
      const ObObj *new_rk_values = NULL;
      uint64_t new_rk_tid = OB_INVALID_ID;
      uint64_t new_rk_cid = OB_INVALID_ID;
      old_row.raw_get_cell(0, new_rk_values, new_rk_tid, new_rk_cid);
      new_rowkey.assign(const_cast<ObObj*>(new_rk_values), rowkey_size);
      int64_t new_rowkey_hash = new_rowkey.hash();

      if(rowkey_set.end() != rowkey_set.find(new_rowkey_hash)) // batch update
      {
        char rowkey_buf[512];
        memset(rowkey_buf, 0, sizeof(rowkey_buf));
        old_row.rowkey_to_string(rowkey_buf, sizeof(rowkey_buf));
        TBSYS_LOG(USER_ERROR, "Duplicate entry '%s' for key 'PRIMARY'", rowkey_buf);
        ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
      }
      else
      {
        rowkey_set.insert(new_rowkey_hash); //hashmap
        ObRowkey old_rowkey;
        const ObObj *rk_values = NULL;
        uint64_t rk_tid = OB_INVALID_ID;
        uint64_t rk_cid = OB_INVALID_ID;
        row->raw_get_cell(0, rk_values, rk_tid, rk_cid);
        old_rowkey.assign(const_cast<ObObj*>(rk_values), rowkey_size);

        if(old_rowkey != new_rowkey)
        {
          table_rpc_scan_for_update_rowkey->reuse();
          if ((ret = table_rpc_scan_for_update_rowkey->set_table(table_item_.table_id_, table_item_.ref_id_)) != OB_SUCCESS)
          {
              TBSYS_LOG(ERROR, "ObTableRpcScan set table failed");
          }
          else if (OB_SUCCESS != (ret = table_rpc_scan_for_update_rowkey->init(&sql_context_, &hint_expr_)))
          {
              TBSYS_LOG(ERROR, "ObTableRpcScan init failed");
          }
          else
          {
            table_rpc_scan_for_update_rowkey->set_rowkey_cell_count(row_desc_.get_rowkey_cell_count());
            table_rpc_scan_for_update_rowkey->set_need_cache_frozen_data(true);
            for (int64_t rowkey_idx = 0; rowkey_idx < rowkey_size; rowkey_idx++)
            {
              uint64_t rowkey_cid = OB_INVALID_ID;
              if(OB_SUCCESS != (ret = rowkey_info_.get_column_id(rowkey_idx, rowkey_cid)))
              {
                TBSYS_LOG(ERROR, "rowkey_info.get_column_id() fail, err=%d", ret);
                break;
              }
              else // get value from update stmt
              {
                ObSqlExpression *filter = ObSqlExpression::alloc();
                if (NULL == filter)
                {
                  TBSYS_LOG(ERROR, "no memory");
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  break;
                }
                else
                {
                  filter->set_tid_cid(OB_INVALID_ID, rowkey_cid);
                  const ObObj *value = NULL;
                  if(OB_SUCCESS != (ret = old_row.get_cell(table_id_, rowkey_cid, value)))
                  {
                    ObSqlExpression::free(filter);
                    TBSYS_LOG(ERROR, "Failed to get cell from row, table_id=%ld, rowkey_cid=%ld, err=%d",
                              table_id_, rowkey_cid, ret);
                    break;
                  }
                  else
                  {
                    if(OB_SUCCESS != (ret = filter->convert_to_equal_expression(table_id_, rowkey_cid, value)))
                    {
                      ObSqlExpression::free(filter);
                      TBSYS_LOG(ERROR, "Failed to convert equal expression, table_id=%ld, rowkey_cid=%ld, value=%s, err=%d",
                                table_id_, rowkey_cid, to_cstring(*value), ret);
                      break;
                    }
                    else if(OB_SUCCESS != (ret = table_rpc_scan_for_update_rowkey->add_filter(filter)))
                    {
                      ObSqlExpression::free(filter);
                      TBSYS_LOG(ERROR, "Failed to add filter, err=%d", ret);
                      break;
                    }
                  }
                }
              }
            }
            if(OB_SUCCESS == ret)
            {
              //add output columns
              for(int32_t col_idx = 0; col_idx < rowkey_size; col_idx++)
              {
                const ColumnItem *col_item = get_column_item(col_idx);
                if (col_item && col_item->table_id_ == table_id_)
                {
                  if(rowkey_info_.is_rowkey_column(col_item->column_id_))
                  {
                    ObBinaryRefRawExpr col_expr(table_id_, col_item->column_id_, T_REF_COLUMN);
                    ObSqlRawExpr col_raw_expr(
                            common::OB_INVALID_ID,
                            table_id_,
                            col_item->column_id_,
                            &col_expr);
                    ObSqlExpression output_expr;
                    if ((ret = col_raw_expr.fill_sql_expression(output_expr)) != OB_SUCCESS
                        || (ret = table_rpc_scan_for_update_rowkey->add_output_column(output_expr)) != OB_SUCCESS)
                    {
                      TBSYS_LOG(ERROR, "Add table output columns faild");
                      break;
                    }
                  }
                }
                else
                {
                  TBSYS_LOG(WARN, "Cannot get column id for table_rpc_scan_for_update_rowkey");
                  ret = OB_ERR_UNEXPECTED;
                  break;
                }
              }
            }
            if(OB_SUCCESS == ret)
            {
              table_rpc_scan_for_update_rowkey->get_phy_plan()->set_curr_frozen_version(sql_context_.session_info_->get_frozen_version());
              table_rpc_scan_for_update_rowkey->get_phy_plan()->set_result_set(sql_context_.session_info_->get_current_result_set());
              if(OB_SUCCESS != (ret = table_rpc_scan_for_update_rowkey->open()))
              {
                  TBSYS_LOG(ERROR, "Fail to open table_rpc_scan_for_update_rowkey");
              }
              else
              {
                while(OB_SUCCESS == ret)
                {
                  const ObRow* new_row = NULL;
                  ret = table_rpc_scan_for_update_rowkey->get_next_row(new_row);
                  if(OB_ITER_END == ret)
                  {
                    ret = OB_SUCCESS;
                    break;
                  }
                  else
                  {
                    ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
                    TBSYS_LOG(USER_ERROR, "Duplicate entry '%s' for key 'PRIMARY'", to_cstring(*new_row));
                    break;
                  }
                }
              }
            }
          }
          if(OB_SUCCESS != (table_rpc_scan_for_update_rowkey->close()))
          {
            TBSYS_LOG(WARN,"Failed to close table_rpc_scan_for_update_rowkey");
          }
        }
      }
    }
  }
  return ret;
}

bool ObFillValues::exist_column(const uint64_t column_id) const
{
  for (int32_t i = 0; i < column_items_.size(); i++)
  {
    ColumnItem& item = column_items_[i];
    if(column_id == item.column_id_)
    {
      return true;
    }
  }
  return false;
}
int ObFillValues::set_column_items(const ObStmt *stmt)
{
  int ret = OB_SUCCESS;
  int32_t num = stmt->get_column_size();
  ColumnItem col_item_for_fill_val;
  for (int32_t i = 0; i < num; i++)
  {
    col_item_for_fill_val = *(stmt->get_column_item(i));
    if(OB_SUCCESS != (ret = column_items_.push_back(col_item_for_fill_val)))
    {
      TBSYS_LOG(WARN, "fail to add column item to ob_fill_values");
      break;
    }
  }
  return ret;
}

int ObFillValues::set_exprs(const ObLogicalPlan* logical_plan, const ObStmt *stmt)
{
  int ret = OB_SUCCESS;
  clear_exprs();
  update_rowkey_columns_.clear();

  int64_t rowkey_size = rowkey_info_.get_size();
  for (int64_t i = 0; i < rowkey_size; i++)
  {
    uint64_t expr_id = OB_INVALID_ID;
    uint64_t rowkey_cid = OB_INVALID_ID;

    if(OB_SUCCESS != (ret = rowkey_info_.get_column_id(i, rowkey_cid)))
    {
      TBSYS_LOG(ERROR, "rowkey_info.get_column_id() fail, err=%d", ret);
      break;
    }
    else if(OB_SUCCESS == stmt->get_update_expr_id(rowkey_cid, expr_id)) // get value from update stmt
    {
      ObSqlRawExpr *update_expr = logical_plan->get_expr(expr_id);
      OB_ASSERT(update_expr);
      ObSqlExpression *filter = ObSqlExpression::alloc();
      if (NULL == filter)
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
        break;
      }
      else if (OB_SUCCESS != (ret = update_expr->fill_sql_expression(*filter)))
      {
        ObSqlExpression::free(filter);
        TBSYS_LOG(ERROR, "Failed to fill expression, err=%d", ret);
        break;
      }
	  else if(OB_SUCCESS != (ret = update_rowkey_columns_.push_back(rowkey_cid)))
      {
        ObSqlExpression::free(filter);
        TBSYS_LOG(WARN, "fail to add filter to update_rowkey_columns_, ret=%d", ret);
        break;
      }
      //add bingo [UpdateRowkey mysql prepare bugfix]20170414:b
      else
      {
        filter->set_owner_op(this);
      }
      //add:e
      if(OB_SUCCESS != (ret = update_rowkey_exprs_.push_back(filter)))
      {
        ObSqlExpression::free(filter);
        TBSYS_LOG(WARN, "fail to add filter to update_rowkey_exprs_, ret-%d", ret);
        break;
      }
    }
  }
  return ret;
}

void ObFillValues::clear_exprs()
{
  for(int32_t i = 0; i < update_rowkey_exprs_.count(); ++i)
  {
    ObSqlExpression::free(update_rowkey_exprs_.at(i));
  }
  update_rowkey_exprs_.clear();
}

ObSqlExpression* ObFillValues::get_expr(const uint64_t column_id) const
{
  ObSqlExpression *expr = NULL;
  int32_t idx = -1;
  for(int64_t i = 0; i < update_rowkey_columns_.count(); i++)
  {
    if(update_rowkey_columns_.at(i) == column_id)
    {
      idx = static_cast<int32_t>(i);
      break;
    }
  }
  if(idx >= 0 && idx < update_rowkey_exprs_.count())
  {
    expr = update_rowkey_exprs_.at(idx);
  }
  return expr;
}
int ObFillValues::set_update_columns_flag(ObArray<uint64_t>& update_columns)
{
  int ret = OB_SUCCESS;
  update_columns_flag_.clear();
  for(int64_t i = 0; i < update_columns.count(); i++)
  {
    if(rowkey_info_.is_rowkey_column(update_columns.at(i)))
    {
      update_columns_flag_.push_back(1);
    }
    else
    {
      update_columns_flag_.push_back(0);
    }
  }
  return ret;
}

//add:e
int ObFillValues::set_rowkey_ids(uint64_t rowkey_id)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = rowkey_ids_.push_back(rowkey_id)))
  {
      TBSYS_LOG(WARN,"fail to push back rowkey");
  }
  return ret;
}

//int ObFillValues::commit()
//{
//  TBSYS_LOG(DEBUG,"enter into commit()");
//  int ret = OB_SUCCESS;
//  ObString start_thx = ObString::make_string("COMMIT");
//  ret = execute_stmt_no_return_rows(start_thx);
//  TBSYS_LOG(DEBUG,"leave commit()");
//  return ret;
//}

//int ObFillValues::start_transaction()
//{
//  TBSYS_LOG(DEBUG,"enter into start_transaction()");
//  int ret = OB_SUCCESS;
//  ObString start_thx = ObString::make_string("START TRANSACTION");
//  ret = execute_stmt_no_return_rows(start_thx);
//  TBSYS_LOG(DEBUG,"leave start_transaction()");
//  return ret;
//}

//int ObFillValues::execute_stmt_no_return_rows(const ObString &stmt)
//{
//  TBSYS_LOG(DEBUG,"enter into execute_stmt_no_return_rows()");
//  int ret = OB_SUCCESS;
//  ObMySQLResultSet tmp_result;
//  if (OB_SUCCESS != (ret = tmp_result.init()))
//  {
//    TBSYS_LOG(WARN, "init result set failed, ret=%d", ret);
//  }
//  else if (OB_SUCCESS != (ret = ObSql::direct_execute(stmt, tmp_result, sql_context_)))
//  {
//    TBSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", stmt.length(), stmt.ptr(), ret);
//  }
//  else if (OB_SUCCESS != (ret = tmp_result.open()))
//  {
//    TBSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", stmt.length(), stmt.ptr(), ret);
//  }
//  else
//  {
//    int err = tmp_result.close();
//    if (OB_SUCCESS != err)
//    {
//      TBSYS_LOG(WARN, "failed to close result set,err=%d", err);
//    }
//    tmp_result.reset();
//  }
//  TBSYS_LOG(DEBUG,"leave execute_stmt_no_return_rows()");
//  return ret;
//}

//int ObFillValues::rollback()
//{
//    TBSYS_LOG(DEBUG,"enter into rollback()");
//    int ret = OB_SUCCESS;
//    ObString start_thx = ObString::make_string("ROLLBACK");
//    ret = execute_stmt_no_return_rows(start_thx);
//    TBSYS_LOG(DEBUG,"leave rollback()");
//    return ret;
//}

int ObFillValues::clear_prepare_select_result()
{
    int ret = OB_SUCCESS;
    if( select_result_set_ && OB_SUCCESS != (ret = select_result_set_->close()))
    {
        TBSYS_LOG(WARN, "failed to close result_set_, ret=%d",ret);
    }
    common::ObString temp_stmt = common::ObString::make_string(prepare_name_.data());
    TBSYS_LOG(ERROR,"temp_stmt=%.*s",temp_stmt.length(),temp_stmt.ptr());
    uint64_t stmt_id = OB_INVALID_ID;
    if (sql_context_.session_info_->plan_exists(temp_stmt, &stmt_id) == false)
    {
        ret = OB_ERR_PREPARE_STMT_UNKNOWN;
        TBSYS_LOG(WARN,"Unknown prepared statement handler (%.*s) given to DEALLOCATE PREPARE",
                  temp_stmt.length(), temp_stmt.ptr());
    }
    else if (OB_SUCCESS != (ret = sql_context_.session_info_->remove_plan(stmt_id)))
    {
        TBSYS_LOG(ERROR,"fail to remove plan, ret=%d",ret);
    }

    return ret;
}

PHY_OPERATOR_ASSIGN(ObFillValues)
{
  int ret = OB_SUCCESS;
  UNUSED(other);
  reset();
  return ret;
}
