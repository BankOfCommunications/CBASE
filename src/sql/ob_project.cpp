/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_project.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_project.h"
#include "ob_sql_expression.h"
#include "common/utility.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObProject::ObProject()
  :columns_(common::OB_COMMON_MEM_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_SQL_ARRAY)),
   rowkey_cell_count_(0)
{
  //add wenghaixing [secondary index upd]20150429
  index_num_ = 0;
  row_store_pre_ = NULL;
  row_store_post_ = NULL;
  //add e
  //add liuzy [sequence select] 20150703:b
  is_set_sequence_select_op_ = false;
  //add 20150703:e
  //add lijianqiang [sequence update] 20150912:b
  sequence_count_in_one_row_ = 0;
  row_num_idx_ = 0;
  //add 20150912:e
  //add lbzhong[Update rowkey] 20151221:b
  is_update_rowkey_ = false;
  new_rowkey_row_ = NULL;
  //add:e
  output_columns_dsttype_.clear ();  //add peiouya [IN_TYPEBUG_FIX] 20151225
}

ObProject::~ObProject()
{
  //add: lbzhong[Update rowkey] 20151221:b
  destroy_new_rowkey_row();
  //add:e
}

void ObProject::reset()
{
  //add lbzhong[Update rowkey] 20151221:b
  is_update_rowkey_ = false;
  idx_of_update_rowkey_expr_.clear();
  destroy_new_rowkey_row();
  //add:e
  if(columns_.count () != 0)
      columns_.clear(); // don't reset for performance
  row_desc_.reset();
  rowkey_cell_count_ = 0;
  //add wenghaixing [secondary index upd]20150720
  index_num_ = 0;
  row_store_pre_ = NULL;
  row_store_post_ = NULL;
  //add e
  //add liuzy [sequence select] 20150703:b
  is_set_sequence_select_op_ = false;
  //add 20150703:e
  //add lijianqiang [sequence update] 20150911:b
  sequence_values_.clear();
  idx_of_expr_with_sequence_.clear();
  idx_of_out_put_columns_with_sequence_.clear();
  sequence_count_in_one_row_ = 0;
  row_num_idx_ = 0;
  //add 20150911:e
  ObSingleChildPhyOperator::reset();
  output_columns_dsttype_.clear ();  //add peiouya [IN_TYPEBUG_FIX] 20151225
}

void ObProject::reuse()
{
  //add lbzhong[Update rowkey] 20151221:b
  is_update_rowkey_ = false;
  destroy_new_rowkey_row();
  //add:e
  columns_.clear();
  row_desc_.reset();
  rowkey_cell_count_ = 0;
  //add wenghaixing [secondary index upd]20150720
  index_num_ = 0;
  row_store_pre_ = NULL;
  row_store_post_ = NULL;
  //add e
  //add liuzy [sequence select] 20150703:b
  is_set_sequence_select_op_ = false;
  //add 20150703:e
  //add lijianqiang [sequence update] 20150911:b
  sequence_values_.clear();
  idx_of_expr_with_sequence_.clear();
  idx_of_out_put_columns_with_sequence_.clear();
  sequence_count_in_one_row_ = 0;
  row_num_idx_ = 0;
  //add 20150911:e
  ObSingleChildPhyOperator::reuse();
  output_columns_dsttype_.clear ();  //add peiouya [IN_TYPEBUG_FIX] 20151225
}

//add liumengzhan_delete_index
void ObProject::reset_iterator()
{
  if(child_op_ && PHY_MULTIPLE_GET_MERGE == child_op_->get_type())
  {
    ObMultipleGetMerge *fuse_op = NULL;
    fuse_op = dynamic_cast<ObMultipleGetMerge*>(child_op_);
    fuse_op->reset_iterator();
  }
  else if(child_op_ && PHY_FILTER == child_op_->get_type())
  {
    ObFilter *filter_op = NULL;
    filter_op = dynamic_cast<ObFilter*>(child_op_);
    filter_op->reset_iterator();
  }
}

bool ObProject::is_duplicated_output_column(uint64_t &col_id)
{
  bool ret = false;
  for (int64_t i = 0; i < get_output_column_size(); i++)
  {
    const ObSqlExpression &expr = columns_.at(i);
    if (col_id == expr.get_column_id())
    {
      ret = true;
      break;
    }
  }
  return ret;
}

//add:e
//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140911:b
/*Exp:the signal to reset project operator when the stm is insert...select... */
void ObProject::reset_stuff()
{
  columns_.clear();
}
//add 20140911:e

int ObProject::add_output_column(const ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == (ret = columns_.push_back(expr)))
  {
    columns_.at(columns_.count() - 1).set_owner_op(this);
  }

  return ret;
}
//add lijianqiang [sequence] 20150910:b
int ObProject::add_nextval_for_update(common::ObString &nextval)
{
  int ret = OB_SUCCESS;
  ObObj nextval_obj;
  if (OB_SUCCESS != (ret = nextval_obj.set_decimal(nextval)))
  {
    TBSYS_LOG(ERROR, "set nextval failed!");
  }
  else if (OB_SUCCESS != (ret = sequence_values_.push_back(nextval_obj)))
  {
    TBSYS_LOG(ERROR, "push nextval to sequence values array failed,ret=[%d]",ret);
  }
  return ret;
}

int ObProject::add_idx_of_next_value(int64_t idx)
{
  return idx_of_expr_with_sequence_.push_back(idx);
}

int ObProject::add_expr_idx_of_out_put_columns(int64_t idx)
{
  return idx_of_out_put_columns_with_sequence_.push_back(idx);
}
int ObProject::check_the_sequence_validity()
{
  int ret = OB_SUCCESS;
  if ((sequence_values_.count() == idx_of_expr_with_sequence_.count())
      && (idx_of_expr_with_sequence_.count() == idx_of_out_put_columns_with_sequence_.count()))
  {
    for (int64_t i=0; i < sequence_values_.count(); i++)//FOR DEBUG
    {
      ObString values;
      sequence_values_.at(i).get_decimal(values);
      TBSYS_LOG(DEBUG,"the value is::[%.*s], the seq idx is::[%ld], the  expr idx is::[%ld]",values.length(),values.ptr(),idx_of_expr_with_sequence_.at(i),idx_of_out_put_columns_with_sequence_.at(i));
    }
    TBSYS_LOG(DEBUG, "the sequence count int one row is::[%ld]",sequence_count_in_one_row_);
    //do nothing
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "the sequence info is not correct!");
  }
  return ret;
}

int64_t ObProject::get_sequence_info_serialize_size() const
{
  int64_t size = 0;
  ObObj obj;
  obj.set_int(sequence_values_.count());
  size += obj.get_serialize_size();
  for (int64_t i = 0; i < sequence_values_.count(); i++)
  {
    size +=  sequence_values_.at(i).get_serialize_size();
    size += serialization::encoded_length_i64(idx_of_expr_with_sequence_.at(i));
    size += serialization::encoded_length_i64(idx_of_out_put_columns_with_sequence_.at(i));
  }
  size += serialization::encoded_length_i64(sequence_count_in_one_row_);
//  size += serialization::encoded_length_i64(row_num_);
  return size;
}

void ObProject::set_sequence_num_in_one_row()
{
  sequence_count_in_one_row_ = sequence_values_.count();
}

void ObProject::reset_row_num_idx()
{
  row_num_idx_ = 0;
}

int ObProject::fill_sequence_info(common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > &out_put_columns)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ret && sequence_values_.count() > 0)
  {
    int64_t expr_idx = OB_INVALID_INDEX;
    int64_t sequence_idx = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_SUCCESS == ret && i < sequence_count_in_one_row_; i++)
    {
      int64_t idx = row_num_idx_ * sequence_count_in_one_row_ + i;
      expr_idx = idx_of_out_put_columns_with_sequence_.at(idx);
      sequence_idx = idx_of_expr_with_sequence_.at(idx);
      ObSqlExpression &expr = out_put_columns.at(expr_idx);
      ObPostfixExpression::ExprArray &post_expr_array = expr.get_expr_array();
      ObObj *expr_array_obj = const_cast<ObObj *>(&post_expr_array[sequence_idx]);
      ObString sequence_value;
      if (OB_SUCCESS != (ret = sequence_values_.at(idx).get_decimal(sequence_value)))
      {
        TBSYS_LOG(WARN, "get sequence value failed, ret=%d",ret);
        break;
      }
      else if (OB_SUCCESS != (ret = expr_array_obj->set_decimal(sequence_value)))
      {
        TBSYS_LOG(WARN, "set sequence value failed, ret=%d",ret);
        break;
      }
    }
    row_num_idx_++;
  }
  return ret;
}
//add 20150910:e

//add peiouya [IN_TYPEBUG_FIX] 20151225:b
int ObProject::get_output_columns_dsttype(common::ObArray<common::ObObjType> &output_columns_dsttype)
{
  output_columns_dsttype = output_columns_dsttype_;
  return OB_SUCCESS;
}
int ObProject::add_dsttype_for_output_columns(common::ObArray<common::ObObjType>& columns_dsttype)
{
  output_columns_dsttype_ = columns_dsttype;
  return OB_SUCCESS;
}
//add 20151225:e

int ObProject::cons_row_desc()
{
  int ret = OB_SUCCESS;
  if(0 != row_desc_.get_column_num())
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "row desc should be empty");
  }
  //add lbzhong [Update rowkey] 20160519:b
  uint32_t rowkey_index = 0;
  //add:e
  for (uint32_t i = 0; OB_SUCCESS == ret && i < columns_.count(); ++i)
  {
    const ObSqlExpression &expr = columns_.at(i);
    //add lbzhong[Update rowkey] 20151221:b
    if(is_update_rowkey_ && is_update_rowkey_column(i, rowkey_index))
    {
      continue; // ignore new rowkey
    }
    //add:e
    if (OB_SUCCESS != (ret = row_desc_.add_column_desc(expr.get_table_id(), expr.get_column_id())))
    {
      TBSYS_LOG(WARN, "failed to add column desc, err=%d", ret);
      break;
    }
  } // end for
  if (0 >= columns_.count())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "no column for output");
  }
  else
  {
    row_desc_.set_rowkey_cell_count(rowkey_cell_count_);
  }
  return ret;
}

int ObProject::open()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ObSingleChildPhyOperator::open()))
  {
    if (!IS_SQL_ERR(ret))
    {
      TBSYS_LOG(WARN, "failed to open child_op, err=%d", ret);
    }
  }
  else if (OB_SUCCESS != (ret = cons_row_desc()))
  {
    TBSYS_LOG(WARN, "failed to construct row desc, err=%d", ret);
  }
  else
  {
    row_.set_row_desc(row_desc_);
  }
  return ret;
}

int ObProject::close()
{
  //add lbzhong [Update rowkey] 20160602:b
  destroy_new_rowkey_row();
  //add:e
  row_desc_.reset();
  return ObSingleChildPhyOperator::close();
}

int ObProject::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= row_desc_.get_column_num()))
  {
    TBSYS_LOG(ERROR, "not init");
    ret = OB_NOT_INIT;
  }
  else
  {
    row_desc = &row_desc_;
  }
  return ret;
}

int ObProject::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const common::ObRow *input_row = NULL;
  const ObRowStore::StoredRow *stored_row = NULL;
  //add lbzhong[Update rowkey] 20151221:b
  bool is_new_rowkey = false;
  bool is_rowkey_change = false;
  uint32_t rowkey_index = 0;
  row_.reset_new_rowkey();
  //add:e
  if (NULL == child_op_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "child_op_ is NULL");
  }
  else if (OB_SUCCESS != (ret = child_op_->get_next_row(input_row)))
  {
    if (OB_ITER_END != ret
        && !IS_SQL_ERR(ret))
    {
      TBSYS_LOG(WARN, "failed to get next row, err=%d", ret);
    }
  }
  else
  {
    //add wenghaixing [secondary index debug_data_correct]20150807
    if(NULL != row_store_pre_)
    {
      if(OB_SUCCESS != (ret = row_store_pre_->add_row(*input_row,stored_row)))
      {
        TBSYS_LOG(WARN, "failed to add row store,ret = %d", ret);
        return ret;
      }
    }
    //add e
    //add lijianqiang [sequence update] 20150911:b
    /*Exp:before calc,we process the sequence in expr if the udpate stmt has sequence with nextval in set clause*/
    if ((OB_SUCCESS == ret)
        && (sequence_values_.count() > 0)
        && (OB_SUCCESS !=(ret = fill_sequence_info(columns_))))
    {
      TBSYS_LOG(ERROR, "fill sequence info failed!, ret=%d",ret);
    }
    //add 20150911:e
    else
    {
      TBSYS_LOG(DEBUG, "PROJECT ret=%d op=%p type=%d %s",
                ret, child_op_, child_op_->get_type(), (NULL == input_row) ? "nil" : to_cstring(*input_row));
      const ObObj *result = NULL;
      for (uint32_t i = 0;
                      //add lbzhong [Update rowkey] fixbug:1150 :b
                      OB_SUCCESS == ret && 
                      //add:e  
	                  i < columns_.count(); ++i)
      {
        ObSqlExpression &expr = columns_.at(i);

        //add huangcc [Update rowkey] bug#1216 201610919:b
        if(is_update_rowkey_)
        {
          if(OB_SUCCESS !=(ret =input_row->get_cell(expr.get_table_id(), expr.get_column_id(), result)))
          {
             TBSYS_LOG(WARN, "failed to get cell from input_row, err=%d", ret);
          }
          else if(result!=NULL && result->get_type()==ObDecimalType && result->get_scale()!=0)
          {
             common::ObRow *rrow=const_cast<common::ObRow *>(input_row);
             rrow->set_is_update_rowkey(true);
          }
          else
          {
              result=NULL;
          }
        }
        //add:e

        if (OB_SUCCESS != (ret = expr.calc(*input_row, result)))
        {
          TBSYS_LOG(WARN, "failed to calculate, err=%d", ret);
          break;
        }
        //add lbzhong[Update rowkey] 20151221:b
        else if(is_update_rowkey_ &&
                OB_SUCCESS != (ret = add_new_rowkey(input_row, i, rowkey_index, expr.get_table_id(),
                              expr.get_column_id(), result, is_new_rowkey, is_rowkey_change)))
        {
          TBSYS_LOG(WARN, "failed to add new rowkey, err=%d", ret);
          break;
        }
        //add:e
        else if (
            //add lbzhong [Update rowkey] 20160509:b
            !is_new_rowkey &&
            //add:e
            OB_SUCCESS != (ret = row_.set_cell(expr.get_table_id(), expr.get_column_id(), *result)))
        {
          TBSYS_LOG(WARN, "failed to set row cell, err=%d", ret);
          break;
        }
      } // end for
    }
    if(OB_SUCCESS == ret) //add lbzhong [Update rowkey] 20151224:b:e
    {
      //add:e
      //add liuzy [sequence select]20150923:b
      if (OB_SUCCESS == ret && is_set_sequence_select_op_)
      {
        row = input_row;
      }
      else
      {
      //add 20150703:e
        row = &row_;
      }//add liuzy [sequence select] 20150923 /*Exp: end else*/
      //add wenghaixing [secondary index debug_data_correct]20150807
      if(NULL != row_store_post_)
      {
        if(OB_SUCCESS != (ret = row_store_post_->add_row(row_,stored_row)))
        {
          TBSYS_LOG(WARN, "failed to add row store,ret = %d", ret);
        }
      }
      //add e
      //add lbzhong [Update rowkey] 20160527:b
      if(is_update_rowkey_ && is_rowkey_change && NULL != new_rowkey_row_)
      {
        (const_cast<ObRow*>(row))->set_new_rowkey(new_rowkey_row_);
      }
      //add:e
    }//add lbzhong [Update rowkey] 20151224:b:e //end if
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObProject, PHY_PROJECT);
  }
}

int64_t ObProject::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Project(rowkey_count=%ld, columns=[", rowkey_cell_count_); //mod lbzhong[Update rowkey] 20151221:b:e
  for (int32_t i = 0; i < columns_.count(); ++i)
  {
    int64_t pos2 = columns_.at(i).to_string(buf+pos, buf_len-pos);
    pos += pos2;
    if (i != columns_.count() -1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
  }
  databuff_printf(buf, buf_len, pos, "])\n");
  if (NULL != child_op_)
  {
    int64_t pos2 = child_op_->to_string(buf+pos, buf_len-pos);
    pos += pos2;
  }
  return pos;
}


DEFINE_SERIALIZE(ObProject)
{
  int ret = OB_SUCCESS;
  ObObj obj;

  obj.set_int(columns_.count());
  if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to serialize expr count. ret=%d", ret);
  }
  //add lbzhong[Update rowkey] 20151221:b
  if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, rowkey_cell_count_)))
  {
    TBSYS_LOG(WARN, "fail to serialize rowkey_cell_count_. ret=%d", ret);
  }
  else if(OB_SUCCESS != (ret = serialize_idx_of_update_rowkey_expr(buf, buf_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to serialize idx_of_update_rowkey_expr. ret=%d", ret);
  }
  //add:e
  else
  {
    for (int64_t i = 0; i < columns_.count(); ++i)
    {
      const ObSqlExpression &expr = columns_.at(i);
      if (ret == OB_SUCCESS && (OB_SUCCESS != (ret = expr.serialize(buf, buf_len, pos))))
      {
        TBSYS_LOG(WARN, "serialize fail. ret=%d", ret);
        break;
      }
    } // end for
  }
  //add wenghaixing [secondary index upd]20150429
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, index_num_)))
    {
      TBSYS_LOG(WARN, "serialize fail index_num_. ret=%d", ret);
    }
  }
  //add e
  //add lijianqiang [sequence update] 20150911:b
  if (OB_SUCCESS == ret)
  {
    ObObj sequence_values;
    sequence_values.set_int(sequence_values_.count());
    int64_t sequence_value_count = sequence_values_.count();
    if (OB_SUCCESS != (ret = sequence_values.serialize(buf, buf_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize sequence values count. ret=%d", ret);
    }
    else
    {
      for (int64_t i = 0; i < sequence_value_count; i++)
      {
        if (ret == OB_SUCCESS && (OB_SUCCESS != (ret = sequence_values_.at(i).serialize(buf, buf_len, pos))))
        {
          TBSYS_LOG(WARN, "serialize sequence_values fail. ret=%d", ret);
          break;
        }
      }
      for (int64_t i = 0; i < sequence_value_count; i++)
      {
        if (ret == OB_SUCCESS && (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, idx_of_expr_with_sequence_.at(i)))))
        {
          TBSYS_LOG(WARN, "serialize idx in expr fail. ret=%d", ret);
          break;
        }
      }
      for (int64_t i = 0; i < sequence_value_count; i++)
      {
        if (ret == OB_SUCCESS && (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, idx_of_out_put_columns_with_sequence_.at(i)))))
        {
          TBSYS_LOG(WARN, "serialize idx in out put columns fail. ret=%d", ret);
          break;
        }
      }
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, sequence_count_in_one_row_)))
        {
          TBSYS_LOG(WARN, "serialize sequence count in one row fail. ret=%d", ret);
        }
      }
    }
  }
  //add 20150911:e
  if (0 >= columns_.count())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "no column for output");
  }
  return ret;
}

DEFINE_DESERIALIZE(ObProject)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  int64_t expr_count = 0, i = 0;
  //reset();
  if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to deserialize expr count. ret=%d", ret);
  }
  //add lbzhong[Update rowkey] 20151221:b
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &rowkey_cell_count_)))
  {
    TBSYS_LOG(WARN, "fail to deserialize rowkey_cell_count_. ret=%d", ret);
  }
  else if(OB_SUCCESS != (ret = deserialize_idx_of_update_rowkey_expr(buf, data_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to deserialize idx_of_update_rowkey_expr. ret=%d", ret);
  }
  //add:e
  else if (OB_SUCCESS != (ret = obj.get_int(expr_count)))
  {
    TBSYS_LOG(WARN, "fail to get expr_count. ret=%d", ret);
  }
  else
  {
    //for (i = 0; i < expr_count; i++)
    //{
    //  ObSqlExpression expr;
    //  if (OB_SUCCESS != (ret = expr.deserialize(buf, data_len, pos)))
    //  {
    //    TBSYS_LOG(WARN, "fail to deserialize expression. ret=%d", ret);
    //    break;
    //  }
    //  else
    //  {
    //    if (OB_SUCCESS != (ret = add_output_column(expr)))
    //    {
    //      TBSYS_LOG(DEBUG, "fail to add expr to project ret=%d. buf=%p, data_len=%ld, pos=%ld", ret, buf, data_len, pos);
    //      break;
    //    }
    //  }
    //}
    ObSqlExpression expr;
    for (i = 0; i < expr_count; i++)
    {
      if (OB_SUCCESS != (ret = add_output_column(expr)))
      {
        TBSYS_LOG(DEBUG, "fail to add expr to project ret=%d. buf=%p, data_len=%ld, pos=%ld", ret, buf, data_len, pos);
        break;
      }
      if (OB_SUCCESS != (ret = columns_.at(columns_.count() - 1).deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(WARN, "fail to deserialize expression. ret=%d", ret);
        break;
      }
    }
  }
  //add wenghaixing [secondary index upd]20150429
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &index_num_)))
    {
      TBSYS_LOG(WARN, "serialize fail index_num_. ret=%d", ret);
    }
  }
  //add e
  //add lijianqiang [sequence update] 20150911:b
  if (OB_SUCCESS == ret)
  {
    ObObj sequence_values;
    int64_t sequence_value_count = 0;
    if (OB_SUCCESS != (ret = sequence_values.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to deserialize sequence values count. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = sequence_values.get_int(sequence_value_count)))
    {
      TBSYS_LOG(WARN, "fail to get sequence values count. ret=%d", ret);
    }
    if (OB_SUCCESS == ret)
    {
      //sequence_values_
      for (int64_t i = 0; OB_SUCCESS == ret && i < sequence_value_count; i++)
      {
        ObObj next_value_obj;
        ObString nextval;
        if (OB_SUCCESS != (ret = next_value_obj.deserialize(buf, data_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to deserialize sequence value. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = next_value_obj.get_decimal(nextval)))
        {
          TBSYS_LOG(WARN, "fail to get decimal. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = add_nextval_for_update(nextval)))
        {
          TBSYS_LOG(WARN, "fail to deserialize add nextval for update,ret=%d",ret);
        }
      }
      //idx_of_expr_with_sequence_
      for (int64_t i = 0; OB_SUCCESS == ret && i < sequence_value_count; i++)
      {
        int64_t idx = 0;
        if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &idx)))
        {
          TBSYS_LOG(WARN, " fail deserialize idx in expr. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = add_idx_of_next_value(idx)))
        {
          TBSYS_LOG(WARN, "fail to deserialize add idx of next value. ret=%d",ret);
        }
      }
      //idx_of_out_put_columns_with_sequence_
      for (int64_t i = 0; OB_SUCCESS == ret && i < sequence_value_count; i++)
      {
        int64_t idx = 0;
        if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &idx)))
        {
          TBSYS_LOG(WARN, " fail deserialize idx in out put columns. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = add_expr_idx_of_out_put_columns(idx)))
        {
          TBSYS_LOG(WARN, "fail to deserialize add idx out put columns. ret=%d",ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &sequence_count_in_one_row_)))
        {
          TBSYS_LOG(WARN, "deserialize sequence count in one row fail. ret=%d", ret);
        }
      }
    }
  }
  //add 20150911:e
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObProject)
{
  int64_t size = 0;
  ObObj obj;
  obj.set_int(columns_.count());
  size += obj.get_serialize_size();
  //add lbzhong[Update rowkey] 20151221:b
  size += serialization::encoded_length_vi64(rowkey_cell_count_);
  size += get_idx_of_update_rowkey_expr_serialize_size();
  //add:e
  for (int64_t i = 0; i < columns_.count(); ++i)
  {
    const ObSqlExpression &expr = columns_.at(i);
    size += expr.get_serialize_size();
  }
  //add wenghaixing [secondary index upd]20150429
  size += serialization::encoded_length_i64(index_num_);
  //add lijianqiang [sequence update] 20150911:b
  size += get_sequence_info_serialize_size();
  //add 20150911:e
  return size;
}

PHY_OPERATOR_ASSIGN(ObProject)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObProject);
  reset();
  for (int64_t i = 0; i < o_ptr->columns_.count(); i++)
  {
    if ((ret = columns_.push_back(o_ptr->columns_.at(i))) == OB_SUCCESS)
    {
      columns_.at(i).set_owner_op(this);
    }
    else
    {
      break;
    }
  }
  rowkey_cell_count_ = o_ptr->rowkey_cell_count_;
  //add wenghaixing [secondary index upd]20150429
  index_num_ = o_ptr->index_num_;
  row_store_post_ = o_ptr->row_store_post_;
  row_store_pre_ = o_ptr->row_store_pre_;
  //add e
  //add lijianqiang [sequence update] 20150911:b
  for (int64_t i = 0; i < o_ptr->sequence_values_.count(); i++)
  {
    if (OB_SUCCESS != (ret = sequence_values_.push_back(o_ptr->sequence_values_.at(i))))
    {
      TBSYS_LOG(WARN, "assign sequence_values failed!");
    }
    else if (OB_SUCCESS != (ret = idx_of_expr_with_sequence_.push_back(o_ptr->idx_of_expr_with_sequence_.at(i))))
    {
      TBSYS_LOG(WARN, "assign idx of expr failed!");
    }
    else if (OB_SUCCESS != (ret = idx_of_out_put_columns_with_sequence_.push_back(o_ptr->idx_of_out_put_columns_with_sequence_.at(i))))
    {
      TBSYS_LOG(WARN, "assign idx of out put columns failed!");
    }
  }
  sequence_count_in_one_row_ = o_ptr->sequence_count_in_one_row_;
  //add 20150911:e
  output_columns_dsttype_ = o_ptr->output_columns_dsttype_;  //add peiouya [IN_TYPEBUG_FIX] 20151225
  return ret;
}

ObPhyOperatorType ObProject::get_type() const
{
  return PHY_PROJECT;
}

//add lbzhong [Update rowkey] 20160111:b
int ObProject::set_new_rowkey_cell(const uint64_t table_id, const uint64_t column_id, const common::ObObj &cell)
{
  int ret = OB_SUCCESS;
  int64_t cell_idx = OB_INVALID_INDEX;
  if(NULL == new_rowkey_row_)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "Fail to alloc new_rowkey_row_");
  }
  else if (OB_INVALID_INDEX == (cell_idx = row_desc_.get_idx(table_id, column_id)))
  {
    TBSYS_LOG(WARN, "failed to find cell, tid=%lu cid=%lu row_desc=%s", table_id, column_id, to_cstring(row_desc_));
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = new_rowkey_row_->set_cell(cell_idx, cell)))
  {
    TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
  }
  return ret;
}
int ObProject::add_new_rowkey(const common::ObRow *&input_row, uint32_t i, uint32_t &rowkey_index,
                              const int64_t table_id, const int64_t column_id, const ObObj *&result,
                              bool &is_new_rowkey, bool& is_rowkey_change)
{
  int ret = OB_SUCCESS;
  if(i < rowkey_cell_count_)
  {
    is_new_rowkey = false;
    if (OB_SUCCESS != (ret = set_new_rowkey_cell(table_id, column_id, *result))) // old value
    {
      TBSYS_LOG(WARN, "failed to set new rowkey, err=%d", ret);
    }
  }
  else
  {
    if(is_update_rowkey_column(i, rowkey_index))
    {
      is_new_rowkey = true;
    }
    else
    {
      is_new_rowkey = false;
    }
  }
  if(is_new_rowkey)
  {
    if (OB_SUCCESS != (ret = set_new_rowkey_cell(table_id, column_id, *result))) // new value
    {
      TBSYS_LOG(WARN, "failed to set new rowkey, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = check_rowkey_change(input_row, table_id, column_id, *result, is_rowkey_change)))
    {
      TBSYS_LOG(WARN, "failed to do is_rowkey_change, err=%d", ret);
    }
  }
  return ret;
}

int ObProject::check_rowkey_change(const common::ObRow *&input_row, const uint64_t table_id,
                                   const uint64_t column_id, const common::ObObj &value, bool& is_rowkey_change)
{
  int ret = OB_SUCCESS;
  if(!is_rowkey_change)
  {
    const ObObj* cell = NULL;
    ObObj tmp_value = value;
    char buf[OB_MAX_ROW_LENGTH];
    ObString cast_buffer;
    cast_buffer.assign_ptr(buf, OB_MAX_ROW_LENGTH);
    if(OB_SUCCESS != (ret = input_row->get_cell(table_id, column_id, cell)))
    {
      TBSYS_LOG(WARN,"get rowkey column cell failed! table_id[%ld], column_id[%ld], ret[%d]",
               table_id, column_id, ret);
    }
    else if(ObNullType == cell->get_type() && ObNullType != tmp_value.get_type()) //if cell = NULL
    {
      is_rowkey_change = true;
    }
    else if (OB_SUCCESS != (ret = obj_cast(tmp_value, cell->get_type(), cast_buffer)))
    {
      TBSYS_LOG(WARN, "failed to cast obj, value=%s, type=%d, err=%d", to_cstring(tmp_value), cell->get_type(), ret);
    }
    else if(tmp_value != *cell) // ignore the same value
    {
      is_rowkey_change = true;
    }
  }
  return ret;
}

int ObProject::fill_sequence_info()
{
  int ret = OB_SUCCESS;
  if (sequence_values_.count() > 0 && (OB_SUCCESS !=(ret = fill_sequence_info(columns_))))
  {
    TBSYS_LOG(ERROR, "fill sequence info failed!, ret=%d",ret);
  }
  return ret;
}

int ObProject::serialize_idx_of_update_rowkey_expr(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  int64_t update_rowkey_expr_count = idx_of_update_rowkey_expr_.count();
  if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, update_rowkey_expr_count)))
  {
    TBSYS_LOG(WARN, "fail to serialize update_rowkey_expr_count. ret=%d", ret);
  }
  else
  {
    for (int64_t i = 0; ret == OB_SUCCESS && i < update_rowkey_expr_count; i++)
    {
      if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, idx_of_update_rowkey_expr_.at(i))))
      {
        TBSYS_LOG(WARN, "serialize idx_of_update_rowkey_expr fail. ret=%d", ret);
        break;
      }
    }
  }
  return ret;
}

int ObProject::deserialize_idx_of_update_rowkey_expr(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t update_rowkey_expr_count = 0;
  if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &update_rowkey_expr_count)))
  {
    TBSYS_LOG(WARN, "fail to deserialize update_rowkey_expr_count. ret=%d", ret);
  }
  else
  {
    if(update_rowkey_expr_count > 0)
    {
      is_update_rowkey_ = true;
      if(NULL == new_rowkey_row_)
      {
        void *raw_buf = ob_malloc(sizeof(ObRawRow), ObModIds::OB_UPDATE_ROWKEY);
        if (NULL == raw_buf)
        {
            TBSYS_LOG(WARN, "no memory");
            ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          new_rowkey_row_ = new (raw_buf) ObRawRow();
        }
      }
    }
    for (int64_t i = 0; ret == OB_SUCCESS && i < update_rowkey_expr_count; i++)
    {
      int32_t index = 0;
      if(OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &index)))
      {
        TBSYS_LOG(WARN, "fail to deserialize idx_of_update_rowkey_expr. ret=%d", ret);
        break;
      }
      else if(OB_SUCCESS != (ret = idx_of_update_rowkey_expr_.push_back(static_cast<uint32_t>(index))))
      {
        TBSYS_LOG(WARN, "fail to push_back idx_of_update_rowkey_expr. ret=%d", ret);
        break;
      }
    }
  }
  return ret;
}

int64_t ObProject::get_idx_of_update_rowkey_expr_serialize_size(void) const
{
  int64_t size = 0;
  int64_t update_rowkey_expr_count = idx_of_update_rowkey_expr_.count();
  size += serialization::encoded_length_vi64(update_rowkey_expr_count);
  for (int64_t i = 0; i < update_rowkey_expr_count; i++)
  {
    size += serialization::encoded_length_vi32(idx_of_update_rowkey_expr_.at(i));
  }
  return size;
}

//add:e
