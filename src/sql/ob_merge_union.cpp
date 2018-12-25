/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_union.cpp
 *
 * Authors:
 *   TIAN GUAN <tianguan.dgb@taobao.com>
 *
 */

#include "ob_merge_union.h"
#include "common/ob_expr_obj.h"
#include "common/utility.h"
#include "common/ob_malloc.h"
#include "common/ob_row_util.h"
//add qianzm [set_operation] 20151222 :b
#include "ob_physical_plan.h"
#include "ob_result_set.h"
//add e
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObMergeUnion::ObMergeUnion()
  :get_next_row_func_(NULL),cur_first_query_row_(NULL), cur_second_query_row_(NULL),
  last_row_buf_(NULL), left_ret_(OB_SUCCESS), right_ret_(OB_SUCCESS),
  last_cmp_(-1), got_first_row_(false), last_output_row_(NULL), right_row_desc_(NULL),
  //add qianzm [set_operation] 20151222 :b
  first_cur_row_has_cast(false),second_cur_row_has_cast(false),
  tmp_string_buf_(ObModIds::OB_STRING_BUF, DEF_MEM_BLOCK_SIZE_)
  //add e
{
}

ObMergeUnion::~ObMergeUnion()
{
}

void ObMergeUnion::reset()
{
  get_next_row_func_ = NULL;
  cur_first_query_row_ = NULL;
  cur_second_query_row_ = NULL;
  if (NULL != last_row_buf_)
  {
    ob_free(last_row_buf_);
    last_row_buf_ = NULL;
  }
  left_ret_ = OB_SUCCESS;
  right_ret_ = OB_SUCCESS;
  last_cmp_ = -1;
  got_first_row_ = false;
  last_output_row_ = NULL;
  right_row_desc_ = NULL;
  //add qianzm [set_operation] 20160107:b
  result_type_array_.clear();
  first_cur_row_has_cast = false;
  second_cur_row_has_cast = false;
  tmp_string_buf_.clear();
  //add 20160107:e
  //last_row_.reset(false, ObRow::DEFAULT_NULL);;
  ObSetOperator::reset();
}

void ObMergeUnion::reuse()
{
  get_next_row_func_ = NULL;
  cur_first_query_row_ = NULL;
  cur_second_query_row_ = NULL;
  if (NULL != last_row_buf_)
  {
    ob_free(last_row_buf_);
    last_row_buf_ = NULL;
  }
  left_ret_ = OB_SUCCESS;
  right_ret_ = OB_SUCCESS;
  last_cmp_ = -1;
  got_first_row_ = false;
  last_output_row_ = NULL;
  right_row_desc_ = NULL;
  //add qianzm [set_operation] 20160107:b
  result_type_array_.clear();
  first_cur_row_has_cast = false;
  second_cur_row_has_cast = false;
  tmp_string_buf_.clear();
  //add 20160107:e
  //last_row_.reset(false, ObRow::DEFAULT_NULL);;
  ObSetOperator::reuse();
}

int ObMergeUnion::cons_row_desc()
{
  int ret = OB_SUCCESS;
  const ObRowDesc *left_row_desc = NULL;
  const ObRowDesc *right_row_desc = NULL;
  if (OB_SUCCESS != (ret = left_op_->get_row_desc(left_row_desc)))
  {
    TBSYS_LOG(WARN, "failed to get row desc of left op, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = right_op_->get_row_desc(right_row_desc)))
  {
    TBSYS_LOG(WARN, "failed to get row desc of right op, ret=%d", ret);
  }
  else
  {
    row_desc_ = left_row_desc;
    right_row_desc_ = right_row_desc;
  }
  return ret;
}
/*
 * When UNION ALL, we get results from two query one by one
 */
int ObMergeUnion::all_get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  // restore the row desc
  if (last_output_row_ != NULL)
  {
    const_cast<ObRow *>(last_output_row_)->set_row_desc(*right_row_desc_);
    last_output_row_ = NULL;
  }
  if (OB_SUCCESS == left_ret_)
  {
    left_ret_ = left_op_->get_next_row(row);
    if (OB_SUCCESS == left_ret_)
    {
      // nothing
    }
    else if (OB_ITER_END == left_ret_)
    {
      right_ret_ = right_op_->get_next_row(row);
      if (OB_SUCCESS == right_ret_)
      {
        const_cast<ObRow *>(row)->set_row_desc(*row_desc_);
        last_output_row_ = row;
      }
      // the first query is not empty, the second query is empty
      else if (OB_ITER_END == right_ret_)
      {
        // iter finished
        ret = OB_ITER_END;
      }
      else
      {
        ret = right_ret_;
        TBSYS_LOG(WARN, "failed to get_next_row from right_op,ret=%d",ret);
      }
    }
    else
    {
      ret = left_ret_;
      TBSYS_LOG(WARN, "failed to get_next_row from left_op, ret=%d", ret);
    }
  }
  else if (OB_ITER_END == left_ret_)
  {
    if (OB_SUCCESS == right_ret_)
    {
      right_ret_ = right_op_->get_next_row(row);
      if (OB_SUCCESS == right_ret_)
      {
        const_cast<ObRow *>(row)->set_row_desc(*row_desc_);
        last_output_row_ = row;
      }
      else if (OB_ITER_END == right_ret_)
      {
        // iter finished
        ret = OB_ITER_END;
      }
      else
      {
        ret = right_ret_;
        TBSYS_LOG(WARN, "failed to get_next_row from right_op,ret=%d",ret);
      }
    }
    else if (OB_ITER_END == right_ret_)
    {
      // iter finished
      ret = OB_ITER_END;
    }
    else
    {
      ret = right_ret_;
      TBSYS_LOG(WARN, "failed to get_next_row from right_op,ret=%d",ret);
    }
  }
  else
  {
    ret = left_ret_;
    TBSYS_LOG(WARN, "failed to get_next_row from left_op,ret=%d",ret);
  }
  if (OB_SUCCESS == ret)
  {
    ObRow *tmp_row;
    tmp_row = const_cast<ObRow*>(row);
    if (OB_SUCCESS == (ret = cast_cells_to_res_type(tmp_row)))
    {
      row = tmp_row;
    }
  }
  return ret;
}
int ObMergeUnion::compare(const ObRow *row1, const ObRow *row2, int &cmp) const
{
  int ret = OB_SUCCESS;
  cmp = 0;
  int64_t column_num = row1->get_column_num();
  int64_t i = 0;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;

  const ObObj *cell1 = NULL;
  const ObObj *cell2 = NULL;
  for (;i < column_num ; ++i)
  {
    ObExprObj expr_obj1;
    ObExprObj expr_obj2;
    if (OB_SUCCESS != (ret = row1->raw_get_cell(i, cell1, table_id, column_id)))
    {
      TBSYS_LOG(ERROR, "unexpected branch, err=%d",ret);
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    else if (OB_SUCCESS != (ret = row2->raw_get_cell(i, cell2, table_id, column_id)))
    {
      TBSYS_LOG(ERROR, "unexpected branch, err=%d",ret);
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    else
    {
      expr_obj1.assign(*cell1);
      expr_obj2.assign(*cell2);
      ret = expr_obj1.compare(expr_obj2, cmp);
      if (OB_SUCCESS == ret)
      {
        if (cmp != 0)
        {
          break;
        }
      }
	  //add qianzm [set_operation Bug 1184] 20160523:b
	  else if (OB_RESULT_UNKNOWN == ret)
      {
          ret = OB_SUCCESS;
          if (cmp != 0)
          {
            break;
          }
      }
	  //add 20160523:e
      else // (OB_RESULT_UNKNOWN == ret)
      {
        cmp = -1;
				ret = OB_SUCCESS;
        break;
      }
    }
  }
  return ret;
}
int ObMergeUnion::do_distinct(ObPhyOperator *op, const ObRow *&row)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  while (true)
  {
    ret = op->get_next_row(row);
    if (OB_SUCCESS == ret)
    {
      ret = compare(row, &last_row_, cmp);
      if (OB_SUCCESS == ret)
      {
        if (cmp != 0)
        {
          break;
        }
      }
      else
      {
        TBSYS_LOG(DEBUG, "failed to compare two row, ret=%d", ret);
        break;
      }
    }
    else if (OB_ITER_END == ret)
    {
      TBSYS_LOG(DEBUG, "reach the end of op when do distinct");
      break;
    }
    else
    {
      TBSYS_LOG(WARN, "failed to get_next_row,ret=%d",ret);
      break;
    }
  }
  return ret;
}
/*
 * When UNION DISTINCT, we need the two query already ordered.
 */
int ObMergeUnion::distinct_get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (last_output_row_ != NULL)
  {
    const_cast<ObRow *>(last_output_row_)->set_row_desc(*right_row_desc_);
    last_output_row_ = NULL;
  }
  ObString compact_row;
  compact_row.assign(last_row_buf_, OB_ROW_BUF_SIZE);
  if (got_first_row_)
  {
    // 去重
    if (last_cmp_ < 0)
    {
      left_ret_ = do_distinct(left_op_, cur_first_query_row_);
      first_cur_row_has_cast = false;
      if (left_ret_ != OB_SUCCESS && left_ret_ != OB_ITER_END)
      {
        ret = left_ret_;
        TBSYS_LOG(WARN, "failed to do_distinct on left_op, ret=%d",left_ret_);
      }
    }
    else if (last_cmp_ == 0)
    {
      left_ret_ = do_distinct(left_op_, cur_first_query_row_);
      first_cur_row_has_cast = false;
      if (left_ret_ != OB_SUCCESS && left_ret_ != OB_ITER_END)
      {
        ret = left_ret_;
        TBSYS_LOG(WARN, "failed to do_distinct on left_op, ret=%d",left_ret_);
      }
      right_ret_ = do_distinct(right_op_, cur_second_query_row_);
      second_cur_row_has_cast = false;
      if (right_ret_ != OB_SUCCESS && right_ret_ != OB_ITER_END)
      {
        ret = right_ret_;
        TBSYS_LOG(WARN, "failed to do_distinct on right_op, ret=%d",right_ret_);
      }
    }
    else
    {
      right_ret_ = do_distinct(right_op_, cur_second_query_row_);
      second_cur_row_has_cast = false;
      if (right_ret_ != OB_SUCCESS && right_ret_ != OB_ITER_END)
      {
        ret = right_ret_;
        TBSYS_LOG(WARN, "failed to do_distinct on right_op, ret=%d",right_ret_);
      }
    }
  }
  //add qianzm [set_operation] 20151222 :b
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS == left_ret_ && !first_cur_row_has_cast)
    {
      ObRow *tmp_left_row = NULL;
      tmp_left_row = const_cast<ObRow*>(cur_first_query_row_);
      ret = cast_cells_to_res_type(tmp_left_row);
      first_cur_row_has_cast = true;
    }
    if (OB_SUCCESS == ret && OB_SUCCESS == right_ret_ && !second_cur_row_has_cast)
    {
      ObRow *tmp_right_row = NULL;
      tmp_right_row = const_cast<ObRow*>(cur_second_query_row_);
      ret = cast_cells_to_res_type(tmp_right_row);
      second_cur_row_has_cast = true;
    }
  }
  //add e
  if(OB_SUCCESS == ret)
  {
    if (OB_SUCCESS == left_ret_ && OB_SUCCESS == right_ret_)
    {
      ret = compare(cur_first_query_row_, cur_second_query_row_, last_cmp_);
      if (OB_SUCCESS == ret)
      {
        if (last_cmp_ < 0)
        {
          //output
          row = cur_first_query_row_;
          got_first_row_ = true;
          // save cur_first_query_row_ to last_row
          if (OB_SUCCESS != (ret = common::ObRowUtil::convert(*cur_first_query_row_, compact_row, last_row_)))
          {
            TBSYS_LOG(WARN, "failed to save current row to last row, ret=%d",ret);
          }
        }
        else if (last_cmp_ == 0)
        {
          //output
          row = cur_first_query_row_;
          got_first_row_ = true;
          // save cur_first_query_row_ to last_row
          if (OB_SUCCESS != (ret = common::ObRowUtil::convert(*cur_first_query_row_, compact_row, last_row_)))
          {
            TBSYS_LOG(WARN, "failed to save current row to last row, ret=%d",ret);
          }
        }
        else
        {
          //output
          // change row desc
          row = cur_second_query_row_;
          got_first_row_ = true;
          const_cast<ObRow*>(cur_second_query_row_)->set_row_desc(*row_desc_);
          // 保留这行的指针，下次调用get_next_row的时候置回来
          last_output_row_ = cur_second_query_row_;
          // save cur_second_query_row_ to last_row
          if (OB_SUCCESS != (ret = common::ObRowUtil::convert(*cur_second_query_row_, compact_row, last_row_)))
          {
            TBSYS_LOG(WARN, "failed to save current row to last row, ret=%d",ret);
          }
        }
      }
      else
      {
        TBSYS_LOG(WARN, "failed to compare two row, ret=%d",ret);
      }
    }
    else if (OB_ITER_END == left_ret_ && OB_SUCCESS == right_ret_)
    {
      //output
      //change row desc
      row = cur_second_query_row_;
      got_first_row_ = true;
      const_cast<ObRow*>(cur_second_query_row_)->set_row_desc(*row_desc_);
      last_output_row_ = cur_second_query_row_;
      if (OB_SUCCESS != (ret = common::ObRowUtil::convert(*cur_second_query_row_, compact_row, last_row_)))
      {
        TBSYS_LOG(WARN, "failed to save current row to last row, ret=%d",ret);
      }
      last_cmp_ = 1;
    }
    else if (OB_SUCCESS == left_ret_ && OB_ITER_END == right_ret_)
    {
      // output
      row = cur_first_query_row_;
      got_first_row_ = true;
      if (OB_SUCCESS != (ret = common::ObRowUtil::convert(*cur_first_query_row_, compact_row, last_row_)))
      {
        TBSYS_LOG(WARN, "failed to save current row to last row, ret=%d",ret);
      }
      last_cmp_ = -1;
    }
    else if (OB_ITER_END == left_ret_ && OB_ITER_END == right_ret_)
    {
      ret = OB_ITER_END;
      last_cmp_ = -1;
    }
    else
    {
      // (left_ret, right_ret) in ((SUCCESS, ERROR), (ITER_END, ERROR), (ERROR, SUCCESS), (ERROR, ITER_END), (ERROR, ERROR))
      ret = (OB_SUCCESS != left_ret_ && OB_ITER_END != left_ret_) ? left_ret_ : right_ret_;
      TBSYS_LOG(WARN, "failed to get next row, err=%d left_err=%d right_err=%d",
                ret, left_ret_, right_ret_);
    }
  }
  return ret;
}
int ObMergeUnion::set_distinct(bool is_distinct)
{
  int ret = OB_SUCCESS;
  ObSetOperator::set_distinct(is_distinct);
  if (is_distinct)
  {
    get_next_row_func_ = &ObMergeUnion::distinct_get_next_row;
  }
  else
  {
    get_next_row_func_ = &ObMergeUnion::all_get_next_row;
  }
  return ret;
}
int ObMergeUnion::open()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ObDoubleChildrenPhyOperator::open()))
  {
    TBSYS_LOG(WARN, "failed to open double child operators, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = cons_row_desc()))
  {
    TBSYS_LOG(WARN, "failed to construct row description, ret=%d", ret);
  }
  else
  {
    got_first_row_ = false;
    if (is_distinct())
    {
      if (NULL == (last_row_buf_ = (char*)ob_malloc(OB_ROW_BUF_SIZE, 0)))
      {
        TBSYS_LOG(ERROR, "failed to ob_malloc %lu bytes memory", OB_ROW_BUF_SIZE);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        left_ret_ = left_op_->get_next_row(cur_first_query_row_);
        right_ret_ = right_op_->get_next_row(cur_second_query_row_);
      }
    }
    //del qianzm [set_operation] 20160107 :b
    /*if (OB_SUCCESS == ret)
    {
      int64_t field_count = get_phy_plan()->get_result_set()->get_field_columns().count();
      for (int index = 0; index < field_count; index ++)
      {
        ObObjType res_type = get_phy_plan()->get_result_set()->get_field_columns().at(index).type_.get_type();
        result_type_array_.push_back(res_type);
      }
    }*/
    //del 20160107:e
  }
  return ret;
}
//add qianzm [set_operation] 20151222 :b
int ObMergeUnion::cast_cells_to_res_type(common::ObRow *&tmp_row)
{
  int ret = OB_SUCCESS;
  //add qianzm [set_operation] 20160115:b
  if (tmp_row->get_column_num() != result_type_array_.count())
  {
    TBSYS_LOG(ERROR, "row_columns_number is not equal result_type_array_size");
    ret = OB_ERR_UNEXPECTED;
  }
  //add 20160115:e
  else
  {
    ObStringBuf *tmp_string = &tmp_string_buf_;
    for(int64_t i = 0; i < tmp_row->get_column_num(); i ++)
    {
      ObObj *tmp_cell = NULL;
      ObExprObj expr_from;
      ObExprObj expr_to;
      ObExprObj expr_tmp;
      if (OB_SUCCESS != tmp_row->raw_get_cell_for_update(i,tmp_cell))
      {
        TBSYS_LOG(ERROR, "unexpected branch, err=%d",ret);
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      else if (!tmp_cell->is_null())
      {
        //expr_from.assign(*tmp_cell);
        ObObjType this_type =  tmp_cell->get_type();
        ObObjType res_type = result_type_array_.at(i);
        if (this_type != res_type)
        {
          expr_from.assign(*tmp_cell);

          if (ObDecimalType == res_type)
          {
            if (ObVarcharType == this_type)
            {
              if (OB_SUCCESS != (ret = expr_from.cast_to(ObDecimalType, expr_to, *tmp_string)))
              {
                ret = OB_ERR_COLUMN_TYPE_NOT_COMPATIBLE;
                TBSYS_LOG(ERROR,"column[%ld] value fail to cast to ObDecimalType", i + 1);
                TBSYS_LOG(USER_ERROR,
                          "Invalid character found in a character string argument or A numeric value 'Decimal' is too long");
                break;
              }
              else if (expr_to.is_null())
              {
                ret = OB_ERR_COLUMN_TYPE_NOT_COMPATIBLE;
                TBSYS_LOG(ERROR,"column[%ld] value fail to cast to ObDecimalType", i + 1);
                TBSYS_LOG(USER_ERROR,
                          "Invalid character found in a character string argument or A numeric value 'Decimal' is too long");
                break;
              }
              else
              {
                expr_to.to(*tmp_cell);
              }
            }
            else
            {
              if (OB_SUCCESS != (ret = expr_from.cast_to(ObVarcharType, expr_tmp, *tmp_string)))
              {
                ret = OB_INVALID_ARGUMENT;
                TBSYS_LOG(ERROR,"column[%ld] value fail to cast to ObVarcharType", i + 1);
              }
              if (OB_SUCCESS != (ret = expr_tmp.cast_to(ObDecimalType, expr_to, *tmp_string)))
              {
                ret = OB_ERR_COLUMN_TYPE_NOT_COMPATIBLE;
                TBSYS_LOG(ERROR,"column[%ld] value fail to cast to ObDecimalType", i + 1);
              }
              else if (expr_to.is_null())
              {
                ret = OB_ERR_COLUMN_TYPE_NOT_COMPATIBLE;
                TBSYS_LOG(ERROR,"column[%ld] value can not cast to ObDecimalType", i + 1);
                break;
              }
              else
              {
                expr_to.to(*tmp_cell);
              }
            }
          }
          else if (ObDecimalType != res_type)
          {
            if (OB_SUCCESS != (ret = expr_from.cast_to(res_type, expr_to, *tmp_string)))
            {
              ret = OB_ERR_COLUMN_TYPE_NOT_COMPATIBLE;
              TBSYS_LOG(ERROR,"column[%ld] value can not cast to dest_type[%d]", i + 1, res_type);
              break;
            }
            else if (expr_to.is_null())
            {
              ret = OB_ERR_COLUMN_TYPE_NOT_COMPATIBLE;
              TBSYS_LOG(ERROR,"column[%ld] value can not cast to dest_type[%d]", i + 1, res_type);
              break;
            }
            else
            {
              expr_to.to(*tmp_cell);
            }
          }
        }
      }
    }
  }
  return ret;
}
//add e
//add qianzm [set_operation] 20160115:b
int ObMergeUnion::add_result_type_array(common::ObArray<common::ObObjType>& result_columns_type)
{
   result_type_array_ = result_columns_type;
   return OB_SUCCESS;
}
//add 20160115:e
int ObMergeUnion::close()
{
  int ret = OB_SUCCESS;
  got_first_row_ = false;
  if (NULL != last_row_buf_)
  {
    ob_free(last_row_buf_);
    last_row_buf_ = NULL;
  }
  //add qianzm [set_operation] 20160115:b
  result_type_array_.clear();
  tmp_string_buf_.clear();
  //add e
  if (OB_SUCCESS != (ret = ObDoubleChildrenPhyOperator::close()))
  {
    TBSYS_LOG(WARN, "failed to close child op,ret=%d", ret);
  }
  return ret;
}

int ObMergeUnion::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (NULL == row_desc_)
  {
    TBSYS_LOG(ERROR, "not init");
    ret = OB_NOT_INIT;
  }
  else if (OB_LIKELY(row_desc_->get_column_num() <= 0))
  {
    TBSYS_LOG(ERROR, "not init");
    ret = OB_NOT_INIT;
  }
  else
  {
    row_desc = row_desc_;
  }
  return ret;
}

int ObMergeUnion::get_next_row(const ObRow *&row)
{
  OB_ASSERT(get_next_row_func_);
  return (this->*(this->ObMergeUnion::get_next_row_func_))(row);
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObMergeUnion, PHY_MERGE_UNION);
  }
}

int64_t ObMergeUnion::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "MergeUnion()\n");
  if (NULL != left_op_)
  {
    databuff_printf(buf, buf_len, pos, "UnionLeftChild=\n");
    pos += left_op_->to_string(buf+pos, buf_len-pos);
  }
  if (NULL != right_op_)
  {
    databuff_printf(buf, buf_len, pos, "UnionRightChild=\n");
    pos += right_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}

PHY_OPERATOR_ASSIGN(ObMergeUnion)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObMergeUnion);//add qianzm [set_operation] 20160115
  reset();
  result_type_array_ = o_ptr->result_type_array_;//add qianzm [set_operation] 20160115
  ObSetOperator::assign(other);
  if (distinct_)
  {
    get_next_row_func_ = &ObMergeUnion::distinct_get_next_row;
  }
  else
  {
    get_next_row_func_ = &ObMergeUnion::all_get_next_row;
  }
  return ret;
}
