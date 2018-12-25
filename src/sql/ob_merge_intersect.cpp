/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_iterset.cpp
 *
 * Authors:
 *   TIAN GUAN <tianguan.dgb@taobao.com>
 *
 */

#include "ob_merge_intersect.h"
#include "common/utility.h"
#include "common/ob_row_util.h"
#include "common/ob_expr_obj.h"
//add qianzm [set_operation] 20151222 :b
#include "ob_physical_plan.h"
#include "ob_result_set.h"
//add e
using namespace oceanbase::common;
using namespace oceanbase::sql;


ObMergeIntersect::ObMergeIntersect()
  :cur_first_query_row_(NULL),cur_second_query_row_(NULL),
    left_ret_(OB_SUCCESS), right_ret_(OB_SUCCESS),
    last_cmp_(-1), got_first_row_(false), left_last_row_buf_(NULL), right_last_row_buf_(NULL),
    //add qianzm [set_operation] 20151222 :b
    first_cur_row_has_cast(false),second_cur_row_has_cast(false),
    tmp_string_buf_(ObModIds::OB_STRING_BUF, DEF_MEM_BLOCK_SIZE_)
    //add e
{
}

ObMergeIntersect::~ObMergeIntersect()
{
}

void ObMergeIntersect::reset()
{
  get_next_row_func_ = NULL;
  //left_last_row_.reset(false, ObRow::DEFAULT_NULL);
  //right_last_row_.reset(false, ObRow::DEFAULT_NULL);
  cur_first_query_row_ = NULL;
  cur_second_query_row_ = NULL;
  left_ret_ = OB_SUCCESS;
  right_ret_ = OB_SUCCESS;
  last_cmp_ = -1;
  got_first_row_ = false;
  left_last_row_buf_ = NULL;
  right_last_row_buf_ = NULL;
  //add qianzm [set_operation] 20160107:b
  result_type_array_.clear();
  first_cur_row_has_cast = false;
  second_cur_row_has_cast = false;
  tmp_string_buf_.clear();
  //add 20160107:e
  ObSetOperator::reset();
}

void ObMergeIntersect::reuse()
{
  get_next_row_func_ = NULL;
  //left_last_row_.reset(false, ObRow::DEFAULT_NULL);
  //right_last_row_.reset(false, ObRow::DEFAULT_NULL);
  cur_first_query_row_ = NULL;
  cur_second_query_row_ = NULL;
  left_ret_ = OB_SUCCESS;
  right_ret_ = OB_SUCCESS;
  last_cmp_ = -1;
  got_first_row_ = false;
  left_last_row_buf_ = NULL;
  right_last_row_buf_ = NULL;
  //add qianzm [set_operation] 20160107:b
  result_type_array_.clear();
  first_cur_row_has_cast = false;
  second_cur_row_has_cast = false;
  tmp_string_buf_.clear();
  //add 20160107:e
  ObSetOperator::reuse();
}

int ObMergeIntersect::set_distinct(bool is_distinct)
{
  int ret = OB_SUCCESS;
  ObSetOperator::set_distinct(is_distinct);
  if (is_distinct)
  {
    get_next_row_func_ = &ObMergeIntersect::distinct_get_next_row;
  }
  else
  {
    get_next_row_func_ = &ObMergeIntersect::all_get_next_row;
  }
  return ret;
}

int ObMergeIntersect::open()
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
  else if (NULL == (left_last_row_buf_ = (char*)ob_malloc(OB_ROW_BUF_SIZE, 0)))
  {
    TBSYS_LOG(ERROR, "failed to ob_malloc %lu bytes memory", OB_ROW_BUF_SIZE);
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (NULL == (right_last_row_buf_ = (char*)ob_malloc(OB_ROW_BUF_SIZE, 0)))
  {
    TBSYS_LOG(ERROR, "failed to ob_malloc %lu bytes memory", OB_ROW_BUF_SIZE);
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    got_first_row_ = false;
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
  //del 20160107: e
  return ret;
}
int ObMergeIntersect::close()
{
  int ret = OB_SUCCESS;
  if (NULL != right_last_row_buf_)
  {
    ob_free(right_last_row_buf_);
    right_last_row_buf_ = NULL;
  }
  if (NULL != left_last_row_buf_)
  {
    ob_free(left_last_row_buf_);
    left_last_row_buf_ = NULL;
  }
  //add qianzm [set_operation] 20160115:b
  result_type_array_.clear();
  tmp_string_buf_.clear();
  //add e
  if (OB_SUCCESS != (ret = ObDoubleChildrenPhyOperator::close()))
  {
    TBSYS_LOG(WARN, "failed to close child op,ret=%d", ret);
  }
  row_desc_ = NULL;
  cur_first_query_row_ = NULL;
  cur_second_query_row_ = NULL;
  left_ret_ = OB_SUCCESS;
  right_ret_ = OB_SUCCESS;
  last_cmp_ = -1;
  got_first_row_ = false;
  return ret;
}
int ObMergeIntersect::get_next_row(const ObRow *&row)
{
  OB_ASSERT(get_next_row_func_);
  return (this->*(this->ObMergeIntersect::get_next_row_func_))(row);
}
int ObMergeIntersect::do_distinct(ObPhyOperator *op, const ObRow *other, const ObRow *&row)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  while (true)
  {
    ret = op->get_next_row(row);
    if (OB_SUCCESS == ret)
    {
      ret = compare(row, other, cmp);
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

int ObMergeIntersect::distinct_get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  ObString left_last_compact_row;
  left_last_compact_row.assign(left_last_row_buf_, OB_ROW_BUF_SIZE);
  ObString right_last_compact_row;
  right_last_compact_row.assign(right_last_row_buf_, OB_ROW_BUF_SIZE);

  int cmp = 0;

  if (got_first_row_)
  {
    left_ret_ = do_distinct(left_op_, &left_last_row_, cur_first_query_row_);
    first_cur_row_has_cast = false;
    if (left_ret_ != OB_SUCCESS && left_ret_ != OB_ITER_END)
    {
      ret = left_ret_;
      TBSYS_LOG(WARN, "failed to do_distinct on left_op, ret=%d", left_ret_);
    }
    right_ret_ = do_distinct(right_op_, &right_last_row_, cur_second_query_row_);
    second_cur_row_has_cast = false;
    if (right_ret_ != OB_SUCCESS && right_ret_ != OB_ITER_END)
    {
      ret = right_ret_;
      TBSYS_LOG(WARN, "failed to do_distinct on right_op, ret=%d", right_ret_);
    }
  }
  else
  {
    left_ret_ = left_op_->get_next_row(cur_first_query_row_);
    first_cur_row_has_cast = false;
    if (left_ret_ != OB_SUCCESS && left_ret_ != OB_ITER_END)
    {
      ret = left_ret_;
      TBSYS_LOG(WARN, "failed to get_next_row on left_op, ret=%d", left_ret_);
    }
    right_ret_ = right_op_->get_next_row(cur_second_query_row_);
    second_cur_row_has_cast = false;
    if (right_ret_ != OB_SUCCESS && right_ret_ != OB_ITER_END)
    {
      ret = right_ret_;
      TBSYS_LOG(WARN, "failed to get_next_row on right_op, ret=%d", right_ret_);
    }
  }
  if (OB_SUCCESS == ret)
  {
    while (true)
    {
      //add qianzm [set_operation] 20151222 :b
      ObRow *tmp_left_row = NULL;
      ObRow *tmp_right_row = NULL;
      if(OB_SUCCESS == ret && OB_SUCCESS == left_ret_ && !first_cur_row_has_cast)
      {
        tmp_left_row = const_cast<ObRow*>(cur_first_query_row_);
        ret = cast_cells_to_res_type(tmp_left_row);
        first_cur_row_has_cast = true;
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == right_ret_ && !second_cur_row_has_cast)
      {
        tmp_right_row = const_cast<ObRow*>(cur_second_query_row_);
        ret = cast_cells_to_res_type(tmp_right_row);
        second_cur_row_has_cast = true;
      }
      //add e
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS == left_ret_ && OB_SUCCESS == right_ret_)
        {
          ret = compare(cur_first_query_row_, cur_second_query_row_, cmp);
          if (OB_SUCCESS == ret)
          {
            if (cmp < 0)
            {
              left_last_compact_row.assign(left_last_row_buf_, OB_ROW_BUF_SIZE);
              // save cur_first_query_row_ to left_last_row_
              if (OB_SUCCESS != (ret = common::ObRowUtil::convert(*cur_first_query_row_, left_last_compact_row, left_last_row_)))
              {
                TBSYS_LOG(WARN, "failed to save cur_first_query_row_ to left last row, ret=%d",ret);
                break;
              }
              else
              {
                left_ret_ = do_distinct(left_op_, &left_last_row_, cur_first_query_row_);
                first_cur_row_has_cast = false;
                continue;
              }
            }
            else if (cmp == 0)
            {
              //output
              row = cur_first_query_row_;
              got_first_row_ = true;
              left_last_compact_row.assign(left_last_row_buf_, OB_ROW_BUF_SIZE);
              right_last_compact_row.assign(right_last_row_buf_, OB_ROW_BUF_SIZE);
              // save cur_first_query_row_ to left_last_row_
              if (OB_SUCCESS != (ret = common::ObRowUtil::convert(*cur_first_query_row_, left_last_compact_row, left_last_row_)))
              {
                TBSYS_LOG(WARN, "failed to save cur_first_query_row_ to left last row, ret=%d",ret);
              }
              // save cur_second_query_row_ to right_last_row_
              else if (OB_SUCCESS != (ret = common::ObRowUtil::convert(*cur_second_query_row_, right_last_compact_row, right_last_row_)))
              {
                TBSYS_LOG(WARN, "failed to save cur_second_query_row_ to right last row, ret=%d",ret);
              }
              break;
            }
            else
            {
              right_last_compact_row.assign(right_last_row_buf_, OB_ROW_BUF_SIZE);
              // save cur_second_query_row_ to right_last_row_
              if (OB_SUCCESS != (ret = common::ObRowUtil::convert(*cur_second_query_row_, right_last_compact_row, right_last_row_)))
              {
                TBSYS_LOG(WARN, "failed to save cur_second_query_row_ to right last row, ret=%d",ret);
                break;
              }
              else
              {
                right_ret_ = do_distinct(right_op_, &right_last_row_, cur_second_query_row_);
                second_cur_row_has_cast = false;
                continue;
              }
            }
          }
          else
          {
            TBSYS_LOG(WARN, "failed to compare two row, ret=%d", ret);
            break;
          }
        }
        else if (OB_ITER_END == left_ret_ || OB_ITER_END == right_ret_)
        {
          ret = OB_ITER_END;
          break;
        }
        else// unexpected branch
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(ERROR, "failed to do distinct, ret=%d, left_ret_=%d, right_ret_=%d", ret, left_ret_, right_ret_);
          break;
        }
      }
      else
      {
        break;
      }
    }//while
  }
  else
  {
    if (got_first_row_)
    {
      TBSYS_LOG(WARN, "failed to do_distinct, left_ret_=%d, right_ret_=%d", left_ret_, right_ret_);
    }
    else
    {
      TBSYS_LOG(WARN, "failed to get next row , left_ret_=%d, right_ret_=%d", left_ret_, right_ret_);
    }
  }
  return ret;
}
int ObMergeIntersect::all_get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  ObString left_last_compact_row;
  left_last_compact_row.assign(left_last_row_buf_, OB_ROW_BUF_SIZE);
  ObString right_last_compact_row;
  right_last_compact_row.assign(right_last_row_buf_, OB_ROW_BUF_SIZE);

  int cmp = 0;

  left_ret_ = left_op_->get_next_row(cur_first_query_row_);
  first_cur_row_has_cast = false;
  if (left_ret_ != OB_SUCCESS && left_ret_ != OB_ITER_END)
  {
    ret = left_ret_;
    TBSYS_LOG(WARN, "failed to get_next_row on left_op, ret=%d", left_ret_);
  }
  right_ret_ = right_op_->get_next_row(cur_second_query_row_);
  second_cur_row_has_cast = false;
  if (right_ret_ != OB_SUCCESS && right_ret_ != OB_ITER_END)
  {
    ret = right_ret_;
    TBSYS_LOG(WARN, "failed to get_next_row on right_op, ret=%d", right_ret_);
  }
  if (OB_SUCCESS == ret)
  {
    while (true)
    {
      //add qianzm [set_operation] 20151222 :b
      ObRow *tmp_left_row = NULL;
      ObRow *tmp_right_row = NULL;
      if(OB_SUCCESS == ret && OB_SUCCESS == left_ret_ && !first_cur_row_has_cast)
      {
        tmp_left_row = const_cast<ObRow*>(cur_first_query_row_);
        ret = cast_cells_to_res_type(tmp_left_row);
        first_cur_row_has_cast = true;
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == right_ret_ && !second_cur_row_has_cast)
      {
        tmp_right_row = const_cast<ObRow*>(cur_second_query_row_);
        ret = cast_cells_to_res_type(tmp_right_row);
        second_cur_row_has_cast = true;
      }
      //add e
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS == left_ret_ && OB_SUCCESS == right_ret_)
        {
          ret = compare(cur_first_query_row_, cur_second_query_row_, cmp);
          if (OB_SUCCESS == ret)
          {
            if (cmp < 0)
            {
              left_last_compact_row.assign(left_last_row_buf_, OB_ROW_BUF_SIZE);
              // save cur_first_query_row_ to left_last_row_
              if (OB_SUCCESS != (ret = common::ObRowUtil::convert(*cur_first_query_row_, left_last_compact_row, left_last_row_)))
              {
                TBSYS_LOG(WARN, "failed to save cur_first_query_row_ to left last row, ret=%d",ret);
                break;
              }
              else
              {
                left_ret_ = do_distinct(left_op_, &left_last_row_, cur_first_query_row_);
                first_cur_row_has_cast = false;
                continue;
              }
            }
            else if (cmp == 0)
            {
              //output
              row = cur_first_query_row_;
              break;
            }
            else
            {
              right_last_compact_row.assign(right_last_row_buf_, OB_ROW_BUF_SIZE);
              // save cur_second_query_row_ to right_last_row_
              if (OB_SUCCESS != (ret = common::ObRowUtil::convert(*cur_second_query_row_, right_last_compact_row, right_last_row_)))
              {
                TBSYS_LOG(WARN, "failed to save cur_second_query_row_ to right last row, ret=%d",ret);
                break;
              }
              else
              {
                right_ret_ = do_distinct(right_op_, &right_last_row_, cur_second_query_row_);
                second_cur_row_has_cast = false;
                continue;
              }

            }
          }
          else
          {
            TBSYS_LOG(WARN, "failed to compare two row, ret=%d", ret);
            break;
          }

        }
        else if (OB_ITER_END == left_ret_ || OB_ITER_END == right_ret_)
        {
          ret = OB_ITER_END;
          break;
        }
        else// unexpected branch
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(ERROR, "failed to do distinct, ret=%d, left_ret_=%d, right_ret_=%d", ret, left_ret_, right_ret_);
          break;
        }
      }
      else
      {
        break;
      }
    }//while
  }
  else
  {
    TBSYS_LOG(WARN, "failed to get next row, left_ret_=%d, right_ret_=%d", left_ret_, right_ret_);
  }
  return ret;
}
int ObMergeIntersect::cons_row_desc()
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
  }
  return ret;
}
int ObMergeIntersect::compare(const ObRow *row1, const ObRow *row2, int &cmp) const
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

int ObMergeIntersect::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (NULL == row_desc_)
  {
    TBSYS_LOG(ERROR, "not init");
    ret = OB_NOT_INIT;
  }
  else if (OB_UNLIKELY(row_desc_->get_column_num() <= 0))
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
//add qianzm [set_operation] 20151222 :b
int ObMergeIntersect::cast_cells_to_res_type(common::ObRow *&tmp_row)
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
                break;
              }
              else if (expr_to.is_null())
              {
                ret = OB_ERR_COLUMN_TYPE_NOT_COMPATIBLE;
                TBSYS_LOG(ERROR,"column[%ld] value fail to cast to ObDecimalType", i + 1);
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
int ObMergeIntersect::add_result_type_array(common::ObArray<common::ObObjType>& result_columns_type)
{
   result_type_array_ = result_columns_type;
   return OB_SUCCESS;
}
//add 20160115:e
namespace oceanbase{
namespace sql{
REGISTER_PHY_OPERATOR(ObMergeIntersect, PHY_MERGE_INTERSECT);
}
}

int64_t ObMergeIntersect::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "MergeIntersect()\n");
  if (NULL != left_op_)
  {
    databuff_printf(buf, buf_len, pos, "IntersectLeftChild=\n");
    pos += left_op_->to_string(buf+pos, buf_len-pos);
  }
  if (NULL != right_op_)
  {
    databuff_printf(buf, buf_len, pos, "IntersectRightChild=\n");
    pos += right_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}

PHY_OPERATOR_ASSIGN(ObMergeIntersect)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObMergeIntersect);//add qianzm [set_operation] 20160115
  reset();
  result_type_array_ = o_ptr->result_type_array_;//add qianzm [set_operation] 20160115
  ObSetOperator::assign(other);
  if (distinct_)
  {
    get_next_row_func_ = &ObMergeIntersect::distinct_get_next_row;
  }
  else
  {
    get_next_row_func_ = &ObMergeIntersect::all_get_next_row;
  }
  return ret;
}
