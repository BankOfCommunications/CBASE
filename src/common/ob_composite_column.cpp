/*
 * (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_composite_column.cpp is for what ...
 *
 * Version: $id: ob_composite_column.cpp, v 0.1 7/29/2011 14:39 xiaochu Exp $
 *
 * Authors:
 *   xiaochu <xiaochu.yh@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_composite_column.h"
#include "ob_cell_array.h"


namespace oceanbase
{
  namespace common
  {
    ObCompositeColumn::ObCompositeColumn()
      :state(NOT_INIT)
    {
    }
    
    /* 用于cs */
    int ObCompositeColumn::set_expression(const ObObj *expr, oceanbase::common::ObStringBuf  & data_buf)
    {
      int err = OB_SUCCESS;
      if(OB_SUCCESS != (err = postfix_.set_expression(expr, data_buf)))
      {
        TBSYS_LOG(WARN, "set postfix expression error.");
      }
      else
      {
        state = USE_POSTFIX;
      }
      return err;
    }
  
    /* 用于client */
    //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
    //int ObCompositeColumn::set_expression(const ObString &expr,
    //                                      const ObString &as_column_name)
    int ObCompositeColumn::set_expression(const ObString&    expr,
                       const ObString&    as_column_name,
                       bool               is_expire_info)
    {
      int err = OB_SUCCESS;
      //if(OB_SUCCESS != (err = infix_.set_expression(expr)))
      if(OB_SUCCESS != (err = infix_.set_expression(expr, is_expire_info)))
      {
        TBSYS_LOG(WARN, "set infix expression error. [expr=%.*s][err=%d]", 
            expr.length(), expr.ptr(), err);
      }
      else
      {
        as_column_name_ = as_column_name;
        state = USE_INFIX;
      }
      return err;
    }
     //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e

    /* 计算复合列的值 */
    int ObCompositeColumn::calc_composite_val(ObObj &composite_val,
        const ObCellArray & org_cells,
        const int64_t org_row_beg,
        const int64_t org_row_end
        )
    {
      int err = OB_SUCCESS;

      /*  FIXME: 目前不支持"select 5+3"这种语句，要求select中至少出现一个列。
       *  出现此类情况时下面将报错 
       */
      if (org_row_beg < 0
          || org_row_end < 0
          || org_row_beg > org_row_end
          || org_row_end >= org_cells.get_cell_size())
      {
        TBSYS_LOG(WARN,"param error [org_row_beg:%ld,org_row_end:%ld,org_cells.get_cell_size():%ld]",
            org_row_beg, org_row_end, org_cells.get_cell_size());
        err = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == err)
      {
        if (USE_POSTFIX == state)
        {
          err = postfix_.calc(composite_val, org_cells,org_row_beg, org_row_end);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to calculate composite column value, err=%d", err);
          }
        }
        else if(USE_INFIX == state)
        {
#if 0 // 必须先转换好，不应该在这里做转换工作       
          // 将中缀表达式转换成后缀表达式          
          err = postfix_.set_expression(infix_.get_expression());
          if (OB_SUCCESS == err)
          {
            state = USE_POSTFIX;
            if(OB_SUCCESS != (err = postfix_.calc(composite_val, org_cells,org_row_beg, org_row_end)))
            {
              TBSYS_LOG(WARN, "failed to calculate composite column value, err=%d", err);
            }
          }
#else          
          err = OB_ERROR;
          TBSYS_LOG(WARN, "please convert infix expression to postfix expression first.");
#endif          
        }
        else
        {
          err = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "Invalid expression calculator.");
        }
     }
      return err;
    }

    DEFINE_SERIALIZE(ObCompositeColumn)
    {
      int err = OB_SUCCESS;
      ObObj obj;

      // serialize postfix expression(ms->ups)
      // or serialize infix expression(client->ms)
      if (USE_POSTFIX == state)
      {
        TBSYS_LOG(DEBUG, "serialize::postfix mode");

        obj.set_int((int64_t)USE_POSTFIX); // indicates postfix
        if (OB_SUCCESS != (err = obj.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "serialize USE_POSTFIX flag failed. err[%d]", err);
        }
        else if (OB_SUCCESS != (err = postfix_.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "serialize postfix expression failed. err[%d]", err);
        }
        else
        {
          TBSYS_LOG(DEBUG, "serialize postfix expression OK");
        }
      }
      else if(USE_INFIX == state)
      {
        TBSYS_LOG(DEBUG, "serialize::infix mode");

        obj.set_int((int64_t)USE_INFIX); // indicates infix 
        if (OB_SUCCESS != (err = obj.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "serialize USE_INFIX flag failed. err[%d]", err);
        }
        else if (OB_SUCCESS != (err = infix_.serialize(buf, buf_len, pos)))
        {
            TBSYS_LOG(WARN, "serialize infix expression failed. err[%d]", err);
        }
        else
        {
          obj.set_varchar(as_column_name_);
          if (OB_SUCCESS != (err = obj.serialize(buf, buf_len, pos)))
          {
             TBSYS_LOG(WARN, "serialize as_column_name_ failed. err[%d]", err);
          }
        }
      }
      else
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "unexpected expression type");
      }
      return err;
    }

    DEFINE_DESERIALIZE(ObCompositeColumn)
    {
      ObObj obj;
      int64_t column_type = NOT_INIT;
      int64_t old_pos = pos;
      int err = obj.deserialize(buf, data_len, pos);
      
      state = NOT_INIT;

      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "deserialize composite column type failed:err[%d]", err);
      }
      else if (ObIntType == obj.get_type())
      {
        err = obj.get_int(column_type);
        if (err != OB_SUCCESS)
        {
          err = OB_NOT_INIT;
          TBSYS_LOG(WARN, "get composite column type int value failed:err[%d]", err);
        }
        if (USE_INFIX == column_type) // infix expression
        {
          TBSYS_LOG(DEBUG, "deserialize::infix mode");
          if(OB_SUCCESS != (err = infix_.deserialize(buf, data_len, pos)))
          {
            TBSYS_LOG(WARN, "deserialize infix expression failed:err[%d]", err);
          }
          else if(OB_SUCCESS != (err = obj.deserialize(buf, data_len, pos)))
          {
            TBSYS_LOG(WARN, "deserialize as_column_name_ failed:err[%d]", err);
          }
          else if(OB_SUCCESS != (err = obj.get_varchar(as_column_name_)))
          {
            TBSYS_LOG(WARN, "get as_column_name_ value failed:err[%d]", err);
          }
          else
          {
            state = USE_INFIX;
          }
        }
        else if (USE_POSTFIX == column_type)  // postfix expression
        {
          TBSYS_LOG(DEBUG, "deserialize::postfix mode");
          if(OB_SUCCESS != (err = postfix_.deserialize(buf, data_len, pos)))
          {
            TBSYS_LOG(WARN, "deserialize postfix expression failed:err[%d]", err);
          }
          else
          {
            state = USE_POSTFIX;
          }
        }
        else
        {
          err = OB_NOT_INIT;
          TBSYS_LOG(WARN, "not initialized.");
        }
      }
      else
      {
        pos = old_pos;     
        err = OB_UNKNOWN_OBJ; 
        /// TBSYS_LOG(WARN, "Int column type expected:[type:%d]", obj.get_type());
      }
      return err;
    }


    DEFINE_GET_SERIALIZE_SIZE(ObCompositeColumn)
    {
      ObObj obj;
      int64_t total_size = 0;
      if (USE_POSTFIX == state)
      {
        obj.set_int((int64_t)USE_POSTFIX); // indicates postfix
        total_size = obj.get_serialize_size();
        total_size += postfix_.get_serialize_size();
      }
      else if(USE_INFIX == state)
      {
        obj.set_int((int64_t)USE_INFIX); // indicates infix
        total_size = obj.get_serialize_size();
        total_size += infix_.get_serialize_size();
        obj.set_varchar(as_column_name_);
        total_size += obj.get_serialize_size();
      }
      return total_size;
    }

  } /* common */
} /* oceanbase */
