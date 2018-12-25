/*
 * (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_postfix_expression.cpp is for what ...
 *
 * Version: $id: ob_postfix_expression.cpp, v 0.1 7/29/2011 14:39 xiaochu Exp $
 *
 * Authors:
 *   xiaochu <xiaochu.yh@taobao.com>
 *     - some work details if you want
 *
 */


#include "ob_postfix_expression.h"
#include "ob_cell_array.h"
using namespace oceanbase::common::ObExpression;

namespace oceanbase
{
  namespace common
  {
    static inline int reserved_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result)
    {
      int err = OB_INVALID_ARGUMENT;
      UNUSED(obj1);
      UNUSED(obj2);
      UNUSED(result);
      return err;
    }


    /* compare function list:
     * >   gt_func
     * >=  ge_func
     * <=  le_func
     * <   lt_func
     * ==  eq_func
     * !=  neq_func
     */
    static int gt_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        //mod peiouya [DATE_TIME] 20150929:b
        //stack_i[idx_i-2].gt(stack_i[idx_i-1], result);
        //if (result.is_null())
          err = stack_i[idx_i-2].gt(stack_i[idx_i-1], result);
          if (err == OB_SUCCESS && result.is_null())
        //mod 20150929:e
        {
          ObObjType this_type = stack_i[idx_i-2].get_type();
          ObObjType other_type = stack_i[idx_i-1].get_type();
          if (this_type == ObNullType || other_type == ObNullType)
          {
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
            //int cmp = this_type - other_type;
            int cmp = other_type - this_type;
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
            result.set_bool(cmp > 0);
          }
          else
          {
            TBSYS_LOG(WARN, "unexpected branch, this_type=%d other_type=%d", this_type, other_type);
            err = OB_ERR_UNEXPECTED;
          }
        }
        idx_i -= 2;
      }
      return err;
    }

    static int ge_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        //mod peiouya [DATE_TIME] 20150929:b
        //stack_i[idx_i-2].ge(stack_i[idx_i-1], result);
        //if (result.is_null())
        err = stack_i[idx_i-2].ge(stack_i[idx_i-1], result);
        if (err == OB_SUCCESS && result.is_null())
        //mod 20150929:e
        {
          ObObjType this_type = stack_i[idx_i-2].get_type();
          ObObjType other_type = stack_i[idx_i-1].get_type();
          if (this_type == ObNullType || other_type == ObNullType)
          {
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
            //int cmp = this_type - other_type;
            int cmp = other_type - this_type;
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
            result.set_bool(cmp >= 0);
          }
          else
          {
            TBSYS_LOG(WARN, "unexpected branch, this_type=%d other_type=%d", this_type, other_type);
            err = OB_ERR_UNEXPECTED;
          }
        }
        idx_i -= 2;
      }
      return err;
    }

    static int le_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        //mod peiouya [DATE_TIME] 20150929:b
        //stack_i[idx_i-2].le(stack_i[idx_i-1], result);
        //if (result.is_null())
        err = stack_i[idx_i-2].le(stack_i[idx_i-1], result);
        if (err == OB_SUCCESS && result.is_null())
        //mod 20150929:e
        {
          ObObjType this_type = stack_i[idx_i-2].get_type();
          ObObjType other_type = stack_i[idx_i-1].get_type();
          if (this_type == ObNullType || other_type == ObNullType)
          {
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
            //int cmp = this_type - other_type;
            int cmp = other_type - this_type;
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
            result.set_bool(cmp <= 0);
          }
          else
          {
            TBSYS_LOG(WARN, "unexpected branch, this_type=%d other_type=%d", this_type, other_type);
            err = OB_ERR_UNEXPECTED;
          }
        }
        idx_i -= 2;
      }
      return err;
    }

    static int lt_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
       //mod peiouya [DATE_TIME] 20150929:b
       // stack_i[idx_i-2].lt(stack_i[idx_i-1], result);
       // if (result.is_null())
        err = stack_i[idx_i-2].lt(stack_i[idx_i-1], result);
        if (err == OB_SUCCESS && result.is_null())
       //mod 20150929:e
        {
          ObObjType this_type = stack_i[idx_i-2].get_type();
          ObObjType other_type = stack_i[idx_i-1].get_type();
          if (this_type == ObNullType || other_type == ObNullType)
          {
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
            //int cmp = this_type - other_type;
            int cmp = other_type - this_type;
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
            result.set_bool(cmp < 0);
          }
          else
          {
            TBSYS_LOG(WARN, "unexpected branch, this_type=%d other_type=%d", this_type, other_type);
            err = OB_ERR_UNEXPECTED;
          }
        }
        idx_i -= 2;
      }
      return err;
    }

    static int eq_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        //mod peiouya [DATE_TIME] 20150929:b
        //stack_i[idx_i-2].eq(stack_i[idx_i-1], result);
        //if (result.is_null())
        err = stack_i[idx_i-2].eq(stack_i[idx_i-1], result);
        if (err == OB_SUCCESS && result.is_null())
        //mod 20150929:e
        {
          ObObjType this_type = stack_i[idx_i-2].get_type();
          ObObjType other_type = stack_i[idx_i-1].get_type();
          if (this_type == ObNullType || other_type == ObNullType)
          {
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
            //int cmp = this_type - other_type;
            int cmp = other_type - this_type;
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
            result.set_bool(cmp == 0);
          }
          else
          {
            TBSYS_LOG(WARN, "unexpected branch, this_type=%d other_type=%d", this_type, other_type);
            err = OB_ERR_UNEXPECTED;
          }
        }
        idx_i -= 2;
      }
      return err;
    }

    static int neq_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
       //mod peiouya [DATE_TIME] 20150929:b
       // stack_i[idx_i-2].ne(stack_i[idx_i-1], result);
       // if (result.is_null())
        err = stack_i[idx_i-2].ne(stack_i[idx_i-1], result);
        if (err == OB_SUCCESS && result.is_null())
       //mod 20150929:e
        {
          ObObjType this_type = stack_i[idx_i-2].get_type();
          ObObjType other_type = stack_i[idx_i-1].get_type();
          if (this_type == ObNullType || other_type == ObNullType)
          {
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
            //int cmp = this_type - other_type;
            int cmp = other_type - this_type;
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
            result.set_bool(cmp != 0);
          }
          else
          {
            TBSYS_LOG(WARN, "unexpected branch, this_type=%d other_type=%d", this_type, other_type);
            err = OB_ERR_UNEXPECTED;
          }
        }
        idx_i -= 2;
      }
      return err;
    }


    static int is_not_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].is_not(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }

    static int is_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].is(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }


    static int add_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        //mod peiouya [DATE_TIME] 20150906:b
        //stack_i[idx_i-2].add(stack_i[idx_i-1], result);
        err = stack_i[idx_i-2].add(stack_i[idx_i-1], result);
        //mod 20150906:e
        idx_i -= 2;
      }
      return err;
    }


    static int sub_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        //mod peiouya [DATE_TIME] 20150906:b
        //stack_i[idx_i-2].sub(stack_i[idx_i-1], result);
        err = stack_i[idx_i-2].sub(stack_i[idx_i-1], result);
        //mod 20150906:e
        idx_i -= 2;
      }
      return err;
    }


    static int mul_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        //mod peiouya [DATE_TIME] 20150906:b
        //stack_i[idx_i-2].mul(stack_i[idx_i-1], result);
        err = stack_i[idx_i-2].mul(stack_i[idx_i-1], result);
        //mod 20150906:e
        idx_i -= 2;
      }
      return err;
    }


    static int div_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        //mod peiouya [DATE_TIME] 20150906:b
        //stack_i[idx_i-2].div(stack_i[idx_i-1], result, true);
        err = stack_i[idx_i-2].div(stack_i[idx_i-1], result, true);
        //mod 20150906:e
        idx_i -= 2;
      }
      return err;
    }

    static int mod_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        //mod peiouya [DATE_TIME] 20150906:b
        //stack_i[idx_i-2].mod(stack_i[idx_i-1], result);
        err = stack_i[idx_i-2].mod(stack_i[idx_i-1], result);
        //mod 20150906:e
        idx_i -= 2;
      }
      return err;
    }


    static int and_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].land(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }

    static int or_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].lor(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }

    static int minus_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;

      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 1)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-1].negate(result);
        idx_i -= 1;
      }
      return err;
    }


    static int plus_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;

      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 1)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        // don't touch it whatever the type is
        result = stack_i[idx_i-1];
        idx_i -= 1;
      }
      return err;
    }



    static int not_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;

      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 1)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-1].lnot(result);
        idx_i -= 1;
      }
      return err;
    }

    static int like_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;

      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        err = stack_i[idx_i-2].old_like(stack_i[idx_i-1], result);
      }
      idx_i -= 2;
      return err;
    }

    //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
    static int cast_thin_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;
      int64_t dest_type = 0;
      if (OB_UNLIKELY(NULL == stack_i))
      {
          TBSYS_LOG(WARN, "stack_i=%p.", stack_i);
          err = OB_INVALID_ARGUMENT;
      }
      else if (OB_UNLIKELY(idx_i < 2))
      {
          TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
          err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = stack_i[idx_i-1].get_int(dest_type)))
      {
          TBSYS_LOG(WARN, "fail to get int value. actual type = %d. err=%d", stack_i[idx_i-1].get_type(), err);
      }
      else if (ObIntType  != dest_type)
      {
        err = OB_NOT_SUPPORTED;
        TBSYS_LOG(ERROR,"cast_thin_func only support cast to ObIntType. but cur cast to type:%ld",dest_type);
      }
      else // if (ObIntType  == dest_type)
      {
        ObStringBuf tmp_str_buf;  //only for input-para, actually not to use

        if(ObVarcharType == stack_i[idx_i-2].get_type ())
        {
           err = stack_i[idx_i-2].cast_to(ObPreciseDateTimeType, result, tmp_str_buf);
        }

        if(OB_SUCCESS == err && ObVarcharType == stack_i[idx_i-2].get_type ())
        {
          result.set_int (result.get_precise_datetime ());
        }
        else if (OB_SUCCESS != (err = stack_i[idx_i-2].cast_to(ObIntType, result, tmp_str_buf)))
        {
          TBSYS_LOG(DEBUG,"fail to cast type:%d--->ObIntType, but we ignore it, along with set result with null!",
                    stack_i[idx_i-2].get_type ());
          result.set_null ();
          err = OB_SUCCESS;
        }
        else
        {
          //nothing todo
        }
        idx_i -= 2;
      }

      return err;
    }
    //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e

    /*     初始化数学运算操作调用表 */
    op_call_func_t ObPostfixExpression::call_func[MAX_FUNC] = {
      /*   WARNING: 下面的顺序不可以调换，
       *   需要与ob_postfix_expression.h中的enum定义对应
       */
      add_func,
      sub_func,
      mul_func,
      div_func,
      mod_func,
      and_func,
      or_func,
      not_func,
      lt_func,
      le_func,
      eq_func,
      neq_func,
      ge_func,
      gt_func,
      like_func,
      is_func,
      is_not_func,
      minus_func,
      plus_func,
      cast_thin_func  //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608
    };

    ObPostfixExpression& ObPostfixExpression::operator=(const ObPostfixExpression &other)
    {
      int i = 0;
      if (&other != this)
      {
        // BUG: not deep copy implementation. WATCH OUT!
        for (i = 0; i < other.postfix_size_; i++)
        {
            expr_[i] = other.expr_[i];
        }
        postfix_size_ = other.postfix_size_;
      }
      return *this;
    }

    int ObPostfixExpression::set_expression(const ObObj *expr, oceanbase::common::ObStringBuf  & data_buf)
    {
      int err = OB_SUCCESS;
      int i = 0;
      postfix_size_ = 0;  // reset postfix size
      int64_t type = 0;

      if (NULL != expr)
      {
        while ((i < OB_MAX_COMPOSITE_SYMBOL_COUNT) && (OB_SUCCESS == err))
        {
          expr_[i] = expr[i];
          postfix_size_++;
          if (ObIntType != expr[i].get_type())
          {
            TBSYS_LOG(WARN, "unexpected postfix expression header element type. ObIntType expected"
                "[type:%d]", expr[i].get_type());
            break;
          }
          else if (OB_SUCCESS != (err = expr[i].get_int(type)))
          {
            TBSYS_LOG(WARN, "get int value failed:err[%d]", err);
            break;
          }
          else if (type == END)
          {
            i++;
            break;
          }
          else
          {
            /*FIXME: 若传入参数中没有END结束标志，可能导致访存出错 */
            if( expr[i + 1].need_deep_copy())
            {
              if(OB_SUCCESS != (err = data_buf.write_obj(expr[i+1], expr_ + i + 1)))
              {
                TBSYS_LOG(WARN,"fail to copy obj [err:%d]", err);
              }
            }
            else
            {
              expr_[i+1] = expr[i+1];
            }
            if(OB_SUCCESS == err)
            {
              postfix_size_++;
              i += 2;
            }
          }
        }
      }
      else
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "postfix expression is null!");
      }

      if (OB_SUCCESS == err && OB_MAX_COMPOSITE_SYMBOL_COUNT <= i)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "END symbol not found postfix expression. [i=%d]", i);
      }
      return err;
    }

    int ObPostfixExpression::set_expression(const ObString &expr, const ObScanParam &scan_param)
    {
      int err = OB_SUCCESS;

      UNUSED(scan_param);
      UNUSED(expr);
#if 0
      ObArrayHelper<ObExprObj> expr_array;
      expr_array.init(OB_MAX_COMPOSITE_SYMBOL_COUNT, expr_);
      err = parser_.parse(expr, expr_array);
      TBSYS_LOG(DEBUG, "parse done. expr_array len= %d", expr_array.get_array_index());

      {
        int i = 0;
        for (i = 0; i < expr_array.get_array_index(); i++)
        {
          expr_array.at(i)->dump();
        }
      }

      if (OB_SUCCESS == err)
      {
        postfix_size_ = expr_array.get_array_index();
      }
      else
      {
        TBSYS_LOG(WARN, "parse infix expression to postfix expression error");
      }

      if (OB_SUCCESS == err)
      {
        // TODO: decode expr_array using scan_param
        // 基本算法如下：
        //  1. 遍历expr_array数组，将其中类型为COLUMN_IDX的Obj读出
        //  2. 将obj中表示列名的值key_col_name读出，到hashmap中查找对应index
        //      scan_param.some_hash_map(key_col_name, val_index)
        //  3. 如果找到，则将obj的内容填充为val_index
        //     如果找不到，则报错返回
        //
      }
#endif
      return err;
    }

    //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
    //int ObPostfixExpression::set_expression(const ObString &expr,
    //    const hash::ObHashMap<ObString,int64_t,hash::NoPthreadDefendMode> & cname_to_idx_map,
    //    ObExpressionParser & parser, common::ObResultCode *rc)
    int ObPostfixExpression::set_expression(const ObString&                                                         expr,
                                            const hash::ObHashMap<ObString,int64_t,hash::NoPthreadDefendMode> &     cname_to_idx_map,
                                            ObExpressionParser &                                                    parser,
                                            common::ObResultCode                                                    *rc,
                                            bool                                                                    is_expire_info)
    //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
    {
      int err = OB_SUCCESS;
      int hash_ret = 0;
      int i = 0;
      int64_t type = 0;
      int64_t val_index = 0;
      ObString key_col_name;

      ObArrayHelper<ObObj> expr_array;
      expr_array.init(OB_MAX_COMPOSITE_SYMBOL_COUNT, expr_);
      //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
      //err = parser.parse(expr, expr_array);
      err = parser.parse(expr, expr_array, is_expire_info);
      //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
      TBSYS_LOG(DEBUG, "parse done. expr_array len= %ld", expr_array.get_array_index());

      if(0){
        for (i = 0; i < expr_array.get_array_index(); i++)
        {
          expr_array.at(i)->dump();
        }
      }

      if (OB_SUCCESS == err)
      {
        postfix_size_ = static_cast<int32_t>(expr_array.get_array_index());
      }
      else
      {
        TBSYS_LOG(WARN, "parse infix expression to postfix expression error");
      }

      if (OB_SUCCESS == err)
      {
        // TODO: decode expr_array using scan_param
        // 基本算法如下：
        //  1. 遍历expr_array数组，将其中类型为COLUMN_IDX的Obj读出
        //  2. 将obj中表示列名的值key_col_name读出，到hashmap中查找对应index
        //      scan_param.some_hash_map(key_col_name, val_index)
        //  3. 如果找到，则将obj的内容填充为val_index
        //     如果找不到，则报错返回

        i = 0;
        while(i < postfix_size_ - 1)
        {
          if (OB_SUCCESS != expr_array.at(i)->get_int(type))
          {
            TBSYS_LOG(WARN, "unexpected data type. int expected, but actual type is %d",
                expr_array.at(i)->get_type());
            err = OB_ERR_UNEXPECTED;
            break;
          }
          else
          {
            if (ObExpression::COLUMN_IDX == type)
            {
              if (OB_SUCCESS != expr_array.at(i+1)->get_varchar(key_col_name))
              {
                TBSYS_LOG(WARN, "unexpected data type. varchar expected, but actual type is %d",
                    expr_array.at(i+1)->get_type());
                err = OB_ERR_UNEXPECTED;
                break;
              }
              else
              {
                hash_ret = cname_to_idx_map.get(key_col_name,val_index);
                if (hash::HASH_EXIST != hash_ret)
                {
                  TBSYS_LOG(WARN,"get key(%.*s) failed", key_col_name.length(), key_col_name.ptr());
                  if(NULL != rc)
                  {
                    snprintf(rc->message_.ptr(), rc->message_.length(), "column name not declared before using [column_name:%.*s]",
                      key_col_name.length(), key_col_name.ptr());
                  }
                  err = OB_ERROR;
                  break;
                }
                else
                {
                  /* decode. quite simple, isn't it. */
                  expr_array.at(i+1)->set_int(val_index);
                  TBSYS_LOG(DEBUG, "succ decode.  key(%.*s) -> %ld",
                      key_col_name.length(), key_col_name.ptr(), val_index);
                }
              }
            }/* only column name needs to decode. other type is ignored */
            i += 2; /// skip <type, data> (2 objects as an element)
          }
        }
      }
      return err;
    }

    //将org_cell中的值代入到expr计算结果
    int ObPostfixExpression::calc(ObObj &composite_val,
        const ObCellArray & org_cells,
        const int64_t org_row_beg,
        const int64_t org_row_end
        )
    {
      int err = OB_SUCCESS;
      int64_t type = 0, value = 0;
      int idx = 0;
      ObExprObj result;

      UNUSED(org_row_end);

      int idx_i = 0;

      do{
        // TBSYS_LOG(DEBUG, "idx=%d, idx_i=%d, type=%d\n", idx, idx_i, type);
        // 获得数据类型:列id、数字、操作符、结束标记
        expr_[idx++].get_int(type);

        // expr_中以END符号表示结束
        if (END == type){
          stack_i[--idx_i].to(composite_val); // assign result
          if (idx_i != 0)
          {
            TBSYS_LOG(WARN,"calculation stack must be empty. check the code for bugs. idx_i=%d", idx_i);
            err = OB_ERROR;
          }
          break;
        }
        else if(type <= BEGIN_TYPE || type >= END_TYPE)
        {
          TBSYS_LOG(WARN,"unsupported operand type [type:%ld]", value);
          err = OB_INVALID_ARGUMENT;
          break;
        }

        if (idx_i < 0 || idx_i >= OB_MAX_COMPOSITE_SYMBOL_COUNT || idx > postfix_size_)
        {
          TBSYS_LOG(WARN,"calculation stack overflow [stack.index:%d] "
              "or run out of operand [operand.used:%d,operand.avaliable:%d]", idx_i, idx, postfix_size_);
          err = OB_ERR_UNEXPECTED;
          break;
        }
        if (OB_SUCCESS == err)
        {
          switch(type)
          {
            case COLUMN_IDX:
              if (OB_SUCCESS  != (err = expr_[idx++].get_int(value)))
              {
                TBSYS_LOG(WARN,"get_int error [err:%d]", err);
              }
              else if (org_row_beg + value > org_row_end)
              {
                TBSYS_LOG(WARN,"invalid row offset [org_row_beg:%ld, offset:%ld, org_row_end:%ld]",org_row_beg, value, org_row_end);
                err = OB_INVALID_ARGUMENT;
              }
              else
              {
                stack_i[idx_i++].assign(org_cells[org_row_beg + value].value_);
              }
              break;
            case CONST_OBJ:
              stack_i[idx_i++].assign(expr_[idx++]);
              break;
            case OP:
              // 根据OP的类型，从堆栈中弹出1个或多个操作数，进行计算
              if(OB_SUCCESS != (err = expr_[idx++].get_int(value)))
              {
                TBSYS_LOG(WARN,"get_int error [err:%d]", err);
              }
              else if (value <= MIN_FUNC || value >= MAX_FUNC)
              {
                TBSYS_LOG(WARN,"unsupported operator type [type:%ld]", value);
                err = OB_INVALID_ARGUMENT;
              }
              else
              {
                if (OB_SUCCESS != (err = (this->call_func[value])(stack_i, idx_i, result)))
                {
                  TBSYS_LOG(WARN,"call calculation function error [value:%ld, idx_i:%d, err:%d]",value, idx_i, err);
                }
                else
                {
                  stack_i[idx_i++] = result;
                }
              }
              break;
            default:
              err = OB_ERR_UNEXPECTED;
              TBSYS_LOG(WARN,"unexpected [type:%ld]", type);
              break;
          }
        }
        if (OB_SUCCESS != err)
        {
          break;
        }
      }while(true);

      return err;
    }

    DEFINE_SERIALIZE(ObPostfixExpression)
    {
      int i;
      int err = OB_SUCCESS;

      for (i = 0; i < postfix_size_; i++)
      {
        err = expr_[i].serialize(buf, buf_len, pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "serialization error. [pos:%ld, err:%d]", pos, err);
          break;
        }
      }
      return err;
    }

    DEFINE_DESERIALIZE(ObPostfixExpression)
    {
      ObObj obj;
      postfix_size_ = 0;
      int err = OB_SUCCESS;
      int64_t type = 0;
      while (postfix_size_ < OB_MAX_COMPOSITE_SYMBOL_COUNT)
      {

        /* 取type value pair中的type */
        err = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "deserialize object error");
          break;
        }
        expr_[postfix_size_++] = obj;

        /* 检查type, 如果type=END则表明为最后一个元素，break */
        if (ObIntType != obj.get_type())
        {
          err = OB_OBJ_TYPE_ERROR;
          TBSYS_LOG(WARN, "unexpected postfix expression header element type. ObIntType expected"
              "[type:%d]", obj.get_type());
          break;
        }
        else if (OB_SUCCESS != (err = obj.get_int(type)))
        {
          TBSYS_LOG(WARN, "get int value failed [err:%d]", err);
          break;
        }
        else if (type == END)
        {
//          TBSYS_LOG(INFO, "postfix expression deserialized");
          break;
        }

        /* 取type value pair中的value */
        if (OB_SUCCESS != (err = obj.deserialize(buf, data_len, pos)))
        {
          TBSYS_LOG(WARN, "deserialize object error");
          break;
        }
        expr_[postfix_size_++] = obj;
      }

      if (END != type)  
      {
        err = OB_ERR_UNEXPECTED; 
        TBSYS_LOG(ERROR, "fail to deserialize. postfix_size_=%d", postfix_size_);  
      }

      if (OB_SUCCESS != err)
      {
        postfix_size_ = 0;  // error found. rollback!
      }

      return err;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObPostfixExpression)
    {
      int i = 0;
      int64_t total_size = 0;

      while(i < postfix_size_)
      {
        total_size += expr_[i].get_serialize_size();
        i++;
      }

      return total_size;
    }


  } /* common */
} /* namespace */
