/*
 * (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_composite_column.h is for what ...
 *
 * Version: $id: ob_composite_column.h, v 0.1 7/29/2011 14:39 xiaochu Exp $
 *
 * Authors:
 *   xiaochu <xiaochu.yh@taobao.com>
 *     - 复合列，可用于复合列求值和复合条件过滤
 *
 */


#ifndef OCEANBASE_COMMON_OB_COMPOSITE_COLUMN_H_
#define OCEANBASE_COMMON_OB_COMPOSITE_COLUMN_H_
#include "ob_string.h"
#include "ob_postfix_expression.h"
#include "ob_infix_expression.h"

namespace oceanbase
{
  namespace common
  {
    class ObScanParam;
    class ObCellArray;
    class ObPostfixExpression;
    class ObCompositeColumn
    {
      public:
        ObCompositeColumn();
        ~ObCompositeColumn(){};

        /* 用于client */
        //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
        //int set_expression(const ObString &expr,
        //                   const ObString &as_column_name);
        int set_expression(const ObString&      expr,
                           const ObString&      as_column_name,
                           bool                 is_expire_info  = false);
        //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
        /* 用于cs */
        int set_expression(const ObObj *expr, ObStringBuf  & data_buf);

        /* 计算本复合列的值 */
        int calc_composite_val(ObObj &composite_val,
            const ObCellArray & org_cells,
            const int64_t org_row_beg,
            const int64_t org_row_end
            );

        inline ObString get_as_column_name()const
        {
          return as_column_name_;
        }

      inline ObString get_infix_expr()const
      {
        ObString str;
        if (USE_INFIX  == state)
        {
          str =  infix_.get_expression();
        }
        return str;
      }

      //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
      inline bool is_expire_info()const { return infix_.is_expire_info ();}
      //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e


      inline const ObObj * get_postfix_expr(void)const
      {
        const ObObj *res = NULL;
        if(USE_POSTFIX == state)
        {
          res = postfix_.get_expression();
        }
        return res;
      }

      inline int to_str(char *buf, const int64_t buf_size, int64_t &pos)
      {
        int err = OB_SUCCESS;
        if ((NULL == buf) || (0 >= buf_size) || (pos >= buf_size))
        {
          TBSYS_LOG(WARN,"argument error [buf:%p,buf_size:%ld, pos:%ld]", buf, buf_size, pos);
          err = OB_INVALID_ARGUMENT;
        }
        if ((OB_SUCCESS == err) && (pos < buf_size))
        {
          int64_t used_len = 0;
          if(USE_INFIX == state)
          {
            used_len = snprintf(buf+pos, (buf_size-pos>0)?(buf_size-pos):0, "%.*s AS %.*s,",
              infix_.get_expression().length(), infix_.get_expression().ptr(),
              as_column_name_.length(), as_column_name_.ptr());
          }
          else
          {
            used_len = snprintf(buf+pos, (buf_size-pos>0)?(buf_size-pos):0, "COMPLEX_COLUMN, ");
          }
          if(used_len > 0)
          {
            pos += used_len;
          }
        }
        return err;
      }


        //
        NEED_SERIALIZE_AND_DESERIALIZE;

      private:
        enum
        {
          NOT_INIT = 0,
          USE_INFIX,
          USE_POSTFIX
        }state;
        ObInfixExpression infix_;
        ObPostfixExpression postfix_;
        ObString as_column_name_;
    }; // class ObCompositeColumn
  } // namespace commom
}// namespace oceanbae

#endif
