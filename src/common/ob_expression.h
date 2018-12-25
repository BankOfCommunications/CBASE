/*
 * (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_expression.h is for what ...
 *
 * Version: $id: ob_expression.h, v 0.1 7/29/2011 14:39 xiaochu Exp $
 *
 * Authors:
 *   xiaochu <xiaochu.yh@taobao.com>
 *     - 表达式求值中用到的常量
 *
 */



#ifndef OCEANBASE_COMMON_OB_EXPRESSION_H_
#define OCEANBASE_COMMON_OB_EXPRESSION_H_

namespace oceanbase
{
  namespace common
  {
    namespace ObExpression
    {
      //public:      
      // WARN: 顺序不可以更换
      enum {
        MIN_FUNC = -1,

        ADD_FUNC = 0,
        SUB_FUNC ,
        MUL_FUNC ,
        DIV_FUNC ,

        MOD_FUNC, 

        /* logical operator */
        AND_FUNC ,  /* note: equals to && */
        OR_FUNC  ,  /* note: equals to || */
        NOT_FUNC,   /* noet: equals to !  */

        /* compare operator */
        LT_FUNC  , /* = 8 */
        LE_FUNC  ,
        EQ_FUNC  ,
        NE_FUNC  ,
        GE_FUNC  , /* = 12 */
        GT_FUNC  ,
        LIKE_FUNC,
        IS_FUNC,
        IS_NOT_FUNC, 
        MINUS_FUNC, /* = 17 */
        PLUS_FUNC,
        CAST_THIN_FUNC,  /*add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608*/

        MAX_FUNC,

        LEFT_PARENTHESE,
        RIGHT_PARENTHESE

      };

      enum {
        BEGIN_TYPE = 0,
        COLUMN_IDX,
        CONST_OBJ,  /* replace following 3 type */
        /*
           NUMBER,
           STRING,
           DATETIME,
           */        
        OP,
        CUR_TIME_OP,
        END,
        END_TYPE
      };
    };

  } // namespace commom
}// namespace oceanbae

#endif

