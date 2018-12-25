/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_item_type_str.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ITEM_TYPE_STR_H
#define _OB_ITEM_TYPE_STR_H 1
#include "ob_item_type.h"

namespace oceanbase
{
  namespace sql
  {
    inline const char* ob_aggr_func_str(ObItemType aggr_func)
    {
      const char* ret = "UNKNOWN_AGGR";
      switch(aggr_func)
      {
        case T_FUN_MAX:
          ret = "MAX";
          break;
        case T_FUN_MIN:
          ret = "MIN";
          break;
        case T_FUN_SUM:
          ret = "SUM";
          break;
        case T_FUN_COUNT:
          ret = "COUNT";
          break;
        //add liumz, [ROW_NUMBER]20150824
        case T_FUN_ROW_NUMBER:
          ret = "ROW_NUMBER";
          break;
        //add:e
        case T_FUN_AVG:
          ret = "AVG";
          break;
        //add gaojt [ListAgg][JHOBv0.1]20150104:b
        case T_FUN_LISTAGG:
          ret = "LISTAGG";
          break;
        //add 20150104:e
        default:
          break;
      }
      return ret;
    }

    inline const char* ob_op_func_str(ObItemType item_type)
    {
      static const char* op_func_str_st[] =
        {
          "NEG",
          "POS",
          "ADD",
          "MINUS",
          "MUL",
          "DIV",
          "POW",
          "REM",
          "MOD",
          "EQ",
          "LE",
          "LT",
          "GE",
          "GT",
          "NE",
          "IS",
          "IS_NOT",
          "BTW",
          "NOT_BTW",
          "LIKE",
          "NOT_LIKE",
          "NOT",
          "AND",
          "OR",
          "IN",
          "NOT_IN",
          "ARG_CASE",
          "CASE",
          "ROW",
          "EXISTS",
          "CNN",
          "SYS",
          "LEFT_PARAM_END",
          "CASE_BEGIN",
          "CASE_WHEN",
          "CASE_THEN",
          "CASE_ELSE",
          "CASE_BREAK",
          "CASE_END",
        };

      const char* ret = "UNKNOWN_OP";
      if (item_type > T_MIN_OP
          && item_type < T_MAX_OP)
      {
        ret = op_func_str_st[item_type - T_OP_NEG];
      }
      return ret;
    }


  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_ITEM_TYPE_STR_H */
