/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_obj_type.cpp
 *
 * Authors:
 *   zhidong<xielun.szd@alipay.com>
 *
 */

#include "ob_define.h"
#include "ob_obj_type.h"

namespace oceanbase
{
  namespace common
  {
    const char* ob_obj_type_str(ObObjType type)
    {
      const char* ret = "unknown_type";
      switch (type)
      {
        case ObNullType:
          ret = "null";
          break;
        case ObIntType:
          ret = "int";
          break;
        //add lijianqiang [INT_32] 20151008:b
        case ObInt32Type:
          ret = "int32";
          break;
        //add 20151008:e
        case ObFloatType:
          ret = "float";
          break;
        case ObDoubleType:
          ret = "double";
          break;
        case ObDateTimeType:
          ret = "datetime";
          break;
        case ObPreciseDateTimeType:
          ret = "timestamp";
          break;
        case ObVarcharType:
          ret = "varchar";
          break;
        case ObSeqType:
          ret = "seq";
          break;
        case ObCreateTimeType:
          ret = "createtime";
          break;
        case ObModifyTimeType:
          ret = "modifytime";
          break;
        case ObExtendType:
          ret = "extend";
          break;
        case ObBoolType:
          ret = "bool";
          break;
        case ObDecimalType:
          ret = "decimal";
          break;
        //add peiouya [DATE_TIME] 20150906:b
        case ObDateType:
          ret = "date";
          break;
        case ObTimeType:
          ret = "time";
          break;
        //add 20150906:e
        //add peiouya [DATE_TIME] 20150909:b
        case ObIntervalType:
          ret = "interval";
          break;
        //add 20150909:e
        default:
          break;
      }
      return ret;
    }

    int64_t ob_obj_type_size(ObObjType type)
    {
      int64_t size = 0;
      switch (type)
      {
        case ObIntType:
          size = sizeof(int64_t);
          break;
        //add lijianqiang [INT_32] 20151008:b
        case ObInt32Type:
          size = sizeof(int32_t);
          break;
        //add 20151008:e
        case ObFloatType:
          size = sizeof(float);
          break;
        case ObDoubleType:
          size = sizeof(double);
          break;
        case ObDateTimeType:
          size = sizeof(ObDateTime);
          break;
        //add peiouya [DATE_TIME] 20150906:b
        case ObDateType:
        case ObTimeType:
        //add 20150906:e
        case ObIntervalType: //add peiouya [DATE_TIME] 20150909
        case ObPreciseDateTimeType:
          size = sizeof(ObPreciseDateTime);
          break;
        case ObCreateTimeType:
          size = sizeof(ObCreateTime);
          break;
        case ObModifyTimeType:
          size = sizeof(ObModifyTime);
          break;
        case ObBoolType:
          size = sizeof(bool);
          break;
        default:
          break;
      }
      return size;
    }
  } // common
} // oceanbase
