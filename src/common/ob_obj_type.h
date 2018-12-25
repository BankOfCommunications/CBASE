/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_obj_type.h
 *
 * Authors:
 *   zhidong<xielun.szd@alipay.com>
 *
 */

#ifndef OCEANBASE_COMMON_OB_OBJECT_TYPE_H_
#define OCEANBASE_COMMON_OB_OBJECT_TYPE_H_

namespace oceanbase
{
  namespace common
  {
    // Obj类型只能增加，不能删除，顺序也不能变，见ob_obj_cast.h
    enum ObObjType
    {
      ObMinType = -1,

      ObNullType,   // 空类型
      ObIntType,
      ObFloatType,              // @deprecated

      ObDoubleType,             // @deprecated
      ObDateTimeType,           // @deprecated
      ObPreciseDateTimeType,    // =5

      ObVarcharType,
      ObSeqType,
      ObCreateTimeType,

      ObModifyTimeType,
      ObExtendType,
      ObBoolType,

      ObDecimalType,            // aka numeric
      //add peiouya [DATE_TIME] peiouya 20150831:e
      ObDateType,
      ObTimeType,
      //this type only use for date and time calculate, is temp type. its measure unit is microsecond.
      ObIntervalType,
      //add 20150831:e
      //add lijianqiang [INT32] 20150929:b
      ObInt32Type,
      //add 20150929:e
      ObMaxType,
    };
    // print obj type string
    const char* ob_obj_type_str(ObObjType type);
    // get obj type size for fixed length type
    int64_t ob_obj_type_size(ObObjType type);
  }
}

#endif //OCEANBASE_COMMON_OB_OBJECT_TYPE_H_
