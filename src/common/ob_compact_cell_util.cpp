/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_compact_cell_util.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_compact_cell_util.h"

namespace oceanbase
{
  namespace common
  {
    /* 得到各种类型的空间占用大小 */
    static int get_type_size(enum ObObjType type)
    {
      int ret = 0;
      switch(type)
      {
      case ObFloatType:
        ret = sizeof(float);
        break;
      case ObDoubleType:
        ret = sizeof(double);
        break;
      case ObDateTimeType:
        ret = sizeof(ObDateTime);
        break;
      //add lijianqiang [INT_32] 20150930:b
      case ObInt32Type:
        ret = sizeof(int32_t);
        break;
      //add 20150930:e
      //add peiouya [DATE_TYPE] 20150831:b
      case ObDateType:
      case ObTimeType:
      //add 20150831:e
      case ObIntervalType:  //add peiouya [DATE_TIME] 20150908
      case ObPreciseDateTimeType:
        ret = sizeof(ObPreciseDateTime);
        break;
      case ObCreateTimeType:
        ret = sizeof(ObCreateTime);
        break;
      case ObModifyTimeType:
        ret = sizeof(ObModifyTime);
        break;
      default:
        TBSYS_LOG(WARN, "type not supported now:type[%d]", type);
        ret = -1;
      };
      return ret;
    }

    int get_compact_cell_max_size(enum ObObjType type)
    {
      int ret = sizeof(ObCellMeta);
      if(ObExtendType == type)
      {
      }
      else if(ObIntType == type)
      {
        ret += static_cast<int32_t>(sizeof(uint64_t));
        ret += 2; // column id length
      }
      else
      {
        int type_size = get_type_size(type);
        if(-1 == type_size)
        {
          ret = -1;
          TBSYS_LOG(WARN, "not support type");
        }
        else
        {
          ret += type_size;
          ret += 2; // column id length
        }
      }
      return ret;
    }

    int get_compact_cell_size(const ObObj &value)
    {
      int ret = sizeof(ObCellMeta);
      if(ObExtendType == value.get_type())
      {
      }
      else if(ObVarcharType == value.get_type())
      {
        ObString varchar_value;
        value.get_varchar(varchar_value);
        ret += static_cast<int32_t>(sizeof(uint16_t));
        ret += varchar_value.length();
        ret += 2; // column id length
      }
      else if(ObIntType == value.get_type())
      {
        int64_t int_value = 0;
        value.get_int(int_value);
        ret += get_int_byte(int_value);
        ret += 2; // column id length
      }
      else
      {
        int type_size = get_type_size(value.get_type());
        if(-1 == type_size)
        {
          ret = -1;
          TBSYS_LOG(WARN, "not support type");
        }
        else
        {
          ret += type_size;
          ret += 2; // column id length
        }
      }
      return ret;
    }

    int get_int_byte(int64_t int_value)
    {
      int ret = 0;
      if( int_value >= -128 && int_value <= 127 )
      {
        ret = 1;
      }
      else if( int_value >= (int16_t)0x8000 && int_value <= (int16_t)0x7FFF )
      {
        ret = 2;
      }
      else if( int_value >= (int32_t)0x80000000 &&
        int_value <= (int32_t)0x7FFFFFFF)
      {
        ret = 4;
      }
      else
      {
        ret = 8;
      }
      return ret;
    }
  }
}
