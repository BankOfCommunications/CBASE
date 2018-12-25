/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_expr_obj.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include <regex.h>
#include <cmath>
#include "utility.h"
#include "ob_expr_obj.h"
#include "ob_string_buf.h"
#include "ob_string_search.h"
#include "ob_malloc.h"
//mod wuna [datetime func] 20150901:b
/*Exp:需要使用cpp中的静态函数"varchar_timestamp()"*/
//#include "ob_obj_cast.h"*/
#include "ob_obj_cast.cpp"
//mod 20150901:e
using namespace oceanbase::common;

void ObExprObj::assign(const ObObj &obj)
{
  type_ = obj.get_type();
  switch(obj.get_type())
  {
    case ObNullType:
      break;
    case ObIntType:
      obj.get_int(v_.int_);
      break;
    //add lijianqiang [INT_32] 20150930:b
    case ObInt32Type:
      obj.get_int32(v_.int32_);
      break;
    //add 20150930:e
    case ObDateTimeType:
      obj.get_datetime(v_.datetime_);
      break;
    case ObPreciseDateTimeType:
      obj.get_precise_datetime(v_.precisedatetime_);
      break;
    case ObVarcharType:
      obj.get_varchar(varchar_);
      break;
    case ObCreateTimeType:
      obj.get_createtime(v_.createtime_);
      break;
    case ObModifyTimeType:
      obj.get_modifytime(v_.modifytime_);
      break;
    case ObBoolType:
      obj.get_bool(v_.bool_);
      break;
    case ObDecimalType:
      //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
      //obj.get_decimal(num_);
      //break;                 old code
    {
      int ret=OB_SUCCESS;
      ObDecimal od;
      //uint32_t ori_pre=MAX_DECIMAL_DIGIT;
      obj.get_decimal(varchar_);
      if(OB_SUCCESS!=(ret=od.from(varchar_.ptr(),varchar_.length())))
      {
          TBSYS_LOG(WARN, "failed to do from in ObExprObj::assign, err=%d", ret);
      }
      //ori_pre=od.get_precision();
       if(OB_SUCCESS!=(ret=od.modify_value(obj.get_precision(),obj.get_scale())))
	   {
			TBSYS_LOG(WARN, "failed to do modify_value in ObExprObj::assign, od=%.*s,p=%d,s=%d",varchar_.length(),varchar_.ptr() ,obj.get_precision(),obj.get_scale());
	   }
      decimal_=od;
      //decimal_.set_precision(ori_pre);
      break;
    }
      //modify:e
    case ObFloatType:
      obj.get_float(v_.float_);
      break;
    case ObDoubleType:
      obj.get_double(v_.double_);
      break;
    case ObExtendType:
      obj.get_ext(v_.ext_);
      break;
    //add peiouya [DATE_TYPE] 20150831:b
    case ObDateType:
      obj.get_date(v_.precisedatetime_);
      break;
    case ObTimeType:
      obj.get_time(v_.precisedatetime_);
      break;
    //add 20150831:e
    //add peiouya [DATE_TIME] 20150908:b
    case ObIntervalType:
      obj.get_interval(v_.precisedatetime_);
      break;
    //add 20150908:e
    default:
      TBSYS_LOG(ERROR, "invalid value type=%d", obj.get_type());
      break;
  }
}

int ObExprObj::to(ObObj &obj) const
{
  int ret = OB_SUCCESS;
  switch(get_type())
  {
    case ObNullType:
      obj.set_null();
      break;
    case ObIntType:
      obj.set_int(v_.int_);
      break;
    //add lijianqiang [INT_32] 20150930:b
    case ObInt32Type:
      obj.set_int32(v_.int32_);
      break;
    //add 20150930:e
    case ObDateTimeType:
      obj.set_datetime(v_.datetime_);
      break;
    //add peiouya [DATE_TIME] 20150831:e
    case ObDateType:
      obj.set_date(v_.precisedatetime_);
      break;
    case ObTimeType:
      obj.set_time(v_.precisedatetime_);
      break;
    //add 20150831:e
    //add peiouya [DATE_TIME] 20150908:b
    case ObIntervalType:
      obj.set_interval (v_.precisedatetime_);
      break;
    //add 20150908:e
    case ObPreciseDateTimeType:
      obj.set_precise_datetime(v_.precisedatetime_);
      break;
    case ObVarcharType:
      obj.set_varchar(varchar_);
      break;
    case ObCreateTimeType:
      obj.set_createtime(v_.createtime_);
      break;
    case ObModifyTimeType:
      obj.set_modifytime(v_.modifytime_);
      break;
    case ObBoolType:
      obj.set_bool(v_.bool_);
      break;
    case ObDecimalType:
      //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
      //ret = obj.set_decimal(num_);
      //break;         old code
    {
      ret = obj.set_decimal(varchar_, decimal_.get_precision(), decimal_.get_scale(),decimal_.get_vscale());
      if(ret!=OB_SUCCESS)
            TBSYS_LOG(ERROR, "failed to do set_decimal,value=%.*s", varchar_.length(),varchar_.ptr());
      break;
    }
      //modify:e
    case ObFloatType:
      obj.set_float(v_.float_);
      break;
    case ObDoubleType:
      obj.set_double(v_.double_);
      break;
    case ObExtendType:
      obj.set_ext(v_.ext_);
      break;
    default:
      TBSYS_LOG(ERROR, "invalid value type=%d", get_type());
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

//add peiouya [DATE_TIME] 20150906:b
bool ObExprObj::is_datetime_for_out() const
{   
  return is_datetime();
}
    
inline bool ObExprObj::is_datetime(const ObObjType type) const
{
  return ((type == ObDateTimeType)
          || (type == ObPreciseDateTimeType)
          || (type == ObCreateTimeType)
          || (type == ObModifyTimeType)
          || (type == ObDateType)
          || (type == ObTimeType)
          );
}


inline bool ObExprObj::is_datetime() const
{
  return ((type_ == ObDateTimeType)
          || (type_ == ObPreciseDateTimeType)
          || (type_ == ObCreateTimeType)
          || (type_ == ObModifyTimeType)
          || (type_ == ObDateType)
          || (type_ == ObTimeType));
}
//add 20150906:e

bool ObExprObj::can_compare(const ObExprObj & other) const
{
  bool ret = false;
  if ((get_type() == ObNullType) || (other.get_type() == ObNullType)
      || (get_type() == other.get_type()) || (is_datetime() && other.is_datetime())
      || (is_numeric() && other.is_numeric()))
  {
    ret = true;
  }
  return ret;
}

inline bool ObExprObj::is_numeric() const
{
  return ((type_ == ObIntType)
          || (type_ == ObDecimalType)
          || (type_ == ObFloatType)
          || (type_ == ObDoubleType)
          || (type_ == ObBoolType)
          //add yushengjuan [INT_32] 20151015:b
          || (type_ == ObInt32Type)
          //add 20151015:e
          );
}

inline int ObExprObj::get_timestamp(int64_t & timestamp) const
{
  int ret = OB_SUCCESS;
  switch(type_)
  {
    case ObDateTimeType:
      timestamp = v_.datetime_ * 1000 * 1000L;
      break;
    //add peiouya [DATE_TIME] 20150831:e
    case ObDateType:
    case ObTimeType:
    //add 20150831:e
    case ObIntervalType:  //add peiouya [DATE_TIME] 20150908
    case ObPreciseDateTimeType:
      timestamp = v_.precisedatetime_;
      break;
    case ObModifyTimeType:
      timestamp = v_.modifytime_;
      break;
    case ObCreateTimeType:
      timestamp = v_.createtime_;
      break;
    default:
      TBSYS_LOG(ERROR, "unexpected branch");
      ret = OB_OBJ_TYPE_ERROR;
  }
  return ret;
}

int ObExprObj::compare_same_type(const ObExprObj &other) const
{
  int cmp = 0;
  OB_ASSERT(get_type() == other.get_type()
            || (is_datetime() && other.is_datetime()));
  switch(get_type())
  {
    case ObIntType:
      if (this->v_.int_ < other.v_.int_)
      {
        cmp = -1;
      }
      else if (this->v_.int_ == other.v_.int_)
      {
        cmp = 0;
      }
      else
      {
        cmp = 1;
      }
      break;
    //add lijianqiang [INT_32] 20150930:b
    case ObInt32Type:
      if (this->v_.int32_ < other.v_.int32_)
      {
        cmp = -1;
      }
      else if (this->v_.int32_ == other.v_.int32_)
      {
        cmp = 0;
      }
      else
      {
        cmp = 1;
      }
      break;
    //add 20150930:e
    case ObDecimalType:
      //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
      {
      ObDecimal cmp1,cmp2,cmp3,cmp4;
      ObString str1,str2;
      str1=get_varchar();
      str2=other.get_varchar();
      int ret1=OB_SUCCESS;
      int ret2=OB_SUCCESS;
	  int ret3=OB_SUCCESS;
	  int ret4=OB_SUCCESS;
      ret1=cmp1.from(str1.ptr(),str1.length());
      ret2=cmp2.from(str2.ptr(),str2.length());
      if(ret1==OB_SUCCESS&&ret2==OB_SUCCESS)
      {
			if(OB_SUCCESS!=(ret3=cmp1.modify_value(this->decimal_.get_precision(),this->decimal_.get_scale())))
			{
				TBSYS_LOG(ERROR, "failed to do modify_value in compare_same_type, p=%d,s=%d,cmp1=%.*s", this->decimal_.get_precision(),this->decimal_.get_scale(),str1.length(),str1.ptr());
			}
			if(OB_SUCCESS!=(ret4=cmp2.modify_value(other.decimal_.get_precision(),other.decimal_.get_scale())))
			{
				TBSYS_LOG(ERROR, "failed to do modify_value in compare_same_type, p=%d,s=%d,cmp2=%.*s",other.decimal_.get_precision(),other.decimal_.get_scale(),str2.length(),str2.ptr());
			}
			cmp =cmp1.compare(cmp2);
      }
      else
      {
            cmp3=get_decimal();
            cmp4=other.get_decimal();
            cmp =cmp3.compare(cmp4);
      }
      break;
      //modify e
      }
    case ObVarcharType:
      cmp = this->varchar_.compare(other.varchar_);
      break;
    case ObFloatType:
    {
      bool float_eq = fabsf(v_.float_ - other.v_.float_) < FLOAT_EPSINON;
      if (float_eq)
      {
        cmp = 0;
      }
      else if (this->v_.float_ < other.v_.float_)
      {
        cmp = -1;
      }
      else
      {
        cmp = 1;
      }
      break;
    }
    case ObDoubleType:
    {
      bool double_eq = fabs(v_.double_ - other.v_.double_) < DOUBLE_EPSINON;
      if (double_eq)
      {
        cmp = 0;
      }
      else if (this->v_.double_ < other.v_.double_)
      {
        cmp = -1;
      }
      else
      {
        cmp = 1;
      }
      break;
    }
    //add peiouya [DATE_TIME] 20150831:b
    case ObDateType:
    case ObTimeType:
    //add 20150831:e
    case ObDateTimeType:
    case ObPreciseDateTimeType:
    case ObCreateTimeType:
    case ObModifyTimeType:
    {
      int64_t ts1 = 0;
      int64_t ts2 = 0;
      get_timestamp(ts1);
      other.get_timestamp(ts2);
      if (ts1 < ts2)
      {
        cmp = -1;
      }
      else if (ts1 == ts2)
      {
        cmp = 0;
      }
      else
      {
        cmp = 1;
      }
      break;
    }
    case ObBoolType:
      cmp = this->v_.bool_ - other.v_.bool_;
      break;
    default:
      TBSYS_LOG(ERROR, "invalid type=%d", get_type());
      break;
  }
  return cmp;
}

//mod peiouya [DATE_TIME] 20150907:b
// @return ObMaxType when not supporting
// do not promote where comparing with the same type
// static ObObjType COMPARE_TYPE_PROMOTION[ObMaxType][ObMaxType] =
// {
  // {
    // /*Null compare with XXX*/
    // ObNullType, ObNullType, ObNullType,
    // ObNullType, ObNullType, ObNullType,
    // ObNullType, ObNullType, ObNullType,
    // ObNullType, ObNullType, ObNullType,
    // ObNullType
  // }
  // ,
  // {
    // /*Int compare with XXX*/
    // ObNullType/*Null*/, ObIntType/*Int*/, ObFloatType,
    // ObDoubleType, ObIntType, ObIntType,
    // ObIntType, ObMaxType/*Seq, not_support*/, ObIntType,
    // ObIntType, ObMaxType/*Extend*/, ObBoolType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*Float compare with XXX*/
    // ObNullType/*Null*/, ObFloatType, ObFloatType,
    // ObDoubleType, ObFloatType, ObFloatType,
    // ObFloatType, ObMaxType/*Seq*/, ObFloatType,
    // ObFloatType, ObMaxType/*Extend*/, ObBoolType,
    // ObDoubleType
  // }
  // ,
  // {
    // /*Double compare with XXX*/
    // ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    // ObDoubleType, ObDoubleType, ObDoubleType,
    // ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    // ObDoubleType, ObMaxType/*Extend*/, ObBoolType,
    // ObDoubleType
  // }
  // ,
  // {
    // /*DateTime compare with XXX*/
    // ObNullType/*Null*/, ObIntType, ObFloatType,
    // ObDoubleType, ObDateTimeType, ObPreciseDateTimeType,
    // ObDateTimeType, ObMaxType/*Seq*/, ObPreciseDateTimeType,
    // ObPreciseDateTimeType, ObMaxType/*Extend*/, ObBoolType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*PreciseDateTime compare with XXX*/
    // ObNullType/*Null*/, ObIntType, ObFloatType,
    // ObDoubleType, ObPreciseDateTimeType, ObPreciseDateTimeType,
    // ObPreciseDateTimeType, ObMaxType/*Seq*/, ObPreciseDateTimeType,
    // ObPreciseDateTimeType, ObMaxType/*Extend*/, ObBoolType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*Varchar compare with XXX*/
    // ObNullType/*Null*/, ObIntType, ObFloatType,
    // ObDoubleType, ObDateTimeType, ObPreciseDateTimeType,
    // ObVarcharType, ObMaxType/*Seq*/, ObCreateTimeType,
    // ObModifyTimeType, ObMaxType/*Extend*/, ObBoolType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*Seq compare with XXX*/
    // ObNullType, ObMaxType, ObMaxType,
    // ObMaxType, ObMaxType, ObMaxType,
    // ObMaxType, ObMaxType, ObMaxType,
    // ObMaxType, ObMaxType, ObMaxType,
    // ObMaxType
  // }
  // ,
  // {
    // /*CreateTime compare with XXX*/
    // ObNullType/*Null*/, ObIntType, ObFloatType,
    // ObDoubleType, ObPreciseDateTimeType, ObPreciseDateTimeType,
    // ObCreateTimeType, ObMaxType/*Seq*/, ObCreateTimeType,
    // ObPreciseDateTimeType, ObMaxType/*Extend*/, ObBoolType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*ModifyTime compare with XXX*/
    // ObNullType/*Null*/, ObIntType, ObFloatType,
    // ObDoubleType, ObPreciseDateTimeType, ObPreciseDateTimeType,
    // ObModifyTimeType, ObMaxType/*Seq*/, ObPreciseDateTimeType,
    // ObModifyTimeType, ObMaxType/*Extend*/, ObBoolType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*Extend compare with XXX*/
    // ObNullType, ObMaxType, ObMaxType,
    // ObMaxType, ObMaxType, ObMaxType,
    // ObMaxType, ObMaxType, ObMaxType,
    // ObMaxType, ObMaxType, ObMaxType,
    // ObMaxType
  // }
  // ,
  // {
    // /*Bool compare with XXX*/
    // ObNullType/*Null*/, ObBoolType, ObBoolType,
    // ObBoolType, ObBoolType, ObBoolType,
    // ObBoolType, ObMaxType/*Seq*/, ObBoolType,
    // ObBoolType, ObMaxType/*Extend*/, ObBoolType,
    // ObBoolType
  // }
  // ,
  // {
    // /*Decimal compare with XXX*/
    // ObNullType/*Null*/, ObDecimalType, ObDoubleType,
    // ObDoubleType, ObDecimalType, ObDecimalType,
    // ObDecimalType, ObMaxType/*Seq*/, ObDecimalType,
    // ObDecimalType, ObMaxType/*Extend*/, ObBoolType,
    // ObDecimalType
  // }
// };

static ObObjType COMPARE_TYPE_PROMOTION[ObMaxType][ObMaxType] =
{
  {
    /*Null compare with XXX*/
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType //add peiouya [DATE_TIME] 20150908  add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Int compare with XXX*/
    ObNullType/*Null*/, ObIntType/*Int*/, ObFloatType,
    ObDoubleType, ObMaxType, ObMaxType,
    /* mod [in post-expression optimization]@20151129 ObIntType,*/ ObDecimalType, ObMaxType/*Seq, not_support*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObBoolType,
    ObDecimalType, ObMaxType, ObMaxType,
    ObMaxType, ObIntType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Float compare with XXX*/
    ObNullType/*Null*/, ObFloatType, ObFloatType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObFloatType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObBoolType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObMaxType, ObFloatType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Double compare with XXX*/
    ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObDoubleType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObBoolType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObMaxType, ObDoubleType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*DateTime compare with XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObDateTimeType, ObPreciseDateTimeType,
    ObDateTimeType, ObMaxType/*Seq*/, ObPreciseDateTimeType,
    ObPreciseDateTimeType, ObMaxType/*Extend*/, ObMaxType,
    ObMaxType, ObPreciseDateTimeType, ObMaxType,
    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*PreciseDateTime compare with XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObPreciseDateTimeType, ObPreciseDateTimeType,
    ObPreciseDateTimeType, ObMaxType/*Seq*/, ObPreciseDateTimeType,
    ObPreciseDateTimeType, ObMaxType/*Extend*/, ObMaxType,
    ObMaxType, ObPreciseDateTimeType, ObMaxType,
    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Varchar compare with XXX*/
    ObNullType/*Null*/, /* mod [in post-expression optimization]@20151129 ObIntType,*/ ObDecimalType, ObFloatType,
    ObDoubleType, ObDateTimeType, ObPreciseDateTimeType,
    ObVarcharType, ObMaxType/*Seq*/, ObCreateTimeType,
    ObModifyTimeType, ObMaxType/*Extend*/, ObBoolType,
    ObDecimalType, ObPreciseDateTimeType, ObTimeType,
    ObMaxType, /* mod [in post-expression optimization]@20151129 ObInt32Type,*/ ObDecimalType//add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Seq compare with XXX*/
    ObNullType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*CreateTime compare with XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObPreciseDateTimeType, ObPreciseDateTimeType,
    ObCreateTimeType, ObMaxType/*Seq*/, ObCreateTimeType,
    ObPreciseDateTimeType, ObMaxType/*Extend*/, ObMaxType,
    ObMaxType, ObPreciseDateTimeType, ObMaxType,
    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*ModifyTime compare with XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObPreciseDateTimeType, ObPreciseDateTimeType,
    ObModifyTimeType, ObMaxType/*Seq*/, ObPreciseDateTimeType,
    ObModifyTimeType, ObMaxType/*Extend*/, ObMaxType,
    ObMaxType, ObPreciseDateTimeType, ObMaxType,
    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Extend compare with XXX*/
    ObNullType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Bool compare with XXX*/
    ObNullType/*Null*/, ObBoolType, ObBoolType,
    ObBoolType, ObMaxType, ObMaxType,
    ObBoolType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObBoolType,
    ObBoolType, ObMaxType, ObMaxType,
    ObMaxType, ObBoolType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Decimal compare with XXX*/
    ObNullType/*Null*/, ObDecimalType, ObDoubleType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObDecimalType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObBoolType,
    ObDecimalType, ObMaxType, ObMaxType,
    ObMaxType, ObDecimalType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  //add peiouya [DATE_TIME] 20150906:b
  ,
  {
    /*Date compare with XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObPreciseDateTimeType, ObPreciseDateTimeType,
    ObPreciseDateTimeType, ObMaxType/*Seq*/, ObPreciseDateTimeType,
    ObPreciseDateTimeType, ObMaxType/*Extend*/, ObMaxType,
    ObMaxType, ObDateType, ObMaxType,
    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Time compare with XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObTimeType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObMaxType,
    ObMaxType, ObMaxType, ObTimeType,
    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  //add peiouya [DATE_TIME] 20150908:b
  ,
  {
    /*Interval compare with XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType //add yushengjuan [INT_32] 20151012
  }
  //add 20150908:e
  //add yushengjuan [INT_32] 20151012:b
  ,
  {
    /*Int32 compare with XXX*/
    ObNullType, ObIntType, ObFloatType,
    ObDoubleType, ObMaxType, ObMaxType,
    /* mod [in post-expression optimization]@20151129 ObInt32Type,*/ ObDecimalType, ObMaxType/*Seq, not_support*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObBoolType,
    ObDecimalType, ObMaxType, ObMaxType,
    ObMaxType, ObInt32Type
  }
  //add 20151012:e
};
//mod 20150906:e
struct IntegrityChecker1
{
  IntegrityChecker1()
  {
    for (ObObjType t1 = static_cast<ObObjType>(ObMinType+1);
         t1 < ObMaxType;
         t1 = static_cast<ObObjType>(t1 + 1))
    {
      for (ObObjType t2 = static_cast<ObObjType>(ObMinType+1);
           t2 < ObMaxType;
           t2 = static_cast<ObObjType>(t2 + 1))
      {
        OB_ASSERT(COMPARE_TYPE_PROMOTION[t1][t2] == COMPARE_TYPE_PROMOTION[t2][t1]);
      }
    }
  }
} COMPARE_TYPE_PROMOTION_CHECKER;

//add qianzm [set_operation] 20151222 :b
int ObExprObj::do_type_promotion(ObObjType l, ObObjType r, ObObjType &res_type)
{
  UNUSED(res_type);
  int ret = OB_SUCCESS;
  OB_ASSERT(ObMinType < l && l < ObMaxType);
  OB_ASSERT(ObMinType < r && r < ObMaxType);
  res_type = COMPARE_TYPE_PROMOTION[l][r];
  return ret;
}
//add :e

inline int ObExprObj::type_promotion(ObObjType type_promote_map[ObMaxType][ObMaxType],
                                     const ObExprObj &this_obj, const ObExprObj &other,
                                     ObExprObj &promoted_obj1, ObExprObj &promoted_obj2,
                                     const ObExprObj *&p_this, const ObExprObj *&p_other)
{
  int ret = OB_SUCCESS;
  ObObjType this_type = this_obj.get_type();
  ObObjType other_type = other.get_type();
  OB_ASSERT(ObMinType < this_type && this_type < ObMaxType);
  OB_ASSERT(ObMinType < other_type && other_type < ObMaxType);
  ObObjType res_type = type_promote_map[this_type][other_type];

  if (ObNullType == res_type)
  {
    ret = OB_RESULT_UNKNOWN;
  }
  else if (ObMaxType == res_type)
  {
    TBSYS_LOG(ERROR, "invalid obj type for type promotion, this=%d other=%d", this_type, other_type);
    ret = OB_ERR_UNEXPECTED;
  }
  else
  {
    p_this = &this_obj;
    p_other = &other;
    ObObjType tmp_type  = res_type;  //mod peiouay [DATE_TIME] 201509012
    if (this_type != res_type)
    {
      //mod peiouay [DATE_TIME] 201509012:b
      if (ObIntervalType == res_type && ObIntervalType != this_type)
      {
        tmp_type = this_type;
      }
      else if (ObIntervalType != res_type && ObIntervalType == this_type)
      {
        tmp_type = ObIntervalType;
      }
      ObObjCastParams params;
      if (OB_SUCCESS != (ret = OB_OBJ_CAST[this_type][tmp_type](params, this_obj, promoted_obj1)))
      {
        TBSYS_LOG(WARN, "failed to cast object, err=%d from_type=%d to_type=%d",
                  ret, this_type, tmp_type);
      }
      else
      {
        p_this = &promoted_obj1;
      }
    }
    if (OB_SUCCESS == ret && other_type != res_type)
    {
      if (ObIntervalType == res_type && ObIntervalType != other_type)
      {
        tmp_type = other_type;
      }
      else if (ObIntervalType != res_type && ObIntervalType == other_type)
      {
        tmp_type = ObIntervalType;
      }
      ObObjCastParams params;
      if (OB_SUCCESS != (ret = OB_OBJ_CAST[other_type][tmp_type](params, other, promoted_obj2)))
      {
        TBSYS_LOG(WARN, "failed to cast object, err=%d from_type=%d to_type=%d",
                  ret, other_type, tmp_type);
      }
      //mod 20150912:e
      else
      {
        p_other = &promoted_obj2;
      }
    }
  }
  return ret;
}

inline int ObExprObj::compare_type_promotion(const ObExprObj &this_obj, const ObExprObj &other,
                                             ObExprObj &promoted_obj1, ObExprObj &promoted_obj2,
                                             const ObExprObj *&p_this, const ObExprObj *&p_other)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = type_promotion(COMPARE_TYPE_PROMOTION, this_obj, other,
                                          promoted_obj1, promoted_obj2, p_this, p_other)))
  {
    if (OB_RESULT_UNKNOWN != ret)
    {
      TBSYS_LOG(WARN, "invalid type promote for compare, err=%d", ret);
    }
  }
  return ret;
}

int ObExprObj::compare(const ObExprObj &other, int &cmp) const
{
  int ret = OB_SUCCESS;
  ObExprObj promoted1;
  ObExprObj promoted2;
  //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
  char buf[MAX_PRINTABLE_SIZE];
  memset(buf, 0, MAX_PRINTABLE_SIZE);
  ObString os;
  os.assign_ptr(buf,MAX_PRINTABLE_SIZE);
  ObObj tmp_obj;
  tmp_obj.set_varchar(os);
  promoted1.assign(tmp_obj);

  char buf2[MAX_PRINTABLE_SIZE];
  memset(buf2, 0, MAX_PRINTABLE_SIZE);
  ObString os2;
  os2.assign_ptr(buf2,MAX_PRINTABLE_SIZE);
  ObObj tmp_obj2;
  tmp_obj2.set_varchar(os2);
  promoted2.assign(tmp_obj2);
  //add:e
  const ObExprObj *p_this = NULL;
  const ObExprObj *p_other = NULL;
  if (OB_SUCCESS != (ret = compare_type_promotion(*this, other, promoted1, promoted2, p_this, p_other)))
  {
    //del peiouya [DATE_TIME] 20151010:b
    //ret = OB_RESULT_UNKNOWN;
    //del 20151010:e
    //TBSYS_LOG(ERROR, "failed to do compare_type_promotion");
    //add peiouya [NULL_type_compare] 20151102:b
    if (ObNullType == this->get_type())
    {
      if (ObNullType == other.get_type())
      {
        cmp = 0;
      }
      else
      {
        cmp = 1;
      }
    }
    else if ( other.get_type() == ObNullType)
    {
      cmp = -1;
    }
    //add:e
  }
  else
  {
    cmp = p_this->compare_same_type(*p_other);
  }
  return ret;
}

int ObExprObj::lt(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(other, cmp)))
  {
    res.set_bool(cmp < 0);
  }
  //add peiouya [DATE_TIME] 20151010:b
  else if(OB_RESULT_UNKNOWN == ret)
  {
    res.set_null();
    ret = OB_SUCCESS;
  }
  //add 20151010:e
  else
  {
    res.set_null();
    ret = OB_ERR_ILLEGAL_TYPE;//add peiouya [DATE_TIME] 20150929
  }
  return ret;
}

int ObExprObj::gt(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(other, cmp)))
  {
    res.set_bool(cmp > 0);
  }
  //add peiouya [DATE_TIME] 20151010:b
  else if(OB_RESULT_UNKNOWN == ret)
  {
    res.set_null();
    ret = OB_SUCCESS;
  }
  //add 20151010:e
  else
  {
    res.set_null();
    ret = OB_ERR_ILLEGAL_TYPE;//add peiouya [DATE_TIME] 20150929
  }
  return ret;
}

int ObExprObj::ge(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(other, cmp)))
  {
    res.set_bool(cmp >= 0);
  }
  //add peiouya [DATE_TIME] 20151010:b
  else if(OB_RESULT_UNKNOWN == ret)
  {
    res.set_null();
    ret = OB_SUCCESS;
  }
  //add 20151010:e
  else
  {
    res.set_null();
    ret = OB_ERR_ILLEGAL_TYPE;//add peiouya [DATE_TIME] 20150929
  }
  return ret;
}

int ObExprObj::le(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(other, cmp)))
  {
    res.set_bool(cmp <= 0);
  }
  //add peiouya [DATE_TIME] 20151010:b
  else if(OB_RESULT_UNKNOWN == ret)
  {
    res.set_null();
    ret = OB_SUCCESS;
  }
  //add 20151010:e
  else
  {
    res.set_null();
    ret = OB_ERR_ILLEGAL_TYPE;//add peiouya [DATE_TIME] 20150929
  }
  return ret;
}

int ObExprObj::eq(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(other, cmp)))
  {
    res.set_bool(cmp == 0);
  }
  //add peiouya [DATE_TIME] 20151010:b
  else if(OB_RESULT_UNKNOWN == ret)
  {
    res.set_null();
    ret = OB_SUCCESS;
  }
  //add 20151010:e
  else
  {
    res.set_null();
    ret = OB_ERR_ILLEGAL_TYPE;//add peiouya [DATE_TIME] 20150929
  }
  return ret;
}

int ObExprObj::ne(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(other, cmp)))
  {
    res.set_bool(cmp != 0);
  }
  //add peiouya [DATE_TIME] 20151010:b
  else if(OB_RESULT_UNKNOWN == ret)
  {
    res.set_null();
    ret = OB_SUCCESS;
  }
  //add 20151010:e
  else
  {
    res.set_null();
    ret = OB_ERR_ILLEGAL_TYPE;//add peiouya [DATE_TIME] 20150929
  }
  return ret;
}

int ObExprObj::btw(const ObExprObj &v1, const ObExprObj &v2, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(v1, cmp)))
  {
    if (cmp >= 0)
    {
      if (OB_SUCCESS == (ret = this->compare(v2, cmp)))
      {
        res.set_bool(cmp <= 0);
      }
      else
      {
        res.set_null();
        ret = OB_ERR_ILLEGAL_TYPE;//add peiouya [DATE_TIME] 20150929
      }
    }
    else
    {
      res.set_bool(false);
    }
  }
  //add peiouya [DATE_TIME] 20151010:b
  else if(OB_RESULT_UNKNOWN == ret)
  {
    res.set_null();
    ret = OB_SUCCESS;
  }
  //add 20151010:e
  else
  {
    res.set_null();
    ret = OB_ERR_ILLEGAL_TYPE;//add peiouya [DATE_TIME] 20150929
  }
  return ret;
}

int ObExprObj::not_btw(const ObExprObj &v1, const ObExprObj &v2, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(v1, cmp)))
  {
    if (cmp >= 0)
    {
      if (OB_SUCCESS == (ret = this->compare(v2, cmp)))
      {
        res.set_bool(cmp > 0);
      }
      else
      {
        res.set_null();
        ret = OB_ERR_ILLEGAL_TYPE;//add peiouya [DATE_TIME] 20150929
      }
    }
    else
    {
      res.set_bool(true);
    }
  }
  //add peiouya [DATE_TIME] 20151010:b
  else if(OB_RESULT_UNKNOWN == ret)
  {
    res.set_null();
    ret = OB_SUCCESS;
  }
  //add 20151010:e
  else
  {
    res.set_null();
    ret = OB_ERR_ILLEGAL_TYPE;//add peiouya [DATE_TIME] 20150929
  }
  return ret;
}

int ObExprObj::is(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  bool left_bool = false;
  if (other.get_type() == ObNullType)
  {
    res.set_bool((get_type() == ObNullType));
  }
  else if (ObBoolType != other.get_type())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid type for is operator, type=%d", other.get_type());
    res.set_bool(false);        // res cannot be UNKNOWN according to the SQL standard
  }
  else
  {
    if (OB_SUCCESS != get_bool(left_bool))
    {
      res.set_bool(false);
    }
    else
    {
      res.set_bool(left_bool == other.v_.bool_);
    }
  }
  return ret;
}

int ObExprObj::is_not(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  bool left_bool = false;
  if (other.get_type() == ObNullType)
  {
    res.set_bool((get_type() != ObNullType));
  }
  else if (ObBoolType != other.get_type())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid type for is operator, type=%d", other.get_type());
    res.set_bool(false);        // res cannot be UNKNOWN according to the SQL standard
  }
  else
  {
    if (OB_SUCCESS != get_bool(left_bool))
    {
      res.set_bool(true);       // NULL is not TRUE/FALSE
    }
    else
    {
      res.set_bool(left_bool != other.v_.bool_);
    }
  }
  return ret;
}

int ObExprObj::get_bool(bool &value) const
{
  int res = OB_SUCCESS;
  switch (type_)
  {
    case ObBoolType:
      value = v_.bool_;
      break;
    case ObVarcharType:
      value = (varchar_.length() > 0);
      break;
    case ObIntType:
      value = (v_.int_ != 0);
      break;
    //add yushengjuan [INT_32] 20151012:b
    case ObInt32Type:
      value = (v_.int32_ != 0);
      break;
    //add 20151012:e
    case ObDecimalType:
      //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
      //change num_ to decimal_
      value = !decimal_.is_zero();
      break;
      //modify:e
    case ObFloatType:
      value = (fabsf(v_.float_) > FLOAT_EPSINON);
      break;
    case ObDoubleType:
      value = (fabs(v_.double_) > DOUBLE_EPSINON);
      break;
    //add peiouya [DATE_TIME] 20150906:b
    case ObDateType:
    case ObTimeType:
    //add 20150906:e
    case ObIntervalType:  //add peiouya [DATE_TIME] 20150908
    case ObDateTimeType:
    case ObPreciseDateTimeType:
    case ObCreateTimeType:
    case ObModifyTimeType:
    {
      int64_t ts1 = 0;
      get_timestamp(ts1);
      value = (0 != ts1);
      break;
    }
    default:
      res = OB_OBJ_TYPE_ERROR;
      break;
  }
  return res;
}

int ObExprObj::land(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  bool left = false;
  bool right = false;
  int err1 = get_bool(left);
  int err2 = other.get_bool(right);
  switch (err1)
  {
    case OB_SUCCESS:
    {
      switch(err2)
      {
        case OB_SUCCESS:
          res.set_bool(left && right);
          break;
        default:
          if (left)
          {
            // TRUE and UNKNOWN
            res.set_null();
          }
          else
          {
            // FALSE and UNKNOWN
            res.set_bool(false);
          }
          break;
      }
      break;
    }
    default:
    {
      switch(err2)
      {
        case OB_SUCCESS:
          if (right)
          {
            // UNKNOWN and TRUE
            res.set_null();
          }
          else
          {
            // UNKNOWN and FALSE
            res.set_bool(false);
          }
          break;
        default:
          res.set_null();
          break;
      }
      break;
    }
  }
  return ret;
}

int ObExprObj::lor(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  bool left = false;
  bool right = false;
  int err1 = get_bool(left);
  int err2 = other.get_bool(right);
  switch (err1)
  {
    case OB_SUCCESS:
    {
      switch(err2)
      {
        case OB_SUCCESS:
          res.set_bool(left || right);
          break;
        default:
          if (left)
          {
            // TRUE or UNKNOWN
            res.set_bool(true);
          }
          else
          {
            // FALSE or UNKNOWN
            res.set_null();
          }
          break;
      }
      break;
    }
    default:
    {
      switch(err2)
      {
        case OB_SUCCESS:
          if (right)
          {
            // UNKNOWN or TRUE
            res.set_bool(true);
          }
          else
          {
            // UNKNOWN or FALSE
            res.set_null();
          }
          break;
        default:
          res.set_null();
          break;
      }
      break;
    }
  }
  return ret;
}

int ObExprObj::lnot(ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  bool val = false;
  int err1 = get_bool(val);
  if (OB_SUCCESS == err1)
  {
    res.set_bool(!val);
  }
  else
  {
    res.set_null();
  }
  return ret;
}
////////////////////////////////////////////////////////////////
// arithmetic operations
////////////////////////////////////////////////////////////////
// @return ObMaxType when not supporting
//mod peiouya [DATE_TIME] 20150906:b
//static ObObjType ARITHMETIC_TYPE_PROMOTION[ObMaxType][ObMaxType] =
// {
  // {
    // /*Null +/- XXX*/
    // ObNullType, ObNullType, ObNullType,
    // ObNullType, ObNullType, ObNullType,
    // ObNullType, ObNullType, ObNullType,
    // ObNullType, ObNullType, ObNullType,
    // ObNullType
  // }
  // ,
  // {
    // /*Int +/- XXX*/
    // ObNullType/*Null*/, ObIntType/*Int*/, ObFloatType,
    // ObDoubleType, ObIntType, ObIntType,
    // ObIntType, ObMaxType/*Seq, not_support*/, ObIntType,
    // ObIntType, ObMaxType/*Extend*/, ObIntType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*Float +/- XXX*/
    // ObNullType/*Null*/, ObFloatType, ObFloatType,
    // ObDoubleType, ObFloatType, ObFloatType,
    // ObFloatType, ObMaxType/*Seq*/, ObFloatType,
    // ObFloatType, ObMaxType/*Extend*/, ObFloatType,
    // ObDoubleType
  // }
  // ,
  // {
    // /*Double +/- XXX*/
    // ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    // ObDoubleType, ObDoubleType, ObDoubleType,
    // ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    // ObDoubleType, ObMaxType/*Extend*/, ObDoubleType,
    // ObDoubleType
  // }
  // ,
  // {
    // /*DateTime +/- XXX*/
    // ObNullType/*Null*/, ObIntType, ObFloatType,
    // ObDoubleType, ObIntType, ObIntType,
    // ObIntType, ObMaxType/*Seq*/, ObIntType,
    // ObIntType, ObMaxType/*Extend*/, ObIntType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*PreciseDateTime +/- XXX*/
    // ObNullType/*Null*/, ObIntType, ObFloatType,
    // ObDoubleType, ObIntType, ObIntType,
    // ObIntType, ObMaxType/*Seq*/, ObIntType,
    // ObIntType, ObMaxType/*Extend*/, ObIntType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*Varchar +/- XXX*/
    // ObNullType/*Null*/, ObIntType, ObFloatType,
    // ObDoubleType, ObIntType, ObIntType,
    // ObIntType, ObMaxType/*Seq*/, ObIntType,
    // ObIntType, ObMaxType/*Extend*/, ObIntType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*Seq +/- XXX*/
    // ObNullType, ObMaxType, ObMaxType,
    // ObMaxType, ObMaxType, ObMaxType,
    // ObMaxType, ObMaxType, ObMaxType,
    // ObMaxType, ObMaxType, ObMaxType,
    // ObMaxType
  // }
  // ,
  // {
    // /*CreateTime +/- XXX*/
    // ObNullType/*Null*/, ObIntType, ObFloatType,
    // ObDoubleType, ObIntType, ObIntType,
    // ObIntType, ObMaxType/*Seq*/, ObIntType,
    // ObIntType, ObMaxType/*Extend*/, ObIntType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*ModifyTime +/- XXX*/
    // ObNullType/*Null*/, ObIntType, ObFloatType,
    // ObDoubleType, ObIntType, ObIntType,
    // ObIntType, ObMaxType/*Seq*/, ObIntType,
    // ObIntType, ObMaxType/*Extend*/, ObIntType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*Extend +/- XXX*/
    // ObNullType, ObMaxType, ObMaxType,
    // ObMaxType, ObMaxType, ObMaxType,
    // ObMaxType, ObMaxType, ObMaxType,
    // ObMaxType, ObMaxType, ObMaxType,
    // ObMaxType
  // }
  // ,
  // {
    // /*Bool +/- XXX*/
    // ObNullType/*Null*/, ObIntType, ObFloatType,
    // ObDoubleType, ObIntType, ObIntType,
    // ObIntType, ObMaxType/*Seq*/, ObIntType,
    // ObIntType, ObMaxType/*Extend*/, ObIntType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*Decimal +/- XXX*/
    // ObNullType/*Null*/, ObDecimalType, ObDoubleType,
    // ObDoubleType, ObDecimalType, ObDecimalType,
    // ObDecimalType, ObMaxType/*Seq*/, ObDecimalType,
    // ObDecimalType, ObMaxType/*Extend*/, ObDecimalType,
    // ObDecimalType
  // }
// };
//delete by xionghui [floor and ceil] 20151215 b:
//static ObObjType ARITHMETIC_TYPE_PROMOTION[ObMaxType][ObMaxType] =
//{
//  {
//    /*Null +/- XXX*/
//    ObNullType, ObNullType, ObNullType,
//    ObNullType, ObNullType, ObNullType,
//    ObNullType, ObNullType, ObNullType,
//    ObNullType, ObNullType, ObNullType,
//    ObNullType, ObNullType, ObNullType,
//    ObNullType, ObNullType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
//  }
//  ,
//  {
//    /*Int +/- XXX*/
//    ObNullType/*Null*/, ObIntType/*Int*/, ObFloatType,
//    ObDoubleType, ObMaxType, ObMaxType,
//    ObIntType, ObMaxType/*Seq, not_support*/, ObMaxType,
//    ObMaxType, ObMaxType/*Extend*/, ObIntType,
//    ObDecimalType, ObMaxType, ObMaxType,
//    ObMaxType, ObIntType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
//  }
//  ,
//  {
//    /*Float +/- XXX*/
//    ObNullType/*Null*/, ObFloatType, ObFloatType,
//    ObDoubleType, ObMaxType, ObMaxType,
//    ObFloatType, ObMaxType/*Seq*/, ObMaxType,
//    ObMaxType, ObMaxType/*Extend*/, ObFloatType,
//    ObDoubleType, ObMaxType, ObMaxType,
//    ObMaxType, ObFloatType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
//  }
//  ,
//  {
//    /*Double +/- XXX*/
//    ObNullType/*Null*/, ObDoubleType, ObDoubleType,
//    ObDoubleType, ObMaxType, ObMaxType,
//    ObDoubleType, ObMaxType/*Seq*/, ObMaxType,
//    ObMaxType, ObMaxType/*Extend*/, ObDoubleType,
//    ObDoubleType, ObMaxType, ObMaxType,
//    ObMaxType, ObDoubleType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
//  }
//  ,
//  {
//    /*DateTime +/- XXX*/
//    ObNullType/*Null*/, ObMaxType, ObMaxType,
//    ObMaxType, ObIntType, ObIntType,
//    ObMaxType, ObMaxType/*Seq*/, ObIntType,
//    ObIntType, ObMaxType/*Extend*/, ObMaxType,
//    ObMaxType, ObIntType, ObMaxType,
//    ObDateTimeType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
//  }
//  ,
//  {
//    /*PreciseDateTime +/- XXX*/
//    ObNullType/*Null*/, ObMaxType, ObMaxType,
//    ObMaxType, ObIntType, ObIntType,
//    ObMaxType, ObMaxType/*Seq*/, ObIntType,
//    ObIntType, ObMaxType/*Extend*/, ObMaxType,
//    ObMaxType, ObIntType, ObMaxType,
//    ObPreciseDateTimeType, ObMaxType  //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
//  }
//  ,
//  {
//    /*Varchar +/- XXX*/
//    ObNullType/*Null*/, ObIntType, ObFloatType,
//    ObDoubleType, ObMaxType, ObMaxType,
//    ObIntType, ObMaxType/*Seq*/, ObMaxType,
//    ObMaxType, ObMaxType/*Extend*/, ObIntType,
//    ObDecimalType, ObMaxType, ObMaxType,
//    ObMaxType, ObInt32Type //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
//  }
//  ,
//  {
//    /*Seq +/- XXX*/
//    ObNullType, ObMaxType, ObMaxType,
//    ObMaxType, ObMaxType, ObMaxType,
//    ObMaxType, ObMaxType, ObMaxType,
//    ObMaxType, ObMaxType, ObMaxType,
//    ObMaxType, ObMaxType, ObMaxType,
//    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
//  }
//  ,
//  {
//    /*CreateTime +/- XXX*/
//    ObNullType/*Null*/, ObMaxType, ObMaxType,
//    ObMaxType, ObIntType, ObIntType,
//    ObMaxType, ObMaxType/*Seq*/, ObIntType,
//    ObIntType, ObMaxType/*Extend*/, ObMaxType,
//    ObMaxType, ObIntType, ObMaxType,
//    ObCreateTimeType, ObMaxType  //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
//  }
//  ,
//  {
//    /*ModifyTime +/- XXX*/
//    ObNullType/*Null*/, ObMaxType, ObMaxType,
//    ObMaxType, ObIntType, ObIntType,
//    ObMaxType, ObMaxType/*Seq*/, ObIntType,
//    ObIntType, ObMaxType/*Extend*/, ObMaxType,
//    ObMaxType, ObIntType, ObMaxType,
//    ObModifyTimeType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
//  }
//  ,
//  {
//    /*Extend +/- XXX*/
//    ObNullType, ObMaxType, ObMaxType,
//    ObMaxType, ObMaxType, ObMaxType,
//    ObMaxType, ObMaxType, ObMaxType,
//    ObMaxType, ObMaxType, ObMaxType,
//    ObMaxType, ObMaxType, ObMaxType,
//    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
//  }
//  ,
//  {
//    /*Bool +/- XXX*/
//    ObNullType/*Null*/, ObIntType, ObFloatType,
//    ObDoubleType, ObMaxType, ObMaxType,
//    ObIntType, ObMaxType/*Seq*/, ObMaxType,
//    ObMaxType, ObMaxType/*Extend*/, ObIntType,
//    ObDecimalType, ObMaxType, ObMaxType,
//    ObMaxType, ObInt32Type //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
//  }
//  ,
//  {
//    /*Decimal +/- XXX*/
//    ObNullType/*Null*/, ObDecimalType, ObDoubleType,
//    ObDoubleType, ObMaxType, ObMaxType,
//    ObDecimalType, ObMaxType/*Seq*/, ObMaxType,
//    ObMaxType, ObMaxType/*Extend*/, ObDecimalType,
//    ObDecimalType, ObMaxType, ObMaxType,
//    ObMaxType, ObDecimalType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
//  }
//  ,
//  {
//    /*Date +/- XXX*/
//    ObNullType/*Null*/, ObMaxType, ObMaxType,
//    ObMaxType, ObIntType, ObIntType,
//    ObMaxType, ObMaxType/*Seq*/, ObIntType,
//    ObIntType, ObMaxType/*Extend*/, ObMaxType,
//    ObMaxType, ObIntType, ObMaxType,
//    ObDateType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
//  }
//  ,
//  {
//    /*Time +/- XXX*/
//    ObNullType/*Null*/, ObMaxType, ObMaxType,
//    ObMaxType, ObMaxType, ObMaxType,
//    ObMaxType, ObMaxType/*Seq*/, ObMaxType,
//    ObMaxType, ObMaxType/*Extend*/, ObMaxType,
//    ObMaxType, ObMaxType, ObIntType,
//    ObTimeType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
//  }
//  //add peiouya [DATE_TIME] 20150908:b
//  ,
//  {
//    /*Interval +/- XXX*/
//    ObNullType/*Null*/, ObMaxType, ObMaxType,
//    ObMaxType, ObIntervalType, ObIntervalType,
//    ObMaxType, ObMaxType, ObIntervalType,
//    ObIntervalType, ObMaxType, ObMaxType,
//    ObMaxType, ObIntervalType, ObIntervalType,
//    ObMaxType, ObMaxType //add yushengjuan [INT_32] 20151012
//  }
//  //add 20150908:e
//  //add yushengjuan [INT_32] 20151012:b
//  ,
//  {
//    /*Int_32 +/- XXX*/
//    ObNullType/*NULL*/, ObIntType/*Int*/, ObFloatType,
//    ObDoubleType/*double*/, ObMaxType, ObMaxType,
//    ObInt32Type/*varchar*/, ObMaxType, ObMaxType,
//    ObMaxType, ObMaxType, ObInt32Type,
//    ObDecimalType, ObMaxType, ObMaxType,
//    ObMaxType, ObInt32Type
//  }
//  //add 20151012:e
//};
////mod 20150906:e
///delete 20151215 e:

//mod xionghui [floor and ceil] 20151225 b:
//convert varchar to decimal when calculation
static ObObjType ARITHMETIC_TYPE_PROMOTION[ObMaxType][ObMaxType] =
{
  {
    /*Null +/- XXX*/
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Int +/- XXX*/
    ObNullType/*Null*/, ObIntType/*Int*/, ObFloatType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObIntType, ObMaxType/*Seq, not_support*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObIntType,
    ObDecimalType, ObMaxType, ObMaxType,
    ObMaxType, ObIntType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Float +/- XXX*/
    ObNullType/*Null*/, ObFloatType, ObFloatType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObFloatType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObFloatType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObMaxType, ObFloatType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Double +/- XXX*/
    ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObDoubleType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObDoubleType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObMaxType, ObDoubleType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*DateTime +/- XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObIntType, ObIntType,
    ObMaxType, ObMaxType/*Seq*/, ObIntType,
    ObIntType, ObMaxType/*Extend*/, ObMaxType,
    ObMaxType, ObIntType, ObMaxType,
    ObDateTimeType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*PreciseDateTime +/- XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObIntType, ObIntType,
    ObMaxType, ObMaxType/*Seq*/, ObIntType,
    ObIntType, ObMaxType/*Extend*/, ObMaxType,
    ObMaxType, ObIntType, ObMaxType,
    ObPreciseDateTimeType, ObMaxType  //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Varchar +/- XXX*/
    ObNullType/*Null*/, ObIntType, ObFloatType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObIntType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObIntType,
    ObDecimalType, ObMaxType, ObMaxType,
    ObMaxType, ObInt32Type //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Seq +/- XXX*/
    ObNullType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*CreateTime +/- XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObIntType, ObIntType,
    ObMaxType, ObMaxType/*Seq*/, ObIntType,
    ObIntType, ObMaxType/*Extend*/, ObMaxType,
    ObMaxType, ObIntType, ObMaxType,
    ObCreateTimeType, ObMaxType  //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*ModifyTime +/- XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObIntType, ObIntType,
    ObMaxType, ObMaxType/*Seq*/, ObIntType,
    ObIntType, ObMaxType/*Extend*/, ObMaxType,
    ObMaxType, ObIntType, ObMaxType,
    ObModifyTimeType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Extend +/- XXX*/
    ObNullType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Bool +/- XXX*/
    ObNullType/*Null*/, ObIntType, ObFloatType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObIntType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObIntType,
    ObDecimalType, ObMaxType, ObMaxType,
    ObMaxType, ObInt32Type //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Decimal +/- XXX*/
    ObNullType/*Null*/, ObDecimalType, ObDoubleType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObDecimalType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObDecimalType,
    ObDecimalType, ObMaxType, ObMaxType,
    ObMaxType, ObDecimalType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Date +/- XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObIntType, ObIntType,
    ObMaxType, ObMaxType/*Seq*/, ObIntType,
    ObIntType, ObMaxType/*Extend*/, ObMaxType,
    ObMaxType, ObIntType, ObMaxType,
    ObDateType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Time +/- XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObMaxType,
    ObMaxType, ObMaxType, ObIntType,
    ObTimeType, ObMaxType //add peiouya [DATE_TIME] 20150908 add yushengjuan [INT_32] 20151012
  }
  //add peiouya [DATE_TIME] 20150908:b
  ,
  {
    /*Interval +/- XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObIntervalType, ObIntervalType,
    ObMaxType, ObMaxType, ObIntervalType,
    ObIntervalType, ObMaxType, ObMaxType,
    ObMaxType, ObIntervalType, ObIntervalType,
    ObMaxType, ObMaxType //add yushengjuan [INT_32] 20151012
  }
  //add 20150908:e
  //add yushengjuan [INT_32] 20151012:b
  ,
  {
    /*Int_32 +/- XXX*/
    ObNullType/*NULL*/, ObIntType/*Int*/, ObFloatType,
    ObDoubleType/*double*/, ObMaxType, ObMaxType,
    ObInt32Type/*varchar*/, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObInt32Type,
    ObDecimalType, ObMaxType, ObMaxType,
    ObMaxType, ObInt32Type
  }
  //add 20151012:e
};
//mod 20150906:e

//add [DATE_TIME] peiouya 20150831
static bool is_datetime(ObObjType type)
{
  return ((type == ObDateTimeType)
      || (type == ObPreciseDateTimeType)
      || (type == ObCreateTimeType)
      || (type == ObModifyTimeType)
      || (type == ObDateType)
      || (type == ObTimeType));
}
//add 20150831:e

struct IntegrityChecker2
{
  IntegrityChecker2()
  {
    for (ObObjType t1 = static_cast<ObObjType>(ObMinType+1);
         t1 < ObMaxType;
         t1 = static_cast<ObObjType>(t1 + 1))
    {
      for (ObObjType t2 = static_cast<ObObjType>(ObMinType+1);
           t2 < ObMaxType;
           t2 = static_cast<ObObjType>(t2 + 1))
      {
        //mod [DATE_TIME] peiouya 20150831:b
        //OB_ASSERT(ARITHMETIC_TYPE_PROMOTION[t1][t2] == ARITHMETIC_TYPE_PROMOTION[t2][t1]);
        if ((ObIntervalType == t1 && is_datetime(t2))
            || (ObIntervalType == t2 && is_datetime(t1)))
        {
          //nothing todo
        }
        else
        {
          OB_ASSERT(ARITHMETIC_TYPE_PROMOTION[t1][t2] == ARITHMETIC_TYPE_PROMOTION[t2][t1]);
        }
        //mod 20150831:e
      }
    }
  }
} ARITHMETIC_TYPE_PROMOTION_CHECKER;

inline int ObExprObj::arith_type_promotion(const ObExprObj &this_obj, const ObExprObj &other,
                                     ObExprObj &promoted_obj1, ObExprObj &promoted_obj2,
                                     const ObExprObj *&p_this, const ObExprObj *&p_other)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = type_promotion(ARITHMETIC_TYPE_PROMOTION, this_obj, other,
                                          promoted_obj1, promoted_obj2, p_this, p_other)))
  {
    if (OB_RESULT_UNKNOWN != ret)
    {
      TBSYS_LOG(WARN, "invalid type promote for arithmetic, err=%d", ret);
    }
  }
  return ret;
}
//add dolphin[coalesce return type] 20151126:b

 bool ObExprObj::is_datetime_compare(const ObObjType type)
{
    return ((type == ObDateTimeType)
            || (type == ObPreciseDateTimeType)
            || (type == ObCreateTimeType)
            || (type == ObModifyTimeType)
            || (type == ObDateType)
            || (type == ObTimeType)
    );
}

 bool ObExprObj::can_compare(const ObObjType &t1,const ObObjType &t2)
{
    bool ret = false;
    if ((t1 == ObNullType) || (t2 == ObNullType)
        || (t1 == t2) || (is_datetime_compare(t1) && is_datetime_compare(t2))
        || (is_numeric(t1) && is_numeric(t2))
        || ( ObVarcharType == t1 && (is_numeric(t2) || is_datetime_compare(t2)))
        || ( ObVarcharType == t2 && (is_numeric(t1) || is_datetime_compare(t1))) )
    {
        ret = true;
    }
    return ret;

}
 bool ObExprObj::is_numeric(const ObObjType &type)
{
    return ((type == ObIntType)
            || (type == ObDecimalType)
            || (type == ObFloatType)
            || (type == ObDoubleType)
            || (type == ObBoolType)
            || (type == ObInt32Type)
    );
}
 int ObExprObj::coalesce_type_compatible(ObObjType& t1,const ObObjType& t2)
{
    int ret = OB_SUCCESS;
    if(ObNullType == t1)
    {
        t1 = t2;
    }
    else if(ObNullType == t2)
    {
        return ret ;
    }
    else
    {
        if(can_compare(t1,t2))
        {
           t1 = COMPARE_TYPE_PROMOTION[t1][t2];
           if(is_numeric(t1))
               t1 = ObDecimalType;
           else if(is_datetime_compare(t1))
               t1 = ObDateTimeType;
        }
        else
        {
          TBSYS_LOG(WARN,"parameters type %d and type %d cann't consistent!",t1,t2);
          ret = OB_CONSISTENT_MATRIX_DATATYPE_INVALID;

        }
    }
  return ret;
}
//add:e
//add [DATE_TIME] peiouya 20150831:b
void ObExprObj::check_and_set_res_type(const ObObjType first_type, const ObObjType second_type, ObExprObj &res) const
{
  if (ObIntervalType == first_type && is_datetime(second_type))
  {
    res.type_ = second_type;
  }
  else if (ObIntervalType == second_type && is_datetime(first_type))
  {
    res.type_ = first_type;
  }
  else
  {
    OB_ASSERT(first_type == second_type && this->is_numeric());
    res.type_ = first_type;
  }
}
//add 20150831:e
//add [DATE_TIME] peiouya 20150908:b
static bool check_arithmetic_valid(ObInterval interval, ObObjType type)
{
  bool ret = true;
  //only use last three bit, 
  static const int8_t HAS_DAYS = 0x4;
  static const int8_t HAS_SECONDS = 0x2;
  static const int8_t HAS_MICROSECONDS = 0x1; 
  int8_t interval_format = 0;
  if (0L != (interval % 1000000L))
  {
    interval_format |= HAS_MICROSECONDS;
  }
  if (0L != (interval % (1000000L * 86400L)))
  {
    interval_format |= HAS_SECONDS;
  }
  if (0L != (interval / 1000000L / 86400L))
  {
    interval_format |= HAS_DAYS;
  }
  int8_t type_format = 0;
  switch (type)
  {
    case ObPreciseDateTimeType:
    case ObCreateTimeType:
    case ObModifyTimeType:
      type_format |= 0x7;
      break;
    case ObDateTimeType:
      type_format |= 0x6;
      break;
    case ObDateType:
      type_format |= 0x4;
      break;
    case ObTimeType:
      type_format |= 0x2;
      break;
    default:
       ret = false;
  }
  return (ret? ((interval_format | type_format) == type_format) : false);
}
//add peiouya [DATE_TIME] 20150914:b
inline int ObExprObj::time_skew(ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  if (!is_datetime(res.get_type ()))
  {
    ret = OB_ERR_ILLEGAL_TYPE;
  }
  if (ObTimeType != res.get_type ())
  {
    time_t time = 0;
    struct tm gtm;
    switch (res.get_type())
    {
      case ObDateTimeType:
        time = static_cast<time_t>(res.get_datetime());
        time *= 1000 * 1000L;
        break;
      case ObDateType:
      case ObPreciseDateTimeType:
        time = static_cast<time_t>(res.get_precise_datetime());
        break;
      case ObModifyTimeType:
        time = static_cast<time_t>(res.get_mtime());
        break;
      case ObCreateTimeType:
        time = static_cast<time_t>(res.get_ctime());
        break;
      default:
        TBSYS_LOG(ERROR, "unexpected branch");
        ret = OB_OBJ_TYPE_ERROR;
    }
    if (OB_SUCCESS == ret)
    {
      time = static_cast<time_t>(time) / 1000000;
      localtime_r(&time, &gtm);
      if ((1 <= gtm.tm_year + 1900) && (9999 >= gtm.tm_year + 1900))
      {
        //nothing todo
      }
      else
      {
        res.set_null ();
        ret = OB_VALUE_OUT_OF_DATES_RANGE; //add peiouya [DATE_TIME] 20150914
      }
    }
  }
  else
  {
    res.v_.precisedatetime_ = res.v_.precisedatetime_ % (86400L * 1000000L);
    if (static_cast<ObPreciseDateTime>(0) > res.v_.precisedatetime_)
    {
        res.v_.precisedatetime_ = res.v_.precisedatetime_ + static_cast<ObPreciseDateTime>(86400L * 1000000L);
    }
  }
  return ret;
}
//add 20150908:e
inline int ObExprObj::add_same_type(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  //mod [DATE_TIME] peiouya 20150831:b
  //OB_ASSERT(get_type() == other.get_type()
  //          && this->is_numeric());
  //res.type_ = get_type();
  check_and_set_res_type(get_type(), other.get_type(), res);
  //mod 20150831:e
  switch(get_type())
  {
    case ObIntType:
      res.v_.int_ = this->v_.int_ + other.v_.int_; // overflow is allowed
      break;
    //add yushengjuan [INT_32] 20151012:b
    case ObInt32Type:
      res.v_.int32_ = this->v_.int32_ + other.v_.int32_; //overflow is allowed
      break;
    //add 20151012:e
    case ObFloatType:
      res.v_.float_ = this->v_.float_ + other.v_.float_;
      break;
    case ObDoubleType:
      res.v_.double_ = this->v_.double_ + other.v_.double_;
      break;
    case ObDecimalType:
      //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
      //change num_ to decimal_
      ret = this->decimal_.add(other.decimal_, res.decimal_);
      break;
      //modify:e
    //add [DATE_TIME] peiouya 20150831:b
    case ObIntervalType:
      if (is_datetime (other.get_type ()) && check_arithmetic_valid(static_cast<ObInterval>(this->v_.precisedatetime_), other.get_type ()))
      {
        res.v_.precisedatetime_ = this->v_.precisedatetime_ + other.v_.precisedatetime_;
        ret = time_skew(res);  //add peiouya [DATE_TIME] 20150914
      }
      else
      {
        ret = OB_ERR_ILLEGAL_TYPE;
      }
      break;
    case ObDateTimeType:
    case ObPreciseDateTimeType:
    case ObCreateTimeType:
    case ObModifyTimeType:
    case ObDateType:
    case ObTimeType:
      if (ObIntervalType == other.get_type () && check_arithmetic_valid(static_cast<ObInterval>(other.v_.precisedatetime_), get_type()))
      {
        res.v_.precisedatetime_ = this->v_.precisedatetime_ + other.v_.precisedatetime_;
        ret = time_skew(res);  //add peiouya [DATE_TIME] 20150914
      }
      else
      {
        ret = OB_ERR_ILLEGAL_TYPE;
      }
      break;
    //add 20150831:e
    default:
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected branch");
      break;
  }
  return ret;
}

int ObExprObj::add(ObExprObj &other, ObExprObj &res)
{
  int ret = OB_SUCCESS;
  ObExprObj promoted1;
  ObExprObj promoted2;
  //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
  char buf[MAX_PRINTABLE_SIZE];
  memset(buf, 0, MAX_PRINTABLE_SIZE);
  ObString os;
  os.assign_ptr(buf,MAX_PRINTABLE_SIZE);
  ObObj tmp_obj;
  tmp_obj.set_varchar(os);
  promoted1.assign(tmp_obj);

  char buf2[MAX_PRINTABLE_SIZE];
  memset(buf2, 0, MAX_PRINTABLE_SIZE);
  ObString os2;
  os2.assign_ptr(buf2,MAX_PRINTABLE_SIZE);
  ObObj tmp_obj2;
  tmp_obj2.set_varchar(os2);
  promoted2.assign(tmp_obj2);

  //add:e
  const ObExprObj *p_this = NULL;
  const ObExprObj *p_other = NULL;
  //mod peiouya [DATE_TIME] 20150906:b
  //if (OB_SUCCESS != (ret = arith_type_promotion(*this, other, promoted1, promoted2, p_this, p_other)))
  if (is_datetime(this->get_type()) && is_datetime(other.get_type()))
  {
    res.set_null();
    ret = OB_ERR_ILLEGAL_TYPE;
  }
  else if (OB_SUCCESS != (ret = arith_type_promotion(*this, other, promoted1, promoted2, p_this, p_other)))
  {
  //mod 20150906:e
    if (OB_RESULT_UNKNOWN == ret)
    {
      ret = OB_SUCCESS;
    }
    res.set_null();
  }
  else
  {
    ret = p_this->add_same_type(*p_other, res);
  }
  return ret;
}

inline int ObExprObj::sub_same_type(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  //mod [DATE_TIME] peiouya 20150831:b
  //OB_ASSERT(get_type() == other.get_type()
  //          && this->is_numeric());
  //res.type_ = get_type();
  check_and_set_res_type(get_type(), other.get_type(), res);
  //mod 20150831:e
  switch(get_type())
  {
    case ObIntType:
      res.v_.int_ = this->v_.int_ - other.v_.int_; // overflow is allowed
      break;
    case ObFloatType:
      res.v_.float_ = this->v_.float_ - other.v_.float_;
      break;
    //add yushengjuan [INT_32] 20151012:b
    case ObInt32Type:
      res.v_.int32_ = this->v_.int32_ - other.v_.int32_;
      break;
    //add 20151012:e
    case ObDoubleType:
      res.v_.double_ = this->v_.double_ - other.v_.double_;
      break;
    case ObDecimalType:
      //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
      //change num_ to decimal_
      ret = this->decimal_.sub(other.decimal_, res.decimal_);
      break;
      //modify:e
    //add [DATE_TIME] peiouya 20150831:b
    case ObDateTimeType:
    case ObPreciseDateTimeType:
    case ObCreateTimeType:
    case ObModifyTimeType:
    case ObDateType:
    case ObTimeType:
      if (ObIntervalType == other.get_type () && check_arithmetic_valid(static_cast<ObInterval>(other.v_.precisedatetime_), get_type()))
      {
        res.v_.precisedatetime_ = this->v_.precisedatetime_ - other.v_.precisedatetime_;
        ret = time_skew(res);  //add peiouya [DATE_TIME] 20150914
      }
      else
      {
        ret = OB_ERR_ILLEGAL_TYPE;
      }
      break;
    //add 20150831:e
    default:
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected branch");
      break;
  }
  return ret;
}

int ObExprObj::sub(ObExprObj &other, ObExprObj &res)
{
  int ret = OB_SUCCESS;
  ObExprObj promoted1;
  ObExprObj promoted2;
  //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
  char buf[MAX_PRINTABLE_SIZE];
  memset(buf, 0, MAX_PRINTABLE_SIZE);
  ObString os;
  os.assign_ptr(buf,MAX_PRINTABLE_SIZE);
  ObObj tmp_obj;
  tmp_obj.set_varchar(os);
  promoted1.assign(tmp_obj);

  char buf2[MAX_PRINTABLE_SIZE];
  memset(buf2, 0, MAX_PRINTABLE_SIZE);
  ObString os2;
  os2.assign_ptr(buf2,MAX_PRINTABLE_SIZE);
  ObObj tmp_obj2;
  tmp_obj2.set_varchar(os2);
  promoted2.assign(tmp_obj2);

  //add:e
  const ObExprObj *p_this = NULL;
  const ObExprObj *p_other = NULL;
  if (OB_SUCCESS != (ret = arith_type_promotion(*this, other, promoted1, promoted2, p_this, p_other)))
  {
    if (OB_RESULT_UNKNOWN == ret)
    {
      ret = OB_SUCCESS;
    }
    res.set_null();
  }
  else
  {
    ret = p_this->sub_same_type(*p_other, res);
  }
  return ret;
}

inline int ObExprObj::mul_same_type(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  //mod [DATE_TIME] peiouya 20150831:b
  //OB_ASSERT(get_type() == other.get_type()
  //          && this->is_numeric());
  //res.type_ = get_type();
  check_and_set_res_type(get_type(), other.get_type(), res);
  //mod 20150831:e
  switch(get_type())
  {
    case ObIntType:
      res.v_.int_ = this->v_.int_ * other.v_.int_; // overflow is allowed
      break;
    //add yushengjuan [INT_32] 20151012:b
    case ObInt32Type:
      res.v_.int32_ = this->v_.int32_ * other.v_.int32_;
      break;
    //add 20151012:e
    case ObFloatType:
      res.v_.float_ = this->v_.float_ * other.v_.float_;
      break;
    case ObDoubleType:
      res.v_.double_ = this->v_.double_ * other.v_.double_;
      break;
    case ObDecimalType:
      //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
      //change num_ to decimal_
      ret = this->decimal_.mul(other.decimal_, res.decimal_);
      break;
      //modify:e
    default:
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected branch");
      break;
  }
  return ret;
}

int ObExprObj::mul(ObExprObj &other, ObExprObj &res)
{
  int ret = OB_SUCCESS;
  ObExprObj promoted1;
  ObExprObj promoted2;
  //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
 char buf[MAX_PRINTABLE_SIZE];
 memset(buf, 0, MAX_PRINTABLE_SIZE);
 ObString os;
 os.assign_ptr(buf,MAX_PRINTABLE_SIZE);
 ObObj tmp_obj;
 tmp_obj.set_varchar(os);
 promoted1.assign(tmp_obj);

 char buf2[MAX_PRINTABLE_SIZE];
 memset(buf2, 0, MAX_PRINTABLE_SIZE);
 ObString os2;
 os2.assign_ptr(buf2,MAX_PRINTABLE_SIZE);
 ObObj tmp_obj2;
 tmp_obj2.set_varchar(os2);
 promoted2.assign(tmp_obj2);

 //add:e
  const ObExprObj *p_this = NULL;
  const ObExprObj *p_other = NULL;
  //mod peiouya [DATE_TIME] 20150906:b
  //if (OB_SUCCESS != (ret = arith_type_promotion(*this, other, promoted1, promoted2, p_this, p_other)))
  if (is_datetime(this->get_type()) && is_datetime(other.get_type()))
  {
    res.set_null();
    ret = OB_ERR_ILLEGAL_TYPE;
  }
  else if (OB_SUCCESS != (ret = arith_type_promotion(*this, other, promoted1, promoted2, p_this, p_other)))
  {  
  //mod 20150906:e
    if (OB_RESULT_UNKNOWN == ret)
    {
      ret = OB_SUCCESS;
    }
    res.set_null();
  }
  else
  {
    ret = p_this->mul_same_type(*p_other, res);
  }
  return ret;
}

// @return ObMaxType when not supporting
// int div int result in double
//mod peiouya [DATE_TIME] 20150906:b
//static ObObjType DIV_TYPE_PROMOTION[ObMaxType][ObMaxType] =
// {
  // {
    // /*Null div XXX*/
    // ObNullType, ObNullType, ObNullType,
    // ObNullType, ObNullType, ObNullType,
    // ObNullType, ObNullType, ObNullType,
    // ObNullType, ObNullType, ObNullType,
    // ObNullType
  // }
  // ,
  // {
    // /*Int div XXX*/
    // ObNullType/*Null*/, ObIntType/*Int*//*ObDecimalType  modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_19:b*//*Int*/, ObFloatType,
    // ObDoubleType, ObDoubleType, ObDoubleType,
    // ObDoubleType, ObMaxType/*Seq, not_support*/, ObDoubleType,
    // ObDoubleType, ObMaxType/*Extend*/, ObDoubleType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*Float div XXX*/
    // ObNullType/*Null*/, ObFloatType, ObFloatType,
    // ObDoubleType, ObDoubleType, ObDoubleType,
    // ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    // ObDoubleType, ObMaxType/*Extend*/, ObFloatType,
    // ObDoubleType
  // }
  // ,
  // {
    // /*Double div XXX*/
    // ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    // ObDoubleType, ObDoubleType, ObDoubleType,
    // ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    // ObDoubleType, ObMaxType/*Extend*/, ObDoubleType,
    // ObDoubleType
  // }
  // ,
  // {
    // /*DateTime div XXX*/
    // ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    // ObDoubleType, ObDoubleType, ObDoubleType,
    // ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    // ObDoubleType, ObMaxType/*Extend*/, ObDoubleType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*PreciseDateTime div XXX*/
    // ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    // ObDoubleType, ObDoubleType, ObDoubleType,
    // ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    // ObDoubleType, ObMaxType/*Extend*/, ObDoubleType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*Varchar div XXX*/
    // ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    // ObDoubleType, ObDoubleType, ObDoubleType,
    // ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    // ObDoubleType, ObMaxType/*Extend*/, ObDoubleType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*Seq div XXX*/
    // ObNullType, ObMaxType, ObMaxType,
    // ObMaxType, ObMaxType, ObMaxType,
    // ObMaxType, ObMaxType, ObMaxType,
    // ObMaxType, ObMaxType, ObMaxType,
    // ObMaxType
  // }
  // ,
  // {
    // /*CreateTime div XXX*/
    // ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    // ObDoubleType, ObDoubleType, ObDoubleType,
    // ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    // ObDoubleType, ObMaxType/*Extend*/, ObDoubleType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*ModifyTime div XXX*/
    // ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    // ObDoubleType, ObDoubleType, ObDoubleType,
    // ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    // ObDoubleType, ObMaxType/*Extend*/, ObDoubleType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*Extend div XXX*/
    // ObNullType, ObMaxType, ObMaxType,
    // ObMaxType, ObMaxType, ObMaxType,
    // ObMaxType, ObMaxType, ObMaxType,
    // ObMaxType, ObMaxType, ObMaxType,
    // ObMaxType
  // }
  // ,
  // {
    // /*Bool div XXX*/
    // ObNullType/*Null*/, ObDoubleType, ObFloatType,
    // ObDoubleType, ObDoubleType, ObDoubleType,
    // ObDoubleType, ObMaxType/*Seq*/, ObDoubleType,
    // ObDoubleType, ObMaxType/*Extend*/, ObDoubleType,
    // ObDecimalType
  // }
  // ,
  // {
    // /*Decimal div XXX*/
    // ObNullType/*Null*/, ObDecimalType, ObDoubleType,
    // ObDoubleType, ObDecimalType, ObDecimalType,
    // ObDecimalType, ObMaxType/*Seq*/, ObDecimalType,
    // ObDecimalType, ObMaxType/*Extend*/, ObDecimalType,
    // ObDecimalType
  // }
// };
static ObObjType DIV_TYPE_PROMOTION[ObMaxType][ObMaxType] =
{
  {
    /*Null div XXX*/
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType, ObNullType,
    ObNullType, ObNullType //add peiouya [DATE_TIME] 20150909 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Int div XXX*/
    ObNullType/*Null*/, ObIntType/*Int*//*ObDecimalType  modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_19:b*//*Int*/, ObFloatType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObDoubleType, ObMaxType/*Seq, not_support*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObDoubleType,
    ObDecimalType, ObMaxType, ObMaxType,
    ObMaxType, ObIntType //add peiouya [DATE_TIME] 20150909 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Float div XXX*/
    ObNullType/*Null*/, ObFloatType, ObFloatType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObDoubleType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObFloatType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObMaxType, ObFloatType //add peiouya [DATE_TIME] 20150909 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Double div XXX*/
    ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObDoubleType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObDoubleType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObMaxType, ObDoubleType //add peiouya [DATE_TIME] 20150909 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*DateTime div XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150909 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*PreciseDateTime div XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150909 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Varchar div XXX*/
    ObNullType/*Null*/, ObDoubleType, ObDoubleType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObDoubleType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObDoubleType,
    ObDecimalType, ObMaxType, ObMaxType,
    ObMaxType, ObDoubleType //add peiouya [DATE_TIME] 20150909 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Seq div XXX*/
    ObNullType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150909 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*CreateTime div XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150909 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*ModifyTime div XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150909 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Extend div XXX*/
    ObNullType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType,  ObMaxType, ObMaxType,
    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150909 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Bool div XXX*/
    ObNullType/*Null*/, ObDoubleType, ObFloatType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObDoubleType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObDoubleType,
    ObDecimalType, ObMaxType, ObMaxType,
    ObMaxType, ObDoubleType //add peiouya [DATE_TIME] 20150909 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*Decimal div XXX*/
    ObNullType/*Null*/, ObDecimalType, ObDoubleType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObDecimalType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObDecimalType,
    ObDecimalType, ObMaxType, ObMaxType,
    ObMaxType, ObDecimalType //add peiouya [DATE_TIME] 20150909 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*date div XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150909 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*time div XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType //add peiouya [DATE_TIME] 20150909 add yushengjuan [INT_32] 20151012
  }
  ,
  {
    /*interval div XXX*/
    ObNullType/*Null*/, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType/*Seq*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObMaxType,
    ObMaxType, ObMaxType, ObMaxType,
    ObMaxType, ObMaxType //add yushengjuan [INT_32] 20151012
  }
  //add yushengjuan [INT_32] 20151012:b
  ,
  {
    /*int_32 div XXX*/
    ObNullType/*Null*/, ObIntType/*to do make sure objtype*/, ObFloatType,
    ObDoubleType, ObMaxType, ObMaxType,
    ObDoubleType, ObMaxType/*Seq, not_support*/, ObMaxType,
    ObMaxType, ObMaxType/*Extend*/, ObDoubleType,
    ObDecimalType, ObMaxType, ObMaxType,
    ObMaxType, ObInt32Type
  }
  //add 20151012:e
};
//mod 20150906:e

struct IntegrityChecker3
{
  IntegrityChecker3()
  {
    for (ObObjType t1 = static_cast<ObObjType>(ObMinType+1);
         t1 < ObMaxType;
         t1 = static_cast<ObObjType>(t1 + 1))
    {
      for (ObObjType t2 = static_cast<ObObjType>(ObMinType+1);
           t2 < ObMaxType;
           t2 = static_cast<ObObjType>(t2 + 1))
      {
        OB_ASSERT(DIV_TYPE_PROMOTION[t1][t2] == DIV_TYPE_PROMOTION[t2][t1]);
      }
    }
  }
} DIV_TYPE_PROMOTION_CHECKER;

inline int ObExprObj::cast_to_int(int64_t &val) const
{
  int ret = OB_SUCCESS;
  ObExprObj casted_obj;

  if (OB_UNLIKELY(this->get_type() == ObNullType))
  {
    // TBSYS_LOG(WARN, "should not be null");
    // NB: becareful, it is not real error, the caller need treat it as NULL
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ObObjCastParams params;
    if (OB_SUCCESS != (ret = OB_OBJ_CAST[this->get_type()][ObIntType](params, *this, casted_obj)))
    {
      // don't report WARN when type NOT_SUPPORT
      /*
      TBSYS_LOG(WARN, "failed to cast object, err=%d from_type=%d to_type=%d",
          ret, this->get_type(), ObVarcharType);
      */
    }
    else
    {
      val = casted_obj.get_int();
    }
  }
  return ret;
}


inline int ObExprObj::cast_to_varchar(ObString &varchar, ObStringBuf &mem_buf) const
{
  int ret = OB_SUCCESS;
  ObExprObj casted_obj;
  char max_tmp_buf[128]; // other type to varchar won't takes too much space, assume 128, should be enough
  ObString tmp_str(128, 128, max_tmp_buf);

  if (OB_UNLIKELY(this->get_type() == ObNullType))
  {
    //TBSYS_LOG(WARN, "should not be null");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_LIKELY(this->get_type() != ObVarcharType))
  {
    casted_obj.set_varchar(tmp_str);
    ObObjCastParams params;
    if (OB_SUCCESS != (ret = OB_OBJ_CAST[this->get_type()][ObVarcharType](params, *this, casted_obj)))
    {
      // don't report WARN when type NOT_SUPPORT
      /*
      TBSYS_LOG(WARN, "failed to cast object, err=%d from_type=%d to_type=%d",
          ret, this->get_type(), ObVarcharType);
      */
    }
    else if (OB_SUCCESS != (ret = mem_buf.write_string(casted_obj.get_varchar(), &varchar)))
    {
      TBSYS_LOG(WARN, "fail to allocate memory for string. ret=%d", ret);
    }
  }
  else if (OB_UNLIKELY(OB_SUCCESS != (ret = mem_buf.write_string(this->get_varchar(), &varchar))))
  {
    TBSYS_LOG(WARN, "fail to allocate memory for string. ret=%d", ret);
  }
  return ret;
}

//add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
inline int ObExprObj::cast_to_decimal(ObString &varchar, ObStringBuf &mem_buf) const
{
  int ret = OB_SUCCESS;
  ObExprObj casted_obj;
  char max_tmp_buf[128]; // other type to varchar won't takes too much space, assume 128, should be enough
  ObString tmp_str(128, 128, max_tmp_buf);

  if (OB_UNLIKELY(this->get_type() == ObNullType))
  {
    //TBSYS_LOG(WARN, "should not be null");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_LIKELY(this->get_type() != ObDecimalType))
  {
    casted_obj.set_varchar(tmp_str);
    ObObjCastParams params;
    if (OB_SUCCESS != (ret = OB_OBJ_CAST[this->get_type()][ObDecimalType](params, *this, casted_obj)))
    {
      // don't report WARN when type NOT_SUPPORT
      /*
      TBSYS_LOG(WARN, "failed to cast object, err=%d from_type=%d to_type=%d",
          ret, this->get_type(), ObVarcharType);
      */
    }
    else if (OB_SUCCESS != (ret = mem_buf.write_string(casted_obj.get_varchar(), &varchar)))
    {
      TBSYS_LOG(WARN, "fail to allocate memory for string. ret=%d", ret);
    }
  }
  else if (OB_UNLIKELY(OB_SUCCESS != (ret = mem_buf.write_string(this->get_varchar(), &varchar))))
  {
    TBSYS_LOG(WARN, "fail to allocate memory for string. ret=%d", ret);
  }
  return ret;
}
//add:e

int ObExprObj::cast_to(int32_t dest_type, ObExprObj &result, ObStringBuf &mem_buf) const
{
  int err = OB_SUCCESS;
  ObObjCastParams params;
  ObString varchar;
  if (dest_type == ObVarcharType)
  {
    if (OB_SUCCESS != this->cast_to_varchar(varchar, mem_buf))
    {
      result.set_null();
    }
    else
    {
      result.set_varchar(varchar);
    }
  }
  else if (dest_type > ObMinType && dest_type < ObMaxType)
  {
    if (OB_SUCCESS != (err = OB_OBJ_CAST[this->get_type()][dest_type](params, *this, result)))
    {
      //del liuzy [date(),time()] 20151228:b
//      err = OB_SUCCESS;
      //del 20151228:e
      result.set_null();
    }
  }
  else
  {
    err = OB_INVALID_ARGUMENT;
  }
  return err;
}

//add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
int ObExprObj::cast_toV2(int32_t dest_type, ObExprObj &result, ObStringBuf &mem_buf,uint32_t precision,uint32_t scale)
{
 int err = OB_SUCCESS;
 ObObjCastParams params;
 params.precision=precision;
 params.scale=scale;
 params.is_modify=true;
 ObString varchar;
 if (dest_type == ObVarcharType)
 {
   if (OB_SUCCESS != this->cast_to_varchar(varchar, mem_buf))
   {
     result.set_null();
   }
   else
   {
     result.set_varchar(varchar);
   }
 }
 else if (dest_type > ObMinType && dest_type < ObMaxType)
 {
   if(this->get_type()==ObDecimalType&&dest_type==ObDecimalType)
   {
     ObDecimal od;
     od=this->get_decimal();    //正确的值，但此时this的成员varchar不一定是正确的
     ObDecimal od_v2;
     char buf_v2[MAX_PRINTABLE_SIZE];
     memset(buf_v2, 0, MAX_PRINTABLE_SIZE);
     ObString os_v2;
     int64_t length=od.to_string(buf_v2,MAX_PRINTABLE_SIZE);
     os_v2.assign_ptr(buf_v2,(int)length);
     od_v2.from(os_v2.ptr(),os_v2.length());
     if((precision-scale)<(od_v2.get_precision()-od_v2.get_vscale()))
     {
       err=OB_DECIMAL_UNLEGAL_ERROR;
       TBSYS_LOG(ERROR, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d",od.get_precision(),od.get_vscale());
     }
     else
     {
       od.set_precision(precision);
       od.set_scale(scale);
       //add fanqiushi_fix_decimal
       char buf_v3[MAX_PRINTABLE_SIZE];
       memset(buf_v3, 0, MAX_PRINTABLE_SIZE);
       ObString os_v3;
       int64_t lengthv2=od.to_string(buf_v3,MAX_PRINTABLE_SIZE);
       os_v3.assign_ptr(buf_v3,(int)lengthv2);
       this->set_varchar(os_v3);
       ObString real_varcahr;
       this->cast_to_varchar(real_varcahr,mem_buf);
       this->set_varchar(real_varcahr);
       this->set_decimal(od);
       //add:e
       result=*this;
     }
   }
   else
   {
     if(dest_type == ObDecimalType)
     {
       if(this->get_type()==ObVarcharType)
       {
         ObString os_v=this->get_varchar();
         if(os_v.ptr()==NULL||(int)(os_v.length())==0)
         {
           ObDecimal od2;
           ObExprObj E_obj;
           char buf2[MAX_PRINTABLE_SIZE];
           memset(buf2, 0, MAX_PRINTABLE_SIZE);
           int64_t length=0;
           length=od2.to_string(buf2, MAX_PRINTABLE_SIZE);
           ObString os2;
           os2.assign_ptr(buf2,static_cast<int32_t>(length));
           E_obj.set_varchar(os2);
           if(OB_SUCCESS != E_obj.cast_to_decimal(varchar, mem_buf) )
           {
             result.set_null();
           }
           else
           {
             result.set_varchar(varchar);
             ObDecimal od;
             if (OB_SUCCESS != (err =od.from(varchar.ptr(),varchar.length())))
             {
               TBSYS_LOG(ERROR, "fail to do from in ObExprObj::cast_toV2 ret=%d", err);
             }
             else
             {
               od.set_precision(precision);
               od.set_scale(scale);
               result.set_decimal(od);
             }
           }
         }
         else
         {
           if (OB_SUCCESS != this->cast_to_decimal(varchar, mem_buf))
           {
             result.set_null();
           }
           else
           {
             result.set_varchar(varchar);
             ObDecimal od;
             if (OB_SUCCESS != (err =od.from(varchar.ptr(),varchar.length())))
             {
               TBSYS_LOG(ERROR, "fail to do from in ObExprObj::cast_toV2 ret=%d", err);
             }
             else
             {
               if((precision-scale)<(od.get_precision()-od.get_vscale()))
               {
                 err=OB_DECIMAL_UNLEGAL_ERROR;
                 TBSYS_LOG(ERROR, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d",od.get_precision(),od.get_vscale());
                }
               else
               {
                 od.set_precision(precision);
                 od.set_scale(scale);
                 result.set_decimal(od);
               }
             }
            }
          }
        }
        else
        {
          if (OB_SUCCESS != this->cast_to_decimal(varchar, mem_buf))
          {
            result.set_null();
          }
          else
          {
            result.set_varchar(varchar);
            ObDecimal od;
            if (OB_SUCCESS != (err =od.from(varchar.ptr(),varchar.length())))
            {
              TBSYS_LOG(ERROR, "fail to do from in ObExprObj::cast_toV2 ret=%d", err);
            }
            else
            {
              if((precision-scale)<(od.get_precision()-od.get_vscale()))
              {
                err=OB_DECIMAL_UNLEGAL_ERROR;
                TBSYS_LOG(ERROR, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d",od.get_precision(),od.get_vscale());
              }
              else
              {
                od.set_precision(precision);
                od.set_scale(scale);
                result.set_decimal(od);
              }
            }
           }
         }
       }
       else if (OB_SUCCESS != (err = OB_OBJ_CAST[this->get_type()][dest_type](params, *this, result)))
       {
         err = OB_SUCCESS;
         result.set_null();
       }
   }
 }
 else
 {
   err = OB_INVALID_ARGUMENT;
 }
 return err;
}
//add:e

inline int ObExprObj::div_type_promotion(const ObExprObj &this_obj, const ObExprObj &other,
                                         ObExprObj &promoted_obj1, ObExprObj &promoted_obj2,
                                         const ObExprObj *&p_this, const ObExprObj *&p_other,
                                         bool int_div_as_double)
{
  int ret = OB_SUCCESS;
  UNUSED(int_div_as_double);
  if (OB_SUCCESS != (ret = type_promotion(DIV_TYPE_PROMOTION, this_obj, other,
                                          promoted_obj1, promoted_obj2, p_this, p_other)))
  {
    if (OB_RESULT_UNKNOWN != ret)
    {
      TBSYS_LOG(WARN, "invalid type promote for compare, err=%d", ret);
    }
  }
  return ret;
}

inline int ObExprObj::div_same_type(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  OB_ASSERT(get_type() == other.get_type()
            && this->is_numeric());
  res.type_ = get_type();
  switch(get_type())
  {
      //add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
    case ObIntType:
      res.v_.int_=this->v_.int_/other.v_.int_;
      break;
    //add e
    //add yushengjuan [INT_32] 20151012:b
    case ObInt32Type:
      res.v_.int32_ = this->v_.int32_ / other.v_.int32_;
      break;
    //add 20151012:e
    case ObFloatType:
      res.v_.float_ = this->v_.float_ / other.v_.float_;
      break;
    case ObDoubleType:
      res.v_.double_ = this->v_.double_ / other.v_.double_;
      break;
    case ObDecimalType:
      //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
      //change num_ to decimal_
      ret = this->decimal_.div(other.decimal_, res.decimal_);
      break;
      //modify:e
    default:
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected branch, type=%d", get_type());
      break;
  }
  return ret;
}

int ObExprObj::div(ObExprObj &other, ObExprObj &res, bool int_div_as_double)
{
  int ret = OB_SUCCESS;
  if(OB_UNLIKELY(other.is_zero()))
  {
    res.set_null();
    ret = OB_DIVISION_BY_ZERO;
  }
  else
  {
    ObExprObj promoted_value1;
    ObExprObj promoted_value2;
    //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
     char buf[MAX_PRINTABLE_SIZE];
     memset(buf, 0, MAX_PRINTABLE_SIZE);
     ObString os;
     os.assign_ptr(buf,MAX_PRINTABLE_SIZE);
     ObObj tmp_obj;
     tmp_obj.set_varchar(os);
     promoted_value1.assign(tmp_obj);

     char buf2[MAX_PRINTABLE_SIZE];
     memset(buf2, 0, MAX_PRINTABLE_SIZE);
     ObString os2;
     os2.assign_ptr(buf2,MAX_PRINTABLE_SIZE);
     ObObj tmp_obj2;
     tmp_obj2.set_varchar(os2);
     promoted_value2.assign(tmp_obj2);

    //add:e
    const ObExprObj *p_this = NULL;
    const ObExprObj *p_other = NULL;
    //mod peiouya [DATE_TIME] 20150906:b
    //    if (OB_SUCCESS != (ret = div_type_promotion(*this, other, promoted_value1,
    //                                            promoted_value2, p_this, p_other, int_div_as_double)))
    if (is_datetime(this->get_type()) && is_datetime(other.get_type()))
    {
      res.set_null();
      ret = OB_ERR_ILLEGAL_TYPE;
    }
    else if (OB_SUCCESS != (ret = div_type_promotion(*this, other, promoted_value1,
                                                promoted_value2, p_this, p_other, int_div_as_double)))  
    //mod 20150906:e
    {
      if (OB_RESULT_UNKNOWN == ret)
      {
        ret = OB_SUCCESS;
      }
      res.set_null();
    }
    else
    {
      ret = p_this->div_same_type(*p_other, res);
    }
  }
  return ret;
}

inline int ObExprObj::mod_type_promotion(const ObExprObj &this_obj, const ObExprObj &other,
                                         ObExprObj &promoted_obj1, ObExprObj &promoted_obj2,
                                         const ObExprObj *&p_this, const ObExprObj *&p_other)
{
  int ret = OB_SUCCESS;
  ObObjType this_type = this_obj.get_type();
  ObObjType other_type = other.get_type();
  OB_ASSERT(ObMinType < this_type && this_type < ObMaxType);
  OB_ASSERT(ObMinType < other_type && other_type < ObMaxType);
  p_this = &this_obj;
  p_other = &other;
  if (ObNullType == this_type
      || ObNullType == other_type)
  {
    ret = OB_RESULT_UNKNOWN;
  }
  if (OB_SUCCESS == ret && ObIntType != this_type)
  {
    ObObjCastParams params;
    if (OB_SUCCESS != (ret = OB_OBJ_CAST[this_type][ObIntType](params, this_obj, promoted_obj1)))
    {
      TBSYS_LOG(WARN, "failed to cast obj to int, err=%d this_type=%d",
                ret, this_type);
    }
    else
    {
      p_this = &promoted_obj1;
    }
  }
  if (OB_SUCCESS == ret && ObIntType != other_type)
  {
    ObObjCastParams params;
    if (OB_SUCCESS != (ret = OB_OBJ_CAST[other_type][ObIntType](params, other, promoted_obj2)))
    {
      TBSYS_LOG(WARN, "failed to cast obj to int, err=%d this_type=%d",
                ret, other_type);
    }
    else
    {
      p_other = &promoted_obj2;
    }
  }
  return ret;
}

/* 取模运算结果为整数 */
int ObExprObj::mod(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  ObExprObj promoted_obj1;
  ObExprObj promoted_obj2;
  const ObExprObj *p_this = this;
  const ObExprObj *p_other = &other;
  if (OB_SUCCESS != (ret = mod_type_promotion(*this, other, promoted_obj1, promoted_obj2,
                                              p_this, p_other)))
  {
    if (OB_RESULT_UNKNOWN == ret)
    {
      ret = OB_SUCCESS;
    }
    res.set_null();
  }
  else
  {
    OB_ASSERT(ObIntType == p_this->type_);
    OB_ASSERT(ObIntType == p_other->type_);
    if (p_other->is_zero())
    {
      res.set_null();
      ret = OB_DIVISION_BY_ZERO;
    }
    else
    {
      res.type_ = ObIntType;
      res.v_.int_ = p_this->v_.int_ % p_other->v_.int_;
    }
  }
  return ret;
}

int ObExprObj::negate(ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  res.type_ = get_type();
  switch(get_type())
  {
    case ObIntType:
      res.v_.int_ = -this->v_.int_; // overflow is allowd
      break;
    //add yushengjuan [INT_32] 20151012:b
    case ObInt32Type:
      res.v_.int32_ = -this->v_.int32_; //overflow is allowd
      break;
    //add 20151012:e
    case ObFloatType:
      res.v_.float_ = -this->v_.float_;
      break;
    case ObDoubleType:
      res.v_.double_ = -this->v_.double_;
      break;
    case ObDecimalType:
      //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
      //change num_ to decimal_
      ret = this->decimal_.negate(res.decimal_);
      break;
      //modify:e
    default:
      res.set_null();
      ret = OB_INVALID_ARGUMENT;
      break;
  }
  return ret;
}

int ObExprObj::old_like(const ObExprObj &pattern, ObExprObj &result) const
{
  int err = OB_SUCCESS;
  if (this->get_type() == ObNullType || pattern.get_type() == ObNullType)
  {
    result.set_null();
  }
  else if (ObVarcharType != this->get_type() || ObVarcharType != pattern.get_type())
  {
    err = OB_INVALID_ARGUMENT;
    result.set_null();
  }
  else if (pattern.varchar_.length() <= 0)
  {
    result.set_bool(true);
  }
  else
  {
    // TODO: 对于常量字符串，此处可以优化。无需每次计算sign
    uint64_t pattern_sign = ObStringSearch::cal_print(pattern.varchar_);
    int64_t pos = ObStringSearch::kr_search(pattern.varchar_, pattern_sign, this->varchar_);
    result.set_bool(pos >= 0);
  }
  return err;
}

int ObExprObj::substr(const ObExprObj &start_pos_obj, const ObExprObj &expect_length_obj, ObExprObj &result, ObStringBuf &mem_buf) const
{
  int ret = OB_SUCCESS;
  int64_t start_pos = 0;
  int64_t expect_length = 0;
  ObString varchar;

  // get start pos value
  if (start_pos_obj.is_null() || expect_length_obj.is_null())
  {
    result.set_null();
  }
  else if (OB_SUCCESS != start_pos_obj.cast_to_int(start_pos))
  {
    result.set_null();
  }
  else if (OB_SUCCESS != expect_length_obj.cast_to_int(expect_length))
  {
    result.set_null();
  }
  else
  {
    ret = this->substr(static_cast<int32_t>(start_pos), static_cast<int32_t>(expect_length), result, mem_buf);
  }
  return ret;
}

int ObExprObj::substr(const ObExprObj &start_pos, ObExprObj &result, ObStringBuf &mem_buf) const
{
  ObExprObj max_guess_len;
  max_guess_len.set_int(OB_MAX_VALID_COLUMN_ID);
  return substr(start_pos, max_guess_len, result, mem_buf);
}



int ObExprObj::substr(const int32_t start_pos, const int32_t expect_length_of_str, ObExprObj &result, ObStringBuf &mem_buf) const
{
  int err = OB_SUCCESS;
  ObString varchar;

  if (OB_UNLIKELY(this->get_type() == ObNullType))
  {
    result.set_null();
  }
  else if (OB_SUCCESS != this->cast_to_varchar(varchar, mem_buf))
  {
    result.set_null();
  }
  else
  {
    if (OB_UNLIKELY(varchar.length() <= 0 || expect_length_of_str <= 0))
    {
      // empty result string
      varchar.assign(NULL, 0);
    }
    else
    {
      int32_t start = start_pos;
      int32_t res_len = 0;
      start  = (start > 0) ? start : ((start == 0) ? 1 : start + varchar.length() + 1);
      if (OB_UNLIKELY(start <= 0 || start > varchar.length()))
      {
        varchar.assign(NULL, 0);
      }
      else
      {
        if (start + expect_length_of_str - 1 > varchar.length())
        {
          res_len = varchar.length() - start + 1;
        }
        else
        {
          res_len = expect_length_of_str;
        }
        varchar.assign(const_cast<char*>(varchar.ptr() + start - 1), res_len);
      }
    }
    result.set_varchar(varchar);
  }
  return err;
}


int ObExprObj::upper_case(ObExprObj &result, ObStringBuf &mem_buf) const
{
  ObString varchar;
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(this->get_type() == ObNullType))
  {
    result.set_null();
  }
  else
  {
    if (OB_SUCCESS != this->cast_to_varchar(varchar, mem_buf))
    {
      result.set_null();
    }
    else
    {
      for (int i = 0; i < varchar.length(); i++)
      {
        varchar.ptr()[i] = static_cast<char>(toupper(varchar.ptr()[i]));
      }
      result.set_varchar(varchar);
    }
  }
  return ret;
}

int ObExprObj::lower_case(ObExprObj &result, ObStringBuf &mem_buf) const
{
  ObString varchar;
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(this->get_type() == ObNullType))
  {
    result.set_null();
  }
  else
  {
    if (OB_SUCCESS != this->cast_to_varchar(varchar, mem_buf))
    {
      result.set_null();
    }
    else
    {
      for (int i = 0; i < varchar.length(); i++)
      {
        varchar.ptr()[i] = static_cast<char>(tolower(varchar.ptr()[i]));
      }
      result.set_varchar(varchar);
    }
  }
  return ret;
}

int ObExprObj::concat(const ObExprObj &other, ObExprObj &result, ObStringBuf &mem_buf) const
{
  int ret = OB_SUCCESS;
  char tmp_buf[OB_MAX_VARCHAR_LENGTH];
  ObString this_varchar;
  ObString that_varchar;
  //__thread char tmp_buf[OB_MAX_VARCHAR_LENGTH];
  if (get_type() == ObNullType || other.get_type() == ObNullType)
  {
    result.set_null();
  }
  else
  {
    if (OB_SUCCESS != this->cast_to_varchar(this_varchar, mem_buf))
    {
      // TBSYS_LOG(WARN, "fail to cast obj to varchar. ret=%d", ret);
      result.set_null();
    }
    else if (OB_SUCCESS != other.cast_to_varchar(that_varchar, mem_buf))
    {
      // TBSYS_LOG(WARN, "fail to cast obj to varchar. ret=%d", ret);
      result.set_null();
    }
    else
    {
      int32_t this_len = this_varchar.length();
      int32_t other_len = that_varchar.length();
      ObString tmp_varchar;
      ObString varchar;
      if (this_len + other_len > OB_MAX_VARCHAR_LENGTH)
      {
        //FIXME: 合并后的字符串长度超过了最大限制，结果设置为NULL
        result.set_null();
        ret = OB_BUF_NOT_ENOUGH;
      }
      else
      {
        memcpy(tmp_buf, this_varchar.ptr(), this_len);
        memcpy(tmp_buf + this_len, that_varchar.ptr(), other_len);
        tmp_varchar.assign(tmp_buf, this_len + other_len);
        if (OB_SUCCESS != (ret = mem_buf.write_string(tmp_varchar, &varchar)))
        {
          result.set_null();
          TBSYS_LOG(WARN, "fail to write string to membuf. ret=%d", ret);
        }
        else
        {
          result.set_varchar(varchar);
        }
      }
    }
  }
  return ret;
}

int ObExprObj::trim(const ObExprObj &trimType, const ObExprObj &trimPattern, ObExprObj &result, ObStringBuf &mem_buf) const
{
  int err = OB_SUCCESS;
  int64_t type = 0;
  ObString pattern;
  ObString src;
  ObString varchar;
  int32_t start = 0;
  int32_t end = 0;

  if (OB_SUCCESS != (err = trimType.get_int(type)))
  {
    TBSYS_LOG(WARN, "fail to get trim type. err=%d", err);
  }
  else if (OB_SUCCESS != (err = trimPattern.cast_to_varchar(pattern, mem_buf)))
  {
    // TBSYS_LOG(WARN, "fail to get trim pattern. err=%d", err);
  }
  else if (pattern.length() <= 0)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "trim pattern is empty");
  }
  else if (OB_SUCCESS != (err = this->cast_to_varchar(src, mem_buf)))
  {
    // TBSYS_LOG(WARN, "fail to get trim source. err=%d", err);
  }
  else
  {
    if (type == 0) // both
    {
      lrtrim(src, pattern, start, end);
    }
    else if (type == 1) // leading
    {
      ltrim(src, pattern, start);
      end = src.length();
    }
    else if (type == 2) // trailing
    {
      start = 0;
      rtrim(src, pattern, end);
    }
    else
    {
      err = OB_INVALID_ARGUMENT;
    }
  }
  if (OB_SUCCESS == err)
  {
    varchar.assign(src.ptr() + start, end - start);
    result.set_varchar(varchar);
  }
  else
  {
    result.set_null();
    err = OB_SUCCESS;
  }
  return err;
}

//add peiouya [IN_TYPEBUG_FIX] 20151225:b
ObObjType ObExprObj::type_promotion_for_in_op (const ObObjType left_type, const ObObjType right_type)
{
  ObObjType res_type = ObMaxType;
  if (ObMinType < left_type
      && ObMaxType > left_type
      && ObMinType < right_type
      && ObMaxType >right_type)
  {
    if (ObNullType == left_type &&  ObNullType != right_type)
    {
      res_type = right_type;
    }
    else if (ObNullType != left_type &&  ObNullType == right_type)
   {
     res_type = left_type;
    }
    else
   {
     res_type = COMPARE_TYPE_PROMOTION[left_type][right_type];
   }
  }
  return res_type;
}
//add 20151225:e

int ObExprObj::lrtrim(const ObString src, const ObString pattern, int32_t &start, int32_t &end) const
{
  int32_t i = 0;
  start = 0;
  end = src.length();
  for (i = 0; i <= src.length() - pattern.length(); i += pattern.length())
  {
    if (0 == memcmp(src.ptr() + i, pattern.ptr(), pattern.length()))
    {
      start += pattern.length();
    }
    else
    {
      break;
    }
  }
  for (i = src.length() - pattern.length(); i >= start; i -= pattern.length())
  {
    if (0 == memcmp(src.ptr() + i, pattern.ptr(), pattern.length()))
    {
      end -= pattern.length();
    }
    else
    {
      break;
    }
  }
  return OB_SUCCESS;
}


int ObExprObj::ltrim(const ObString src, const ObString pattern, int32_t &start) const
{
  int32_t i = 0;
  start = 0;
  for (i = 0; i <= src.length() - pattern.length(); i += pattern.length())
  {
    if (0 == memcmp(src.ptr() + i, pattern.ptr(), pattern.length()))
    {
      start += pattern.length();
    }
    else
    {
      break;
    }
  }
  return OB_SUCCESS;
}

int ObExprObj::rtrim(const ObString src, const ObString pattern, int32_t &end) const
{
  int32_t i = 0;
  end = src.length();
  for (i = src.length() - pattern.length(); i >= 0; i -= pattern.length())
  {
    if (0 == memcmp(src.ptr() + i, pattern.ptr(), pattern.length()))
    {
      end -= pattern.length();
    }
    else
    {
      break;
    }
  }
  return OB_SUCCESS;
}

int ObExprObj::like(const ObExprObj &pattern, ObExprObj &result) const
{
  int err = OB_SUCCESS;
  if (this->get_type() == ObNullType || pattern.get_type() == ObNullType)
  {
    result.set_null();
  }
  else if (ObVarcharType != this->get_type() || ObVarcharType != pattern.get_type())
  {
    err = OB_INVALID_ARGUMENT;
    result.set_null();
  }
  else if (pattern.varchar_.length() <= 0 && varchar_.length() <= 0)
  {
    // empty string
    result.set_bool(true);
  }
  else
  {
    bool b = ObStringSearch::is_matched(pattern.varchar_, this->varchar_);
    result.set_bool(b == true);
  }
  return err;
}

int ObExprObj::not_like(const ObExprObj &pattern, ObExprObj &result) const
{
  int err = OB_SUCCESS;
  if (this->get_type() == ObNullType || pattern.get_type() == ObNullType)
  {
    result.set_null();
  }
  else if (ObVarcharType != this->get_type() || ObVarcharType != pattern.get_type())
  {
    err = OB_INVALID_ARGUMENT;
    result.set_null();
  }
  else if (pattern.varchar_.length() <= 0 && varchar_.length() <= 0)
  {
    // empty string
    result.set_bool(false);
  }
  else
  {
    bool b = ObStringSearch::is_matched(pattern.varchar_, this->varchar_);
    result.set_bool(b != true);
  }
  return err;
}

int ObExprObj::set_decimal(const char* dec_str)
{
  int ret = OB_SUCCESS;
  ObObj obj;
   //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
 // ObNumber num;
   ObDecimal num;
  //modify e
  static const int8_t TEST_PREC = 38;
  static const int8_t TEST_SCALE = 4;
  if (OB_SUCCESS != (ret = num.from(dec_str)))
  {
    TBSYS_LOG(WARN, "failed to construct decimal from string, err=%d str=%s", ret, dec_str);
  }
 else{
    //add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
    char buff[MAX_PRINTABLE_SIZE];
    memset(buff,0,MAX_PRINTABLE_SIZE);
    int length=(int)num.to_string(buff,MAX_PRINTABLE_SIZE);
    ObString os;
    os.assign_ptr(buff,length);

    //add e
    if (OB_SUCCESS != (ret = obj.set_decimal(os, TEST_PREC, TEST_SCALE,TEST_SCALE)))
   {
    TBSYS_LOG(WARN, "obj failed to set decimal, err=%d", ret);
   }
   else
   {
    this->assign(obj);
   }
   }


  return ret;
}

void ObExprObj::set_varchar(const char* value)
{
  ObString str;
  str.assign_ptr(const_cast<char*>(value), static_cast<int32_t>(strlen(value)));
  ObObj obj;
  obj.set_varchar(str);
  this->assign(obj);
}

int ObExprObj::get_int(int64_t& value) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS != (ret = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", ret);
  }
  else
  {
    ret = obj.get_int(value);
  }
  return ret;
}

//add yushengjuan [INT_32] 20151012:b
int ObExprObj::get_int32(int32_t& value) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if(OB_SUCCESS != (ret = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", ret);
  }
  else
  {
    ret = obj.get_int32(value);
  }
  return ret;
}
//add 20151012:e

int ObExprObj::get_datetime(ObDateTime& value) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS != (ret = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", ret);
  }
  else
  {
    ret = obj.get_datetime(value);
  }
  return ret;
}

int ObExprObj::get_precise_datetime(ObPreciseDateTime& value) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS != (ret = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", ret);
  }
  else
  {
    ret = obj.get_precise_datetime(value);
  }
  return ret;
}

int ObExprObj::get_varchar(ObString& value) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS != (ret = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", ret);
  }
  else
  {
    ret = obj.get_varchar(value);
  }
  return ret;
}

int ObExprObj::get_float(float &f) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS != (ret = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", ret);
  }
  else
  {
    ret = obj.get_float(f);
  }
  return ret;
}

int ObExprObj::get_double(double &d) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS != (ret = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", ret);
  }
  else
  {
    ret = obj.get_double(d);
  }
  return ret;
}

int ObExprObj::get_decimal(char * buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (type_ != ObDecimalType)
  {
    ret = OB_OBJ_TYPE_ERROR;
  }
  else
  {
    //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
    //change num_ to decimal_
    decimal_.to_string(buf, buf_len);
    //modify:e
  }
  return ret;
}

int ObExprObj::unhex(ObExprObj &res, ObStringBuf & mem_buf)
{
  int ret = OB_SUCCESS;
  ObString result;
  ObObj obj;
  if (get_type() != ObVarcharType)
  {
    res.set_null();
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "type not match, ret=%d", ret);
  }
  else if (varchar_.length() % 2 != 0)
  {
    res.set_null();
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "length is odd, ret=%d", ret);
  }
  else
  {
    int i = 0;
    char value = 0;
    char *buf = reinterpret_cast<char*>(mem_buf.alloc(varchar_.length() / 2 + 1));
    if (NULL == buf)
    {
      res.set_null();
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(ERROR, "alloc memory failed, ret=%d", ret);
    }
    else
    {
      for (i = 0;i < varchar_.length();i = i + 2)
      {
        char &c1 = (varchar_.ptr())[i];
        char &c2 = (varchar_.ptr())[i + 1];
        if (isxdigit(c1) && isxdigit(c2))
        {
          if (c1 >= 'a' && c1 <= 'f')
          {
            value = (char)((c1 - 'a' + 10) * 16);
          }
          else if (c1 >= 'A' && c1 <= 'F')
          {
            value = (char)((c1 - 'A' + 10) * 16);
          }
          else
          {
            value = (char)((c1 - '0') * 16);
          }
          if (c2 >= 'a' && c2 <= 'f')
          {
            value = (char)(value + (c2 - 'a' + 10));
          }
          else if (c2 >= 'A' && c2 <= 'F')
          {
            value = (char)(value + (c2 - 'A' + 10));
          }
          else
          {
            value = (char)(value + (c2 - '0'));
          }
          buf[i / 2] = value;

        }
        else
        {
          ret = OB_ERR_UNEXPECTED;
          res.set_null();
          TBSYS_LOG(WARN, "data invalid, ret=%d", ret);
          break;
        }
      }
      if (OB_SUCCESS == ret)
      {
        result.assign_ptr(buf, varchar_.length() / 2);
        obj.set_varchar(result);
        res.assign(obj);
      }
    }
  }
  return ret;
}
int ObExprObj::ip_to_int(ObExprObj &res)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_UNLIKELY(get_type() != ObVarcharType))
  {
    res.set_null();
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "type not match, ret=%d", ret);
  }
  else
  {
    char buf[16];
    if (varchar_.length() > 15)
    {
      res.set_null();
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "ip format invalid, ret=%d", ret);
    }
    else
    {
      memcpy(buf, varchar_.ptr(), varchar_.length());
      int len = varchar_.length();
      buf[len] = '\0';
      int cnt = 0;
      for (int i = 0;i < len; ++i)
      {
        if (varchar_.ptr()[i] == '.')
        {
          cnt++;
        }
      }
      if (cnt != 3)
      {
        obj.set_null();
        TBSYS_LOG(WARN, "ip format invalid");
      }
      else
      {
        struct in_addr addr;
        int err = inet_aton(buf, &addr);
        if (err != 0)
        {
          obj.set_int(addr.s_addr);
        }
        else
        {
          obj.set_null();
          TBSYS_LOG(WARN, "ip format invalid");
        }
      }
      res.assign(obj);
    }
  }
  return ret;
}
int ObExprObj::int_to_ip(ObExprObj &res, ObStringBuf &mem_buf)
{
  int ret = OB_SUCCESS;
  ObString result;
  ObObj obj;
  if (OB_UNLIKELY(get_type() != ObIntType))
  {
    res.set_null();
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "type not match, ret=%d", ret);
  }
  else
  {
    // 255.255.255.255  共15 bit
    char *buf = reinterpret_cast<char*>(mem_buf.alloc(16));
    if (NULL == buf)
    {
      res.set_null();
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(ERROR, "alloc memory failed, ret=%d", ret);
    }
    else
    {
      int cnt = snprintf(buf, 16, "%ld.%ld.%ld.%ld",
          (v_.int_ & 0xFF),
          (v_.int_ >> 8) & 0xFF,
          (v_.int_ >> 16) & 0xFF,
          (v_.int_ >> 24) & 0xFF);
      OB_ASSERT(cnt > 0);
      result.assign_ptr(buf, cnt);
      obj.set_varchar(result);
      res.assign(obj);
    }
  }
  return ret;
}
int ObExprObj::hex(ObExprObj &res, ObStringBuf & mem_buf)
{
  int ret = OB_SUCCESS;
  int cnt = 0;
  ObString result;
  ObObj obj;
  if (get_type() == ObVarcharType)
  {
    int i = 0;
    char *buf = reinterpret_cast<char*>(mem_buf.alloc(varchar_.length() * 2 + 1));
    if (NULL == buf)
    {
      res.set_null();
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(ERROR, "alloc memory failed, ret=%d", ret);
    }
    else
    {
      for (i = 0;i < varchar_.length(); ++i)
      {
        char &c = (varchar_.ptr())[i];
        cnt = snprintf(buf + i * 2, 3, "%02hhx", c);
        OB_ASSERT(cnt == 2);
      }
      result.assign_ptr(buf, varchar_.length() * 2);
      obj.set_varchar(result);
      res.assign(obj);
    }
  }
  else if (get_type() == ObIntType)
  {
    char *buf = reinterpret_cast<char*>(mem_buf.alloc(16 + 1));
    if (NULL == buf)
    {
      res.set_null();
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(ERROR, "alloc memory failed, ret=%d", ret);
    }
    else
    {
      cnt = snprintf(buf, 16 + 1, "%lx", v_.int_);
      OB_ASSERT(cnt > 0);
      result.assign_ptr(buf, cnt);
      obj.set_varchar(result);
      res.assign(obj);
    }
  }
  else
  {
    ret = OB_INVALID_ARGUMENT;
    res.set_null();
    TBSYS_LOG(WARN, "type not match, ret=%d", ret);
  }
  return ret;
}
//add liuzy [datetime func] 20151019:b
int ObExprObj::check_real_type(const ObString &in) const
{
  int real_type = -1;
  std::string test_str;
  test_str.assign(in.ptr(),in.length());
  boost::regex r_timestamp(patt_timestamp);
  if (!boost::regex_match(test_str, r_timestamp))
  {
    boost::regex r_date(patt_date);
    if (!boost::regex_match(test_str, r_date))
    {
      boost::regex r_time(patt_time);
      if (boost::regex_match(test_str, r_time))
      {
        real_type = TIME_CHECK;
      }
    }
    else
    {
      real_type = DATE_CHECK;
    }
  }
  else
  {
    real_type = TIMESTAMP_CHECK;
  }
  return real_type;
}
//add 20151019:e
//add wuna [datetime func] 20150831:b
inline int ObExprObj::get_gtm(struct tm &gtm, int unexpected_type) const
{
  int ret = OB_SUCCESS;
  int64_t timestamp;
  ObExprObj in;
  int real_type = 0;
  switch(get_type())
  {
  case ObNullType:
  case ObTimeType:
    break;
  case ObDateTimeType:
    timestamp = v_.datetime_ * 1000 * 1000L;
    break;
  case ObDateType:
  case ObPreciseDateTimeType:
    timestamp = v_.precisedatetime_;
    break;
  case ObModifyTimeType:
    timestamp = v_.modifytime_;
    break;
  case ObCreateTimeType:
    timestamp = v_.createtime_;
    break;
  case ObVarcharType:
    in.set_varchar(varchar_);
    real_type = check_real_type(varchar_);
    if (unexpected_type == real_type || -1 == real_type)
    {
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(ERROR, "param's type is incorrect.");
    }
    else
    {
      ret = varchar_timestamp(in,timestamp);
      if(ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "convert varchar to timestamp failed, ret=%d", ret);
      }
    }
    break;
  default:
    TBSYS_LOG(ERROR, "unexpected branch");
    ret = OB_OBJ_TYPE_ERROR;
    break;
  }
  if(ret == OB_SUCCESS)
  {
    if (get_type() == ObTimeType)
    {
      get_timestamp(timestamp);
      timestamp = timestamp / 1000000L;
      gtm.tm_year  = 0;
      gtm.tm_mon   = 0;
      gtm.tm_mday  = 0;
      gtm.tm_hour  = static_cast<int32_t>(timestamp / 3600);
      gtm.tm_min   = static_cast<int32_t>(timestamp % 3600 / 60);
      gtm.tm_sec   = static_cast<int32_t>(timestamp % 60);
    }
    else if (get_type() != ObNullType)
    {
      time_t t = static_cast<time_t>(timestamp / 1000000L);
      localtime_r(&t, &gtm);
    }
  }
  return ret;
}
int ObExprObj::year(ObExprObj &res)
{
  int ret = OB_SUCCESS;
  int unexpected_type = TIME_CHECK;
  ObObj obj;
  struct tm gtm;
  int year = 0;
  res.set_null();
  if (get_type() == ObNullType)
  {}
  else if(get_type() == ObTimeType)
  {
    ret = OB_OBJ_TYPE_ERROR;
    TBSYS_LOG(ERROR, "year param's type not match, ret=%d", ret);
  }
  else if(OB_SUCCESS == (ret = get_gtm(gtm, unexpected_type)))
  {
    if ((gtm.tm_year + 1900) < 0)
    {
      ret = OB_OBJ_TYPE_ERROR;
      TBSYS_LOG(ERROR, "param's type not match, ret=%d", ret);
    }
    else
    {
      year = gtm.tm_year+1900;
      //mod hongchen [int64->int32] 20161118:b
      //obj.set_int(year);
      obj.set_int32(year);
      //mod hongchen [int64->int32] 20161118:e
      res.assign(obj);
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "year param is incorrect, ret=%d", ret);
  }
  return ret;
}

int ObExprObj::month(ObExprObj &res)
{
  int ret = OB_SUCCESS;
  int unexpected_type = TIME_CHECK;
  ObObj obj;
  struct tm gtm;
  int month = 0;
  res.set_null();
  if (get_type() == ObNullType)
  {}
  else if(get_type() == ObTimeType)
  {
    ret = OB_OBJ_TYPE_ERROR;
    TBSYS_LOG(ERROR, "month param's type not match, ret=%d", ret);
  }
  else if(OB_SUCCESS == (ret = get_gtm(gtm, unexpected_type)))
  {
    if ((gtm.tm_year + 1900) < 0)
    {
      ret = OB_OBJ_TYPE_ERROR;
      TBSYS_LOG(ERROR, "param's type not match, ret=%d", ret);
    }
    else
    {
      month = gtm.tm_mon+1;
      //mod hongchen [int64->int32] 20161118:b
      //obj.set_int(month);
      obj.set_int32(month);
      //mod hongchen [int64->int32] 20161118:e
      res.assign(obj);
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "month param is incorrect, ret=%d", ret);
  }
  return ret;
}

int ObExprObj::day(ObExprObj &res)
{
  int ret = OB_SUCCESS;
  int unexpected_type = TIME_CHECK;
  ObObj obj;
  struct tm gtm;
  int day = 0;
  res.set_null();
  if (get_type() == ObNullType)
  {}
  else if(get_type() == ObTimeType)
  {
    ret = OB_OBJ_TYPE_ERROR;
    TBSYS_LOG(ERROR, "day param's type not match, ret=%d", ret);
  }
  else if(OB_SUCCESS == (ret = get_gtm(gtm, unexpected_type)))
  {
    if ((gtm.tm_year + 1900) < 0)
    {
      ret = OB_OBJ_TYPE_ERROR;
      TBSYS_LOG(ERROR, "param's type not match, ret=%d", ret);
    }
    else
    {
      day = gtm.tm_mday;
      //mod hongchen [int64->int32] 20161118:b
      //obj.set_int(day);
      obj.set_int32(day);
      //mod hongchen [int64->int32] 20161118:e
      res.assign(obj);
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "day param is incorrect, ret=%d", ret);
  }
  return ret;
}
int ObExprObj::days(ObExprObj &res)
{
  int ret = OB_SUCCESS;
  int unexpected_type = TIME_CHECK;
  ObObj obj;
  time_t days = 0;
  struct tm gtm;
  res.set_null();
  if(get_type() == ObNullType)
  {}
  else if(get_type() == ObTimeType)
  {
    ret = OB_OBJ_TYPE_ERROR;
    TBSYS_LOG(ERROR, "days param's type not match, ret=%d", ret);
  }
  else if (OB_SUCCESS == (ret = get_gtm(gtm, unexpected_type)))
  {
    if ((gtm.tm_year + 1900) < 0)
    {
      ret = OB_OBJ_TYPE_ERROR;
      TBSYS_LOG(ERROR, "param's type not match, ret=%d", ret);
    }
    else
    {
      long delsum;
      int temp;
      int y = gtm.tm_year+1900;       /* may be < 0 temporarily */
      int month = gtm.tm_mon+1;
      int day = gtm.tm_mday;
      if (y == 0 || month == 0 ||day == 0)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR,"days param is incorrect.");
      }
      else
      {
        /* Cast to int to be able to handle month == 0 */
        delsum= (long) (365 * y + 31 *((int) month - 1) + (int) day);
        if (month <= 2)
          y--;
        else
          delsum-= (long) ((int) month * 4 + 23) / 10;
        temp=(int) ((y/100+1)*3)/4;
        TBSYS_LOG(DEBUG,"year: %d  month: %d  day: %d -> daynr: %ld",
                  y+(month <= 2),month,day,delsum+y/4-temp);
        days =delsum+(int) y/4-temp-365;
        //mod hongchen [int64->int32] 20161118:b
        //obj.set_int(days);
        OB_ASSERT(days <= INT32_MAX);
        obj.set_int32(static_cast<int>(days));
        //mod hongchen [int64->int32] 20161118:e
        res.assign(obj);
      }
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "days param is incorrect, ret=%d", ret);
  }
  return ret;
}
//add 20150831:e
//add liuzy [datetime func] 20150901:b
/*Exp: get hour from timestamps*/
int ObExprObj::hour(ObExprObj &res)
{
  int ret = OB_SUCCESS;
  int unexpected_type = DATE_CHECK;
  ObObj obj;
  struct tm gtm;
  int hour = 0;
  res.set_null();
  if (get_type() == ObNullType)
  {}
  else if(get_type() == ObDateType)
  {
    ret = OB_OBJ_TYPE_ERROR;
    TBSYS_LOG(ERROR, "hour param's type not match, ret=%d", ret);
  }
  else if (OB_SUCCESS == (ret = get_gtm(gtm, unexpected_type)))
  {
    hour = gtm.tm_hour;
    //mod hongchen [int64->int32] 20161118:b
    //obj.set_int32(hour);
    obj.set_int(hour);
    //mod hongchen [int64->int32] 20161118:e
    res.assign(obj);
  }
  else
  {
    TBSYS_LOG(ERROR, "hour param is incorrect, ret=%d", ret);
  }
  return ret;
}
/*Exp: get minute from timestamps*/
int ObExprObj::minute(ObExprObj &res)
{
  int ret = OB_SUCCESS;
  int unexpected_type = DATE_CHECK;
  ObObj obj;
  struct tm gtm;
  int minute = 0;
  res.set_null();
  if (get_type() == ObNullType)
  {}
  else if(get_type() == ObDateType)
  {
    ret = OB_OBJ_TYPE_ERROR;
    TBSYS_LOG(ERROR, "minute param's type not match, ret=%d", ret);
  }
  else if (OB_SUCCESS == (ret = get_gtm(gtm, unexpected_type)))
  {
    minute = gtm.tm_min;
    //mod hongchen [int64->int32] 20161118:b
    //obj.set_int(minute);
    obj.set_int32(minute);
    //mod hongchen [int64->int32] 20161118:b
    res.assign(obj);
  }
  else
  {
    TBSYS_LOG(ERROR, "minute param is incorrect, ret=%d", ret);
  }
  return ret;
}
/*Exp: get second from timestamps*/
int ObExprObj::second(ObExprObj &res)
{
  int ret = OB_SUCCESS;
  int unexpected_type = DATE_CHECK;
  ObObj obj;
  int second = 0;
  struct tm gtm;
  res.set_null();
  if (get_type() == ObNullType)
  {}
  else if(get_type() == ObDateType)
  {
    ret = OB_OBJ_TYPE_ERROR;
    TBSYS_LOG(ERROR, "second param's type not match, ret=%d", ret);
  }
  else if (OB_SUCCESS == (ret = get_gtm(gtm, unexpected_type)))
  {
    second = gtm.tm_sec;
    //mod hongchen [int64->int32] 20161118:b
    //obj.set_int(second);
    obj.set_int32(second);
    //mod hongchen [int64->int32] 20161118:e
    res.assign(obj);
  }
  else
  {
    TBSYS_LOG(ERROR, "second param is incorrect, ret=%d", ret);
  }
  return ret;
}
//add 20150901:e
int ObExprObj::varchar_length(ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  if (ObVarcharType != get_type())
  {
    ret = OB_INVALID_ARGUMENT;
    res.set_null();
  }
  else
  {
    res.type_ = ObIntType;
    res.v_.int_ = varchar_.length();
  }
  return ret;
}

ObObj ObExprObj::type_arithmetic(const ObObj& t1, const ObObj& t2)
{
  ObObj ret;
  ret.meta_.type_ = ObNullType;
  int err = OB_SUCCESS;
  ObExprObj v1;
  v1.type_ = t1.get_type();
  ObExprObj v2;
  v2.type_ = t2.get_type();
  ObExprObj promoted1;
  ObExprObj promoted2;
  //add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
  char buf[MAX_PRINTABLE_SIZE];
  memset(buf, 0, MAX_PRINTABLE_SIZE);
  ObString os;
  os.assign_ptr(buf,MAX_PRINTABLE_SIZE);
  ObObj tmp_obj;
  tmp_obj.set_varchar(os);
  promoted1.assign(tmp_obj);

  char buf2[MAX_PRINTABLE_SIZE];
  memset(buf2, 0, MAX_PRINTABLE_SIZE);
  ObString os2;
  os2.assign_ptr(buf2,MAX_PRINTABLE_SIZE);
  ObObj tmp_obj2;
  tmp_obj2.set_varchar(os2);
  promoted2.assign(tmp_obj2);

  //add:e
  const ObExprObj *p_this = NULL;
  const ObExprObj *p_other = NULL;
  if (OB_SUCCESS != (err = arith_type_promotion(v1, v2, promoted1, promoted2, p_this, p_other)))
  {
    TBSYS_LOG(WARN, "failed to promote type, err=%d", err);
  }
  else
  {
    ret.meta_.type_ = p_this->type_;
    if (ObDecimalType == p_this->type_)
    {
      // @todo decimal precision and scale
    }
  }
  return ret;
}
//add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
ObObj ObExprObj::type_add_v2(const ObObj& t1, const ObObj& t2){

   ObObj ret;
   ObObjType type1,type2;
   type1=t1.get_type();
   type2=t2.get_type();
   ret.set_type(ARITHMETIC_TYPE_PROMOTION[type1][type2]);

   return ret;

}
ObObj ObExprObj::type_sub_v2(const ObObj& t1, const ObObj& t2){

   ObObj ret;
   ObObjType type1,type2;
   type1=t1.get_type();
   type2=t2.get_type();
   ret.set_type(ARITHMETIC_TYPE_PROMOTION[type1][type2]);

   return ret;

}
ObObj ObExprObj::type_mul_v2(const ObObj& t1, const ObObj& t2){

   ObObj ret;
   ObObjType type1,type2;
   type1=t1.get_type();
   type2=t2.get_type();
   ret.set_type(ARITHMETIC_TYPE_PROMOTION[type1][type2]);

   return ret;

}

ObObj ObExprObj::type_div_v2(const ObObj& t1, const ObObj& t2, bool int_div_as_double){
   UNUSED(int_div_as_double);
   ObObj ret;
   ObObjType type1,type2;
   type1=t1.get_type();
   type2=t2.get_type();
   ret.set_type(DIV_TYPE_PROMOTION[type1][type2]);

   return ret;

}
//add e
ObObj ObExprObj::type_add(const ObObj& t1, const ObObj& t2)
{
  return type_arithmetic(t1, t2);
}

ObObj ObExprObj::type_sub(const ObObj& t1, const ObObj& t2)
{
  return type_arithmetic(t1, t2);
}

ObObj ObExprObj::type_mul(const ObObj& t1, const ObObj& t2)
{
  return type_arithmetic(t1, t2);
}

ObObj ObExprObj::type_div(const ObObj& t1, const ObObj& t2, bool int_div_as_double)
{
  ObObj ret;
  ret.meta_.type_ = ObNullType;
  int err = OB_SUCCESS;
  ObExprObj v1;
  v1.type_ = t1.get_type();
  ObExprObj v2;
  v2.type_ = t2.get_type();
  ObExprObj promoted_value1;
  ObExprObj promoted_value2;
  //add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
    char buf[MAX_PRINTABLE_SIZE];
    memset(buf, 0, MAX_PRINTABLE_SIZE);
    ObString os;
    os.assign_ptr(buf,MAX_PRINTABLE_SIZE);
    ObObj tmp_obj;
    tmp_obj.set_varchar(os);
    promoted_value1.assign(tmp_obj);

    char buf2[MAX_PRINTABLE_SIZE];
    memset(buf2, 0, MAX_PRINTABLE_SIZE);
    ObString os2;
    os2.assign_ptr(buf2,MAX_PRINTABLE_SIZE);
    ObObj tmp_obj2;
    tmp_obj2.set_varchar(os2);
    promoted_value2.assign(tmp_obj2);
    //add e
  const ObExprObj *p_this = NULL;
  const ObExprObj *p_other = NULL;
  if (OB_SUCCESS != (err = div_type_promotion(v1, v2, promoted_value1, promoted_value2,
                                                  p_this, p_other, int_div_as_double)))
  {
    TBSYS_LOG(WARN, "failed to promote type, err=%d", err);
  }
  else
  {
    ret.meta_.type_ = p_this->type_;
    if (ObDecimalType == p_this->type_)
    {
      // @todo decimal precision and scale
    }
  }
  return ret;
}

ObObj ObExprObj::type_mod(const ObObj& t1, const ObObj& t2)
{
  ObObj ret;
  ret.meta_.type_ = ObNullType;
  int err = OB_SUCCESS;
  ObExprObj v1;
  v1.type_ = t1.get_type();
  ObExprObj v2;
  v2.type_ = t2.get_type();
  ObExprObj promoted_obj1;
  ObExprObj promoted_obj2;
  const ObExprObj *p_this = &v1;
  const ObExprObj *p_other = &v2;
  if (OB_SUCCESS != (err = mod_type_promotion(v1, v2, promoted_obj1, promoted_obj2,
                                              p_this, p_other)))
  {
    TBSYS_LOG(WARN, "failed to promote type, err=%d type1=%d type2=%d", err, t1.get_type(), t2.get_type());
  }
  else
  {
    ret.meta_.type_ = ObIntType;
  }
  return ret;
}

ObObj ObExprObj::type_negate(const ObObj& t1)
{
  ObObj ret;
  ret.meta_.type_ = ObNullType;
  switch(t1.get_type())
  {
    case ObIntType:
    case ObFloatType:
    case ObDoubleType:
    case ObDecimalType:
      ret.meta_.type_ = static_cast<uint8_t>(t1.get_type());
      break;
    default:
      TBSYS_LOG(WARN, "not supported type for negate, type=%d", t1.get_type());
      break;
  }
  return ret;
}

ObObj ObExprObj::type_varchar_length(const ObObj& t1)
{
  ObObj ret;
  ret.meta_.type_ = ObNullType;
  if (ObVarcharType != t1.get_type())
  {
    TBSYS_LOG(WARN, "not supported type for varchar_length, type=%d", t1.get_type());
  }
  else
  {
    ret.meta_.type_ = ObIntType;
  }
  return ret;
}

int64_t ObExprObj::to_string(char* buffer, const int64_t length) const
{
  ObObj tmp;
  this->to(tmp);
  return tmp.to_string(buffer, length);
}
//add gaojt [ListAgg][JHOBv0.1]20150104:b
int64_t ObExprObj::to_string_v2(std::string &s) const
{
  ObObj tmp;
  this->to(tmp);
  return tmp.to_string_v2(s);
}
//add 20150104:e
