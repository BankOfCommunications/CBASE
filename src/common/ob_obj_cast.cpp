/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_obj_cast.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_obj_cast.h"
#include <time.h>
#include <math.h>//add liuzy [datetime bug] 20150906
#include <limits.h>//add yushengjuan [INT_32] 20151015

namespace oceanbase
{
  namespace common
  {
    static int identity(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      if (&in != &out)
      {
        out = in;
      }
      return OB_SUCCESS;
    }
    static int not_support(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      UNUSED(in);
      UNUSED(out);
      return OB_NOT_SUPPORTED;
    }
    ////////////////////////////////////////////////////////////////
    // Int -> XXX
    static int int_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObIntType);
      out.set_float(static_cast<float>(in.get_int()));
      return OB_SUCCESS;
    }
    static int int_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObIntType);
      out.set_double(static_cast<double>(in.get_int()));
      return OB_SUCCESS;
    }
    static int int_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObIntType);
      out.set_datetime(static_cast<ObDateTime>(in.get_int()));
      return OB_SUCCESS;
    }
    static int int_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObIntType);
      out.set_precise_datetime(static_cast<ObPreciseDateTime>(in.get_int()));
      return OB_SUCCESS;
    }
    static int varchar_printf(ObExprObj &out, const char* format, ...)  __attribute__ ((format (printf, 2, 3)));
    static int varchar_printf(ObExprObj &out, const char* format, ...)
    {
      int ret = OB_SUCCESS;
      ObString varchar = out.get_varchar();
      if (NULL == varchar.ptr() || varchar.length() <= 0)
      {
        TBSYS_LOG(WARN, "output buffer for varchar not enough, buf_len=%u", varchar.length());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        va_list args;
        va_start(args, format);
        int length = vsnprintf(varchar.ptr(), varchar.length(), format, args);
        va_end(args);
        ObString varchar2(varchar.length(), length, varchar.ptr());
        out.set_varchar(varchar2);
      }
      return ret;
    }
    static int int_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObIntType);
      ret = varchar_printf(out, "%ld", in.get_int());
      return ret;
    }
    //add fanqiushi
    static int int_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      OB_ASSERT(in.get_type() == ObIntType);
      ret = varchar_printf(out, "%ld", in.get_int());
      if(ret==OB_SUCCESS)
      {
          ObString os;
          out.get_varchar(os);
          ObDecimal od;
          if(OB_SUCCESS!=(ret=od.from(os.ptr(),os.length())))
          {
                TBSYS_LOG(WARN, "failed to do from in int_decimal,os=%.*s", os.length(),os.ptr());
          }
          else
          {
             // TBSYS_LOG(WARN, "test::f2 ,,(p==%d,,s==%d,,,,od.p=%d,,,s=%d", params.precision,params.scale,od.get_precision(),od.get_vscale());
              if(params.is_modify)
              {
                  //mod dolphin[coalesce return type]@20151126:b
                  // if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
                  if((params.precision >0 && params.scale < params.precision ) && ((params.precision-params.scale)<(od.get_precision()-od.get_vscale())))
                  {
                              ret=OB_DECIMAL_UNLEGAL_ERROR;
                              TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
                  }
                  else
                  {
                      od.set_precision(params.precision);
                      od.set_scale(params.scale);
                  }

              }
              if(ret==OB_SUCCESS)
              {
                  out.set_varchar(os);
                  out.set_decimal(od);
              }
          }
      }
      return ret;
    }
    //add:e
    static int int_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObIntType);
      out.set_ctime(static_cast<ObCreateTime>(in.get_int()));
      return OB_SUCCESS;
    }
    static int int_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObIntType);
      out.set_mtime(static_cast<ObModifyTime>(in.get_int()));
      return OB_SUCCESS;
    }
    static int int_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObIntType);
      out.set_bool(static_cast<bool>(in.get_int()));
      return OB_SUCCESS;
    }
    /*static int int_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObIntType);
      ObNumber num;
      num.from(in.get_int());
      out.set_decimal(num);     // @todo optimize
      return OB_SUCCESS;
    }*/
    //add yushengjuan [INT_32] 20150929:b
    static int int_int32(const ObObjCastParams &params,const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      int ret = OB_SUCCESS;
      OB_ASSERT(in.get_type() == ObIntType);
      if(INT_MIN <= in.get_int() && in.get_int() <= INT_MAX)
      {
        out.set_int32(static_cast<int32_t>(in.get_int()));
      }
      else
      {
        ret = OB_VALUE_OUT_OF_RANGE;
      }
      return ret;
    }
    //add 20150929:e
    ////////////////////////////////////////////////////////////////
    // Float -> XXX
    static int float_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObFloatType);
      out.set_int(static_cast<int64_t>(in.get_float()));
      return OB_SUCCESS;
    }
    static int float_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObFloatType);
      out.set_double(static_cast<double>(in.get_float()));
      return OB_SUCCESS;
    }
    static int float_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObFloatType);
      out.set_datetime(static_cast<ObDateTime>(in.get_float()));
      return OB_SUCCESS;
    }
    static int float_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObFloatType);
      out.set_precise_datetime(static_cast<ObPreciseDateTime>(in.get_float()));
      return OB_SUCCESS;
    }
    static int float_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObFloatType);
      return varchar_printf(out, "%f", in.get_float());
    }
    static int float_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObFloatType);
      out.set_ctime(static_cast<ObCreateTime>(in.get_float()));
      return OB_SUCCESS;
    }
    static int float_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObFloatType);
      out.set_mtime(static_cast<ObModifyTime>(in.get_float()));
      return OB_SUCCESS;
    }
    static int float_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObFloatType);
      out.set_bool(static_cast<bool>(in.get_float()));
      return OB_SUCCESS;
    }
    static int float_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      /*int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObFloatType);
      static const int64_t MAX_FLOAT_PRINT_SIZE = 64;
      char buf[MAX_FLOAT_PRINT_SIZE];
      snprintf(buf, MAX_FLOAT_PRINT_SIZE, "%f", in.get_float());
      ObNumber num;
      if (OB_SUCCESS != (ret = num.from(buf)))
      {
        TBSYS_LOG(WARN, "failed to convert float to decimal, err=%d", ret);
      }
      else
      {
        out.set_decimal(num);
      }
      return ret;*/
      int ret = OB_SUCCESS;
      OB_ASSERT(in.get_type() == ObFloatType);
      ret=varchar_printf(out, "%f", in.get_float());
      if(ret==OB_SUCCESS)
      {
          ObString os;
          out.get_varchar(os);
          ObDecimal od;
          if(OB_SUCCESS!=(ret=od.from(os.ptr(),os.length())))
          {
                TBSYS_LOG(WARN, "failed to do from in float_decimal,os=%.*s", os.length(),os.ptr());
          }
          else
          {
             // TBSYS_LOG(WARN, "test::f2 ,,(p==%d,,s==%d,,,,od.p=%d,,,s=%d", params.precision,params.scale,od.get_precision(),od.get_vscale());
              if(params.is_modify)
              {
                  //mod dolphin[coalesce return type]@20151126:b
                  // if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
                  if((params.precision >0 && params.scale < params.precision ) && ((params.precision-params.scale)<(od.get_precision()-od.get_vscale())))
                  {
                      ret=OB_DECIMAL_UNLEGAL_ERROR;
                      TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
                  }
                  else
                  {
                      od.set_precision(params.precision);
                      od.set_scale(params.scale);
                  }

              }
              if(ret==OB_SUCCESS)
              {
                  out.set_varchar(os);
                  out.set_decimal(od);
              }
          }
      }
      return ret;
    }
    //add yushengjuan [INT_32] 20151009:b
    static int float_int32(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      int ret = OB_SUCCESS;
      OB_ASSERT(in.get_type() == ObFloatType);
      if(INT_MIN <= in.get_float() && in.get_float() <= INT_MAX)
      {
        out.set_int32(static_cast<int32_t>(in.get_float()));
      }
      else
      {
        ret = OB_VALUE_OUT_OF_RANGE;
      }
      return OB_SUCCESS;
    }
    //add 20151009:e
    ////////////////////////////////////////////////////////////////
    // Double -> XXX
    static int double_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDoubleType);
      out.set_int(static_cast<int64_t>(in.get_double()));
      return OB_SUCCESS;
    }
    static int double_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDoubleType);
      out.set_float(static_cast<float>(in.get_double()));
      return OB_SUCCESS;
    }
    static int double_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDoubleType);
      out.set_datetime(static_cast<ObDateTime>(in.get_double()));
      return OB_SUCCESS;
    }
    static int double_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDoubleType);
      out.set_precise_datetime(static_cast<ObPreciseDateTime>(in.get_double()));
      return OB_SUCCESS;
    }
    static int double_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDoubleType);
      return varchar_printf(out, "%f", in.get_double());
    }
    static int double_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDoubleType);
      out.set_ctime(static_cast<ObCreateTime>(in.get_double()));
      return OB_SUCCESS;
    }
    static int double_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDoubleType);
      out.set_mtime(static_cast<ObModifyTime>(in.get_double()));
      return OB_SUCCESS;
    }
    static int double_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDoubleType);
      out.set_bool(static_cast<bool>(in.get_double()));
      return OB_SUCCESS;
    }
    static int double_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      OB_ASSERT(in.get_type() == ObDoubleType);
      ret=varchar_printf(out, "%f", in.get_double());
      if(ret==OB_SUCCESS)
      {
          ObString os;
          out.get_varchar(os);
          ObDecimal od;
          if(OB_SUCCESS!=(ret=od.from(os.ptr(),os.length())))
          {
                TBSYS_LOG(WARN, "failed to do from in double_decimal,os=%.*s", os.length(),os.ptr());
          }
          else
          {
             // TBSYS_LOG(WARN, "test::f2 ,,(p==%d,,s==%d,,,,od.p=%d,,,s=%d", params.precision,params.scale,od.get_precision(),od.get_vscale());
              if(params.is_modify)
              {
                  //mod dolphin[coalesce return type]@20151126:b
                  // if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
                  if((params.precision >0 && params.scale < params.precision ) && ((params.precision-params.scale)<(od.get_precision()-od.get_vscale())))
                  {
                      ret=OB_DECIMAL_UNLEGAL_ERROR;
                      TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
                  }
                  else
                  {
                      od.set_precision(params.precision);
                      od.set_scale(params.scale);
                  }

              }
              if(ret==OB_SUCCESS)
              {
                  out.set_varchar(os);
                  out.set_decimal(od);
              }
          }
      }
      return ret;
    }
    //add yushengjuan [INT_32] 20151009:b
    static int double_int32(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      int ret = OB_SUCCESS;
      OB_ASSERT(in.get_type() == ObDoubleType);
      if(INT_MIN <= in.get_double() && in.get_double() <= INT_MAX)
      {
        out.set_int32(static_cast<int32_t>(in.get_double()));
      }
      else
      {
        ret = OB_VALUE_OUT_OF_RANGE;
      }
      return OB_SUCCESS;
    }
    //add 20151009:e
    ////////////////////////////////////////////////////////////////
    // DateTime -> XXX
    static int datetime_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      out.set_int(static_cast<int>(in.get_datetime()));
      return OB_SUCCESS;
    }
    static int datetime_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      out.set_float(static_cast<float>(in.get_datetime()));
      return OB_SUCCESS;
    }
    static int datetime_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      out.set_double(static_cast<double>(in.get_datetime()));
      return OB_SUCCESS;
    }
    static int datetime_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      out.set_precise_datetime(static_cast<ObPreciseDateTime>(1000L*1000L*in.get_datetime()));
      return OB_SUCCESS;
    }
    static int datetime_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      time_t t = static_cast<time_t>(in.get_datetime());
      struct tm gtm;
      localtime_r(&t, &gtm);
      ret = varchar_printf(out, "%04d-%02d-%02d %02d:%02d:%02d",
                           gtm.tm_year+1900, gtm.tm_mon+1, gtm.tm_mday,
                           gtm.tm_hour, gtm.tm_min, gtm.tm_sec);
      return ret;
    }
    static int datetime_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      out.set_ctime(static_cast<ObCreateTime>(1000L*1000L*in.get_datetime()));
      return OB_SUCCESS;
    }
    static int datetime_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      out.set_mtime(static_cast<ObModifyTime>(1000L*1000L*in.get_datetime()));
      return OB_SUCCESS;
    }
    static int datetime_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      out.set_bool(static_cast<bool>(in.get_datetime()));
      return OB_SUCCESS;
    }
    static int datetime_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      /*UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      ObNumber num;
      num.from(static_cast<int64_t>(in.get_datetime()));
      out.set_decimal(num);
      return OB_SUCCESS;*/
      int ret = OB_SUCCESS;
      OB_ASSERT(in.get_type() == ObDateTimeType);
      //mod liuzy [datetime to decimal format fix] 20151124:b
      //Exp:原有的式样是将微秒数作为decimal,现在修改为‘YYYYMMDDHHMMSS.SSSSSS’格式
//      int64_t date_time_int=static_cast<int64_t>(in.get_datetime());
//      ret = varchar_printf(out, "%ld", date_time_int);
      int32_t usec = abs(static_cast<int32_t>(in.get_datetime() % 1000000L));
      time_t t = static_cast<time_t>(in.get_datetime()/1000000L);
      //add hongchen [USEC_BUG_FIX] 20161031:b
      if (0 > in.get_datetime() && 0 != usec)
      {
          t = t - 1;
          usec = 1000000 - usec;
      }
      //add hongchen [USEC_BUG_FIX] 20161031:e
      struct tm gtm;
      localtime_r(&t, &gtm);
      ret = varchar_printf(out, "%04d%02d%02d%02d%02d%02d.%06d",
                           gtm.tm_year + 1900, gtm.tm_mon + 1, gtm.tm_mday,
                           gtm.tm_hour, gtm.tm_min, gtm.tm_sec, usec);
      //mod 20151124:e

      if(ret==OB_SUCCESS)
      {
          ObString os;
          out.get_varchar(os);
          ObDecimal od;
          if(OB_SUCCESS!=(ret=od.from(os.ptr(),os.length())))
          {
                TBSYS_LOG(WARN, "failed to do from in datetime_decimal,os=%.*s", os.length(),os.ptr());
          }
          else
          {
             // TBSYS_LOG(WARN, "test::f2 ,,(p==%d,,s==%d,,,,od.p=%d,,,s=%d", params.precision,params.scale,od.get_precision(),od.get_vscale());
              if(params.is_modify)
              {
                  //mod dolphin[coalesce return type]@20151126:b
                  // if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
                  if((params.precision >0 && params.scale < params.precision ) && ((params.precision-params.scale)<(od.get_precision()-od.get_vscale())))
                  {
                      ret=OB_DECIMAL_UNLEGAL_ERROR;
                      TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
                  }
                  else
                  {
                      od.set_precision(params.precision);
                      od.set_scale(params.scale);
                  }

              }
              if(ret==OB_SUCCESS)
              {
                  out.set_varchar(os);
                  out.set_decimal(od);
              }
          }
      }
      return ret;
    }
    //add peiouya [DATE_TIME] 20150906:b
    static int varchar_date(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out);
    static int varchar_time(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out);
    static int datetime_date(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      int ret = OB_SUCCESS;
      time_t t = static_cast<time_t>(in.get_datetime());
      struct tm gtm;
      localtime_r(&t, &gtm);
      if (0 <= gtm.tm_year && 0 <= gtm.tm_mon && 0 < gtm.tm_mday)
      {
        ObExprObj casted_obj;
        char max_tmp_buf[128]; // other type to varchar won't takes too much space, assume 128, should be enough
        ObString tmp_str(128, 128, max_tmp_buf);
        casted_obj.set_varchar (tmp_str);
        ret = varchar_printf(casted_obj, "%04d-%02d-%02d", gtm.tm_year+1900, gtm.tm_mon+1, gtm.tm_mday);
        if (OB_SUCCESS == ret)
        {
          //not need to check return value
          varchar_date(params, casted_obj, out);
        }
      }
      else
      {
        ret = OB_CONVERT_ERROR;
      }
      return ret;
    }
    static int datetime_time(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      int ret = OB_SUCCESS;
      time_t t = static_cast<time_t>(in.get_datetime());
      struct tm gtm;
      localtime_r(&t, &gtm);
      ObExprObj casted_obj;
      char max_tmp_buf[128]; // other type to varchar won't takes too much space, assume 128, should be enough
      ObString tmp_str(128, 128, max_tmp_buf);
      casted_obj.set_varchar (tmp_str);
      ret = varchar_printf(casted_obj, "%02d:%02d:%02d", gtm.tm_hour, gtm.tm_min, gtm.tm_sec);
      if (OB_SUCCESS == ret)
      {
        //not need to check return value
        varchar_time(params, casted_obj, out);
      }
      else
      {
        ret = OB_CONVERT_ERROR;
      }
      return ret;
    }
    //add 20150906:e
    ////////////////////////////////////////////////////////////////
    // PreciseDateTime -> XXX
    static int pdatetime_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      out.set_int(static_cast<int64_t>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int pdatetime_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      out.set_float(static_cast<float>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int pdatetime_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      out.set_double(static_cast<double>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int pdatetime_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      out.set_datetime(static_cast<ObDateTime>(in.get_precise_datetime()/1000000L));
      return OB_SUCCESS;
    }
    static int timestamp_varchar(const int64_t timestamp, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      time_t t = static_cast<time_t>(timestamp/1000000L);
      //mod liuzy [datetime bug] 20150909:b
      /*Exp: usec before 1970-01-01 08:00:00 is negative, change it to positive, when we output it*/
      //int64_t usec = static_cast<int32_t>(timestamp % 1000000L);
      int32_t usec = abs(static_cast<int32_t>(timestamp % 1000000L));
      //mod 20150909:e
      //add hongchen [USEC_BUG_FIX] 20161031:b
      if (0 > timestamp && 0 != usec)
      {
          t = t - 1;
          usec = 1000000 - usec;
      }
      //add hongchen [USEC_BUG_FIX] 20161031:e
      struct tm gtm;
      localtime_r(&t, &gtm);
      ret = varchar_printf(out, "%04d-%02d-%02d %02d:%02d:%02d.%06d",
                           gtm.tm_year+1900, gtm.tm_mon+1, gtm.tm_mday,
                           gtm.tm_hour, gtm.tm_min, gtm.tm_sec, usec);
      return ret;
    }
    static int pdatetime_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      return timestamp_varchar(in.get_precise_datetime(), out);
    }
    static int pdatetime_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      out.set_ctime(static_cast<ObCreateTime>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int pdatetime_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      out.set_mtime(static_cast<ObModifyTime>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int pdatetime_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      out.set_bool(static_cast<bool>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int pdatetime_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      /*UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      ObNumber num;
      num.from(static_cast<int64_t>(in.get_precise_datetime()));
      out.set_decimal(num);
      return OB_SUCCESS;*/
      int ret = OB_SUCCESS;
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      //mod liuzy [datetime to decimal format fix] 20151124:b
      //Exp:原有的式样是将微秒数作为decimal值,现在修改为‘YYYYMMDDHHMMSS.SSSSSS’格式
//      int64_t pdate_time_int=static_cast<int64_t>(in.get_precise_datetime());
//      ret = varchar_printf(out, "%ld", pdate_time_int);
      int32_t usec = abs(static_cast<int32_t>(in.get_precise_datetime() % 1000000L));
      time_t t = static_cast<time_t>(in.get_precise_datetime()/1000000L);
      //add hongchen [USEC_BUG_FIX] 20161031:b
      if ( 0 > in.get_precise_datetime() && 0 != usec)
      {
          t = t - 1;
          usec = 1000000 - usec;
      }
      //add hongchen [USEC_BUG_FIX] 20161031:e
      struct tm gtm;
      localtime_r(&t, &gtm);
      ret = varchar_printf(out, "%04d%02d%02d%02d%02d%02d.%06d",
                           gtm.tm_year + 1900, gtm.tm_mon + 1, gtm.tm_mday,
                           gtm.tm_hour, gtm.tm_min, gtm.tm_sec, usec);
      //mod 20151124:e
      if(ret==OB_SUCCESS)
      {
          ObString os;
          out.get_varchar(os);
          ObDecimal od;
          if(OB_SUCCESS!=(ret=od.from(os.ptr(),os.length())))
          {
                TBSYS_LOG(WARN, "failed to do from in pdatetime_decimal,os=%.*s", os.length(),os.ptr());
          }
          else
          {
             // TBSYS_LOG(WARN, "test::f2 ,,(p==%d,,s==%d,,,,od.p=%d,,,s=%d", params.precision,params.scale,od.get_precision(),od.get_vscale());
              if(params.is_modify)
              {
                   //mod dolphin[coalesce return type]@20151126:b
                  // if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
                  if((params.precision >0 && params.scale < params.precision ) && ((params.precision-params.scale)<(od.get_precision()-od.get_vscale())))
                  {
                       ret=OB_DECIMAL_UNLEGAL_ERROR;
                       TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
                  }
                  else
                  {
                      od.set_precision(params.precision);
                      od.set_scale(params.scale);
                  }

              }
              if(ret==OB_SUCCESS)
              {
                  out.set_varchar(os);
                  out.set_decimal(od);
              }
          }
      }
      return ret;
    }
    //add peiouya [DATE_TIME] 20150906:b
    static int pdatetime_date(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      time_t t = static_cast<time_t>(in.get_precise_datetime()/1000000L);
      //add hongchen [USEC_BUG_FIX] 20161031:b
      if (0 > in.get_precise_datetime() && 0 != (in.get_precise_datetime()%1000000L))
      {
          t = t - 1;
      }
      //add hongchen [USEC_BUG_FIX] 20161031:e
      struct tm gtm;
      localtime_r(&t, &gtm);
      if (0 <= gtm.tm_year && 0 <= gtm.tm_mon && 0 < gtm.tm_mday)
      {
        ObExprObj casted_obj;
        char max_tmp_buf[128]; // other type to varchar won't takes too much space, assume 128, should be enough
        ObString tmp_str(128, 128, max_tmp_buf);
        casted_obj.set_varchar (tmp_str);
        ret = varchar_printf(casted_obj, "%04d-%02d-%02d", gtm.tm_year+1900, gtm.tm_mon+1, gtm.tm_mday);
        if (OB_SUCCESS == ret)
        {
          //not need to check return value
          varchar_date(params, casted_obj, out);
        }
      }
      else
      {
        ret = OB_CONVERT_ERROR;
      }
      return ret;
    }
    static int pdatetime_time(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      time_t t = static_cast<time_t>(in.get_precise_datetime()/1000000L);
      //add hongchen [USEC_BUG_FIX] 20161031:b
      if (0 > in.get_precise_datetime() && 0 != (in.get_precise_datetime()%1000000L))
      {
          t = t - 1;
      }
      //add hongchen [USEC_BUG_FIX] 20161031:e
      struct tm gtm;
      localtime_r(&t, &gtm);
      ObExprObj casted_obj;
      char max_tmp_buf[128]; // other type to varchar won't takes too much space, assume 128, should be enough
      ObString tmp_str(128, 128, max_tmp_buf);
      casted_obj.set_varchar (tmp_str);
      ret = varchar_printf(casted_obj, "%02d:%02d:%02d", gtm.tm_hour, gtm.tm_min, gtm.tm_sec);
      if (OB_SUCCESS == ret)
      {
        //not need to check return value
        varchar_time(params, casted_obj, out);
      }
      else
      {
        ret = OB_CONVERT_ERROR;
      }
      return ret;
    }
    //add 20150906:e
    ////////////////////////////////////////////////////////////////
    // Varchar -> XXX
    static int varchar_scanf(const ObExprObj &in, const int items, const char* format, ...) __attribute__ ((format (scanf, 3, 4)));
    static int varchar_scanf(const ObExprObj &in, const int items, const char* format, ...)
    {
      int ret = OB_SUCCESS;
      const ObString& varchar = in.get_varchar();
      char* str = strndupa(varchar.ptr(), varchar.length()); // alloc from the stack, no need to free
      if (NULL == str)
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        va_list args;
        va_start(args, format);
        int matched = vsscanf(str, format, args);
        va_end(args);
        if (items != matched)
        {
          ret = OB_INVALID_ARGUMENT;
        }
      }
      return ret;
    }
    //add liuzy [datetime bug] 20150925:b
    /*Exp: check input format*/
    static int days_in_month[]= {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0};
    static const char *patt_timestamp = "\\s*[0-9]{1,4}(-[0-9]{1,2}){2}[ ]+[0-9]{1,2}(:[0-9]{1,2}){2}([.][0-9]{1,6})?\\s*$";
    static const char *patt_date = "\\s*[0-9]{1,4}(-[0-9]{1,2}){2}\\s*$";
    static const char *patt_time = "\\s*[0-9]{1,2}(:[0-9]{1,2}){2}\\s*$";
    static bool is_leap_year(int year)
    {
        bool leap_year = false;
        if ((year & 3) == 0 && (year%100 || (year%400 == 0 && year)))
        {
            leap_year = true;
        }
        return leap_year;
    }
    //mod liuzy [datetime bug] [bugfix]20160517:b
//    static bool check_format_timestamp(std::string test_str, const ObExprObj &in, bool &is_input_date, struct tm &gtm, int &ret, bool &is_match)
    static bool check_format_timestamp(const char* test_str, const ObExprObj &in, bool &is_input_date, struct tm &gtm, int &ret, bool &is_match)
    //mod 20160517:e
    {
      is_match = false;
      int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0, usec = 0;
      //mod liuzy [datetime bug] [bugfix]20160517:b
//      std::string pattern(patt_timestamp);
//      boost::regex r(pattern);
      boost::regex r(patt_timestamp);
      //mod 20160517:e
      if (boost::regex_match(test_str, r))
      {
        is_match = true;
        ret = varchar_scanf(in, 7, "%4d-%2d-%2d %2d:%2d:%2d.%6d",
                                  &year, &month, &day,
                                  &hour, &minute, &second, &usec);
        if (OB_SUCCESS != ret)
        {
          year = month = day = hour = minute = second = usec = 0;
          ret = varchar_scanf(in, 6, "%4d-%2d-%2d %2d:%2d:%2d",
                              &year, &month, &day,
                              &hour, &minute, &second);
        }
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "Failed to get values of TIMESTAMP.");
        }
        else
        {
          is_input_date = true;
          gtm.tm_year = year;
          gtm.tm_mon = month;
          gtm.tm_mday = day;
          gtm.tm_hour = hour;
          gtm.tm_min = minute;
          gtm.tm_sec = second;
        }
      }
      return is_match;
    }
    //mod liuzy [datetime bug] [bugfix]20160517:b
//    static bool check_format_date(std::string test_str, const ObExprObj &in, bool &is_input_date, struct tm &gtm, int &ret, bool &is_match)
    static bool check_format_date(const char* test_str, const ObExprObj &in, bool &is_input_date, struct tm &gtm, int &ret, bool &is_match)
    //mod 20160517:e
    {
      is_match = false;
      int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
      //mod liuzy [datetime bug] [bugfix]20160517:b
//      std::string pattern(patt_date);
//      boost::regex r(pattern);
      boost::regex r(patt_date);
      //mod 20160517:e
      if (boost::regex_match(test_str, r))
      {
        is_match = true;
        ret = varchar_scanf(in, 3, "%4d-%2d-%2d",
                                  &year, &month, &day);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "Failed to get values of DATE.");
        }
        else
        {
          is_input_date = true;
          gtm.tm_year = year;
          gtm.tm_mon = month;
          gtm.tm_mday = day;
          gtm.tm_hour = hour;
          gtm.tm_min = minute;
          gtm.tm_sec = second;
        }
      }
      return is_match;
    }
    //mod liuzy [datetime bug] [bugfix]20160517:b
//    static bool check_format_time(std::string test_str, const ObExprObj &in, struct tm &gtm, int &ret, bool &is_match)
    static bool check_format_time(const char* test_str, const ObExprObj &in, struct tm &gtm, int &ret, bool &is_match)
    //mod 20160517:e
    {
      is_match = false;
      int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
      //mod liuzy [datetime bug] [bugfix]20160517:b
//      std::string pattern(patt_time);
//      boost::regex r(pattern);
      boost::regex r(patt_time);
      //mod 20160517:e

      if (boost::regex_match(test_str, r))
      {
        is_match = true;
        ret = varchar_scanf(in, 3, "%2d:%2d:%2d",
                            &hour, &minute, &second);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "Failed to get values of TIME.");
        }
        else
        {
          gtm.tm_year = year;
          gtm.tm_mon = month;
          gtm.tm_mday = day;
          gtm.tm_hour = hour;
          gtm.tm_min = minute;
          gtm.tm_sec = second;
        }
      }
      return is_match;
    }
    static int check_format_and_content(const ObExprObj &in, enum DateTimeType type)
    {
      int ret = OB_SUCCESS;
      int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
      struct tm gtm;
      memset(&gtm, 0, sizeof(gtm));
      gtm.tm_year = year;
      gtm.tm_mon = month;
      gtm.tm_mday = day;
      gtm.tm_hour = hour;
      gtm.tm_min = minute;
      gtm.tm_sec = second;
      bool is_match = false;
      /*Exp: is_input_date is used to judge whether the value of date is input*/
      bool is_input_date = false;
      //mod liuzy [datetime bug] [bugfix]20160517:b
//      std::string test_str;
      int64_t data_len = 1024;
      char buf[data_len];
      memset(buf, 0, data_len);
      common::ObString test_str(static_cast<ObString::obstr_size_t>(data_len),
                                static_cast<ObString::obstr_size_t>(0), buf);
      test_str.write(in.get_varchar().ptr(),in.get_varchar().length());
      test_str.write('\0', static_cast<ObString::obstr_size_t>(1));
      //mod 20160517:e
      switch (type)
      {
      case TIMESTAMP_CHECK:
        if (!check_format_timestamp(test_str.ptr(), in, is_input_date, gtm, ret, is_match))
        {
          if (!check_format_date(test_str.ptr(), in, is_input_date, gtm, ret, is_match))
          {
            check_format_time(test_str.ptr(), in, gtm, ret, is_match);
          }
        }
        if (!is_match)
        {
          ret = OB_ERR_TIMESTAMP_TYPE_FORMAT;
          TBSYS_LOG(WARN, "timestamp_str:[%.*s]", test_str.length(), test_str.ptr());
          TBSYS_LOG(USER_ERROR, "Format of TIMESTAMP value is incorrect.");
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "Failed to get values of TIMESTAMP.");
        }
        break;
      case DATE_CHECK:
        if (!check_format_timestamp(test_str.ptr(), in, is_input_date, gtm, ret, is_match))
        {
          check_format_date(test_str.ptr(), in, is_input_date, gtm, ret, is_match);
        }
        if (!is_match)
        {
          ret = OB_ERR_DATE_TYPE_FORMAT;
          TBSYS_LOG(WARN, "date_str:[%.*s]", test_str.length(), test_str.ptr());
          TBSYS_LOG(USER_ERROR, "Format of DATE value is incorrect.");
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "Failed to get values of DATE.");
        }
        break;
      case TIME_CHECK:
        if (!check_format_timestamp(test_str.ptr(), in, is_input_date, gtm, ret, is_match))
        {
          check_format_time(test_str.ptr(), in, gtm, ret, is_match);
        }
        if (!is_match)
        {
          ret = OB_ERR_TIME_TYPE_FORMAT;
          TBSYS_LOG(WARN, "time_str:[%.*s]", test_str.length(), test_str.ptr());
          TBSYS_LOG(USER_ERROR, "Format of TIME value is incorrect.");
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "Failed to get values of TIME.");
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "The second parameter of function check_format_and_content is unexpected.");
        break;
      }
      /*Exp: if the format is correct, we will check values*/
      if (is_match && OB_SUCCESS == ret)
      {
        year = gtm.tm_year;
        month = gtm.tm_mon;
        day = gtm.tm_mday;
        hour = gtm.tm_hour;
        minute = gtm.tm_min;
        second = gtm.tm_sec;
        if (minute == 0 && second == 0)
        {
          if (hour < 0 || hour > 24)
          {
            ret = OB_ERR_VALUE_OF_TIME;
            TBSYS_LOG(WARN, "datetime_str:[%.*s]", test_str.length(), test_str.ptr());
            TBSYS_LOG(USER_ERROR, "Value of TIME part is incorrect.");
          }
        }
        else
        {
        if (hour < 0 || hour > 23 ||
            minute < 0 || minute > 59 ||
            second < 0 || second > 59)
        {
          ret = OB_ERR_VALUE_OF_TIME;
          TBSYS_LOG(WARN, "datetime_str:[%.*s]", test_str.length(), test_str.ptr());
          TBSYS_LOG(USER_ERROR, "Value of TIME part is incorrect.");
          }
        }

        if (OB_SUCCESS == ret)
        {
          if (is_input_date)
          {
            bool is_leap = false;
            int day_range = 0;
            if (year < 1 || year > 9999 ||
                month < 1 || month > 12)
            {
              ret = OB_ERR_VALUE_OF_DATE;
              TBSYS_LOG(WARN, "datetime_str:[%.*s]", test_str.length(), test_str.ptr());
              TBSYS_LOG(USER_ERROR, "Value of DATE part is incorrect.");
            }
            else
            {
              is_leap = is_leap_year(year);
              day_range = days_in_month[month - 1];
              if (is_leap && 2 == month)
              {
                ++day_range;
              }
              if (day < 1 || day > day_range)
              {
                ret = OB_ERR_VALUE_OF_DATE;
                TBSYS_LOG(WARN, "datetime_str:[%.*s]", test_str.length(), test_str.ptr());
                TBSYS_LOG(USER_ERROR, "Value of DATE part is incorrect.");
              }
            }
          }
        }//end else
      }//end if
      return ret;
    }
    //add 20150925:e
    static int varchar_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      int64_t value = 0;
      ret = varchar_scanf(in, 1, "%ld", &value);
      if (OB_SUCCESS == ret)
      {
        out.set_int(value);
      }
      else
      {
        out.set_int(0);
        ret = OB_SUCCESS;
      }
      return ret;
    }
    static int varchar_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      float value = 0.0f;
      ret = varchar_scanf(in, 1, "%f", &value);
      if (OB_SUCCESS == ret)
      {
        out.set_float(value);
      }
      else
      {
        out.set_float(0.0f);
        ret = OB_SUCCESS;
      }
      return ret;
    }
    static int varchar_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      double value = 0.0;
      ret = varchar_scanf(in, 1, "%lf", &value);
      if (OB_SUCCESS == ret)
      {
        out.set_double(value);
      }
      else
      {
        out.set_double(0.0);
        ret = OB_SUCCESS;
      }
      return ret;
    }
    static int varchar_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      //add liuzy [datetime bug] 20150929:b
      if (OB_SUCCESS != (ret = check_format_and_content(in, TIMESTAMP_CHECK)))
      {
        TBSYS_LOG(WARN, "Format or content is incorrect.");
      }
      else
      {
      //add 20150929:e
        int year = 0;
        int month = 0;
        int day = 0;
        int hour = 0;
        int minute = 0;
        int second = 0;
        ret = varchar_scanf(in, 6, "%4d-%2d-%2d %2d:%2d:%2d",
                            &year, &month, &day,
                            &hour, &minute, &second);
        if (OB_SUCCESS != ret)
        {
          year = month = day = hour = minute = second = 0;
          ret = varchar_scanf(in, 3, "%4d-%2d-%2d",
                              &year, &month, &day);
        }
        if (OB_SUCCESS != ret)
        {
          year = month = day = hour = minute = second = 0;
          ret = varchar_scanf(in, 3, "%2d:%2d:%2d",
                              &hour, &minute, &second);
        }
        //add liuzy [datetime bug] 20150928:b
        /*Exp: bugfix, when user don't input date, the default value of date is '1970-01-01'*/
        if (0 == year && 0 == month && 0 == day)
        {
          year = 1970;
          month = day = 1;
        }
        //add 20150928:e
        if (OB_SUCCESS == ret)
        {
          struct tm gtm;
          memset(&gtm, 0, sizeof(gtm));
          gtm.tm_year = year - 1900;
          gtm.tm_mon = month - 1;
          gtm.tm_mday = day;
          gtm.tm_hour = hour;
          gtm.tm_min = minute;
          gtm.tm_sec = second;
          gtm.tm_isdst = -1;
          time_t t = mktime(&gtm);
          out.set_datetime(static_cast<ObDateTime>(t));
        }
        else
        {
          const ObString& varchar = in.get_varchar();
          TBSYS_LOG(WARN, "failed to convert string `%.*s' to datetime type",
                    varchar.length(), varchar.ptr());
          out.set_datetime(static_cast<ObDateTime>(0));
          //del liuzy [datetime bug] 20150928:b
          /*Exp: should not assign ret OB_SUCCESS*/
//          ret = OB_SUCCESS;
          //del 20150928:e
        }
      }//add liuzy [datetime bug] 20150929
      return ret;
    }
    static int varchar_timestamp(const ObExprObj &in, int64_t &timestamp)
    {
      int ret = OB_SUCCESS;
      //add liuzy [datetime bug] 20150928:b
      if (OB_SUCCESS != (ret = check_format_and_content(in, TIMESTAMP_CHECK)))
      {
        TBSYS_LOG(WARN, "Format or content is incorrect.");
      }
      else
      {
      //add 20150928:e
        int year = 0;
        int month = 0;
        int day = 0;
        int hour = 0;
        int minute = 0;
        int second = 0;
        int usec = 0;
        char usec_char[7] = {'\0'};//add liuzy [datetime bug] 20150906
        //mod liuzy [datetime bug] 20150906:b
        //      ret = varchar_scanf(in, 7, "%4d-%2d-%2d %2d:%2d:%2d.%6d",
        //                          &year, &month, &day,
        //                          &hour, &minute, &second, &usec);
        ret = varchar_scanf(in, 7, "%4d-%2d-%2d %2d:%2d:%2d.%6c",
                            &year, &month, &day,
                            &hour, &minute, &second, usec_char);
        //mod 20150906:e
        //add liuzy [datetime bug] 20150906:b
        if (OB_SUCCESS == ret)
        {
          for (int index = 0; usec_char[index] != '\0'; ++index)
          {
            usec += static_cast<int>(pow(10, (5 - index)) * (usec_char[index] - '0'));
          }
        }
        //add 20150906:e
        if (OB_SUCCESS != ret)
        {
          year = month = day = hour = minute = second = usec = 0;
          ret = varchar_scanf(in, 6, "%4d-%2d-%2d %2d:%2d:%2d",
                              &year, &month, &day,
                              &hour, &minute, &second);
        }
        if (OB_SUCCESS != ret)
        {
          year = month = day = hour = minute = second = usec = 0;
          ret = varchar_scanf(in, 3, "%4d-%2d-%2d",
                              &year, &month, &day);
        }
        if (OB_SUCCESS != ret)
        {
          year = month = day = hour = minute = second = usec = 0;
          ret = varchar_scanf(in, 3, "%2d:%2d:%2d",
                              &hour, &minute, &second);
        }
        //add liuzy [datetime bug] 20150928:b
        /*Exp: bugfix, when user don't input date, the default value of date is '1970-01-01'*/
        if (0 == year && 0 == month && 0 == day)
        {
          year = 1970;
          month = day = 1;
        }
        //add 20150928:e
        if (OB_SUCCESS == ret)
        {
          struct tm gtm;
          memset(&gtm, 0, sizeof(gtm));
          gtm.tm_year = year - 1900;
          gtm.tm_mon = month - 1;
          gtm.tm_mday = day;
          gtm.tm_hour = hour;
          gtm.tm_min = minute;
          gtm.tm_sec = second;
          gtm.tm_isdst = -1;
          time_t t = mktime(&gtm);
          //mod hongchen [USEC_BUG_FIX] 20161031:b
          ////mod liuzy [datetime bug] 20150909:b
          ///*Exp: bugfix, timestamp before ?970-01-01 08:00:00?is negative, so we should minus usec*/
          ////timestamp = static_cast<int64_t>(t) * 1000000L + usec;
          //timestamp = static_cast<int64_t>(t) * 1000000L;
          //if (timestamp < 0)
          //{
          //  timestamp -= usec;
          //}
          //else
          //{
          //  timestamp += usec;
          //}
          ////mod 20150909:e
          timestamp = static_cast<int64_t>(t) * 1000000L + usec;
          //mod hongchen [USEC_BUG_FIX] 20161031:e
       }
      }//add liuzy [datetime bug] 20150928
      return ret;
    }
    static int varchar_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      int64_t timestamp = 0;
      ret = varchar_timestamp(in, timestamp);
      if (OB_SUCCESS == ret)
      {
        out.set_precise_datetime(static_cast<ObPreciseDateTime>(timestamp));
      }
      else
      {
        const ObString& varchar = in.get_varchar();
        TBSYS_LOG(WARN, "failed to convert string `%.*s' to precise_datetime type",
                  varchar.length(), varchar.ptr());
        out.set_precise_datetime(static_cast<ObPreciseDateTime>(0));
        //del liuzy [datetime bug] 20150928:b
        /*Exp: should not assign ret OB_SUCCESS*/
//        ret = OB_SUCCESS;
        //del 20150928:e
      }
      return ret;
    }
    static int varchar_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      int64_t timestamp = 0;
      ret = varchar_timestamp(in, timestamp);
      if (OB_SUCCESS == ret)
      {
        out.set_ctime(static_cast<ObCreateTime>(timestamp));
      }
      else
      {
        const ObString& varchar = in.get_varchar();
        TBSYS_LOG(WARN, "failed to convert string `%.*s' to createtime type",
                  varchar.length(), varchar.ptr());
        out.set_ctime(static_cast<ObCreateTime>(0));
        ret = OB_SUCCESS;
      }
      return ret;
    }
    static int varchar_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      int64_t timestamp = 0;
      ret = varchar_timestamp(in, timestamp);
      if (OB_SUCCESS == ret)
      {
        out.set_mtime(static_cast<ObModifyTime>(timestamp));
      }
      else
      {
        const ObString& varchar = in.get_varchar();
        TBSYS_LOG(WARN, "failed to convert string `%.*s' to modifytime type",
                  varchar.length(), varchar.ptr());
        out.set_mtime(static_cast<ObModifyTime>(0));
        ret = OB_SUCCESS;
      }
      return ret;
    }
    static int varchar_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      const ObString &varchar = in.get_varchar();
      bool value = false;
      if (varchar.ptr() != NULL && varchar.length() > 0)
      {
        static const int64_t len_true = strlen("true");
        static const int64_t len_t = strlen("T");
        static const int64_t len_yes = strlen("yes");
        static const int64_t len_y = strlen("y");
        if (varchar.length() == len_true
            && 0 == strncasecmp(varchar.ptr(), "true", len_true))
        {
          value = true;
        }
        else if (varchar.length() == len_t
            && 0 == strncasecmp(varchar.ptr(), "T", len_t))
        {
          value = true;
        }
        else if (varchar.length() == len_yes
            && 0 == strncasecmp(varchar.ptr(), "yes", len_yes))
        {
          value = true;
        }
        else if (varchar.length() == len_y
            && 0 == strncasecmp(varchar.ptr(), "y", len_y))
        {
          value = true;
        }
      }
      out.set_bool(value);
      return OB_SUCCESS;
    }
    /*static int varchar_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      const ObString &varchar = in.get_varchar();
      ObNumber num;
      if (OB_SUCCESS != (ret = num.from(varchar.ptr(), varchar.length())))
      {
        TBSYS_LOG(WARN, "failed to convert varchar to decimal, err=%d varchar=%.*s",
                  ret, varchar.length(), varchar.ptr());
      }
      else
      {
        out.set_decimal(num);   // @todo optimize
      }
      return OB_SUCCESS;
    }*/
    //add fanqiushi
    static int varchar_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
     // UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      const ObString &varchar = in.get_varchar();
	  if(varchar.ptr()==NULL||(int)(varchar.length())==0)
      {
        int64_t int_result;
	    int_result=0;
	    ret = varchar_printf(out, "%ld", int_result);
	    if(ret==OB_SUCCESS)
        {
         	 ObString os;
          	 out.get_varchar(os);
         	 ObDecimal od;
         	 if(OB_SUCCESS!=(ret=od.from(os.ptr(),os.length())))
         	 {
               		 TBSYS_LOG(WARN, "failed to do from in int_decimal,os=%.*s", os.length(),os.ptr());
         	 }
         	 else
         	 {
			         if(params.is_modify)
             		 {
                         //mod dolphin[coalesce return type]@20151126:b
                        // if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
                        if((params.precision >0 && params.scale < params.precision ) && ((params.precision-params.scale)<(od.get_precision()-od.get_vscale())))
                 		{
                            ret=OB_DECIMAL_UNLEGAL_ERROR;
                            TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
                  		}
                  		else
                 		 {
                     			 od.set_precision(params.precision);
                     			 od.set_scale(params.scale);
                 		 }

             		 }
             		 if(ret==OB_SUCCESS)
             		 {
                 		 out.set_varchar(os);
                 		 out.set_decimal(od);
              		 }
		     }
	    }
      }
     else{
      //ObNumber num;
      ObDecimal od;
      if (OB_SUCCESS != (ret = od.from(varchar.ptr(), varchar.length())))
      {
        TBSYS_LOG(WARN, "failed to convert varchar to decimal, err=%d varchar=%.*s",
                  ret, varchar.length(), varchar.ptr());
      }
      else
      {
        if(params.is_modify)
        {
            //mod dolphin[coalesce return type]@20151126:b
            // if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
            if((params.precision >0 && params.scale < params.precision ) && ((params.precision-params.scale)<(od.get_precision()-od.get_vscale())))
            {
                ret=OB_DECIMAL_UNLEGAL_ERROR;
                TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
            }
            else{
            od.set_precision(params.precision);
            od.set_scale(params.scale);
            }
        }
        if(ret==OB_SUCCESS){
        out.set_varchar(varchar);
        out.set_decimal(od);   // @todo optimize
        }
      }
	  }
      return ret;
    }
    //add:e
    //add peiouya [DATE_TIME] 20150906:b
    static int varchar_date(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      //add liuzy [datetime bug] 20150929:b
      if (OB_SUCCESS != (ret = check_format_and_content(in, DATE_CHECK)))
      {
        TBSYS_LOG(WARN, "Format or content is incorrect.");
      }
      else
      {
      //add 20150929:e
        int year = 0;
        int month = 0;
        int day = 0;
        int hour = 0;
        int minute = 0;
        int second = 0;
        int usec = 0;
        ret = varchar_scanf(in, 7, "%4d-%2d-%2d %2d:%2d:%2d.%6d",
                            &year, &month, &day,
                            &hour, &minute, &second, &usec);
        if (OB_SUCCESS != ret)
        {
          year = month = day = hour = minute = second = usec = 0;
          ret = varchar_scanf(in, 6, "%4d-%2d-%2d %2d:%2d:%2d",
                              &year, &month, &day,
                              &hour, &minute, &second);
        }

        if (OB_SUCCESS != ret)
        {
          year = month = day = hour = minute = second = usec = 0;
          ret = varchar_scanf(in, 3, "%4d-%2d-%2d",
                              &year, &month, &day);
        }
        if ( OB_SUCCESS == ret)
        {
          struct tm gtm;
          int64_t timestamp = 0;
          memset(&gtm, 0, sizeof(gtm));
          gtm.tm_year = year - 1900;
          gtm.tm_mon = month - 1;
          gtm.tm_mday = day;
          gtm.tm_hour = 0;
          gtm.tm_min = 0;
          gtm.tm_sec = 0;
          gtm.tm_isdst = -1;
          time_t t = mktime(&gtm);
          timestamp = static_cast<int64_t>(t) * 1000000L;
          out.set_date(static_cast<ObDateTime>(timestamp));
        }
        else
        {
          ret = OB_CONVERT_ERROR;
        }
      }//add liuzy [datetime bug] 20150929
      return ret;
    }
    static int varchar_time(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      //add liuzy [datetime bug] 20150929:b
      if (OB_SUCCESS != (ret = check_format_and_content(in, TIME_CHECK)))
      {
        TBSYS_LOG(WARN, "Format or content is incorrect.");
      }
      else
      {
        //add 20150929:e
        int year = 0;
        int month = 0;
        int day = 0;
        int hour = 0;
        int minute = 0;
        int second = 0;
        int usec = 0;
        ret = varchar_scanf(in, 7, "%4d-%2d-%2d %2d:%2d:%2d.%6d",
                            &year, &month, &day,
                            &hour, &minute, &second, &usec);
        if (OB_SUCCESS != ret)
        {
          year = month = day = hour = minute = second = usec = 0;
          ret = varchar_scanf(in, 6, "%4d-%2d-%2d %2d:%2d:%2d",
                              &year, &month, &day,
                              &hour, &minute, &second);
        }
        if (OB_SUCCESS != ret)
        {
          year = month = day = hour = minute = second = usec = 0;
          ret = varchar_scanf(in, 3, "%4d-%2d-%2d",
                              &year, &month, &day);
        }
        if (OB_SUCCESS != ret)
        {
          year = month = day = hour = minute = second = usec = 0;
          ret = varchar_scanf(in, 3, "%2d:%2d:%2d",
                              &hour, &minute, &second);
        }

        if (OB_SUCCESS == ret)
        {
          int64_t timestamp = 0;
          timestamp = static_cast<int64_t>(hour * 3600 + minute * 60 + second) * 1000000L;
          out.set_time(static_cast<ObTime>(timestamp));
        }
        else
        {
          ret = OB_CONVERT_ERROR;
        }
      }//add liuzy [datetime bug] 20150929
      return ret;
    }
    //add 20150906:e
	
	//add yushengjuan [INT_32] 20151009:b
    static int varchar_int32(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      int64_t value = 0;
      ret = varchar_scanf(in, 1, "%ld", &value);
      if (OB_SUCCESS == ret )
      {
        if (INT_MIN <= value && value <= INT_MAX)
        {
          int32_t stored_value = static_cast<int32_t>(value);
          out.set_int32(stored_value);
        }
        else
        {
          ret = OB_SIZE_OVERFLOW;
        }
      }
      else
      {
        //out.set_int32(0);
        ret = OB_NOT_SUPPORTED;
      }
      return ret;
    }
    //add 20151009:e

    ////////////////////////////////////////////////////////////////
    // CreateTime -> XXX
    static int ctime_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      out.set_int(static_cast<int64_t>(in.get_ctime()));
      return OB_SUCCESS;
    }
    static int ctime_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      out.set_float(static_cast<float>(in.get_ctime()));
      return OB_SUCCESS;
    }
    static int ctime_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      out.set_double(static_cast<double>(in.get_ctime()));
      return OB_SUCCESS;
    }
    static int ctime_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      out.set_datetime(static_cast<ObDateTime>(in.get_ctime()/1000000L));
      return OB_SUCCESS;
    }
    static int ctime_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      out.set_precise_datetime(static_cast<ObPreciseDateTime>(in.get_ctime()));
      return OB_SUCCESS;
    }
    static int ctime_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      return timestamp_varchar(in.get_ctime(), out);
    }
    static int ctime_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      out.set_mtime(static_cast<ObModifyTime>(in.get_ctime()));
      return OB_SUCCESS;
    }
    static int ctime_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      out.set_bool(static_cast<bool>(in.get_ctime()));
      return OB_SUCCESS;
    }
    static int ctime_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      /*UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      ObNumber num;
      num.from(static_cast<int64_t>(in.get_ctime()));
      out.set_decimal(num);
      return OB_SUCCESS;*/
      int ret = OB_SUCCESS;
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      int64_t c_time_int=static_cast<int64_t>(in.get_ctime());
      ret = varchar_printf(out, "%ld", c_time_int);
      if(ret==OB_SUCCESS)
      {
          ObString os;
          out.get_varchar(os);
          ObDecimal od;
          if(OB_SUCCESS!=(ret=od.from(os.ptr(),os.length())))
          {
                TBSYS_LOG(WARN, "failed to do from in ctime_decimal,os=%.*s", os.length(),os.ptr());
          }
          else
          {
             // TBSYS_LOG(WARN, "test::f2 ,,(p==%d,,s==%d,,,,od.p=%d,,,s=%d", params.precision,params.scale,od.get_precision(),od.get_vscale());
              if(params.is_modify)
              {
                  //mod dolphin[coalesce return type]@20151126:b
                  // if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
                  if((params.precision >0 && params.scale < params.precision ) && ((params.precision-params.scale)<(od.get_precision()-od.get_vscale())))
                  {
                      ret=OB_DECIMAL_UNLEGAL_ERROR;
                      TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
                  }
                  else
                  {
                      od.set_precision(params.precision);
                      od.set_scale(params.scale);
                  }

              }
              if(ret==OB_SUCCESS)
              {
                  out.set_varchar(os);
                  out.set_decimal(od);
              }
          }
      }
      return ret;

    }
    //add peiouya [DATE_TIME] 20150906:b
    static int ctime_date(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      ObExprObj casted_obj;
      casted_obj.set_precise_datetime(static_cast<ObPreciseDateTime>(in.get_ctime()));
      return pdatetime_date (params, casted_obj, out);
    }
    static int ctime_time(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      ObExprObj casted_obj;
      casted_obj.set_precise_datetime(static_cast<ObPreciseDateTime>(in.get_ctime()));
      return pdatetime_time (params, casted_obj, out);
    }
    //add 20150906:e
    ////////////////////////////////////////////////////////////////
    // ModifyTime -> XXX
    static int mtime_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      out.set_int(static_cast<int64_t>(in.get_mtime()));
      return OB_SUCCESS;
    }
    static int mtime_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      out.set_float(static_cast<float>(in.get_mtime()));
      return OB_SUCCESS;
    }
    static int mtime_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      out.set_double(static_cast<double>(in.get_mtime()));
      return OB_SUCCESS;
    }
    static int mtime_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      out.set_datetime(static_cast<ObDateTime>(in.get_mtime()/1000000L));
      return OB_SUCCESS;
    }
    static int mtime_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      out.set_precise_datetime(static_cast<ObPreciseDateTime>(in.get_mtime()));
      return OB_SUCCESS;
    }
    static int mtime_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      return timestamp_varchar(in.get_mtime(), out);
    }
    static int mtime_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      out.set_ctime(static_cast<ObModifyTime>(in.get_mtime()));
      return OB_SUCCESS;
    }
    static int mtime_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      out.set_bool(static_cast<bool>(in.get_mtime()));
      return OB_SUCCESS;
    }
    static int mtime_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      /*UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      ObNumber num;
      num.from(static_cast<int64_t>(in.get_mtime()));
      out.set_decimal(num);
      return OB_SUCCESS;*/
      int ret = OB_SUCCESS;
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      int64_t m_time_int=static_cast<int64_t>(in.get_mtime());
      ret = varchar_printf(out, "%ld", m_time_int);
      if(ret==OB_SUCCESS)
      {
          ObString os;
          out.get_varchar(os);
          ObDecimal od;
          if(OB_SUCCESS!=(ret=od.from(os.ptr(),os.length())))
          {
                TBSYS_LOG(WARN, "failed to do from in mtime_decimal,os=%.*s", os.length(),os.ptr());
          }
          else
          {
             // TBSYS_LOG(WARN, "test::f2 ,,(p==%d,,s==%d,,,,od.p=%d,,,s=%d", params.precision,params.scale,od.get_precision(),od.get_vscale());
              if(params.is_modify)
              {
                  //mod dolphin[coalesce return type]@20151126:b
                  // if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
                  if((params.precision >0 && params.scale < params.precision ) && ((params.precision-params.scale)<(od.get_precision()-od.get_vscale())))
                  {
                     ret=OB_DECIMAL_UNLEGAL_ERROR;
                     TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
                  }
                  else
                  {
                      od.set_precision(params.precision);
                      od.set_scale(params.scale);
                  }

              }
              if(ret==OB_SUCCESS)
              {
                  out.set_varchar(os);
                  out.set_decimal(od);
              }
          }
      }
      return ret;
    }
    //add peiouya [DATE_TIME] 20150906:b
    static int mtime_date(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      ObExprObj casted_obj;
      casted_obj.set_precise_datetime(static_cast<ObPreciseDateTime>(in.get_mtime()));
      return pdatetime_date (params, casted_obj, out);
    }
    static int mtime_time(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      ObExprObj casted_obj;
      casted_obj.set_precise_datetime(static_cast<ObPreciseDateTime>(in.get_mtime()));
      return pdatetime_time (params, casted_obj, out);
    }
    //add 20150906:e
    ////////////////////////////////////////////////////////////////
    // Bool -> XXX
    static int bool_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObBoolType);
      out.set_int(static_cast<int64_t>(in.get_bool()));
      return OB_SUCCESS;
    }
    static int bool_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObBoolType);
      out.set_float(static_cast<float>(in.get_bool()));
      return OB_SUCCESS;
    }
    static int bool_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObBoolType);
      out.set_double(static_cast<double>(in.get_bool()));
      return OB_SUCCESS;
    }
    static int bool_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObBoolType);
      out.set_datetime(static_cast<ObDateTime>(in.get_bool()));
      return OB_SUCCESS;
    }
    static int bool_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObBoolType);
      out.set_precise_datetime(static_cast<ObPreciseDateTime>(in.get_bool()));
      return OB_SUCCESS;
    }
    static int bool_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObBoolType);
      ObString varchar;
      if (in.get_bool())
      {
        varchar.assign_ptr(const_cast<char*>("true"), sizeof("true")-1);
      }
      else
      {
        varchar.assign_ptr(const_cast<char*>("false"), sizeof("false")-1);
      }
      out.set_varchar(varchar);
      return OB_SUCCESS;
    }
    static int bool_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObBoolType);
      out.set_ctime(static_cast<ObCreateTime>(in.get_bool()));
      return OB_SUCCESS;
    }
    static int bool_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObBoolType);
      out.set_mtime(static_cast<ObModifyTime>(in.get_bool()));
      return OB_SUCCESS;
    }
    static int bool_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      /*UNUSED(params);
      OB_ASSERT(in.get_type() == ObBoolType);
      ObNumber num;
      num.from(static_cast<int64_t>(in.get_bool()));
      out.set_decimal(num);
      return OB_SUCCESS;*/
      int ret = OB_SUCCESS;
      OB_ASSERT(in.get_type() == ObBoolType);
      int64_t bool_int=static_cast<int64_t>(in.get_bool());
      ret = varchar_printf(out, "%ld", bool_int);
      if(ret==OB_SUCCESS)
      {
          ObString os;
          out.get_varchar(os);
          ObDecimal od;
          if(OB_SUCCESS!=(ret=od.from(os.ptr(),os.length())))
          {
                TBSYS_LOG(WARN, "failed to do from in bool_decimal,os=%.*s", os.length(),os.ptr());
          }
          else
          {
             // TBSYS_LOG(WARN, "test::f2 ,,(p==%d,,s==%d,,,,od.p=%d,,,s=%d", params.precision,params.scale,od.get_precision(),od.get_vscale());
              if(params.is_modify)
              {
                  //mod dolphin[coalesce return type]@20151126:b
                  // if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
                  if((params.precision >0 && params.scale < params.precision ) && ((params.precision-params.scale)<(od.get_precision()-od.get_vscale())))
                  {
                     ret=OB_DECIMAL_UNLEGAL_ERROR;
                     TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
                  }
                  else
                  {
                      od.set_precision(params.precision);
                      od.set_scale(params.scale);
                  }

              }
              if(ret==OB_SUCCESS)
              {
                  out.set_varchar(os);
                  out.set_decimal(od);
              }
          }
      }
      return ret;
    }
    //add yushengjuan [INT_32] 20151009:b
    static int bool_int32(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObBoolType);
      out.set_int32(static_cast<int32_t>(in.get_bool()));
      return OB_SUCCESS;
    }
    //add 20151009:e
    ////////////////////////////////////////////////////////////////
    // Decimal -> XXX
    static int decimal_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      /*int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      int64_t i64 = 0;
      if (OB_SUCCESS != (ret = in.get_decimal().cast_to_int64(i64)))
      {
        TBSYS_LOG(WARN, "failed to cast to int64, err=%d", ret);
      }
      else
      {
        out.set_int(i64);
      }
      return ret;*/
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      int64_t i64 = 0;
      //ObString os;
      //os=in.get_varchar();
     // ObDecimal od=in.get_decimal();
      ObDecimal od_result=in.get_decimal();
      //if(OB_SUCCESS != (ret = od_result.from(os.ptr(),os.length())))
      //{
            // TBSYS_LOG(WARN, "failed to do from in decimal_int,os=%.*s", os.length(),os.ptr());
     // }
     // else
     // {
       // uint32_t p=od.get_precision();
       // uint32_t s=od.get_scale();
        //if (OB_SUCCESS != (ret=od_result.modify_value(p,s)))
        //{
           // TBSYS_LOG(WARN, "failed to do modify_value in od_result.modify_value(p,s),od_result=%.*s,p=%d,s=%d", os.length(),os.ptr(),p,s);
       // }
        if (OB_SUCCESS != (ret = od_result.cast_to_int64(i64)))
        {
            TBSYS_LOG(WARN, "failed to do cast_to_int64 in od_result.cast_to_int64(i64)");
        }
        else
        {
            out.set_int(i64);
        }
      //}
      return ret;
    }
    static int decimal_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      /*OB_ASSERT(in.get_type() == ObDecimalType);
      // decimal -> string -> float
      char buf[ObNumber::MAX_PRINTABLE_SIZE];
      memset(buf, 0, ObNumber::MAX_PRINTABLE_SIZE);
      in.get_decimal().to_string(buf, ObNumber::MAX_PRINTABLE_SIZE);
      float v = 0.0f;
      if (1 == sscanf(buf, "%f", &v))
      {
        out.set_float(v);
      }
      else
      {
        out.set_float(0.0f);
      }
      return OB_SUCCESS;*/
      int ret = OB_SUCCESS;
      OB_ASSERT(in.get_type() == ObDecimalType);
      char buf[MAX_PRINTABLE_SIZE];
      memset(buf, 0, MAX_PRINTABLE_SIZE);
      float v = 0.0f;
      //int64_t i64 = 0;
      //ObString os;
      //os=in.get_varchar();
     // ObDecimal od=in.get_decimal();
      ObDecimal od_result=in.get_decimal();
      //if(OB_SUCCESS != (ret = od_result.from(os.ptr(),os.length())))
      //{
        //    TBSYS_LOG(WARN, "failed to do from in decimal_float,os=%.*s", os.length(),os.ptr());
     // }
     // else
      //{
       // uint32_t p=od.get_precision();
        //uint32_t s=od.get_scale();
       // od_result.set_precision(p);
        //od_result.set_scale(s);
        od_result.to_string(buf, MAX_PRINTABLE_SIZE);
        if (1 == sscanf(buf, "%f", &v))
        {
            out.set_float(v);
        }
        else
        {
            out.set_float(0.0f);
        }
      //}
      return ret;
    }
    static int decimal_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      /*UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      // decimal -> string -> float
      char buf[ObNumber::MAX_PRINTABLE_SIZE];
      memset(buf, 0, ObNumber::MAX_PRINTABLE_SIZE);
      in.get_decimal().to_string(buf, ObNumber::MAX_PRINTABLE_SIZE);
      double v = 0.0;
      if (1 == sscanf(buf, "%lf", &v))
      {
        out.set_double(v);
      }
      else
      {
        out.set_double(0.0);
      }
      return OB_SUCCESS;*/
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      char buf[MAX_PRINTABLE_SIZE];
      memset(buf, 0, MAX_PRINTABLE_SIZE);
      double v = 0.0;
      //int64_t i64 = 0;
     // ObString os;
      //os=in.get_varchar();
      //add fanqiushi
      //TBSYS_LOG(ERROR, "test:: os=%.*s", os.length(),os.ptr());
     // ObDecimal od=in.get_decimal();
      ObDecimal od_result=in.get_decimal();
      //if(OB_SUCCESS != (ret = od_result.from(os.ptr(),os.length())))
      //{
           // TBSYS_LOG(WARN, "failed to do from in decimal_double,os=%.*s", os.length(),os.ptr());
      //}
     // else
     // {
       // uint32_t p=od.get_precision();
       // uint32_t s=od.get_scale();
       // od_result.set_precision(p);
       // od_result.set_scale(s);
        od_result.to_string(buf, MAX_PRINTABLE_SIZE);
        if (1 == sscanf(buf, "%lf", &v))
        {
            out.set_double(v);
        }
        else
        {
            out.set_double(0.0);
        }
      //}
      return ret;
    }
    static int decimal_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      /*int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      int64_t i64 = 0;
      if (OB_SUCCESS != (ret = in.get_decimal().cast_to_int64(i64)))
      {
        TBSYS_LOG(WARN, "failed to cast to int64, err=%d", ret);
      }
      else
      {
        out.set_datetime(static_cast<ObDateTime>(i64));
      }
      return ret;*/
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      int64_t i64 = 0;
      //ObString os;
      //os=in.get_varchar();
     // ObDecimal od=in.get_decimal();
      ObDecimal od_result=in.get_decimal();
      //if(OB_SUCCESS != (ret = od_result.from(os.ptr(),os.length())))
     // {
           // TBSYS_LOG(WARN, "failed to do from in decimal_datetime,os=%.*s", os.length(),os.ptr());
      //}
     // else
     // {
        //uint32_t p=od.get_precision();
       // uint32_t s=od.get_scale();
       // if (OB_SUCCESS != (ret=od_result.modify_value(p,s)))
       // {
           //  TBSYS_LOG(WARN, "failed to do modify_value in od_result.modify_value(p,s),od_result=%.*s,p=%d,s=%d", os.length(),os.ptr(),p,s);
       // }
        if (OB_SUCCESS != (ret = od_result.cast_to_int64(i64)))
        {
            TBSYS_LOG(WARN, "failed to do cast_to_int64 in od_result.cast_to_int64(i64)");
        }
        else
        {
            out.set_datetime(static_cast<ObDateTime>(i64));
        }
     // }
      return ret;
    }
    static int decimal_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      /*int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      int64_t i64 = 0;
      if (OB_SUCCESS != (ret = in.get_decimal().cast_to_int64(i64)))
      {
        TBSYS_LOG(WARN, "failed to cast to int64, err=%d", ret);
      }
      else
      {
        out.set_precise_datetime(static_cast<ObPreciseDateTime>(i64));
      }
      return ret;*/
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      int64_t i64 = 0;
     // ObString os;
     // os=in.get_varchar();
      //ObDecimal od=in.get_decimal();
      ObDecimal od_result=in.get_decimal();
      //if(OB_SUCCESS != (ret = od_result.from(os.ptr(),os.length())))
     // {
        //    TBSYS_LOG(WARN, "failed to do from in decimal_pdatetime,os=%.*s", os.length(),os.ptr());
     // }
     // else
     // {
       // uint32_t p=od.get_precision();
       // uint32_t s=od.get_scale();
       // if (OB_SUCCESS != (ret=od_result.modify_value(p,s)))
       // {
          //  TBSYS_LOG(WARN, "failed to do modify_value in od_result.modify_value(p,s),od_result=%.*s,p=%d,s=%d", os.length(),os.ptr(),p,s);
       // }
        if (OB_SUCCESS != (ret = od_result.cast_to_int64(i64)))
        {
            TBSYS_LOG(WARN, "failed to do cast_to_int64 in od_result.cast_to_int64(i64)");
        }
        else
        {
            out.set_precise_datetime(static_cast<ObPreciseDateTime>(i64));
        }
     // }
      return ret;
    }
    static int decimal_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      ObString varchar = out.get_varchar();
      if (varchar.length() < (int)MAX_PRINTABLE_SIZE)
      {
        TBSYS_LOG(WARN, "output buffer for varchar not enough, buf_len=%d", varchar.length());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
         // ObDecimal od=in.get_decimal();
          //ObString os;
         // os=in.get_varchar();
          ObDecimal od_result=in.get_decimal();
          //if(OB_SUCCESS != (ret = od_result.from(os.ptr(),os.length())))
          //{
              //  TBSYS_LOG(WARN, "failed to do from in decimal_varchar,os=%.*s", os.length(),os.ptr());
         // }
         // else
         // {
              //od_result.set_precision(od.get_precision());
             // od_result.set_scale(od.get_scale());
              int64_t length = od_result.to_string(varchar.ptr(), varchar.length());
              ObString varchar2(varchar.length(), static_cast<int32_t>(length), varchar.ptr());
              out.set_varchar(varchar2);
          //}

      }
      return ret;
    }
    static int decimal_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      /*int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      int64_t i64 = 0;
      if (OB_SUCCESS != (ret = in.get_decimal().cast_to_int64(i64)))
      {
        TBSYS_LOG(WARN, "failed to cast to int64, err=%d", ret);
      }
      else
      {
        out.set_ctime(static_cast<ObCreateTime>(i64));
      }
      return ret;*/
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      int64_t i64 = 0;
     // ObString os;
     // os=in.get_varchar();
     // ObDecimal od=in.get_decimal();
      ObDecimal od_result=in.get_decimal();
      //if(OB_SUCCESS != (ret = od_result.from(os.ptr(),os.length())))
     // {
          //  TBSYS_LOG(WARN, "failed to do from in decimal_ctime,os=%.*s", os.length(),os.ptr());
      //}
     // else
     // {
       // uint32_t p=od.get_precision();
       // uint32_t s=od.get_scale();
      //  if (OB_SUCCESS != (ret=od_result.modify_value(p,s)))
       // {
           //  TBSYS_LOG(WARN, "failed to do modify_value in od_result.modify_value(p,s),od_result=%.*s,p=%d,s=%d", os.length(),os.ptr(),p,s);
        //}
        if (OB_SUCCESS != (ret = od_result.cast_to_int64(i64)))
        {
             TBSYS_LOG(WARN, "failed to do cast_to_int64 in od_result.cast_to_int64(i64)");
        }
        else
        {
            out.set_ctime(static_cast<ObCreateTime>(i64));
        }
      //}
      return ret;
    }
    static int decimal_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      /*int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      int64_t i64 = 0;
      if (OB_SUCCESS != (ret = in.get_decimal().cast_to_int64(i64)))
      {
        TBSYS_LOG(WARN, "failed to cast to int64, err=%d", ret);
      }
      else
      {
        out.set_mtime(static_cast<ObModifyTime>(i64));
      }
      return ret;*/
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      int64_t i64 = 0;
      //ObString os;
      //os=in.get_varchar();
      //ObDecimal od=in.get_decimal();
      ObDecimal od_result=in.get_decimal();
     // if(OB_SUCCESS != (ret = od_result.from(os.ptr(),os.length())))
      //{
            //TBSYS_LOG(WARN, "failed to do from in decimal_mtime,os=%.*s", os.length(),os.ptr());
      //}
      //else
     // {
        //uint32_t p=od.get_precision();
       // uint32_t s=od.get_scale();
       // if (OB_SUCCESS != (ret=od_result.modify_value(p,s)))
       // {
           // TBSYS_LOG(WARN, "failed to do modify_value in od_result.modify_value(p,s),od_result=%.*s,p=%d,s=%d", os.length(),os.ptr(),p,s);
       // }
        if (OB_SUCCESS != (ret = od_result.cast_to_int64(i64)))
        {
            TBSYS_LOG(WARN, "failed to do cast_to_int64 in od_result.cast_to_int64(i64)");
        }
        else
        {
            out.set_mtime(static_cast<ObModifyTime>(i64));
        }
     // }
      return ret;
    }
    static int decimal_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      //ObString os;
     // os=in.get_varchar();
     // ObDecimal od=in.get_decimal();
      ObDecimal od_result=in.get_decimal();
      //if(OB_SUCCESS != (ret = od_result.from(os.ptr(),os.length())))
      //{
           //  TBSYS_LOG(WARN, "failed to do from in decimal_bool,os=%.*s", os.length(),os.ptr());
      //}
      //else
     // {
          //od_result.set_precision(od.get_precision());
          //od_result.set_scale(od.get_scale());
          out.set_bool(!od_result.is_zero());
     // }

      return ret;
    }
    //add 20150831:e
    //add yushengjuan [INT_32] 20151009:b
    static int decimal_int32(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      int32_t i32 = 0;
      ObDecimal od_result=in.get_decimal();
        if (OB_SUCCESS != (ret = od_result.cast_to_int32(i32)))//to do cast_to_int32
        {
            TBSYS_LOG(WARN, "failed to do cast_to_int64 in od_result.cast_to_int32(i32)");
        }
        else
        {
            out.set_int32(i32);
        }
      return ret;
    }
    //add 20151009:e
    ////////////////////////////////////////////////////////////////
    // date -> XXX
    static int date_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateType);
      out.set_int(static_cast<int64_t>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int date_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateType);
      out.set_float(static_cast<float>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int date_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateType);
      out.set_double(static_cast<double>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int date_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateType);
      out.set_datetime(static_cast<ObDateTime>(in.get_precise_datetime()/1000000L));
      return OB_SUCCESS;
    }
    static int date_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateType);
      //mod peiouya [DATE_TIME] 20150913:b
      //fix precise BUG
      //out.set_precise_datetime (static_cast<ObPreciseDateTime>(in.get_precise_datetime()/1000000L));
      out.set_precise_datetime (static_cast<ObPreciseDateTime>(in.get_precise_datetime()));
      //mod 20150913:e
      return OB_SUCCESS;
    }
    static int date_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateType);
      int ret = OB_SUCCESS;
      time_t t = static_cast<time_t>(in.get_precise_datetime()/1000000L);
      struct tm gtm;
      localtime_r(&t, &gtm);
      ret = varchar_printf(out, "%04d-%02d-%02d", gtm.tm_year+1900, gtm.tm_mon+1, gtm.tm_mday);
      return ret;
    }
    static int date_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateType);
      out.set_ctime(static_cast<ObCreateTime>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int date_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateType);
      out.set_mtime(static_cast<ObModifyTime>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int date_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateType);
      out.set_bool(static_cast<bool>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int date_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateType);
      //del liuzy [datetime to decimal format fix] 20151124:b
      //Exp:删除对pdatetime_decimal()函数的调用,改为自行实现
//      ObExprObj casted_obj;
//      casted_obj.set_precise_datetime(static_cast<ObPreciseDateTime>(in.get_precise_datetime()));
//      return pdatetime_decimal (params, casted_obj, out);
      //del 20151124:e

      //add liuzy [datetime to decimal format fix] 20151124:b
      //Exp:原有的式样是将微秒数作为decimal,现在修改为‘YYYYMMDD’格式
      int ret = OB_SUCCESS;
      time_t t = static_cast<time_t>(in.get_precise_datetime()/1000000L);
      struct tm gtm;
      localtime_r(&t, &gtm);
      ret = varchar_printf(out, "%04d%02d%02d",
                           gtm.tm_year + 1900, gtm.tm_mon + 1, gtm.tm_mday);
      if(ret==OB_SUCCESS)
      {
        ObString os;
        out.get_varchar(os);
        ObDecimal od;
        if(OB_SUCCESS!=(ret=od.from(os.ptr(),os.length())))
        {
          TBSYS_LOG(WARN, "failed to do from in pdatetime_decimal,os=%.*s", os.length(),os.ptr());
        }
        else
        {
          // TBSYS_LOG(WARN, "test::f2 ,,(p==%d,,s==%d,,,,od.p=%d,,,s=%d", params.precision,params.scale,od.get_precision(),od.get_vscale());
          if(params.is_modify)
          {
            //mod dolphin[coalesce return type]@20151126:b
            // if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
            if((params.precision >0 && params.scale < params.precision ) && ((params.precision-params.scale)<(od.get_precision()-od.get_vscale())))
            {
              ret=OB_DECIMAL_UNLEGAL_ERROR;
              TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
            }
            else
            {
              od.set_precision(params.precision);
              od.set_scale(params.scale);
            }
          }
          if(ret==OB_SUCCESS)
          {
            out.set_varchar(os);
            out.set_decimal(od);
          }
        }
      }
      return ret;
      //add 20151124:e
    }
    ////////////////////////////////////////////////////////////////
    // time -> XXX
    static int time_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObTimeType);
      out.set_int(static_cast<int64_t>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int time_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObTimeType);
      out.set_float(static_cast<float>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int time_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObTimeType);
      out.set_double(static_cast<double>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    //del peiouya [DATE_TIME] 20150913:b
    /*
    static int time_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObTimeType);
      out.set_datetime(static_cast<ObDateTime>(in.get_precise_datetime()/1000000L));
      return OB_SUCCESS;
    }
    static int time_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObTimeType);
      out.set_precise_datetime (static_cast<ObPreciseDateTime>(in.get_precise_datetime()/1000000L));
      return OB_SUCCESS;
    }
    */
    //del 20150913:e
    static int time_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObTimeType);
      int ret = OB_SUCCESS;
      time_t t = static_cast<time_t>(in.get_precise_datetime()/1000000L);
      //int64_t usec = in.get_precise_datetime() % 1000000L;
      int hour = 0;
      int min = 0;
      int sec = 0;
      //localtime_r(&t, &gtm);
      hour = static_cast<int32_t>(t / 3600);
      min  = static_cast<int32_t>(t % 3600 / 60);
      sec  = static_cast<int32_t>(t % 60);
      ret = varchar_printf(out, "%02d:%02d:%02d", hour, min, sec);
      return ret;
    }
    //del peiouya [DATE_TIME] 20150913:b
    /*
    static int time_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObTimeType);
      out.set_ctime(static_cast<ObCreateTime>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int time_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObTimeType);
      out.set_mtime(static_cast<ObModifyTime>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    */
    //del 20150913:e
    static int time_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObTimeType);
      out.set_bool(static_cast<bool>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int time_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObTimeType);
      //del liuzy [datetime to decimal format fix] 20151124:b
      //Exp:删除对pdatetime_decimal()的调用,自行实现
//      ObExprObj casted_obj;
//      casted_obj.set_precise_datetime(static_cast<ObPreciseDateTime>(in.get_precise_datetime()));
//      return pdatetime_decimal (params, casted_obj, out);
      //del 20151124:e

      //add liuzy [datetime to decimal format fix] 20151124:b
      //Exp:原有的式样是将微秒数作为decimal,现在修改为‘HHMMSS.SSSSSS’格式
      int ret = OB_SUCCESS;
      int32_t usec = static_cast<int32_t>(in.get_precise_datetime()%1000000L);
      if (usec != 0)
      {
        TBSYS_LOG(WARN, "Format of TIME value is incorrect.");
      }
      time_t t = static_cast<time_t>(in.get_precise_datetime()/1000000L);
      struct tm gtm;
      localtime_r(&t, &gtm);
      if (gtm.tm_hour > 8)
      {
      ret = varchar_printf(out, "%02d%02d%02d",
                           gtm.tm_hour - 8, gtm.tm_min, gtm.tm_sec);
      }
      else
      {
        ret = varchar_printf(out, "%02d%02d%02d",
                             gtm.tm_hour + 24 - 8, gtm.tm_min, gtm.tm_sec);
      }
      if(ret==OB_SUCCESS)
      {
        ObString os;
        out.get_varchar(os);
        ObDecimal od;
        if(OB_SUCCESS!=(ret=od.from(os.ptr(),os.length())))
        {
          TBSYS_LOG(WARN, "failed to do from in pdatetime_decimal,os=%.*s", os.length(),os.ptr());
        }
        else
        {
          // TBSYS_LOG(WARN, "test::f2 ,,(p==%d,,s==%d,,,,od.p=%d,,,s=%d", params.precision,params.scale,od.get_precision(),od.get_vscale());
          if(params.is_modify)
          {
            //mod dolphin[coalesce return type]@20151126:b
            // if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
            if((params.precision >0 && params.scale < params.precision ) && ((params.precision-params.scale)<(od.get_precision()-od.get_vscale())))
            {
              ret=OB_DECIMAL_UNLEGAL_ERROR;
              TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
            }
            else
            {
              od.set_precision(params.precision);
              od.set_scale(params.scale);
            }
          }
          if(ret==OB_SUCCESS)
          {
            out.set_varchar(os);
            out.set_decimal(od);
          }
        }
      }
      return ret;
      //add 20151124:e
    }

    ////////////////////////////////////////////////////////////////
    //mod peiouya [DATE_TIME] 20150906:b
// ObObjCastFunc OB_OBJ_CAST[ObMaxType][ObMaxType] =
    // {
      // {
        // /*Null -> XXX*/
        // identity, identity, identity,
        // identity, identity, identity,
        // identity, identity, identity,
        // identity, identity, identity,
        // identity
      // }
      // ,
      // {
        // /*Int -> XXX*/
        // not_support/*Null*/, identity/*Int*/, int_float,
        // int_double, int_datetime, int_pdatetime,
        // int_varchar, not_support, int_ctime,
        // int_mtime, not_support, int_bool,
        // int_decimal
      // }
      // ,
      // {
        // /*Float -> XXX*/
        // not_support/*Null*/, float_int, identity,
        // float_double, float_datetime, float_pdatetime,
        // float_varchar, not_support, float_ctime,
        // float_mtime, not_support, float_bool,
        // float_decimal
      // }
      // ,
      // {
        // /*Double -> XXX*/
        // not_support/*Null*/, double_int, double_float,
        // identity, double_datetime, double_pdatetime,
        // double_varchar, not_support, double_ctime,
        // double_mtime, not_support, double_bool,
        // double_decimal
      // }
      // ,
      // {
        // /*DateTime -> XXX*/
        // not_support/*Null*/, datetime_int, datetime_float,
        // datetime_double, identity, datetime_pdatetime,
        // datetime_varchar, not_support/*Seq*/, datetime_ctime,
        // datetime_mtime, not_support/*Extend*/, datetime_bool,
        // datetime_decimal
      // }
      // ,
      // {
        // /*PreciseDateTime -> XXX*/
        // not_support/*Null*/, pdatetime_int, pdatetime_float,
        // pdatetime_double, pdatetime_datetime, identity,
        // pdatetime_varchar, not_support/*Seq*/,pdatetime_ctime,
        // pdatetime_mtime, not_support/*Extend*/,pdatetime_bool,
        // pdatetime_decimal
      // }
      // ,
      // {
        // /*Varchar -> XXX*/
        // not_support/*Null*/, varchar_int, varchar_float,
        // varchar_double, varchar_datetime, varchar_pdatetime,
        // identity, not_support/*Seq*/, varchar_ctime,
        // varchar_mtime, not_support/*Extend*/, varchar_bool,
        // varchar_decimal
      // }
      // ,
      // {
        // /*Seq -> XXX*/
        // not_support, not_support, not_support,
        // not_support, not_support, not_support,
        // not_support, not_support, not_support,
        // not_support, not_support, not_support,
        // not_support
      // }
      // ,
      // {
        // /*CreateTime -> XXX*/
        // not_support/*Null*/, ctime_int, ctime_float,
        // ctime_double, ctime_datetime, ctime_pdatetime,
        // ctime_varchar, not_support/*Seq*/, identity,
        // ctime_mtime, not_support/*Extend*/, ctime_bool,
        // ctime_decimal
      // }
      // ,
      // {
        // /*ModifyTime -> XXX*/
        // not_support/*Null*/, mtime_int, mtime_float,
        // mtime_double, mtime_datetime, mtime_pdatetime,
        // mtime_varchar, not_support/*Seq*/, mtime_ctime,
        // identity, not_support/*Extend*/, mtime_bool,
        // mtime_decimal
      // }
      // ,
      // {
        // /*Extend -> XXX*/
        // not_support, not_support, not_support,
        // not_support, not_support, not_support,
        // not_support, not_support, not_support,
        // not_support, not_support, not_support,
        // not_support
      // }
      // ,
      // {
        // /*Bool -> XXX*/
        // not_support/*Null*/, bool_int, bool_float,
        // bool_double, bool_datetime, bool_pdatetime,
        // bool_varchar, not_support/*Seq*/, bool_ctime,
        // bool_mtime, not_support/*Extend*/, identity,
        // bool_decimal
      // }
      // ,
      // {
        // /*Decimal -> XXX*/
        // not_support/*Null*/, decimal_int, decimal_float,
        // decimal_double, decimal_datetime, decimal_pdatetime,
        // decimal_varchar, not_support/*Seq*/, decimal_ctime,
        // decimal_mtime, not_support/*Extend*/, decimal_bool,
        // identity
      // }
    // };
    //add yushengjuan [INT_32] 20151009:b
    ////////////////////////////////////////////////////////////////
    //int32->XXX
    static int int32_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObInt32Type);
      out.set_int(static_cast<int64_t>(in.get_int32()));
      return OB_SUCCESS;
    }
    static int int32_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObInt32Type);
      out.set_float(static_cast<float>(in.get_int32()));
      return OB_SUCCESS;
    }
    static int int32_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObInt32Type);
      out.set_double(static_cast<double>(in.get_int32()));
      return OB_SUCCESS;
    }
    static int int32_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObInt32Type);
      ret = varchar_printf(out, "%d", in.get_int32());
      return ret;
    }
    static int int32_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObInt32Type);
      out.set_bool(static_cast<bool>(in.get_int32()));
      return OB_SUCCESS;
    }
    static int int32_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      OB_ASSERT(in.get_type() == ObInt32Type);
      ret = varchar_printf(out, "%d", in.get_int32());
      if(ret == OB_SUCCESS)
      {
        ObString os;
        out.get_varchar(os);
        ObDecimal od;
        if(OB_SUCCESS != (ret = od.from(os.ptr(),os.length())))
        {
          TBSYS_LOG(WARN, "failed to do from in int32_decimal,os=%.*s", os.length(),os.ptr());
        }
        else
        {
          if(params.is_modify)
          {
            //mod dolphin[coalesce return type]@20151126:b
            // if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
            if((params.precision >0 && params.scale < params.precision ) && ((params.precision-params.scale)<(od.get_precision()-od.get_vscale())))
            {
              ret = OB_DECIMAL_UNLEGAL_ERROR;
              TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
            }
            else
            {
              od.set_precision(params.precision);
              od.set_scale(params.scale);
            }
          }
          if(ret == OB_SUCCESS)
          {
            out.set_varchar(os);
            out.set_decimal(od);
          }
        }
      }
      return ret;
    }
    ////////////////////////////////////////////////////////////////
    //add 20151009:e
    ObObjCastFunc OB_OBJ_CAST[ObMaxType][ObMaxType] =
    {
      {
        /*Null -> XXX*/
        identity, identity, identity,
        identity, identity, identity,
        identity, identity, identity,
        identity, identity, identity,
        identity, identity, identity,
        identity, identity//add peiouya [DATE_TIME] 20150909  yushengjuan [INT_32] 20151009
      }
      ,
      {
        /*Int -> XXX*/
        not_support/*Null*/, identity/*Int*/, int_float,
        int_double, int_datetime, int_pdatetime,
        int_varchar, not_support, int_ctime,
        int_mtime, not_support, int_bool,
        int_decimal, not_support, not_support,
        not_support, int_int32//add peiouya [DATE_TIME] 20150909   yushengjuan [INT_32] 20151009
      }
      ,
      {
        /*Float -> XXX*/
        not_support/*Null*/, float_int, identity,
        float_double, float_datetime, float_pdatetime,
        float_varchar, not_support, float_ctime,
        float_mtime, not_support, float_bool,
        float_decimal, not_support, not_support,
        not_support, float_int32//add peiouya [DATE_TIME] 20150909  yushengjuan [INT_32] 20151009
      }
      ,
      {
        /*Double -> XXX*/
        not_support/*Null*/, double_int, double_float,
        identity, double_datetime, double_pdatetime,
        double_varchar, not_support, double_ctime,
        double_mtime, not_support, double_bool,
        double_decimal, not_support, not_support,
        not_support, double_int32//add peiouya [DATE_TIME] 20150909  yushengjuan [INT_32] 20151009
      }
      ,
      {
        /*DateTime -> XXX*/
        not_support/*Null*/, datetime_int, datetime_float,
        datetime_double, identity, datetime_pdatetime,
        datetime_varchar, not_support/*Seq*/, datetime_ctime,
        datetime_mtime, not_support/*Extend*/, datetime_bool,
        datetime_decimal, datetime_date, datetime_time,
        not_support, not_support//add peiouya [DATE_TIME] 20150909  add yushengjuan [INT_32] fix_overflow bug 20151014
      }
      ,
      {
        /*PreciseDateTime -> XXX*/
        not_support/*Null*/, pdatetime_int, pdatetime_float,
        pdatetime_double, pdatetime_datetime, identity,
        pdatetime_varchar, not_support/*Seq*/,pdatetime_ctime,
        pdatetime_mtime, not_support/*Extend*/,pdatetime_bool,
        pdatetime_decimal, pdatetime_date, pdatetime_time,
        not_support, not_support //add peiouya [DATE_TIME] 20150909  add yushengjuan [INT_32] fix_overflow bug 20151014
      }
      ,
      {
        /*Varchar -> XXX*/
        not_support/*Null*/, varchar_int, varchar_float,
        varchar_double, varchar_datetime, varchar_pdatetime,
        identity, not_support/*Seq*/, varchar_ctime,
        varchar_mtime, not_support/*Extend*/, varchar_bool,
        varchar_decimal, varchar_date, varchar_time,
        not_support, varchar_int32//add peiouya [DATE_TIME] 20150909  yushengjuan [INT_32] 20151009
      }
      ,
      {
        /*Seq -> XXX*/
        not_support, not_support, not_support,
        not_support, not_support, not_support,
        not_support, not_support, not_support,
        not_support, not_support, not_support,
        not_support, not_support, not_support,
        not_support, not_support //add peiouya [DATE_TIME] 20150909  yushengjuan [INT_32] 20151009
      }
      ,
      {
        /*CreateTime -> XXX*/
        not_support/*Null*/, ctime_int, ctime_float,
        ctime_double, ctime_datetime, ctime_pdatetime,
        ctime_varchar, not_support/*Seq*/, identity,
        ctime_mtime, not_support/*Extend*/, ctime_bool,
        ctime_decimal, ctime_date, ctime_time,
        not_support, not_support //add peiouya [DATE_TIME] 20150909  yushengjuan [INT_32] 20151009
      }
      ,
      {
        /*ModifyTime -> XXX*/
        not_support/*Null*/, mtime_int, mtime_float,
        mtime_double, mtime_datetime, mtime_pdatetime,
        mtime_varchar, not_support/*Seq*/, mtime_ctime,
        identity, not_support/*Extend*/, mtime_bool,
        mtime_decimal, mtime_date, mtime_time,
        not_support, not_support //add peiouya [DATE_TIME] 20150909  yushengjuan [INT_32] 20151009
      }
      ,
      {
        /*Extend -> XXX*/
        not_support, not_support, not_support,
        not_support, not_support, not_support,
        not_support, not_support, not_support,
        not_support, not_support, not_support,
        not_support, not_support, not_support,
        not_support, not_support //add peiouya [DATE_TIME] 20150909  yushengjuan [INT_32] 20151009
      }
      ,
      {
        /*Bool -> XXX*/
        not_support/*Null*/, bool_int, bool_float,
        bool_double, bool_datetime, bool_pdatetime,
        bool_varchar, not_support/*Seq*/, bool_ctime,
        bool_mtime, not_support/*Extend*/, identity,
        bool_decimal, not_support, not_support,
        not_support, bool_int32 //add peiouya [DATE_TIME] 20150909  yushengjuan [INT_32] 20151009
      }
      ,
      {
        /*Decimal -> XXX*/
        not_support/*Null*/, decimal_int, decimal_float,
        decimal_double, decimal_datetime, decimal_pdatetime,
        decimal_varchar, not_support/*Seq*/, decimal_ctime,
        decimal_mtime, not_support/*Extend*/, decimal_bool,
        identity, not_support, not_support,
        not_support, decimal_int32 //add peiouya [DATE_TIME] 20150909  yushengjuan [INT_32] 20151009
      }
      //add peiouya [DATE_TIME] 20150831:b
      ,
      {
        /*Date -> XXX*/
        not_support/*Null*/, date_int, date_float,
        date_double, date_datetime, date_pdatetime,
        date_varchar, not_support/*Seq*/,date_ctime,
        date_mtime, not_support/*Extend*/,date_bool,
        date_decimal, identity, not_support,
        not_support, not_support //add peiouya [DATE_TIME] 20150909 yushengjuan [INT_32] 20151009
      }
      ,
      //mod peiouya [DATE_TIME] 20150913:b
      //{
      //  /*Time -> XXX*/
      //  not_support/*Null*/, time_int, time_float,
      //  time_double, time_datetime, time_pdatetime,
      //  time_varchar, not_support/*Seq*/,time_ctime,
      //  time_mtime, not_support/*Extend*/,time_bool,
      //  time_decimal, not_support, identity,
      //  not_support //add peiouya [DATE_TIME] 20150909
      //}
      {
        /*Time -> XXX*/
        not_support/*Null*/, time_int, time_float,
        time_double, not_support, not_support,
        time_varchar, not_support/*Seq*/,not_support,
        not_support, not_support/*Extend*/,time_bool,
        time_decimal, not_support, identity,
        not_support, not_support //add peiouya [DATE_TIME] 20150909  yushengjuan [INT_32] 20151009
      }
      //mod 20150913:e
      ,
      {
        /*interval -> XXX*/
        not_support, not_support, not_support,
        not_support, not_support, not_support,
        not_support, not_support, not_support,
        not_support, not_support, not_support,
        not_support, not_support, not_support,
        identity, not_support //mod peiouay [DATE_TIME] 201509012  yushengjuan [INT_32] 20151009
      }
      ,
      //add yushengjuan [INT_32] 20150930:b
      {
        /*Int32 -> XXX*/
        not_support/*Null*/, int32_int/*Int*/, int32_float,
        int32_double, not_support, not_support,
        int32_varchar, not_support, not_support,
        not_support, not_support, int32_bool,
        int32_decimal, not_support, not_support,
        not_support, identity
      }
      //add 20151009:e
    };
    //mod 20150906:e
    ////////////////////////////////////////////////////////////////
    int obj_cast(const ObObj &orig_cell, const ObObj &expected_type,
                 ObObj &casted_cell, const ObObj *&res_cell)
    {
      int ret = OB_SUCCESS;
      ObObj obj_tmp;
      //add fanqiushi
      //uint32_t nwords=orig_cell.get_nwords();
      if(orig_cell.get_type()==ObDecimalType&&expected_type.get_type()==ObDecimalType)
      {
            ObString os;
            orig_cell.get_decimal(os);
            if(OB_SUCCESS!=(ret=(casted_cell.set_decimal(os))))
            {
                TBSYS_LOG(WARN, "failed to do set_decimal in obj_cast(),os=%.*s", os.length(),os.ptr());
            }
            else
            {
                uint32_t pre=casted_cell.get_precision();
                //uint32_t scale=casted_cell.get_scale();
                uint32_t vscale=casted_cell.get_vscale();
                uint32_t schema_p=expected_type.get_precision();
                uint32_t schema_s=expected_type.get_scale();
                if((pre-vscale)>(schema_p-schema_s))
                {
                    ret=OB_DECIMAL_UNLEGAL_ERROR;
                    TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,os=%.*s,schema_p=%d,schema_s=%d,casted_cell.get_precision()=%d,casted_cell.get_vscale()=%d", os.length(),os.ptr(),schema_p,schema_s,casted_cell.get_precision(),expected_type.get_scale());
                }
                else
                {
                casted_cell.set_precision(schema_p);
                casted_cell.set_scale(schema_s);
                //casted_cell.set_vscale(orig_cell.get_vscale());
                res_cell = &casted_cell;
                }
            }

      }
      else
      {
          if (orig_cell.get_type() != expected_type.get_type())
          {
            // type cast
            ObObjCastParams params;
            params.is_modify=true;
            params.precision=expected_type.get_precision();
            params.scale=expected_type.get_scale();
            ObExprObj from;
            ObExprObj to;
            if(orig_cell.get_type()==ObDecimalType)
            {
                ObString os;
                orig_cell.get_decimal(os);
                if (OB_SUCCESS != (ret=obj_tmp.set_decimal(os)))
                {
                    TBSYS_LOG(WARN, "failed to do set_decimal in obj_cast(),os=%.*s", os.length(),os.ptr());
                }
                else
                {
                    from.assign(obj_tmp);
                }

            }
            else
            {
                from.assign(orig_cell);
            }
            if(OB_SUCCESS==ret)
            {
                to.assign(casted_cell);
                if (OB_SUCCESS != (ret = OB_OBJ_CAST[orig_cell.get_type()][expected_type.get_type()](params, from, to)))
                {
                    TBSYS_LOG(WARN, "failed to type cast obj, err=%d", ret);
                }
                else if (OB_SUCCESS != (ret = to.to(casted_cell)))
                {
                    TBSYS_LOG(WARN, "failed to convert expr_obj to obj, err=%d", ret);
                }
                else
                {
                    res_cell = &casted_cell;
                }
            }
          }
          else
          {
            res_cell = &orig_cell;
          }

      }
     // TBSYS_LOG(WARN, "test::obj_cast::p=%d,s=%d, type=%d", res_cell->get_precision(),res_cell->get_scale(),res_cell->get_type());
      return ret;
    }

      //add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_14:b
      int obj_cast_for_rowkey(const ObObj &orig_cell, const ObObj &expected_type,
                 ObObj &casted_cell, const ObObj *&res_cell)
    {
      int ret = OB_SUCCESS;
      ObObj obj_tmp;

          if (orig_cell.get_type() != expected_type.get_type())
          {
            // type cast
            ObObjCastParams params;
            params.is_modify=false;
            //params.precision=expected_type.get_precision();
           // params.scale=expected_type.get_scale();
            ObExprObj from;
            ObExprObj to;
            if(orig_cell.get_type()==ObDecimalType)
            {
                ObString os;
                orig_cell.get_decimal(os);
                if (OB_SUCCESS != (ret=obj_tmp.set_decimal(os)))
                {
                     TBSYS_LOG(WARN, "failed to do set_decimal in obj_cast(),os=%.*s", os.length(),os.ptr());
                }
                else
                {
                    from.assign(obj_tmp);
                }

            }
            else
            {
                from.assign(orig_cell);
            }
            if(OB_SUCCESS==ret)
            {
                to.assign(casted_cell);
                if (OB_SUCCESS != (ret = OB_OBJ_CAST[orig_cell.get_type()][expected_type.get_type()](params, from, to)))
                {
                    TBSYS_LOG(WARN, "failed to type cast obj, err=%d", ret);
                }
                else if (OB_SUCCESS != (ret = to.to(casted_cell)))
                {
                    TBSYS_LOG(WARN, "failed to convert expr_obj to obj, err=%d", ret);
                }
                else
                {
                    res_cell = &casted_cell;
                }
            }
          }
          else
          {
            res_cell = &orig_cell;
          }


     // TBSYS_LOG(WARN, "test::obj_cast::p=%d,s=%d, type=%d", res_cell->get_precision(),res_cell->get_scale(),res_cell->get_type());
      return ret;
    }
    //add e
    int obj_cast(ObObj &cell, const ObObjType expected_type, char* buf, int64_t buf_size, int64_t &used_buf_len)
    {
      int ret = OB_SUCCESS;
      used_buf_len = 0;
      if (cell.get_type() != expected_type)
      {
        ObObjCastParams params;
        ObExprObj from;
        ObExprObj to;

        from.assign(cell);
        if (ObVarcharType == expected_type)
        {
          ObString buffer;
          buffer.assign_ptr(buf, static_cast<ObString::obstr_size_t>(buf_size));
          ObObj varchar_cell;
          varchar_cell.set_varchar(buffer);
          to.assign(varchar_cell);
        }
        //add fanqiushi
        if(ObDecimalType == expected_type)
        {
             ObString buffer;
          buffer.assign_ptr(buf, static_cast<ObString::obstr_size_t>(buf_size));
          ObObj varchar_cell;
          varchar_cell.set_varchar(buffer);
          to.assign(varchar_cell);
        }
        //add:e

        if (OB_SUCCESS != (ret = OB_OBJ_CAST[cell.get_type()][expected_type](params, from, to)))
        {
          TBSYS_LOG(WARN, "failed to type cast obj, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = to.to(cell)))
        {
          TBSYS_LOG(WARN, "failed to convert expr_obj to obj, err=%d", ret);
        }
        else
        {
          if (ObVarcharType == expected_type)
          {
            ObString varchar;
            cell.get_varchar(varchar);
            used_buf_len = varchar.length(); // used buffer length for casting to varchar type
          }
          if (ObDecimalType == expected_type)
          {
            ObString varchar;
            cell.get_decimal(varchar);
            used_buf_len = varchar.length(); // used buffer length for casting to varchar type
          }
        }
      }
      return ret;
    }

    int obj_cast(ObObj &cell, const ObObjType expected_type, ObString &cast_buffer)
    {
      int64_t used_buf_len = 0;
      return obj_cast(cell, expected_type, cast_buffer.ptr(), cast_buffer.length(), used_buf_len);
    }
  } // end namespace common
} // end namespace oceanbase
