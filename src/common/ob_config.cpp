/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-03-05 20:09:39 fufeng.syd>
 * Version: $Id$
 * Filename: ob_config.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#include <algorithm>
#include <cstring>
#include <ctype.h>
#include <tbsys.h>
#include "ob_config.h"

using namespace oceanbase::common;

bool ObConfigIntegralItem::parse_range(const char *range)
{
  char buff[64] = {0};
  const char *p_left = NULL;
  char *p_middle = NULL;
  char *p_right = NULL;
  bool ret = true;

  if (NULL == range)
  {
    TBSYS_LOG(ERROR, "check range is NULL!");
    ret = false;
  }
  else if ('\0' == range[0])
  { }
  else
  {
    snprintf(buff, sizeof (buff), "%s", range);
    for (uint32_t i = 0; i < strlen(buff); i++)
    {
      if (buff[i] == '(' || buff[i] == '[')
        p_left = buff + i;
      else if (buff[i] == ',')
        p_middle = buff + i;
      else if (buff[i] == ')' || buff[i] == ']')
        p_right = buff + i;
    }
    if (!p_left || !p_middle || !p_right
        || p_left >= p_middle || p_middle >= p_right)
    {
      ret = false;
      /* not validated */
    }
    else
    {
      bool valid = true;
      char ch_right = *p_right;
      *p_right = '\0';
      *p_middle = '\0';

      get(p_left + 1, valid);
      if (valid)
      {
        if (*p_left == '(')
        {
          add_checker(new (std::nothrow) ObConfigGreaterThan(p_left + 1));
        }
        else if (*p_left == '[')
        {
          add_checker(new (std::nothrow) ObConfigGreaterEqual(p_left + 1));
        }
      }

      get(p_middle + 1, valid);
      if (valid)
      {
        if (ch_right == ')')
        {
          add_checker(new (std::nothrow) ObConfigLessThan(p_middle + 1));
        }
        else if (ch_right == ']')
        {
          add_checker(new (std::nothrow) ObConfigLessEqual(p_middle + 1));
        }
      }
      ret = true;
    }
  }
  return ret;
}

int64_t ObConfigIntItem::get(const char* str, bool &valid) const
{
  char *p_end = NULL;
  int64_t value = 0;

  if (NULL == str || '\0' == str[0])
  {
    valid = false;
  }
  else
  {
    valid = true;
    value = strtol(str, &p_end, 0);
    if ('\0' == *p_end)
    {
      valid = true;
    }
    else
    {
      valid = false;
      TBSYS_LOG(ERROR, "set int error! name: [%s], str: [%s]", name(), str);
    }
  }
  return value;
}

int64_t ObConfigTimeItem::get(const char* str, bool &valid) const
{
  char* p_unit = NULL;
  int64_t value = 0;

  if (NULL == str || '\0' == str[0])
  {
    valid = false;
  }
  else
  {
    valid = true;
    value = std::max(0L, strtol(str, &p_unit, 0));
    if (0 == strcasecmp("us", p_unit))
    {
      value *= TIME_MICROSECOND;
    }
    else if (0 == strcasecmp("ms", p_unit))
    {
      value *= TIME_MILLISECOND;
    }
    else if ('\0' == *p_unit || 0 == strcasecmp("s", p_unit))
    {
      /* default is second */
      value *= TIME_SECOND;
    }
    else if (0 == strcasecmp("m", p_unit))
    {
      value *= TIME_MINUTE;
    }
    else if (0 == strcasecmp("h", p_unit))
    {
      value *= TIME_HOUR;
    }
    else if (0 == strcasecmp("d", p_unit))
    {
      value *= TIME_DAY;
    }
    else
    {
      valid = false;
      TBSYS_LOG(ERROR, "set time error! name: [%s], str: [%s]", name(), str);
    }
  }
  return value;
}

int64_t ObConfigCapacityItem::get(const char* str, bool &valid) const
{
  char* p_unit = NULL;
  int64_t value = 0;

  if (NULL == str || '\0' == str[0])
  {
    valid = false;
  }
  else
  {
    valid = true;
    value = std::max(0L, strtol(str, &p_unit, 0));

    if (0 == strcasecmp("b", p_unit)
        || 0 == strcasecmp("byte", p_unit))
    {
    }
    else if (0 == strcasecmp("kb", p_unit)
             || 0 == strcasecmp("k", p_unit))
    {
      value <<= CAP_KB;
    }
    else if ('\0' == *p_unit
             || 0 == strcasecmp("mb", p_unit)
             || 0 == strcasecmp("m", p_unit))
    {
      /* default is metabyte */
      value <<= CAP_MB;
    }
    else if (0 == strcasecmp("gb", p_unit)
             || 0 == strcasecmp("g", p_unit))
    {
      value <<= CAP_GB;
    }
    else if (0 == strcasecmp("tb", p_unit)
             || 0 == strcasecmp("t", p_unit))
    {
      value <<= CAP_TB;
    }
    else if (0 == strcasecmp("pb", p_unit)
             || 0 == strcasecmp("p", p_unit))
    {
      value <<= CAP_PB;
    }
    else
    {
      valid = false;
      TBSYS_LOG(ERROR, "set capacity error! name: [%s], str: [%s]", name(), str);
    }
  }

  return value;
}

bool ObConfigBoolItem::get(const char* str, bool &valid) const
{
  bool value = true;
  if (NULL == str)
  {
    valid = false;
    TBSYS_LOG(ERROR, "Get bool config item fail, str is NULL!");
  }
  else if (0 == strcasecmp(str, "false"))
  {
    valid = true;
    value = false;
  }
  else if (0 == strcasecmp(str, "true"))
  {
    valid = true;
    value = true;
  }
  else if (0 == strcasecmp(str, "off"))
  {
    valid = true;
    value = false;
  }
  else if (0 == strcasecmp(str, "on"))
  {
    valid = true;
    value = true;
  }
  else if (0 == strcasecmp(str, "no"))
  {
    valid = true;
    value = false;
  }
  else if (0 == strcasecmp(str, "yes"))
  {
    valid = true;
    value = true;
  }
  else if (0 == strcasecmp(str, "f"))
  {
    valid = true;
    value = false;
  }
  else if (0 == strcasecmp(str, "t"))
  {
    valid = true;
    value = true;
  }
  else
  {
    TBSYS_LOG(ERROR, "Get bool config item fail, str: [%s]", str);
    valid = false;
  }
  return value;
}


//del peiouya [MULTI_DUTY_TIME_BUG_FIX] 20150417:b
//bool ObConfigMomentItem::set(const char* str)
//{
//  int ret = true;
//  struct tm tm_value;
//  //add peiouya [MULTI_DUTY_TIME] 20141125:b
//  char *sub_str = NULL;
//  char str_tmp[OB_MAX_CONFIG_VALUE_LEN];
//  snprintf(str_tmp,sizeof(str_tmp),str);
//  //add 20141125:e
//  if (0 == strcasecmp(str, "disable"))
//  {
//    disable_ = true;
//  }
//  //del peiouya [MULTI_DUTY_TIME] 20141125:b
//  /*
//  else if (NULL == strptime(str, "%H:%M", &tm_value))
//  {
//    disable_ = true;
//    ret = false;
//    TBSYS_LOG(ERROR, "Not well-formed moment item value! str: [%s]", str);
//  }
//  else
//  {
//    disable_ = false;
//    hour_ = tm_value.tm_hour;
//    minute_ = tm_value.tm_min;
//  }
//  */
//  //del 20141125:e
//  //add peiouya [MULTI_DUTY_TIME] 20141125:b
//  else
//  {
//    bool flag = true;
//    int32_t idx = 0;
//    if(NULL != (sub_str = strtok(str_tmp,"|")))
//    {
//      if(NULL != strptime(sub_str, "%H:%M", &tm_value))
//      {
//        DTN[idx].hour_ = tm_value.tm_hour;
//        DTN[idx].minute_ = tm_value.tm_min;
//        while(NULL != (sub_str = strtok(NULL,"|")))
//        {
//	      if(MAXNUM <= idx)
//          {
//		    flag = false;
//            break;
//          }
//          if(NULL != strptime(sub_str, "%H:%M", &tm_value))
//          {
//            ++idx;
//            DTN[idx].hour_ = tm_value.tm_hour;
//            DTN[idx].minute_ = tm_value.tm_min;
//          }
//          else
//          {
//            flag = false;
//            break;
//          }
//        }
//	  }
//	  else
//	  {
//	    flag = false;
//	  }
//    }
//    else
//    {
//      flag = false;
//    }
//    if(!flag)
//    {
//      disable_ = true;
//      ret = false;
//      //TBSYS_LOG(ERROR, "Not well-formed moment item value! str: [%s]", str);
//	  TBSYS_LOG(ERROR, "Not well-formed moment item value or dutytime num larger than %d! str: [%s]", MAXNUM, str);
//      for(idx = 0; idx < MAXNUM; idx++)
//      {
//        DTN[idx].hour_ = -1;
//        DTN[idx].minute_ = -1;
//      }
//    }
//    else
//    {
//      disable_ = false;
//    }
//  }
//  //add 20141125:e
//  return ret;
//}
//del 20150417:e

//add peiouya [MULTI_DUTY_TIME_BUG_FIX] 20150417:b
void ObConfigMomentItem::reset_DTN()
{
  for (int32_t idx = 0; idx < MAXNUM; ++idx)
  {
    DTN[idx].hour_ = -1;
    DTN[idx].minute_ = -1;
  }
}
//add 20150417:e

//add peiouya [MULTI_DUTY_TIME_BUG_FIX] 20150417:b
//BUG_fix:the deleted dutytime is valid when dutytime exists more than one
bool ObConfigMomentItem::set(const char* str)
{
  int ret = true;
  struct tm tm_value;
  char *sub_str = NULL;
  char str_tmp[OB_MAX_CONFIG_VALUE_LEN];
  snprintf(str_tmp,sizeof(str_tmp),str);
  if (0 == strcasecmp(str, "disable"))
  {
    disable_ = true;
    reset_DTN();
  }
  else
  {
    bool flag = true;
    int32_t idx = 0;
    struct DutyTimeNode Tmp_DTN[MAXNUM];
    if(NULL != (sub_str = strtok(str_tmp,"|")))
    {
      if(NULL != strptime(sub_str, "%H:%M", &tm_value))
      {
        Tmp_DTN[idx].hour_ = tm_value.tm_hour;
        Tmp_DTN[idx].minute_ = tm_value.tm_min;
        while(NULL != (sub_str = strtok(NULL,"|")))
        {
	      if(MAXNUM <= idx)
          {
		    flag = false;
            break;
          }
          if(NULL != strptime(sub_str, "%H:%M", &tm_value))
          {
            ++idx;
            Tmp_DTN[idx].hour_ = tm_value.tm_hour;
            Tmp_DTN[idx].minute_ = tm_value.tm_min;
          }
          else
          {
            flag = false;
            break;
          }
        }
	  }
	  else
	  {
	    flag = false;
	  }
    }
    else
    {
      flag = false;
    }
    if(!flag)
    {
      disable_ = true;
      reset_DTN();
      ret = false;
	  TBSYS_LOG(ERROR, "Not well-formed moment item value or dutytime num larger than %d! str: [%s]", MAXNUM, str);
    }
    else
    {
      for (int32_t i = 0; i < MAXNUM; i++)
      {
        DTN[i].hour_ = Tmp_DTN[i].hour_;
        DTN[i].minute_ = Tmp_DTN[i].minute_;
      }
      disable_ = false;
    }
  }
  //add 20141125:e
  return ret;
}
