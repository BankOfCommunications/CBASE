/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-03-07 13:45:43 fufeng.syd>
 * Version: $Id$
 * Filename: ob_system_config_value.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#ifndef _OB_SYSTEM_CONFIG_VALUE_H_
#define _OB_SYSTEM_CONFIG_VALUE_H_

#include "common/ob_define.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace common
  {
    class ObSystemConfigValue
    {
      public:
        ObSystemConfigValue();

        void set_value(const char* value);
        void set_value(const ObString &value);
        const char* value() const { return value_; }
        void set_info(const char* info);
        void set_info(const ObString &info);
        const char* info() const { return info_; }
      private:
        char value_[OB_MAX_CONFIG_VALUE_LEN];
        char info_[OB_MAX_CONFIG_INFO_LEN];
    };

    inline ObSystemConfigValue::ObSystemConfigValue()
    {
      memset(value_, 0, OB_MAX_CONFIG_VALUE_LEN);
      memset(info_, 0, OB_MAX_CONFIG_INFO_LEN);
    }

    inline void ObSystemConfigValue::set_value(const ObString &value)
    {
      int64_t value_length = value.length();
      if (value_length >= OB_MAX_CONFIG_VALUE_LEN)
      {
        value_length = OB_MAX_CONFIG_VALUE_LEN;
      }
      snprintf(value_, OB_MAX_CONFIG_VALUE_LEN, "%.*s",
               static_cast<int>(value_length), value.ptr());
    }

    inline void ObSystemConfigValue::set_value(const char* value)
    {
      set_value(ObString::make_string(value));
    }

    inline void ObSystemConfigValue::set_info(const ObString &info)
    {
      int64_t info_length = info.length();
      if (info_length >= OB_MAX_CONFIG_INFO_LEN)
      {
        info_length = OB_MAX_CONFIG_INFO_LEN;
      }
      snprintf(info_, OB_MAX_CONFIG_INFO_LEN, "%.*s",
               static_cast<int>(info_length), info.ptr());
    }

    inline void ObSystemConfigValue::set_info(const char* info)
    {
      set_info(ObString::make_string(info));
    }

  }
}



#endif /* _OB_SYS_CONFIG_VALUE_H_ */
