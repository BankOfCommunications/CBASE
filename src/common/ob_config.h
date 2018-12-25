/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-03-13 20:39:59 fufeng.syd>
 * Version: $Id$
 * Filename: Config.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#ifndef _OB_CONFIG_H_
#define _OB_CONFIG_H_

#include "common/ob_config_helper.h"

namespace oceanbase
{
  namespace common
  {
    enum ObConfigItemType
    {
      OB_CONFIG_DYNAMIC = 1,   /* for which works immediately */
      OB_CONFIG_MANUAL,        /* for which should work manually */
      OB_CONFIG_STATIC,        /* for which won't work until restart server */
    };

    class ObConfigItem
    {
      public:
        ObConfigItem()
          : ck_(NULL), type_(OB_CONFIG_DYNAMIC)
        {
          memset(value_str_, sizeof (value_str_), 0);
          memset(name_str_, sizeof (name_str_), 0);
          memset(info_str_, sizeof (info_str_), 0);
        }
        virtual ~ObConfigItem()
        {
          if (NULL != ck_)
            delete ck_;
        }

        void init(const char* name, const char* def,
                  ObConfigItemType type, const char* info)
        {
          if (NULL == name || NULL == def || NULL == info)
          {
            TBSYS_LOG(ERROR, "Null pointer!");
          }
          else
          {
            set_name(name);
            if (!set_value(def))
            {
              TBSYS_LOG(ERROR, "Set config item value fail! name: [%s], value: [%s]", name, def);
            }
            set_type(type);
            set_info(info);
          }
        }

        void add_checker(const ObConfigChecker *new_ck)
        { ck_ = new (std::nothrow) ObConfigConsChecker(ck_, new_ck);}
        bool check() const
        { return ck_ == NULL ? true : ck_->check(*this); }

        ObConfigItemType set_type(ObConfigItemType type)
        { return type_ = type; }
        ObConfigItemType type() const
        { return type_; }

        bool set_value(const char* str)
        {
          snprintf(value_str_, sizeof (value_str_), "%s", str);
          return set(str);
        }
        void set_name(const char* name)
        {
          snprintf(name_str_, sizeof (name_str_), "%s", name);
        }
        void set_info(const char* info)
        {
          snprintf(info_str_, sizeof (info_str_), "%s", info);
        }

        const char* str() const
        { return value_str_; }
        const char* name() const
        { return name_str_; }
        const char* info() const
        { return info_str_; }
      protected:
        virtual bool set(const char* str) = 0;

      protected:
        const ObConfigChecker *ck_;
        ObConfigItemType type_;
        char value_str_[OB_MAX_CONFIG_VALUE_LEN];
        char name_str_[OB_MAX_CONFIG_NAME_LEN];
        char info_str_[OB_MAX_CONFIG_INFO_LEN];
    };

    class ObConfigIntegralItem
      : public ObConfigItem
    {
      public:
        ObConfigIntegralItem()
          : value_(0)
        {}
        void init(const char* name, const char* def,
                        const char* range, ObConfigItemType type,
                        const char* info)
        {
          ObConfigItem::init(name, def, type, info);
          if (NULL == range)
          {
            TBSYS_LOG(ERROR, "Range is NULL");
          }
          else if (!parse_range(range))
          {
            TBSYS_LOG(ERROR, "Parse check range fail! range: [%s]", range);
          }
        }

        bool operator >(const char* str) const
        { bool valid = true; return get() > get(str, valid) && valid; }
        bool operator >=(const char* str) const
        { bool valid = true; return get() >= get(str, valid) && valid; }
        bool operator <(const char* str) const
        { bool valid = true; return get() < get(str, valid) && valid; }
        bool operator <=(const char* str) const
        { bool valid = true; return get() <= get(str, valid) && valid; }

        bool parse_range(const char* range);
        int64_t get() const { return value_; }
        operator const int64_t& () const { return value_; }

      protected:
        bool set(const char* str)
        { bool valid = true; value_ = get(str, valid); return valid; }

        virtual int64_t get(const char* str, bool &valid) const = 0;

      private:
        int64_t value_;
    };

    class ObConfigCapacityItem
      : public ObConfigIntegralItem
    {
      private:
        enum CAP_UNIT {
          /* shift bits between unit of byte and that */
          CAP_B = 0,
          CAP_KB = 10,
          CAP_MB = 20,
          CAP_GB = 30,
          CAP_TB = 40,
          CAP_PB = 50,
        };
      public:
        ObConfigCapacityItem(ObConfigContainer *container,
                         const char* name, const char* def,
                         const char* range, ObConfigItemType type, const char* info)
        {
          if (NULL != container)
          {
            container->set(name, this);
          }
          init(name, def, range, type, info);
        }

        ObConfigCapacityItem(ObConfigContainer *container,
                         const char* name, const char* def,
                         const char* range, const char* info)
        {
          if (NULL != container)
          {
            container->set(name, this);
          }
          init(name, def, range, OB_CONFIG_DYNAMIC, info);
        }

        ObConfigCapacityItem(ObConfigContainer *container,
                         const char* name, const char* def, const char* info)
        {
          if (NULL != container)
          {
            container->set(name, this);
          }
          init(name, def, "", OB_CONFIG_DYNAMIC, info);
        }

        ObConfigCapacityItem& operator = (int64_t value)
        {
          char buf[OB_MAX_CONFIG_VALUE_LEN];
          snprintf(buf, sizeof (buf), "%ldB", value);
          set_value(buf);
          return *this;
        }

      protected:
        int64_t get(const char* str, bool &valid) const;
    };

    class ObConfigTimeItem
      : public ObConfigIntegralItem
    {
      public:
        ObConfigTimeItem(ObConfigContainer *container,
                        const char* name, const char* def,
                        const char* range, ObConfigItemType type, const char* info)
        {
          if (NULL != container)
          {
            container->set(name, this);
          }
          init(name, def, range, type, info);
        }

        ObConfigTimeItem(ObConfigContainer *container,
                        const char* name, const char* def,
                        const char* range, const char* info)
        {
          if (NULL != container)
          {
            container->set(name, this);
          }
          init(name, def, range, OB_CONFIG_DYNAMIC, info);
        }

        ObConfigTimeItem(ObConfigContainer *container,
                        const char* name, const char* def, const char* info)
        {
          if (NULL != container)
          {
            container->set(name, this);
          }
          init(name, def, "", OB_CONFIG_DYNAMIC, info);
        }

        ObConfigTimeItem& operator = (int64_t value)
        {
          char buf[OB_MAX_CONFIG_VALUE_LEN];
          snprintf(buf, sizeof (buf), "%ldus", value);
          set_value(buf);
          return *this;
        }

      protected:
        int64_t get(const char* str, bool &valid) const;

      private:
        const static int64_t TIME_MICROSECOND = 1UL;
        const static int64_t TIME_MILLISECOND = 1000UL;
        const static int64_t TIME_SECOND = 1000 * 1000UL;
        const static int64_t TIME_MINUTE = 60 * 1000 * 1000UL;
        const static int64_t TIME_HOUR = 60 * 60 * 1000 * 1000UL;
        const static int64_t TIME_DAY = 24 * 60 * 60 * 1000 * 1000UL;
    };

    class ObConfigIntItem
      : public ObConfigIntegralItem
    {
      public:
        ObConfigIntItem(ObConfigContainer *container,
                        const char* name, const char* def,
                        const char* range, ObConfigItemType type, const char* info)
        {
          if (NULL != container)
          {
            container->set(name, this);
          }
          init(name, def, range, type, info);
        }

        ObConfigIntItem(ObConfigContainer *container,
                        const char* name, const char* def,
                        const char* range, const char* info)
        {
          if (NULL != container)
          {
            container->set(name, this);
          }
          init(name, def, range, OB_CONFIG_DYNAMIC, info);
        }

        ObConfigIntItem(ObConfigContainer *container,
                        const char* name, const char* def, const char* info)
        {
          if (NULL != container)
          {
            container->set(name, this);
          }
          init(name, def, "", OB_CONFIG_DYNAMIC, info);
        }

        ObConfigIntItem& operator = (int64_t value)
        {
          char buf[OB_MAX_CONFIG_VALUE_LEN];
          snprintf(buf, sizeof (buf), "%ld", value);
          set_value(buf);
          return *this;
        }

      protected:
        int64_t get(const char* str, bool &valid) const;
    };
    //add peiouya [MULTI_DUTY_TIME] 20141125:b
    /**
      *@brief   每日合并时间点由单一时间点设定，变更为多个合并时间点;目前暂定为MAXNUM
      *@author  裴欧亚
      *@date    20141125
      *@version 0.1
      */
    struct DutyTimeNode {
        DutyTimeNode():hour_(-1),minute_(-1){};
        int hour_;
        int minute_;
    };
    //add 20141125:e
    class ObConfigMomentItem
      : public ObConfigItem
    {
      public:
        static const int32_t MAXNUM = 8;  //add peiouya [MULTI_DUTY_TIME] 20141125
        ObConfigMomentItem(ObConfigContainer *container,
                           const char* name, const char* def,
                           ObConfigItemType type, const char* info)
          : disable_(true)/*, hour_(-1), minute_(-1)*/
        {
          if (NULL != container)
          {
            container->set(name, this);
          }
          init(name, def, type, info);
        }
        bool set(const char* str);
        bool disable() const
        { return disable_; }
        //del peiouya [MULTI_DUTY_TIME] 20141125:b
        /*
        int hour() const
        { return hour_; }
        int minute() const
        { return minute_; }
        */
        //del 20141125:e
        //add peiouya [MULTI_DUTY_TIME] 20141125:b
        bool is_duty_time(const struct DutyTimeNode& dutytime) const
        {
          bool ret = false;
          for (int32_t i = 0; i < MAXNUM; i++)
          {
            if(dutytime.hour_ == DTN[i].hour_ && dutytime.minute_ == DTN[i].minute_)
            {
              ret = true;
              break;
            }
          }
          return ret;
        }
        //add 20141125:e
        //add peiouya [MULTI_DUTY_TIME_BUG_FIX] 20150417:b
      private:
        void reset_DTN();
        //add 20150417:e
      private:
        bool disable_;
        //del peiouya [MULTI_DUTY_TIME] 20141125:b
        /*
        int hour_;
        int minute_;
        */
        //del 20141125:e
        struct DutyTimeNode DTN[MAXNUM];  //add peiouya [MULTI_DUTY_TIME] 20141125
    };

    class ObConfigBoolItem
      : public ObConfigItem
    {
      public:
        ObConfigBoolItem(ObConfigContainer *container,
                         const char* name, const char* def,
                         ObConfigItemType type, const char* info)
          : value_(false)
        {
          if (NULL != container)
          {
            container->set(name, this);
          }
          init(name, def, type, info);
        }

        ObConfigBoolItem(ObConfigContainer *container,
                         const char* name, const char* def, const char* info)
          : value_(false)
        {
          if (NULL != container)
          {
            container->set(name, this);
          }
          init(name, def, OB_CONFIG_DYNAMIC, info);
        }

        operator bool() const { return value_; }
        ObConfigBoolItem& operator = (bool value)
        { set_value(value ? "True" : "False"); return *this; }

      protected:
        bool set(const char* str)
        { bool valid = true; value_ = get(str, valid); return valid; }
        bool get(const char* str, bool &valid) const;

      private:
        bool value_;
    };

    class ObConfigStringItem
      : public ObConfigItem
    {
      public:
        ObConfigStringItem(ObConfigContainer *container,
                           const char* name, const char* def,
                           ObConfigItemType type, const char* info)
        {
          if (NULL != container)
          {
            container->set(name, this);
          }
          init(name, def, type, info);
        }

        ObConfigStringItem(ObConfigContainer *container,
                           const char* name, const char* def, const char* info)
        {
          if (NULL != container)
          {
            container->set(name, this);
          }
          init(name, def, OB_CONFIG_DYNAMIC, info);
        }

        operator const char*() const { return value_str_; }

      protected:
        bool set(const char* str)
        { UNUSED(str); return true; }
    };
  } /* namespace common */
} /* namesapce oceanbase */

#endif /* _OB_CONFIG_H_ */

