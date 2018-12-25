/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-03-06 20:53:56 fufeng.syd>
 * Version: $Id$
 * Filename: ob_config_helper.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#ifndef _OB_CONFIG_HELPER_H_
#define _OB_CONFIG_HELPER_H_

#include <arpa/inet.h>
#include "common/ob_define.h"
#include "common/murmur_hash.h"
#include "common/hash/ob_hashmap.h"

#define _DEF_CONFIG_EASY(cfg,name,args...)                              \
  class ObConfig##cfg##Item##_##name                                    \
    : public ObConfig##cfg##Item                                        \
    {                                                                   \
      public:                                                           \
      ObConfig##cfg##Item##_##name()                                    \
        : ObConfig##cfg##Item(p_container_,#name,args)                  \
      {}                                                                \
        template <class T>                                              \
          ObConfig##cfg##Item##_##name& operator=(T value)              \
          {                                                             \
            ObConfig##cfg##Item::operator=(value);                      \
              return *this;                                             \
          }                                                             \
    } name;

#define _DEF_CONFIG_RANGE_EASY(cfg,name,args...)                        \
  class ObConfig##cfg##Item##_##name                                    \
    : public ObConfig##cfg##Item                                        \
    {                                                                   \
      public:                                                           \
      ObConfig##cfg##Item##_##name()                                    \
        : ObConfig##cfg##Item(p_container_,#name,args)                  \
      {}                                                                \
        ObConfig##cfg##Item##_##name& operator=(int64_t value)          \
          {                                                             \
            ObConfig##cfg##Item::operator=(value);                      \
              return *this;                                             \
          }                                                             \
    } name;

#define _DEF_CONFIG_CHECKER_EASY(cfg,name,def,checker,args...)  \
  class ObConfig##cfg##Item##_##name                            \
    : public ObConfig##cfg##Item                                \
    {                                                           \
      public:                                                   \
      ObConfig##cfg##Item##_##name()                            \
        : ObConfig##cfg##Item(p_container_,#name,def,args)      \
      { add_checker(new checker()); }                           \
        template <class T>                                      \
          ObConfig##cfg##Item##_##name& operator=(T value)      \
          {                                                     \
            ObConfig##cfg##Item::operator=(value);              \
              return *this;                                     \
          }                                                     \
    } name;

#define _DEF_CONFIG_IP_EASY(cfg,name,def,args...)       \
  _DEF_CONFIG_CHECKER_EASY(cfg,name,                    \
                           def,ObConfigIpChecker,args)

#define DEF_INT(args...)                        \
  _DEF_CONFIG_RANGE_EASY(Int,args)
#define DEF_CAP(args...)                        \
  _DEF_CONFIG_RANGE_EASY(Capacity,args)
#define DEF_TIME(args...)                       \
  _DEF_CONFIG_RANGE_EASY(Time,args)
#define DEF_BOOL(args...)                       \
  _DEF_CONFIG_EASY(Bool,args)
#define DEF_STR(args...)                        \
  _DEF_CONFIG_EASY(String,args)
#define DEF_IP(args...)                         \
  _DEF_CONFIG_IP_EASY(String,args)
#define DEF_MOMENT(args...)                     \
  _DEF_CONFIG_EASY(Moment,args)

namespace oceanbase
{
  namespace common
  {
    class ObConfigItem;
    class ObConfigIntegralItem;
    class ObConfigAlwaysTrue;

    typedef ObConfigAlwaysTrue True;
  }
}

namespace oceanbase
{
  namespace common
  {
    class ObConfigChecker
    {
      public:
        virtual ~ObConfigChecker() {}
        virtual bool check(const ObConfigItem &t) const = 0;
    };

    class ObConfigAlwaysTrue
      : public ObConfigChecker
    {
      public:
        bool check(const ObConfigItem &t) const
        { UNUSED(t); return true; }
    };

    class ObConfigIpChecker
      : public ObConfigChecker
    {
      public:
        bool check(const ObConfigItem &t) const;
    };

    class ObConfigConsChecker
      : public ObConfigChecker
    {
      public:
        ObConfigConsChecker(const ObConfigChecker *left,
                            const ObConfigChecker *right)
          : left_(left), right_(right)
        {}

        ~ObConfigConsChecker()
        {
          if (NULL != left_)
            delete left_;
          if (NULL != right_)
            delete right_;
        }
        bool check(const ObConfigItem &t) const;
      private:
        const ObConfigChecker *left_;
        const ObConfigChecker *right_;
    };

    class ObConfigBinaryChecker
      : public ObConfigChecker
    {
      public:
        ObConfigBinaryChecker(const char* str)
        {
          snprintf(val_, sizeof (val_), "%s", str);
        }
        const char* value() const { return val_; }
      protected:
        char val_[OB_MAX_CONFIG_VALUE_LEN];
    };

    class ObConfigGreaterThan
      : public ObConfigBinaryChecker
    {
      public:
        ObConfigGreaterThan(const char* str)
          : ObConfigBinaryChecker(str)
        {}
        bool check(const ObConfigItem& t) const;
    };

    class ObConfigGreaterEqual
      : public ObConfigBinaryChecker
    {
      public:
        ObConfigGreaterEqual(const char* str)
          : ObConfigBinaryChecker(str)
        {}
        bool check(const ObConfigItem& t) const;
    };

    class ObConfigLessThan
      : public ObConfigBinaryChecker
    {
      public:
        ObConfigLessThan(const char* str)
          : ObConfigBinaryChecker(str)
        {}
        bool check(const ObConfigItem& t) const;
    };

    class ObConfigLessEqual
      : public ObConfigBinaryChecker
    {
      public:
        ObConfigLessEqual(const char* str)
          : ObConfigBinaryChecker(str)
        {}
        bool check(const ObConfigItem& t) const;
    };

    /*
     * config item container
     */
    class ObConfigStringKey
    {
      public:
        ObConfigStringKey()
        {
          memset(str_, sizeof (str_), 0);
        }
        ObConfigStringKey(const char* str)
        {
          snprintf(str_, sizeof (str_), "%s", str);
        }

        int64_t hash() const
        {
          return murmurhash2(str_, (int32_t)strlen(str_), 0);
        }

        /* case unsensitive */
        bool operator == (const ObConfigStringKey& str) const
        {
          return 0 == strcasecmp(str.str_, this->str_);
        }

        const char* str() const
        {
          return str_;
        }
      private:
        char str_[OB_MAX_CONFIG_NAME_LEN];
    };

    template <class Key, class Value, int num>
    class __ObConfigContainer
      : public hash::ObHashMap<Key, Value*>
    {
      public:
        __ObConfigContainer(__ObConfigContainer *&p)
        {
          /* The first server config object */
          if (NULL == p)
          {
            p = this;
            this->create(num);
          }
        }
    };

    typedef __ObConfigContainer<ObConfigStringKey,
                                ObConfigItem, OB_MAX_CONFIG_NUMBER> ObConfigContainer;
  } /* namespace common */
}   /* namespace oceanbase */


#endif /* _OB_CONFIG_HELPER_H_ */


