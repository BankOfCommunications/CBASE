/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_flag.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_FLAG_H
#define _OB_FLAG_H 1
#include <tbsys.h>
#include "common/ob_define.h"
#include "common/serialization.h"

namespace oceanbase
{
  namespace common
  {
    int string_copy(char* dst, const char* src, int64_t max_length);
    int decode_string(char* str_buf, const char* buf, const int64_t data_len, int64_t& pos,  int64_t max_length);

    // @see ../rootserver/ob_root_config.h as an example
    template<class T>
    class ObFlag
    {
      public:
        ObFlag();
        ~ObFlag();
        int set_flag(const char* section_str, const char* flag_str, const T& default_value);
        const T& get() const;
        int set(const T& v);
        const char* get_section_str() const;
        const char* get_flag_str() const;
        const T& get_default() const;

        NEED_SERIALIZE_AND_DESERIALIZE;

      private:
        // disallow copy
        ObFlag(const ObFlag &other);
        ObFlag& operator=(const ObFlag &other);
      private:
        // data members
        T value_;
        T default_value_;
        char section_str_[OB_MAX_SECTION_NAME_LENGTH]; // section string in config file
        char flag_str_[OB_MAX_FLAG_NAME_LENGTH];    // flag string in config file
    };

    template<class T>
    ObFlag<T>::ObFlag()
    { 
      memset(section_str_, 0, sizeof(section_str_));
      memset(flag_str_, 0, sizeof(flag_str_));
    }

    template<class T>
    int ObFlag<T>::set_flag(const char* section_str, const char* flag_str, const T& default_value)
    {
      int ret = OB_SUCCESS; 
      if(OB_SUCCESS == ret)
      {
        ret = string_copy(this->section_str_, section_str, OB_MAX_SECTION_NAME_LENGTH);
      }
      if(OB_SUCCESS == ret)
      {
        ret = string_copy(this->flag_str_, flag_str, OB_MAX_SECTION_NAME_LENGTH);
      }
      if(OB_SUCCESS == ret)
      {
        default_value_ = default_value;
      }
      return ret;
    }

    template<class T>
    ObFlag<T>::~ObFlag()
    {
    }
    
    template<class T>
    const T& ObFlag<T>::get() const
    {
      return value_;
    }
    
    template<class T>
    int ObFlag<T>::set(const T& v)
    {
      value_ = v;
      return common::OB_SUCCESS;
    }

    template<class T>
    const char* ObFlag<T>::get_section_str() const
    {
      return section_str_;
    }
    
    template<class T>
    const char* ObFlag<T>::get_flag_str() const
    {
      return flag_str_;
    }

    template<class T>
    const T& ObFlag<T>::get_default() const
    {
      return default_value_;
    }
 
    // specialization of ObFlag<const char*>
    template<>
    class ObFlag<const char*>
    {
      public:
        ObFlag();
        ~ObFlag();
        int set_flag(const char* section_str, const char* flag_str, const char* default_value);
        const char* get() const;
        int set(const char* v);
        const char* get_section_str() const;
        const char* get_flag_str() const;
        const char* get_default() const;

        NEED_SERIALIZE_AND_DESERIALIZE;

      private:
        // disallow copy
        ObFlag(const ObFlag &other);
        ObFlag& operator=(const ObFlag &other);
      private:
        // data members
        char value_[OB_MAX_FLAG_VALUE_LENGTH];
        char default_value_[OB_MAX_FLAG_VALUE_LENGTH];
        char section_str_[OB_MAX_SECTION_NAME_LENGTH]; // section string in config file
        char flag_str_[OB_MAX_FLAG_NAME_LENGTH]; // flag string in config file
    };

    inline ObFlag<const char*>::ObFlag()
    {
      memset(section_str_, 0, sizeof(section_str_));
      memset(flag_str_, 0, sizeof(flag_str_));
      memset(value_, 0, sizeof(value_));
      memset(default_value_, 0, sizeof(default_value_));
    }
    
    inline int ObFlag<const char*>::set_flag(const char* section_str, const char* flag_str, const char* default_value)
    {
      int ret = OB_SUCCESS;
      if(OB_SUCCESS == ret)
      {
        ret = string_copy(this->section_str_, section_str, OB_MAX_SECTION_NAME_LENGTH);
      }
      if(OB_SUCCESS == ret)
      {
        ret = string_copy(this->flag_str_, flag_str, OB_MAX_FLAG_NAME_LENGTH);
      }
      if(OB_SUCCESS == ret)
      {
        ret = string_copy(this->default_value_, default_value, OB_MAX_FLAG_VALUE_LENGTH);
      }
      return ret;
    }

    inline ObFlag<const char*>::~ObFlag()
    {
    }

    inline const char* ObFlag<const char*>::get() const
    {
      return value_;
    }

    inline int ObFlag<const char*>::set(const char* v)
    {
      int ret = common::OB_SUCCESS;
      int len = -1;
      if (NULL == v)
      {
        TBSYS_LOG(ERROR, "invalid flag value, flag=%s v=NULL", flag_str_);
        ret = common::OB_INVALID_ARGUMENT;
      }
      else if (common::OB_MAX_FLAG_VALUE_LENGTH <= (len = static_cast<int32_t>(strlen(v))))
      {
        TBSYS_LOG(ERROR, "buffer not enough, flag=%s v=%s", flag_str_, v);
        ret = common::OB_BUF_NOT_ENOUGH;
      }
      else
      {
        memcpy(value_, v, len+1);
      }
      return ret;
    }

    inline const char* ObFlag<const char*>::get_section_str() const
    {
      return section_str_;
    }    

    inline const char* ObFlag<const char*>::get_flag_str() const
    {
      return flag_str_;
    }

    inline const char* ObFlag<const char*>::get_default() const
    {
      return default_value_;
    }

    inline void g_print_flag(const ObFlag<int>& flag)
    {
      TBSYS_LOG(INFO, "flag %s=%d", flag.get_flag_str(), flag.get());
    }
    
    inline void g_print_flag(const ObFlag<int64_t>& flag)
    {
      TBSYS_LOG(INFO, "flag %s=%ld", flag.get_flag_str(), flag.get());
    }

    inline void g_print_flag(const ObFlag<const char*>& flag)
    {
      TBSYS_LOG(INFO, "flag %s=%s", flag.get_flag_str(), flag.get());
    }
  } // end namespace common
} // end namespace oceanbase

#define LOAD_FLAG_INT(cconfig, flag)                                    \
  if (OB_SUCCESS == ret)                                                \
  {                                                                     \
    int tmp_int = cconfig.getInt((flag).get_section_str(), flag.get_flag_str(), flag.get_default()); \
    if (oceanbase::common::OB_SUCCESS != (ret = flag.set(tmp_int)))     \
    {                                                                   \
      TBSYS_LOG(ERROR, "invalid flag value, set default value");        \
      flag.set(flag.get_default());                                     \
    }                                                                   \
  }

#define LOAD_FLAG_STRING(cconfig, flag)                                 \
  if (OB_SUCCESS == ret)                                                \
  {                                                                     \
    const char* tmp_str = cconfig.getString(flag.get_section_str(), flag.get_flag_str(), flag.get_default()); \
    if (oceanbase::common::OB_SUCCESS != (ret = flag.set(tmp_str)))     \
    {                                                                   \
      TBSYS_LOG(ERROR, "invalid flag value, set default value");        \
      flag.set(flag.get_default());                                     \
    }                                                                   \
  }

#define LOAD_FLAG_INT64(cconfig, flag)                                  \
  if (OB_SUCCESS == ret)                                                \
  {                                                                     \
    const char* tmp_str = cconfig.getString(flag.get_section_str(), flag.get_flag_str(), NULL); \
    if (NULL == tmp_str)                                                \
    {                                                                   \
      ret = flag.set(flag.get_default());                               \
    }                                                                   \
    else if ('\0' == *tmp_str)                                          \
    {                                                                   \
      TBSYS_LOG(WARN, "flag value is empty, flag=%s", flag.get_flag_str()); \
      ret = flag.set(flag.get_default());                               \
    }                                                                   \
    else                                                                \
    {                                                                   \
      char *endptr = NULL;                                              \
      ret = flag.set(strtoll(tmp_str, &endptr, 10));                    \
      if (oceanbase::common::OB_SUCCESS != ret)                         \
      {                                                                 \
        TBSYS_LOG(ERROR, "invalid flag value");                         \
      }                                                                 \
      else if ('\0' != *endptr)                                         \
      {                                                                 \
        ret = flag.set(flag.get_default());                             \
        TBSYS_LOG(ERROR, "invalid flag value, flag=%s v=%s", flag.get_flag_str(), tmp_str); \
        ret = oceanbase::common::OB_INVALID_ARGUMENT;                   \
      }                                                                 \
    }                                                                   \
  }

#endif /* _OB_FLAG_H */

