/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Filename: ob_system_config_key.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@taobao.com>
 *
 */

#ifndef _OB_SYSTEM_CONFIG_KEY_H_
#define _OB_SYSTEM_CONFIG_KEY_H_

#include "common/ob_define.h"
#include "common/hash/ob_hashmap.h"
#include "common/murmur_hash.h"
#include "common/ob_string.h"
#include "common/utility.h"

using oceanbase::common::ObString;
using oceanbase::common::OB_MAX_CONFIG_NAME_LEN;
using oceanbase::common::MurmurHash2;

namespace oceanbase
{
  namespace common
  {
    class ObSystemConfigKey
    {
      public:
        ObSystemConfigKey();
        /* with exactly equal comparision */
        bool operator == (const ObSystemConfigKey &other) const;
        int32_t hash() const;
        /* check if key's information can fit this. i.e. can use the
         * `key' to get config accordingly */
        bool match(const ObSystemConfigKey &key) const;
        bool match_ip_port(const ObSystemConfigKey &key) const;
        bool match_server_type(const ObSystemConfigKey &key) const;
        bool match_cluster_id(const ObSystemConfigKey &key) const;
        int set_int(const ObString &key, int64_t intval);
        int set_varchar(const ObString &key, const char * strval);
        void set_version(const int64_t version);
        int64_t get_version() const;
        /* set config name */
        void set_name(const common::ObString &name);
        void set_name(const char* name);
        void set_server_type(const ObString &server_type);
        void set_server_ip(const ObString &server_ip);
        const char* name() const { return name_; }
        int64_t to_string(char* buf, const int64_t len) const;
      private:
        static const char * DEFAULT_VALUE;
      private:
        char name_[common::OB_MAX_CONFIG_NAME_LEN];
        int64_t cluster_id_;
        char server_type_[OB_SERVER_TYPE_LENGTH];
        char server_ip_[OB_MAX_SERVER_ADDR_SIZE];
        int64_t server_port_;
        int64_t version_;
    };

    inline ObSystemConfigKey::ObSystemConfigKey()
      : name_(), cluster_id_(0), server_port_(0), version_(0)
    {
      memset(name_, 0, common::OB_MAX_CONFIG_NAME_LEN);
      strncpy(server_type_, DEFAULT_VALUE, sizeof(server_type_));
      strncpy(server_ip_, DEFAULT_VALUE, sizeof(server_ip_));
    }

    inline bool ObSystemConfigKey::operator == (const ObSystemConfigKey &other) const
    {
      return (this == &other
              || (0 == strcmp(name_, other.name_)
                  && cluster_id_ == other.cluster_id_
                  && (0 == strcmp(server_type_, other.server_type_))
                  && (0 == strcmp(server_ip_, other.server_ip_))
                  && server_port_ == other.server_port_));
    }

    inline bool ObSystemConfigKey::match(const ObSystemConfigKey &other) const
    {
      bool ret = false;
      ret = (this == &other
             || (0 == strcmp(name_, other.name_)
                 && (cluster_id_ == other.cluster_id_ || cluster_id_ == 0)
                 && ((0 == strcmp(server_type_, other.server_type_))
                 || (0 == strcmp(server_type_, DEFAULT_VALUE)))
                 && ((0 == strcmp(server_ip_, other.server_ip_))
                 || (0 == strcmp(server_ip_, DEFAULT_VALUE)))
                 && (server_port_ == other.server_port_ || server_port_ == 0)));
      if (!ret)
      {
        /* TBSYS_LOG(INFO, "Mine: [%s], urs: [%s]", */
        /*           common::to_cstring(*this), common::to_cstring(other)); */
      }
      return ret;
    }

    inline bool ObSystemConfigKey::match_ip_port(const ObSystemConfigKey &other) const
    {
      return match(other) && server_port_ != 0 && (0 != strcmp(server_ip_, DEFAULT_VALUE));
    }

    inline bool ObSystemConfigKey::match_server_type(const ObSystemConfigKey &other) const
    {
      return match(other) && (0 != strcmp(server_type_, DEFAULT_VALUE));
    }

    inline bool ObSystemConfigKey::match_cluster_id(const ObSystemConfigKey &other) const
    {
      return match(other) && cluster_id_ != 0;
    }

    inline void ObSystemConfigKey::set_name(const ObString &name)
    {
      int64_t name_length = name.length();
      if (name_length >= OB_MAX_CONFIG_NAME_LEN)
      {
        name_length = OB_MAX_CONFIG_NAME_LEN;
      }
      snprintf(name_, OB_MAX_CONFIG_NAME_LEN, "%.*s",
               static_cast<int>(name_length), name.ptr());
    }

    inline void ObSystemConfigKey::set_name(const char* name)
    {
      set_name(ObString::make_string(name));
    }

    inline void ObSystemConfigKey::set_server_type(const ObString &server_type)
    {
      int64_t server_type_length = server_type.length();
      if (server_type_length >= OB_SERVER_TYPE_LENGTH)
      {
        server_type_length = OB_SERVER_TYPE_LENGTH;
      }
      snprintf(server_type_, OB_SERVER_TYPE_LENGTH, "%.*s",
               static_cast<int>(server_type_length), server_type.ptr());
    }

    inline void ObSystemConfigKey::set_server_ip(const ObString &server_ip)
    {
      int64_t server_ip_length = server_ip.length();
      if (server_ip_length >= OB_MAX_SERVER_ADDR_SIZE)
      {
        server_ip_length = OB_MAX_SERVER_ADDR_SIZE;
      }
      snprintf(server_ip_, OB_MAX_SERVER_ADDR_SIZE, "%.*s",
               static_cast<int>(server_ip_length), server_ip.ptr());
    }

    inline int32_t ObSystemConfigKey::hash() const
    {
      int32_t hash = MurmurHash2()(this, (int32_t) sizeof (*this));
      return hash;
    }
  }
}

#endif /* _OB_SYS_CONFIG_KEY_H_ */
