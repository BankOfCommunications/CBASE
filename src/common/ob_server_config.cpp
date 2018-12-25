/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 15:16:01 fufeng.syd>
 * Version: $Id$
 * Filename: ob_server_config.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#include "ob_server_config.h"
#include "common/ob_config.h"

using namespace oceanbase::common;

ObConfigContainer *ObServerConfig::p_container_;

ObServerConfig::ObServerConfig()
  : container_(p_container_), system_config_(NULL)
{
}

ObServerConfig::~ObServerConfig()
{
}

int ObServerConfig::init(const ObSystemConfig &config)
{
  int ret = OB_SUCCESS;
  system_config_ = &config;
  if (NULL == system_config_)
  {
    ret = OB_INIT_FAIL;
  }
  return ret;
}

int ObServerConfig::read_config()
{
  int ret = OB_SUCCESS;
  ObSystemConfigKey key;
  ObServer server;
  server.set_ipv4_addr(tbsys::CNetUtil::getLocalAddr(devname), 0);
  char local_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (server.ip_to_string(local_ip, sizeof(local_ip)) != true)
  {
    ret = OB_CONVERT_ERROR;
  }
  else
  {
    key.set_varchar(ObString::make_string("svr_type"), print_role(get_server_type()));
    key.set_int(ObString::make_string("svr_port"), port);
    key.set_varchar(ObString::make_string("svr_ip"), local_ip);
    key.set_int(ObString::make_string("cluster_id"), int64_t(cluster_id));
    ObConfigContainer::const_iterator it = container_.begin();
    for (; it != container_.end(); it++)
    {
      key.set_name(it->first.str());
      if (OB_SUCCESS !=
          (ret = system_config_->read_config(key, *(it->second))))
      {
        TBSYS_LOG(ERROR, "Read config error! name: [%s], ret: [%d]", it->first.str(), ret);
      }
    }
  }
  return ret;
}

int ObServerConfig::get_update_sql(char* sql, int64_t len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObServer server;
  server.set_ipv4_addr(tbsys::CNetUtil::getLocalAddr(devname), 0);
  char local_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (server.ip_to_string(local_ip, sizeof(local_ip)) != true)
  {
    ret = OB_CONVERT_ERROR;
  }
  else
  {
    pos += snprintf(sql, len, "REPLACE INTO __all_sys_config_stat("
                    "cluster_id,svr_type,svr_ip,svr_port,name,value,info"
                    ") VALUES");
    ObConfigContainer::const_iterator it = container_.begin();
    for (; it != container_.end(); it++)
    {
      if (pos < 0 || pos >= len -1)
      {
        ret = OB_BUF_NOT_ENOUGH;
        break;
      }
      pos += snprintf(sql + pos, len - pos, "(%ld,'%s','%s',%d,'%s','%s','%s'),",
                      int64_t(cluster_id), print_role(get_server_type()), local_ip, static_cast<int32_t>(port),
                      it->first.str(), it->second->str(), it->second->info());
    }
    if (pos >= 0 && pos < len)
    {
      /* remove last dot */
      sql[pos - 1] = '\0';
    }
    else
    {
      ret = OB_BUF_NOT_ENOUGH;
    }
  }
  return ret;
}

int ObServerConfig::check_all() const
{
  int ret = OB_SUCCESS;
  ObConfigContainer::const_iterator it = container_.begin();
  for (; it != container_.end(); it++)
  {
    if (!it->second->check())
    {
      TBSYS_LOG(INFO, "Configure setting invalid! name: [%s], value: [%s]",
                it->first.str(), it->second->str());
      ret = OB_INVALID_CONFIG;
    }
  }
  return ret;
}

void ObServerConfig::print() const
{
  TBSYS_LOG(INFO, "===================== *begin server config report * =====================");
  ObConfigContainer::const_iterator it = container_.begin();
  for (; it != container_.end(); it++)
  {
    TBSYS_LOG(INFO, "| %-36s = %s", it->first.str(), it->second->str());
  }
  TBSYS_LOG(INFO, "===================== *stop server config report* =======================");
}


int ObServerConfig::add_extra_config(const char* config_str, bool check_name /* = false */)
{
  int ret = OB_SUCCESS;
  char *saveptr = NULL;
  char *token = NULL;
  char buf[OB_MAX_EXTRA_CONFIG_LENGTH] = {0};

  if (strlen(config_str) > sizeof (buf) - 1)
  {
    TBSYS_LOG(ERROR, "Extra config is too long!");
    ret = OB_BUF_NOT_ENOUGH;
  }
  else
  {
    snprintf(buf, sizeof (buf), "%s", config_str);
  }

  token = strtok_r(buf, ",\n", &saveptr);
  while (NULL != token && OB_SUCCESS == ret)
  {
    char *saveptr_one = NULL;
    char *name = NULL;
    char *value = NULL;
    ObConfigItem * const *pp_item = NULL;
    if (NULL == (name = strtok_r(token, "=", &saveptr_one)))
    {
      TBSYS_LOG(ERROR, "Invalid config string: [%s]", config_str);
      ret = OB_INVALID_CONFIG;
    }
    else if (NULL == (value = strtok_r(NULL, "=", &saveptr_one)))
    {
      TBSYS_LOG(ERROR, "Invalid config string: [%s]", config_str);
      ret = OB_INVALID_CONFIG;
    }
    else if (NULL != strtok_r(NULL, "=", &saveptr_one))
    {
      /* another equal sign */
      TBSYS_LOG(ERROR, "Invalid config string: [%s]", config_str);
      ret = OB_INVALID_CONFIG;
    }
    else if (NULL == (pp_item = container_.get(name)))
    {
      TBSYS_LOG(WARN, "Invalid config string, no such config item!"
                "name: [%s] value: [%s]", name, value);
      /* make compatible with previous configuration */
      ret = check_name ? OB_INVALID_CONFIG : OB_SUCCESS;
    }
    else
    {
      (*pp_item)->set_value(value);
      TBSYS_LOG(INFO, "Load config succ! name: [%s], value: [%s]", name, value);
    }
    token = strtok_r(NULL, ",\n", &saveptr);
  }

  return ret;
}

DEFINE_SERIALIZE(ObServerConfig)
{
  static const int HEADER_LENGTH = sizeof (uint32_t) + sizeof (uint64_t);
  const int64_t pos_beg = pos;
  char * const p_hash = buf + pos_beg;
  char * const p_length = buf + pos_beg + sizeof (uint32_t);

  int ret = OB_SUCCESS;

  if (pos + HEADER_LENGTH >= buf_len)
  {
    ret = OB_BUF_NOT_ENOUGH;
  }
  else
  {
    pos += HEADER_LENGTH;
    ObConfigContainer::const_iterator it = container_.begin();
    for (; it != container_.end(); it++)
    {
      databuff_printf(buf, buf_len, pos, "%s=%s\n", it->first.str(), it->second->str());
    }

    *reinterpret_cast<uint64_t*>(p_length) = pos - (pos_beg + HEADER_LENGTH);
    const int32_t lengthed_content_len = static_cast<int32_t>(pos - (pos_beg + sizeof (uint32_t)));
    uint32_t hash = murmurhash2(p_length, lengthed_content_len, 0);
    *reinterpret_cast<uint32_t*>(p_hash) = hash;

    ret = pos < buf_len ? OB_SUCCESS : OB_BUF_NOT_ENOUGH;
  }

  return ret;
}

DEFINE_DESERIALIZE(ObServerConfig)
{
  int ret = OB_SUCCESS;
  if (data_len - pos < 12)
  {
    ret = OB_BUF_NOT_ENOUGH;
  }
  else
  {
    uint32_t hash_r = *reinterpret_cast<const uint32_t*>(buf + pos);
    pos += sizeof (uint32_t);
    uint64_t length = *reinterpret_cast<const uint64_t*>(buf + pos);
    const int32_t lengthed_content_len = static_cast<int32_t>(data_len - pos);
    uint32_t hash = murmurhash2(buf + pos, lengthed_content_len, 0);
    pos += sizeof (uint64_t);

    if (hash_r != hash)
    {
      TBSYS_LOG(ERROR, "Config file broken, deserialize server config error!");
      ret = OB_DESERIALIZE_ERROR;
    }
    else if (length != strlen(buf + pos))
    {
      TBSYS_LOG(ERROR, "Config file broken, deserialize server config error!");
      ret = OB_DESERIALIZE_ERROR;
    }
    else if (OB_SUCCESS != (ret = add_extra_config(buf + pos)))
    {
      TBSYS_LOG(ERROR, "Read server config failed!");
    }
    else
    {
      pos += length;
    }
  }
  return ret;
}
