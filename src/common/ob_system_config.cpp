/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-03-14 14:14:43 fufeng.syd>
 * Version: $Id$
 * Filename: ob_system_config.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#include "common/ob_new_scanner.h"
#include "ob_system_config.h"
#include "ob_config.h"

using namespace oceanbase;

int ObSystemConfig::update(
  const FieldArray &fields, ObNewScanner &scanner)
{
  int ret = OB_SUCCESS;
  ObRow row;
  const ObObj *cell = NULL;
  uint64_t tid = 0;
  uint64_t cid = 0;

  TBSYS_LOG(DEBUG, "%s", to_cstring(fields));

  while (OB_SUCCESS == (ret = scanner.get_next_row(row)))
  {
    ObSystemConfigKey key;
    ObSystemConfigValue value;
    for (int64_t cell_idx = 0;
         cell_idx < row.get_column_num();
         cell_idx++)
    {
      ObString strval;
      int64_t intval = 0;
      const ObString &column_name = fields.at(cell_idx).cname_;

      if (OB_SUCCESS != (ret = row.raw_get_cell(cell_idx, cell, tid, cid)))
      {
        TBSYS_LOG(ERROR, "fail to get cell, ret = [%d]", ret);
        break;
      }
      else if (cell->get_type() == ObVarcharType)
      {
        if (OB_SUCCESS != (ret = cell->get_varchar(strval)))
        {
          TBSYS_LOG(ERROR, "get name column error, column = [%.*s],"
                    " ret = [%d]", column_name.length(),
                    column_name.ptr(), ret);
          break;
        }
        else if (column_name == "name")
        {
          key.set_name(strval);
        }
        else if (column_name == "svr_type")
        {
          key.set_server_type(strval);
        }
        else if (column_name == "svr_ip")
        {
          key.set_server_ip(strval);
        }
        else if (column_name == "value")
        {
          value.set_value(strval);
        }
        else if (column_name == "info")
        {
          value.set_info(strval);
        }
        else
        {
          /* some other columns don't care */
          continue;
        }
      }
      else if (cell->get_type() == ObIntType)
      {
        if (OB_SUCCESS != (ret = cell->get_int(intval)))
        {
          TBSYS_LOG(ERROR, "get column error, obj: %s, ret = [%d]",
                    to_cstring(*cell), ret);
          break;
        }
        else if (OB_SUCCESS != (ret = key.set_int(column_name, intval)))
        {
          TBSYS_LOG(ERROR, "set int key error, ret = [%d]", ret);
          break;
        }
      }
      else if (cell->get_type() == ObModifyTimeType)
      {
        int64_t modify_time = 0;
        if (OB_SUCCESS == (ret = cell->get_timestamp(modify_time)))
        {
          key.set_version(modify_time);
          version_ = max(modify_time, version_);
        }
        else
        {
          TBSYS_LOG(ERROR, "get modify time error, column: [%.*s], ret: [%d]",
                    column_name.length(), column_name.ptr(), ret);
          break;
        }
      }
      else
      {
        if (column_name != ObString::make_string("gm_create")
            && column_name != ObString::make_string("gm_modify"))
        {
          TBSYS_LOG(WARN, "mismatch column type, column: %.*s",
                    column_name.length(), column_name.ptr());
          continue;
        }
      }
    }
    if (OB_SUCCESS == ret)
    {
      int hash_ret = OB_SUCCESS;
      hash_ret = map_.set(key, value);
      if(hash::HASH_INSERT_SUCC != hash_ret)
      {
        if(hash::HASH_EXIST == hash_ret)
        {
          TBSYS_LOG(WARN, "sys config insert repeatly! key name: [%s]",
                    key.name());
        }
        else
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "sys config map set fail, ret = [%d],"
                    " key name = [%s]", ret, key.name());
        }
      }
    }
    else
    {
      TBSYS_LOG(WARN, "ignore the row: %s", to_cstring(row));
      break;
    }
  }

  return ret;
}

int ObSystemConfig::dump2file(const char *path) const
{
  int ret = OB_SUCCESS;
  int fd = 0;
  ssize_t size = 0;

  fd = open(path, O_WRONLY | O_CREAT | O_TRUNC,
            S_IRUSR  | S_IWUSR | S_IRGRP);
  if (fd <= 0)
  {
    TBSYS_LOG(WARN, "fail to create config file [%s], msg: [%s]",
              path, strerror(errno));
    ret = OB_ERROR;
  }
  else
  {
    hashmap::const_iterator it = map_.begin();
    hashmap::const_iterator last = map_.end();

    for ( ;it != last; it++ )
    {
      /* write key */
      size = write(fd, &it->first, sizeof (it->first));
      if (size != sizeof (it->first))
      {
        TBSYS_LOG(WARN, "fail to dump config to file: [%s], msg: [%s]",
                  path, strerror(errno));
        ret = OB_ERROR;
      }
      size = write(fd, &it->second, sizeof (it->second));
      if (size != sizeof (it->second))
      {
        TBSYS_LOG(WARN, "fail to dump config to file: [%s], msg: [%s]",
                  path, strerror(errno));
        ret = OB_ERROR;
      }
    }
    int err = close(fd);
    if (err < 0)
    {
      TBSYS_LOG(WARN, "fail to close file fd: [%d]", fd);
    }
  }
  return ret;
}

int ObSystemConfig::find_all_matched(
  const ObSystemConfigKey &key,
  ObArray<hashmap::const_iterator> &all_config) const
{
  int ret = OB_SUCCESS;
  all_config.clear();
  hashmap::const_iterator it = map_.begin();
  hashmap::const_iterator last = map_.end();

  for ( ;it != last; it++ )
  {
    if (it->first.match(key))
    {
      all_config.push_back(it);
    }
  }

  return ret;
}

int ObSystemConfig::find_newest(const ObSystemConfigKey &key,
                                const ObSystemConfigValue *&pvalue) const
{
  int ret = OB_SEARCH_NOT_FOUND;
  hashmap::const_iterator it = map_.begin();
  hashmap::const_iterator last = map_.end();
  int64_t max_version = 0;
  pvalue = NULL;

  for ( ;it != last; it++ )
  {
    if (it->first.match(key) && it->first.get_version() > max_version)
    {
      max_version = it->first.get_version();
      pvalue = &it->second;
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObSystemConfig::find(const ObSystemConfigKey &key,
                         const ObSystemConfigValue *&pvalue) const
{
  int ret = OB_SUCCESS;

  hashmap::const_iterator it = map_.begin();
  hashmap::const_iterator last = map_.end();
  pvalue = NULL;

  if (NULL == pvalue)
  {
    /* check if ip and port both matched */
    for (it = map_.begin();it != last; it++)
    {
      if (it->first.match_ip_port(key))
      {
        pvalue = &it->second;
        break;
      }
    }
  }
  if (NULL == pvalue)
  {
    /* check if server type matched */
    for (it = map_.begin();it != last; it++)
    {
      if (it->first.match_server_type(key))
      {
        pvalue = &it->second;
        break;
      }
    }
  }
  if (NULL == pvalue)
  {
    /* check if cluster id matched */
    for (it = map_.begin();it != last; it++)
    {
      if (it->first.match_cluster_id(key))
      {
        pvalue = &it->second;
        break;
      }
    }
  }
  if (NULL == pvalue)
  {
    /* check if matched */
    for (it = map_.begin();it != last; it++)
    {
      if (it->first.match(key))
      {
        pvalue = &it->second;
        break;
      }
    }
  }

  if (NULL == pvalue)
  {
    ret = OB_NO_RESULT;
  }
  return ret;
}

int ObSystemConfig::reload(FILE *fp)
{
  int ret = OB_SUCCESS;
  size_t cnt = 0;
  if (NULL == fp)
  {
    TBSYS_LOG(ERROR, "Got NULL file pointer!");
    ret = OB_ERROR;
  }
  else
  {
    ObSystemConfigKey key;
    ObSystemConfigValue value;
    while (1)
    {
      if (1 != (cnt = fread(&key, sizeof (key), 1, fp)))
      {
        if (0 == cnt)
        {
          break;
        }
        else
        {
          TBSYS_LOG(WARN, "fail to read config from file, msg: [%s]",
                    strerror(errno));
          ret = OB_ERROR;
        }
      }
      else if (1 != (cnt = fread(&value, sizeof (value), 1, fp)))
      {
        TBSYS_LOG(WARN, "fail to read config from file, msg: [%s]",
                  strerror(errno));
        ret = OB_ERROR;
      }
      else
      {
        int hash_ret = OB_SUCCESS;
        hash_ret = map_.set(key, value);
        if(hash::HASH_INSERT_SUCC != hash_ret)
        {
          if(hash::HASH_EXIST == hash_ret)
          {
            TBSYS_LOG(WARN, "system config insert repeatly! key name: [%s]",
                      key.name());
          }
          else
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "system config map set fail, ret = [%d],"
                      " key name = [%s]", ret, key.name());
          }
        }
      }
    }
  }
  return ret;
}

int ObSystemConfig::read_int32(const ObSystemConfigKey &key,
                               int32_t &value,
                               const int32_t &def) const
{
  int ret = OB_SUCCESS;
  const ObSystemConfigValue *pvalue = NULL;
  char *p = NULL;
  if (OB_SUCCESS == (ret = find_newest(key, pvalue)))
  {
    value = static_cast<int32_t>(strtol(pvalue->value(), &p, 0));
    if (p == pvalue->value())
    {
      TBSYS_LOG(ERROR, "not integer, key name: [%s], value: [%s]",
                key.name(), pvalue->value());
    }
    else if (p == NULL || *p != '\0')
    {
      TBSYS_LOG(WARN, "config truncated, key name: [%s]", key.name());
    }
    else
    {
      TBSYS_LOG(INFO, "use internal conf: %s:[%d]", key.name(), value );
    }
  }
  else
  {
    value = def;
    TBSYS_LOG(INFO, "use default conf: %s:[%d]", key.name(), value);
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSystemConfig::read_int64(const ObSystemConfigKey &key,
                               int64_t &value,
                               const int64_t &def) const
{
  int ret = OB_SUCCESS;
  const ObSystemConfigValue *pvalue = NULL;
  char *p = NULL;
  if (OB_SUCCESS == (ret = find_newest(key, pvalue)))
  {
    value = strtoll(pvalue->value(), &p, 0);
    if (p == pvalue->value())
    {
      TBSYS_LOG(ERROR, "not integer, key name: [%s], value: [%s]",
                key.name(), pvalue->value());
    }
    else if (p == NULL || *p != '\0')
    {
      TBSYS_LOG(WARN, "config truncated, key name: [%s]", key.name());
    }
    else
    {
      TBSYS_LOG(INFO, "use internal conf: %s:[%ld]", key.name(), value );
    }
  }
  else
  {
    value = def;
    TBSYS_LOG(INFO, "use default conf: %s:[%ld]", key.name(), value );
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSystemConfig::read_str(const ObSystemConfigKey &key,
                             char buf[], int64_t len,
                             const char* def) const
{
  int ret = OB_SUCCESS;
  const ObSystemConfigValue *pvalue = NULL;
  if (OB_SUCCESS == (ret = find_newest(key, pvalue)))
  {
    int wlen = 0;
    if ((wlen = snprintf(buf, len, "%s", pvalue->value())) < 0)
    {
      TBSYS_LOG(ERROR, "reload %s error", key.name());
    }
    else if (wlen >= len)
    {
      TBSYS_LOG(WARN, "config [%s] truncated", key.name());
    }
    else
    {
      TBSYS_LOG(INFO, "use internal conf: %s:[%.*s]", key.name(), static_cast<int>(len - 1), buf);
    }
    /* for safe */
    buf[len - 1] = '\0';
  }
  else
  {
    if (buf != def)
    {
      snprintf(buf, len, "%s", def);
    }
    TBSYS_LOG(INFO, "use default conf: %s:[%s]", key.name(), def);
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSystemConfig::read_config(const ObSystemConfigKey &key,
                                ObConfigItem &item) const
{
  int ret = OB_SUCCESS;
  const ObSystemConfigValue *pvalue = NULL;
  if (OB_SUCCESS == find_newest(key, pvalue))
  {
    item.set_value(pvalue->value());
  }
  return ret;
}

int64_t ObSystemConfig::to_string(char* buf,
                                  const int64_t len) const
{
  int64_t pos = 0;
  hashmap::const_iterator it = map_.begin();
  hashmap::const_iterator last = map_.end();

  pos += snprintf(buf + pos, len - pos, "total: [%ld]\n", map_.size());

  for (; it != last; it++)
  {
    pos += snprintf(buf + pos, len - pos, "name: [%s], value: [%s]\n",
                    it->first.name(), it->second.value());
  }

  return pos;
}
