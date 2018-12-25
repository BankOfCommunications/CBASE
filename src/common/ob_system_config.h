/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Filename: ob_system_config.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@taobao.com>
 *
 */

#ifndef _OB_SYSTEM_CONFIG_H_
#define _OB_SYSTEM_CONFIG_H_

#include "common/hash/ob_hashmap.h"
#include "common/ob_array.h"
#include "common/ob_new_scanner.h"
#include "common/ob_system_config_key.h"
#include "common/ob_system_config_value.h"
#include "sql/ob_result_set.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

class TestSystemConfig;

namespace oceanbase
{
  namespace common
  {
    class ObConfigItem;
    class ObSystemConfig
    {
      public:
        friend class ::TestSystemConfig;
        typedef hash::ObHashMap<ObSystemConfigKey, ObSystemConfigValue> hashmap;
        typedef ObArray<sql::ObResultSet::Field> FieldArray;
      public:
        ObSystemConfig() : version_(0) {};
        ~ObSystemConfig() {};

        void clear();
        int init();
        int update(const FieldArray &fields, ObNewScanner &scanner);

        int find(const ObSystemConfigKey &key,
                 const ObSystemConfigValue *&pvalue) const;
        int find_newest(const ObSystemConfigKey &key,
                        const ObSystemConfigValue *&pvalue) const;
        int read_int32(const ObSystemConfigKey &key, int32_t &value,
                       const int32_t &def) const;
        int read_int64(const ObSystemConfigKey &key, int64_t &value,
                       const int64_t &def) const;
        int read_int(const ObSystemConfigKey &key, int64_t &value,
                     const int64_t &def) const;
        int read_str(const ObSystemConfigKey &key, char buf[],
                     int64_t len, const char* def) const;
        int read_config(const ObSystemConfigKey &key, ObConfigItem &item) const;
        int64_t to_string(char* buf, const int64_t len) const;
        int reload(FILE *fp);
        int dump2file(const char* path) const;
        const hashmap& get_map() const { return map_; }
        int64_t get_version() { return version_; }
      private:
        int find_all_matched(const ObSystemConfigKey &key,
                             ObArray<hashmap::const_iterator> &all_config) const;
      private:
        hashmap map_;
        int64_t version_;
        static const int64_t MAP_SIZE = 512;
    };

    inline void ObSystemConfig::clear()
    {
      map_.clear();
    }

    inline int ObSystemConfig::init()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = map_.create(MAP_SIZE)))
      {
        TBSYS_LOG(WARN, "create params_map_ fail, ret:[%d]", ret);
      }
      return ret;
    }

    inline int ObSystemConfig::read_int(const ObSystemConfigKey &key,
                                        int64_t &value,
                                        const int64_t &def) const
    {
      return read_int64(key, value, def);
    }
  }
}

#endif /* _OB_SYS_CONFIG_H_ */
