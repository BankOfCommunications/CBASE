/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 15:15:45 fufeng.syd>
 * Version: $Id$
 * Filename: ob_server_config.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#ifndef _OB_SERVER_CONFIG_H_
#define _OB_SERVER_CONFIG_H_

#include "common/ob_system_config.h"
#include "common/ob_config.h"

namespace oceanbase
{
  namespace common
  {
    class ObServerConfig
    {
      public:
        ObServerConfig();
        virtual ~ObServerConfig();

        int init(const ObSystemConfig &config);

        /* read all config from system_config_ */
        virtual int read_config();
        /* return a sql string to update current config into internal
         * table
         */
        int get_update_sql(char *sql, int64_t len) const;
        /* check if all config is validated */
        virtual int check_all() const;
        int add_extra_config(const char* config_str, bool check_name = false);

        /* print all config to log file */
        void print() const;

        NEED_SERIALIZE_AND_DESERIALIZE;
      protected:
        /* return OB_CHUNKSERVER, OB_MERGESERVER, OB_ROOTSERFER,
         * OB_UPDATESERVER */
        virtual ObRole get_server_type() const = 0;

      protected:
        /* This pointer won't change after one config object
         * created. */
        static ObConfigContainer *p_container_;
        /* Should before any config declearation. */
        ObConfigContainer container_;
        const ObSystemConfig *system_config_;

      public:
        DEF_IP(root_server_ip, "0.0.0.0", "root server ip address");
        DEF_INT(port, "0", "(1024,65536)", "listen port");
        DEF_STR(devname, "bond0", "listen device");
        DEF_INT(retry_times, "3", "[1,]", "retry times if failed");
        DEF_INT(cluster_id, "0", "cluster id");
    };
  }
}

#endif /* _OB_SERVER_CONFIG_H_ */
