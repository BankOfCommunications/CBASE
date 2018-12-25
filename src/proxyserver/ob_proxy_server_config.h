/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 12:58:44 fufeng.syd>
 * Version: $Id$
 * Filename: ob_proxy_server_config.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@taobao.com>
 *
 */

#ifndef _OB_PROXY_SERVER_CONFIG_H_
#define _OB_PROXY_SERVER_CONFIG_H_

#include "common/ob_define.h"
#include "common/ob_server.h"

namespace oceanbase
{
  namespace proxyserver
  {

    class ObProxyServerConfig
    {
      public:
        ObProxyServerConfig();
        virtual ~ObProxyServerConfig();

      public:
        static const char* YUNTI_PROXY;
        static const char* PUBLIC;
        static const char* DEFAULT_DEVNAME;
        const static int64_t TIME_MILLISECOND = 1000UL;
        const static int64_t TIME_SECOND = 1000 * 1000UL;
        int load_proxy_config(const char* file_name){return proxy_config_.load(file_name);}
        const char* get_java_home(){return proxy_config_.getString(PUBLIC, "java_home", NULL);}
        const char* get_hadoop_home(){return proxy_config_.getString(YUNTI_PROXY, "hadoop_home", NULL);}
        const char* get_hdfs_config(){return proxy_config_.getString(YUNTI_PROXY, "hdfs_conf_dir", NULL);}

        int get_yt_buf_size(){return proxy_config_.getInt(YUNTI_PROXY, "buf_size", 64000000);}
        int get_yt_mem_limit(){return proxy_config_.getInt(YUNTI_PROXY, "mem_limit", 10000000);}
        int get_all_proxy_name(std::vector<std::string> &sections){return proxy_config_.getSectionName(sections);}

        int get_task_queue_size(){return proxy_config_.getInt(PUBLIC, "task_queue_size", 10000);}
        int get_task_thread_count(){return proxy_config_.getInt(PUBLIC, "task_thread_count", 32);}
        int get_io_thread_count(){return proxy_config_.getInt(PUBLIC, "io_thread_count", 8);}
        //unit second
        int64_t get_network_timeout(){return proxy_config_.getInt(PUBLIC, "network_timeout", 30) * TIME_SECOND;}
        //unit millisecond
        int64_t get_task_left_time(){return proxy_config_.getInt(PUBLIC, "task_left_time", 300) * TIME_MILLISECOND;}

        int get_port(){return port_;}
        void set_port(int cmd_port){port_ = cmd_port;}

        void set_devname(char* cmd_devname){devname_ = cmd_devname;}
        const char* get_devname(){return devname_;}
      private:
        tbsys::CConfig proxy_config_;
        int port_;
        const char* devname_;
    };
  }
}
#endif /* _OB_PROXY_SERVER_CONFIG_H_ */
