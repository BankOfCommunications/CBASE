/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 12:58:44 fufeng.syd>
 * Version: $Id$
 * Filename: ob_backup_server_config.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@taobao.com>
 *
 */

#ifndef _OB_BACKUP_SERVER_CONFIG_H_
#define _OB_BACKUP_SERVER_CONFIG_H_

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_server_config.h"

namespace oceanbase
{
  namespace backupserver
  {
    //typedef chunkserver::ObChunkServerConfig  ObBackupServerConfig;

    class ObBackupServerConfig
        :public common::ObServerConfig
    {
      public:
        ObBackupServerConfig();
        virtual ~ObBackupServerConfig();
      protected:
        common::ObRole get_server_type() const
        { return OB_BACKUPSERVER; }

      public:

        DEF_CAP(location_cache_size, "32MB", "location cache size");
        DEF_TIME(location_cache_timeout, "600s", "location cache timeout");
        DEF_TIME(vtable_location_cache_timeout, "8s", "virtual table location cache timeout");
        DEF_TIME(migrate_wait_time, "30s", "waiting time for backup thread to start work");
        DEF_TIME(backup_worker_idle_time, "5s", "[1s,]", "backup worker idle wait time");
        DEF_STR(rs_commit_log_dir, "data/rs_commitlog", "root server commit log directory");
        DEF_STR(store_root, "data/ups_data", "ups data directory");
        DEF_STR(raid_regex, "^raid[0-9]+$", "raid regex to find raid directory");
        DEF_STR(dir_regex, "^store[0-9]+$", "store regex to find store directory");
        DEF_INT(inner_port, "2800", "(1024,65536)", "inner port for cluster mergeserver");
        DEF_TIME(tablet_backup_timeout, "30s", "(0,)", "timeout which backup data from chunkserver");
        DEF_INT(tablet_backup_retry_times, "1", "[1,]", "retry times which backup data from chunkserver");
        /*
        static const char* DEFAULT_DEVNAME;
        const static int64_t TIME_MILLISECOND = 1000UL;
        const static int64_t TIME_SECOND = 1000 * 1000UL;
        int load_backup_config(const char* file_name){return backup_config_.load(file_name);}
        const char* get_backup_dir(){return backup_config_.getString(PUBLIC, "backup_dir", "./backup");}
        int get_backup_buf_size(){return backup_config_.getInt(PUBLIC, "buf_size", 64000000);}
        int get_backup_mem_limit(){return backup_config_.getInt(PUBLIC, "mem_limit", 10000000);}

        int get_backup_band_limit(){return backup_config_.getInt(PUBLIC,"backup_band_limit",50*1000);} //50M per second
        int get_task_queue_size(){return backup_config_.getInt(PUBLIC, "task_queue_size", 10000);}
        int get_task_thread_count(){return backup_config_.getInt(PUBLIC, "task_thread_count", 32);}
        int get_io_thread_count(){return backup_config_.getInt(PUBLIC, "io_thread_count", 8);}
        //unit second
        int64_t get_network_timeout(){return backup_config_.getInt(PUBLIC, "network_timeout", 30) * TIME_SECOND;}
        //unit millisecond
        int64_t get_task_left_time(){return backup_config_.getInt(PUBLIC, "task_left_time", 300) * TIME_MILLISECOND;}

           */
    };
  }
}
#endif /* _OB_BACKUP_SERVER_CONFIG_H_ */
