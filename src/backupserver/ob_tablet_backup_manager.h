/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-10-16 15:16:13 fufeng.syd>
 * Version: $Id$
 * Filename: ob_config_manager.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#ifndef _OB_TABLET_BACKUP_MANAGER_H_
#define _OB_TABLET_BACKUP_MANAGER_H_

#include "tbsys.h"
#include "common/ob_array.h"
#include "common/location/ob_tablet_location_cache.h"
#include "common/location/ob_tablet_location_cache_proxy.h"
#include "common/ob_system_config.h"
#include "common/ob_reload_config.h"
#include "common/ob_data_source_desc.h"
#include "ob_tablet_backup_runnable.h"
#include "ob_backup_server.h"



using namespace oceanbase::common;

namespace oceanbase
{
  namespace backupserver
  {
    const static int MAX_NAME_LENGTH = OB_MAX_DATBASE_NAME_LENGTH+OB_MAX_TABLE_NAME_LENGTH+1;
    enum BACKUP_STATUS
    {
      BACKUP_INIT = 0,
      BACKUP_WAITING,
      BACKUP_DONE,
      BACKUP_ERROR,
      BACKUP_DOING,
      BACKUP_CANCELLED,
      BACKUP_STATUS_MAX,
    };

    inline const char * parse_backup_status(int status)
    {
      const char *cmd_str = NULL;
      switch(status)
      {
      case BACKUP_INIT:
        cmd_str = "INIT";
        break;
      case BACKUP_WAITING:
        cmd_str = "WAITING";
        break;
      case BACKUP_DONE:
        cmd_str = "DONE";
        break;
      case BACKUP_ERROR:
        cmd_str = "ERROR";
        break;
      case BACKUP_DOING:
        cmd_str = "DOING";
        break;
      case BACKUP_CANCELLED:
        cmd_str = "CANCELLED";
        break;
      default:
        cmd_str = "UNSUPPORTED";
      }
      return cmd_str;
    }
    struct TableBackupTask
    {
      int64_t start_time_;
      int64_t end_time_;
      char table_name[MAX_NAME_LENGTH];
      int64_t table_id;
      BACKUP_STATUS status;
      TableBackupTask()
      {
        memset(table_name,0,MAX_NAME_LENGTH);
        table_id = 0;
        status = BACKUP_INIT;
      }

    };

    struct TabletBackupTask
    {
      int64_t start_time_;
      int64_t retry_time_;
      common::ObNewRange range_;
      int32_t replica_count_;
      int32_t cur_replica_cs_;
      common::ObChunkServerItem replica_[ObTabletLocationList::MAX_REPLICA_COUNT];
      BACKUP_STATUS status_;
      TabletBackupTask()
      {
        start_time_ = 0;
        retry_time_ = 0;
        range_.reset();
        status_ = BACKUP_INIT;
        cur_replica_cs_ = -1;
        replica_count_ = -1;
      }
      bool operator ==(const TabletBackupTask& r) const;
    };

    class ObTabletBackupRunnable;
    class ObBackupServer;
    class ObTabletBackupManager
    {
      const static int64_t MAX_COCURRENT_BACKUP_TASK = 2;
      public:
        ObTabletBackupManager(const common::ObSchemaManagerV2 & schema);
        ~ObTabletBackupManager();

        int initialize(ObBackupServer * server);
        int print();

        int do_start_backup_task( const char* db_name = NULL, const char* table_name = NULL);
        int do_dispatch_backup_task();
        int do_abort_backup_task();
        int do_tablet_backup_over(int code, ObDataSourceDesc & desc);

        inline bool backup_started() {return backup_start_;}
        inline void set_backup_thread(ObTabletBackupRunnable *thread) {backup_thread_ = thread;}

        const ObBackupServer * get_backup_server() const;
        ObBackupServer * get_backup_server();
      private:
        void clear();
        int generate_tablet_backup_task();
        int async_do_tablet_backup_task(int32_t & count);
        int inner_fill_backup_task(ObString &dt);

      private:
        static const uint64_t MAX_INNER_TABLE_COUNT = 32;
        static const uint64_t MAX_ROOT_SERVER_ACCESS_COUNT = 32;
        static const uint64_t MAX_PARALLEL_BACKUP_TASK = 10;
        bool inited_;
        volatile bool backup_start_;
        int64_t start_time_;
        int64_t end_time_;
        //schema
        const ObSchemaManagerV2 & schema_mgr_;

        int64_t frozen_version_;

        ObBackupServer * backup_server_;
        ObTabletBackupRunnable * backup_thread_;

        //tablet location cache
        common::ObTabletLocationCache *location_cache_;
        common::ObTabletLocationCacheProxy * cache_proxy_;
        common::ObGeneralRootRpcProxy * root_rpc_;
        ObStringBuf buffer_pool_;

        //this for only one thread modify task_array_
        mutable tbsys::CThreadMutex task_thread_mutex_;

        //this for only one thread modify tablet_task_array_
        mutable tbsys::CThreadMutex tablet_task_thread_mutex_;

        //every query task_array_ should rlock this
        mutable tbsys::CRWLock task_thread_rwlock_;

        //every query tablet_task_array_ should rlock this
        mutable tbsys::CRWLock tablet_task_thread_rwlock_;

        volatile int64_t doing_task_idx;

        //backup task
        ObArray<TableBackupTask> task_array_;
        ObArray<TabletBackupTask> tablet_task_array_;

        volatile int64_t min_done_tablet_task_idx;
        volatile int64_t max_doing_tablet_task_idx;
    };
  }
}

#endif /* _OB_TABLET_BACKUP_MANAGER_H_ */
