/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_BACKUPSERVER_OB_UPS_FETCH_RUNNABLE_H_
#define OCEANBASE_BACKUPSERVER_OB_UPS_FETCH_RUNNABLE_H_

#include "updateserver/ob_sstable_mgr.h"
#include "common/ob_fetch_runnable.h"

namespace oceanbase
{
  namespace backupserver
  {
    using namespace oceanbase::common;
    /*struct ObSlaveInfo
    {
      common::ObServer self;
      uint64_t min_sstable_id;
      uint64_t max_sstable_id;

      NEED_SERIALIZE_AND_DESERIALIZE;
    };
    */

    class ObBackupServer;
    class ObBackupUpsFetchRunnable : public common::ObFetchRunnable
    {
    public:
      ObBackupUpsFetchRunnable();
      virtual ~ObBackupUpsFetchRunnable();
    public:

      virtual void run(tbsys::CThread* thread, void* arg);

      /// @brief 初始化
      /// @param [in] master Master地址
      /// @param [in] log_dir 日志目录
      /// @param [in] param Fetch线程需要获取的日志序号范围，checkpoint号
      virtual int init(const common::ObServer &master, const char* log_dir, const updateserver::ObUpsFetchParam &param, updateserver::SSTableMgr *sstable_mgr, ObBackupServer * server);


      int init(const ObServer &master, const char* log_dir, const ObFetchParam &param, common::ObRoleMgr *role_mgr = NULL, common::ObLogReplayRunnable *replay_thread = NULL);
      /// @brief set fetch param
      /// @param [in] param fetch param indicates the log range to be fetched
      virtual int set_fetch_param(const updateserver::ObUpsFetchParam& param);

      //int got_log(uint64_t log_id);

    protected:

      int gen_fetch_sstable_cmd_(const char* name, const char* src_path, const char* dst_path, char* cmd, const int64_t size) const;

      int remote_cp_sst_(const char* name, const char* src_path, const char* dst_path) const;

      virtual int get_sstable_();

    protected:
      ObBackupServer *backup_server_;
      updateserver::ObUpsFetchParam ups_param_;
      updateserver::SSTableMgr *sstable_mgr_;
      bool is_backup_done_;
    };

  } // end namespace backupserver
} // end namespace oceanbase

#endif // OCEANBASE_BACKUPSERVER_OB_UPS_FETCH_RUNNABLE_H_
