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

#ifndef OCEANBASE_UPDATESERVER_OB_UPS_FETCH_RUNNABLE_H_
#define OCEANBASE_UPDATESERVER_OB_UPS_FETCH_RUNNABLE_H_

#include "common/ob_fetch_runnable.h"

#include "ob_sstable_mgr.h"

namespace oceanbase
{
  //forward decleration
  namespace tests
  {
    namespace updateserver
    {
      class TestObUpsFetchRunnable_test_gen_fetch_sstable_cmd__Test;
      class TestObUpsFetchRunnable_test_vsystem__Test;
    } // end namespace updateserver
  } // end namespace tests

  namespace updateserver
  {
    struct ObSlaveInfo
    {
      common::ObServer self;
      uint64_t min_sstable_id;
      uint64_t max_sstable_id;
      //add wangjiahao [Paxos ups_replication] 20150817 :b
      uint64_t log_id;
      //add :e

      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    class ObUpsFetchRunnable : public common::ObFetchRunnable
    {
      friend class tests::updateserver::TestObUpsFetchRunnable_test_gen_fetch_sstable_cmd__Test;
      friend class tests::updateserver::TestObUpsFetchRunnable_test_vsystem__Test;
    public:
      ObUpsFetchRunnable();
      virtual ~ObUpsFetchRunnable();

      virtual void run(tbsys::CThread* thread, void* arg);

      /// @brief 初始化
      /// @param [in] master Master地址
      /// @param [in] log_dir 日志目录
      /// @param [in] param Fetch线程需要获取的日志序号范围，checkpoint号
      virtual int init(const common::ObServer &master, const char* log_dir, const ObUpsFetchParam &param, common::ObRoleMgr *role_mgr, common::ObLogReplayRunnable *replay_thread, SSTableMgr *sstable_mgr);

      /// @brief set fetch param
      /// @param [in] param fetch param indicates the log range to be fetched
      virtual int set_fetch_param(const ObUpsFetchParam& param);

    protected:
      int gen_fetch_sstable_cmd_(const char* name, const char* src_path, const char* dst_path, char* cmd, const int64_t size) const;

      int remote_cp_sst_(const char* name, const char* src_path, const char* dst_path) const;

      virtual int get_sstable_();

    protected:
      ObUpsFetchParam ups_param_;
      SSTableMgr *sstable_mgr_;
    };

  } // end namespace updateserver
} // end namespace oceanbase

#endif // OCEANBASE_UPDATESERVER_OB_FETCH_RUNNABLE_H_
