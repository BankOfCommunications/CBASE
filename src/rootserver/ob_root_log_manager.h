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
 *   ruohai <ruohai@taobao.com>
 */

#ifndef OCEANBASE_ROOT_SERVER_LOG_MANAGER_H_
#define OCEANBASE_ROOT_SERVER_LOG_MANAGER_H_

#include "common/ob_define.h"
#include "common/ob_log_writer.h"
#include "rootserver/ob_root_log_worker.h"
#include "rootserver/ob_root_server2.h"

namespace oceanbase
{
  namespace rootserver
  {
    class ObRootLogManager : public common::ObLogWriter
    {
      public:
        static const int UINT64_MAX_LEN;
        ObRootLogManager();
        ~ObRootLogManager();

      public:
        int init(ObRootServer2* root_server, common::ObSlaveMgr* slave_mgr, const common::ObServer* server);

        /// @brief replay all commit log from replay point
        /// after initialization, invoke this method to replay all commit log
        int replay_log();
        int do_after_recover_check_point();
        int write_log_hook(const bool is_master,
                           const ObLogCursor start_cursor, const ObLogCursor end_cursor,
                           const char* log_data, const int64_t data_len);

        int recover_checkpoint(uint64_t ckpt_id);

        int load_server_status();

        int do_check_point(const uint64_t checkpoint_id = 0);

        int add_slave(const common::ObServer& server, uint64_t &new_log_file_id);

        const char* get_log_dir_path() const { return log_dir_; }
        inline uint64_t get_replay_point() {return replay_point_;}
        inline uint64_t get_check_point() {return ckpt_id_;}

        ObRootLogWorker* get_log_worker() { return &log_worker_; }
        tbsys::CThreadMutex* get_log_sync_mutex() { return &log_sync_mutex_; }

      private:
        uint64_t ckpt_id_;
        uint64_t replay_point_;
        uint64_t max_log_id_;
        int rt_server_status_;
        bool is_initialized_;
        bool is_log_dir_empty_;
        char log_dir_[common::OB_MAX_FILE_NAME_LENGTH];

        ObRootServer2* root_server_;
        ObRootLogWorker log_worker_;
        tbsys::CThreadMutex log_sync_mutex_;
    };
  } /* rootserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_ROOT_SERVER_LOG_MANAGER_H_ */
