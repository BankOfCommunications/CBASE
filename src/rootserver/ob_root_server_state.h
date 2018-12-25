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
 *   rongxuan<rongxuan.lc@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_SERVER_STATE_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_SERVER_STATE_H_

#include <tbsys.h>
#include "common/ob_atomic.h"
#include "common/utility.h"
#include "common/ob_define.h"
#include "ob_chunk_server_manager.h"
using namespace oceanbase::common;
namespace oceanbase
{
  namespace rootserver
  {
    class ObRootServer2;
    enum CsTabletExistState
    {
      INIT_STATE = -2,
      IN_CHUNKSERVER = -1,
      INIT_SAME = 0,
      SAME = 1,
      IN_ROOTTABLE = 2,
      CHECKED = 3,
      NEEDLESS = 4,
    };

    /// @brief for check_and_delete
    /// root_table_addr_:记录rootTable的首地址
    /// cs_addr_:汇报的CS在cs_list中的位置
    /// label_array_:标记位，0表示正常，-1表示要求CS删除，1表示RT要删除
    struct CheckArray
    {
      // ObRootMeta2* root_table_addr_;
      int32_t cs_index_;
      common::ObArray<CsTabletExistState> label_array_;
      bool is_report_;
      CheckArray()
      {
        cs_index_ = -1;
        is_report_ = false;
      }
    };
    /// @brief 定义RS的状态
    class ObRootServerState
    {
      public:
        /// @brief RS状态位
        /// IDLE状态: 缺省状态，即如果RS不在其他状态，它就在该缺省状态
        /// DAILY_MERGE状态: 每日合并状态
        /// CHECK_AND_DELETE状态:tablet核对与删除状态
        /// TABLET_MERGE: Slave切换为Master过程中的状态
        enum State
        {
          UNKNOWN = 0,
          IDLE = 1,
          DAILY_MERGE = 2,
          CHECK_AND_DELETE = 3,
          TABLET_MERGE = 4,
        };

        enum DeleteProcess
        {
          NOTALLOW = 0,
          START = 1,
          DONE = 2,
          INTERRUPT = 3,
        };

      public:
        ObRootServerState()
        {
          state_ = UNKNOWN;
          check_and_delete_flag_ = false;
          delete_process_ = NOTALLOW;
          tablet_merge_flag_ = false;
          is_bypass_ = false;
        }

        virtual ~ObRootServerState() { }

        /// @brief 获取state
        inline State get_state() const
        {
          tbsys::CThreadGuard guard(&mutex_);
          return state_;
        }

        inline void set_delete_process(DeleteProcess process)
        {
          tbsys::CThreadGuard guard(&mutex_);
          delete_process_ = process;
        }
        inline DeleteProcess get_delete_process() const
        {
          tbsys::CThreadGuard guard(&mutex_);
          return delete_process_;
        }
        inline bool get_check_and_delete_flag() const
        {
          tbsys::CThreadGuard guard(&mutex_);
          return check_and_delete_flag_;
        }
        inline bool get_tablet_merge_flag() const
        {
          tbsys::CThreadGuard guard(&mutex_);
          return tablet_merge_flag_;
        }
        inline int set_bypass_flag(const bool flag)
        {
          int ret = OB_SUCCESS;
          tbsys::CThreadGuard guard(&mutex_);
          if (is_bypass_ && flag)
          {
            ret = OB_EAGAIN;
            TBSYS_LOG(WARN, "alreay have some operation doing, retry it later.");
          }
          else
          {
            TBSYS_LOG(INFO, "set bypass flag = %s", flag ? "true" : "false");
            is_bypass_ = flag;
          }
          return ret;
        }
        inline bool get_bypass_flag()
        {
          tbsys::CThreadGuard guard(&mutex_);
          return is_bypass_;
        }
        inline bool is_ready_for_delete()
        {
          tbsys::CThreadGuard guard(&mutex_);
          TBSYS_LOG(DEBUG, "state=%s, delete_proces=%s", get_state_str(), get_delete_str());
          return ((state_ == CHECK_AND_DELETE) && check_and_delete_flag_ && START == delete_process_);
        }

        /// 修改State
        //while RS in daily_merge, cs OFFLINE event will not case state change.
        ////so while rs change state from daily_merge to IDLE, force_change shoule be false
        inline void set_state(const State state, const bool check_and_delete_flag,
            const bool tablet_merge_flag, const bool interrupt_event = false)
        {
          tbsys::CThreadGuard guard(&mutex_);
          if (interrupt_event && ObRootServerState::DAILY_MERGE == state_)
          {
            TBSYS_LOG(INFO, "rootserver in DAILY_MERGE state, refuse to change state");
          }
          else
          {
            common::atomic_exchange(reinterpret_cast<uint32_t*>(&state_), state);
          }
          check_and_delete_flag_ = check_and_delete_flag;
          tablet_merge_flag_ = tablet_merge_flag;
          TBSYS_LOG(INFO, "set_state=%s, check_and_delete_flag=%s, tablet_merge_flag=%s",
              get_state_str(), check_and_delete_flag ? "true":"false", tablet_merge_flag ? "true":"false");
        }

        inline const char* get_state_str() const
        {
          switch (state_)
          {
            case UNKNOWN:   return "UNKNOWN";
            case IDLE:      return "IDLE";
            case DAILY_MERGE:    return "DAILY_MERGE";
            case CHECK_AND_DELETE: return "CHECK_AND_DELETE";
            case TABLET_MERGE:      return "TABLET_MERGE";
            default:        return "ERROR";
          }
        }
        inline const char* get_delete_str() const
        {
          switch (delete_process_)
          {
            case NOTALLOW:  return "NOTALLOW";
            case START:     return "START";
            case DONE:      return "DONE";
            case INTERRUPT: return "INTERRUPT";
            default:        return "ERROR";
          }
        }
        inline const void print(char* buf, const int64_t buf_len, int64_t &pos) const
        {
          databuff_printf(buf, buf_len, pos, "state: %s; ", get_state_str());
          databuff_printf(buf, buf_len, pos, "merge_flag %s; ", tablet_merge_flag_ ? "TRUE" : "FALSE");
          databuff_printf(buf, buf_len, pos, "check_and_delete_flag %s", check_and_delete_flag_ ? "TRUE" : "FALSE");
        }
      public:
        int init(ObRootServer2 *root_server, tbsys::CRWLock *server_manager_lock, const ObChunkServerManager *server_manager);
        //void set_root_server_state();
        int set_daily_merge_error(const char* msg_err, const int64_t length);

        void clean_daily_merge_error();
        bool is_daily_merge_tablet_error()const;
        char* get_error_msg();
      private:
        mutable tbsys::CThreadMutex mutex_;
        State state_;
        bool check_and_delete_flag_;
        DeleteProcess delete_process_;
        bool tablet_merge_flag_;
        bool is_bypass_;
        bool is_daily_merge_error_;
        char err_msg_[OB_MAX_ERROR_MSG_LEN];
        ObRootServer2 *root_server_;
        mutable tbsys::CRWLock *server_manager_rwlock_;
        ObChunkServerManager *server_manager_;
    };
  } // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_ROLE_MGR_H_
