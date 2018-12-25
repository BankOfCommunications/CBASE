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

#ifndef OCEANBASE_COMMON_OB_SLAVE_MGR_H_
#define OCEANBASE_COMMON_OB_SLAVE_MGR_H_

#include "Mutex.h"

#include "ob_role_mgr.h"
#include "ob_server.h"
#include "ob_link.h"
#include "ob_common_rpc_stub.h"
#include "ob_lease_common.h"
#include "ob_log_cursor.h"

namespace oceanbase
{
  namespace tests
  {
    //forward declaration
    namespace common
    {
      class TestObSlaveMgr_test_server_race_condition_Test;
    }
  }
  namespace common
  {
    class ObSlaveMgr
    {
      friend class oceanbase::tests::common::TestObSlaveMgr_test_server_race_condition_Test;
      public:
      static const int DEFAULT_VERSION;
      static const int DEFAULT_LOG_SYNC_TIMEOUT;  // ms
      static const int CHECK_LEASE_VALID_INTERVAL; // us
      static const int GRANT_LEASE_TIMEOUT; // ms
      static const int MASTER_LEASE_CHECK_REDUNDANCE; // ms
      static const int DEFAULT_SEND_LOG_RETRY_TIMES = 1;
      struct ServerNode
      {
        ObDLink server_list_link;
        ObServer server;
        ObLease lease;
        uint64_t send_log_point;
        //add wangjiahao [Paxos ups_replication] 20150603 :b
        int64_t last_reply_log_seq;
        int64_t last_log_sync_time;
        //add :e

        bool is_lease_valid(int64_t redun_time)
        {
          return lease.is_lease_valid(redun_time);
        }

        void reset()
        {
          lease.lease_time = 0;
          lease.lease_interval = 0;
          lease.renew_interval = 0;
          //add wangjiahao [Paxos ups_replication] 20150603 :b
          last_reply_log_seq = -1; //TODO change -1 to a const static variable
          last_log_sync_time = 0;
          //add :e
        }
      };

      public:
      ObSlaveMgr();
      virtual ~ObSlaveMgr();

      /// @brief 初始化
      int init(ObRoleMgr *role_mgr,
               const uint32_t vip,
               ObCommonRpcStub *rpc_stub,
               int64_t log_sync_timeout,
               int64_t lease_interval,
               int64_t lease_reserved_time,
               int64_t send_retry_times = DEFAULT_SEND_LOG_RETRY_TIMES,
               bool exist_wait_lease_on = false);

      /// reset vip (for debug only)
      void reset_vip(const int32_t vip) {vip_ = vip;}

      /// @brief 添加一台Slave机器
      /// 如果所添加机器已经存在, 则直接返回成功
      /// @param [in] server 添加的机器
      int add_server(const ObServer& server);
      //add wangjiahao [Paxos ups_replication] 20150817 :b
      int add_server_with_log_id(const ObServer& server, const int64_t log_id);
      //add :e
      /// @brief 删除一台Slave机器
      /// @param [in] server 待删除的机器
      /// @retval OB_ENTRY_NOT_EXIST 待删除的机器不存在
      /// @retval OB_ERROR 失败
      /// @retval OB_SUCCESS 成功
      int delete_server(const ObServer& server);

      /// @brief 清空整个list 列表
      /// @brief 当主切换成备时使用
      int reset_slave_list();

      /// @brief 备主用来设置备备的日志同步点
      int set_send_log_point(const ObServer &server, const uint64_t send_log_point);
      //add wangjiahao [Paxos ups_replication] 20150603 :b
      int set_last_reply_log_seq(const ObServer &server, const int64_t last_reply_log_seq);
      bool need_to_del(const ObServer& server, const int64_t wait_time);
      //add :e
        int set_log_sync_timeout_us(const int64_t timeout);
      /// @brief 向各台Slave发送数据
      /// 目前依次向各台Slave发送数据, 并且等待Slave的成功返回
      /// Slave返回操作失败或者发送超时的情况下, 将Slave下线并等待租约(Lease)超时
      /// @param [in] data 发送数据缓冲区
      /// @param [in] length 缓冲区长度
      /// @retval OB_SUCCESS 成功
      /// @retval OB_PARTIAL_FAILED 同步Slave过程中有失败
      /// @retval otherwise 其他错误
      virtual int send_data(const char* data, const int64_t length);
        virtual int post_log_to_slave(const ObLogCursor& start_cursor, const ObLogCursor& end_cursor, const char* data, const int64_t length);
        virtual int wait_post_log_to_slave(const char* data, const int64_t length, int64_t& delay_us);
        virtual int64_t get_acked_clog_id() const { return 0; }
        //add wangjiahao [Paxos ups_replication] 20150807 :b
        virtual int set_acked_clog_id(int64_t seq) { UNUSED(seq); return 0; }
        //add :e
      /// @brief 获取Slave个数
      /// @retval slave_num_ Slave个数
      inline int get_num() const {return slave_num_;}

      /// @brief 延长server租约(Lease)时间
      /// @param [in] server slave地址
      int extend_lease(const ObServer& server, ObLease& lease);

      /// @brief 检查各个Slave租约(Lease)是否超时
      ///  依次检查各Slave租约(Lease)剩余时间, 发现租约(Lease)超时的Slave会直接将Slave下线
      int check_lease_expiration();

      /// @brief 检查Slave租约(Lease)是否还有效
      /// @param [in] server slave地址
      /// @retval true 租约(Lease)有效
      /// @retval false 租约(Lease)已失效或者server不存在
      bool is_lease_valid(const ObServer& server) const;

      int set_obi_role(ObiRole obi_role);
      void print(char *buf, const int64_t buf_len, int64_t& pos);

      protected:
      ServerNode* find_server_(const ObServer& server);

      inline int check_inner_stat() const
      {
        int ret = OB_SUCCESS;
        if (!is_initialized_)
        {
          TBSYS_LOG(ERROR, "ObSlaveMgr has not been initialized.");
          ret = OB_NOT_INIT;
        }
        return ret;
      }

      DISALLOW_COPY_AND_ASSIGN(ObSlaveMgr);

      // private:
      protected:
      ObRoleMgr *role_mgr_;
      int64_t log_sync_timeout_;
      int64_t lease_interval_;
      int64_t lease_reserved_time_;
      int64_t send_retry_times_;
      int slave_num_;  //Slave个数
      uint32_t vip_;
      ServerNode slave_head_;  //slave链表头
      ObCommonRpcStub *rpc_stub_;
      tbutil::Mutex slave_info_mutex_;
      //add wangjiahao [Paxos ups_replication] 20150827 :b
      tbutil::Mutex slave_info_read_mutex_;
      int64_t read_lock_count_;
      //add :e
      bool slave_fail_wait_lease_on_;
      bool is_initialized_;
    };
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_SLAVE_MGR_H_
