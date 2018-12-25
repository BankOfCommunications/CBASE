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
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_UPDATESERVER_OB_SLAVE_MGR_H_
#define OCEANBASE_UPDATESERVER_OB_SLAVE_MGR_H_

#include "common/ob_slave_mgr.h"
#include "common/ob_ack_queue.h"
#include "common/ob_config_manager.h"
#include "ob_update_server_config.h"
#include "ob_ups_role_mgr.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace updateserver
  {
    //mod chujiajia [Paxos ups_replication] 20160115
    //class ObUpsSlaveMgr : private common::ObSlaveMgr
    class ObUpsSlaveMgr : public common::ObSlaveMgr
    //mod:e
    {
      public:
        static const int32_t RPC_VERSION = 1;
        //mod pangtianze [Paxos] 20170214:b
        //static const int64_t MAX_SLAVE_NUM = 8;
        static const int64_t MAX_SLAVE_NUM = OB_MAX_UPS_COUNT - 1;
        //mod:e
        static const int64_t DEFAULT_ACK_QUEUE_LEN = 65536;
        //mod chujiajia [Paxos rs_election] 20151229:b
        //ObUpsSlaveMgr();
        //ObUpsSlaveMgr::ObUpsSlaveMgr()
        ObUpsSlaveMgr(const int64_t &config_quorum_scale);
        //mod:e
        virtual ~ObUpsSlaveMgr();

        /// @brief 初始化
        int init(IObAsyncClientCallback* callback, ObUpsRoleMgr *role_mgr, ObCommonRpcStub *rpc_stub,
            int64_t log_sync_timeout);
        /// @brief 向各台Slave发送数据
        /// 目前依次向各台Slave发送数据, 并且等待Slave的成功返回
        /// Slave返回操作失败或者发送超时的情况下, 将Slave下线并等待租约(Lease)超时
        /// @param [in] data 发送数据缓冲区
        /// @param [in] length 缓冲区长度
        /// @retval OB_SUCCESS 成功
        /// @retval OB_PARTIAL_FAILED 同步Slave过程中有失败
        /// @retval otherwise 其他错误
        int send_data(const char* data, const int64_t length);
        int set_log_sync_timeout_us(const int64_t timeout);
        int post_log_to_slave(const common::ObLogCursor& start_cursor, const common::ObLogCursor& end_cursor, const char* data, const int64_t length);
        int wait_post_log_to_slave(const char* data, const int64_t length, int64_t& delay);
        int64_t get_acked_clog_id() const;
        //add wangjiahao [Paxos ups_replication] 20150807 :b
        int set_acked_clog_id(const int64_t seq);
        //add :e
        inline int64_t get_last_send_log_id() const { return last_send_log_id_; } // add lbzhong[Clog Monitor] 20151218:b:e
        int get_slaves(ObServer* slaves, int64_t limit, int64_t& slave_count);
        int grant_keep_alive();
        int add_server(const ObServer &server);
        int delete_server(const ObServer &server);
        int reset_slave_list();
        int set_send_log_point(const ObServer &server, const uint64_t send_log_point);
        int get_num() const;
        void print(char *buf, const int64_t buf_len, int64_t& pos);
        //add wangjiahao [Paxos ups_replication] 20150602:b
        int get_quorum_seq(int64_t& seq);
        int set_last_reply_log_seq(const ObServer &server, const int64_t last_reply_log_seq);
        bool need_to_del(const ObServer& server, const int64_t wait_time);
        //add :e
      private:
        int64_t n_slave_last_post_;
        ObUpsRoleMgr *role_mgr_;
        ObAckQueue ack_queue_;

        //add wangjiahao [Paxos ups_replication] 20150601:b
        //mod chujiajia [Paxos rs_election] 20151229:b
        //int64_t quorum_scale_;
        const int64_t &quorum_scale_;
        //mod:e
        int64_t my_log_seq_;
        int64_t last_send_log_id_; // add lbzhong[Clog Monitor] 20151218:b:e
        //add:e
    };
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_SLAVE_MGR_H_
