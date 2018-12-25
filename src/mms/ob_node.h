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

#ifndef OCEANBASE_MMS_OB_NODE_H_
#define OCEANBASE_MMS_OB_NODE_H_

#include <stdint.h>
#include <common/ob_packet.h>
#include <common/ob_server.h>
#include "common/ob_packet_queue_thread.h"
#include "ob_monitor.h"
#include "common/ob_lease_common.h"
#include "ob_mms_checklease_task.h"
#include "common/ob_result.h"

namespace oceanbase
{
  namespace mms
  {

    /**************************************
     * Node事件通知接口
     **************************************/
    class IMMSStopSlave
    {
      public:
        virtual ~IMMSStopSlave()
        {
        }
        virtual int handleMMSStopSlave() = 0;
    };

    /*class ITransferState
    {
      public:
        virtual ~ITransferState();
        virtual int handleTransferState(const int32_t old_state, const int32_t new_state) = 0;
    };*/

    class ISlaveDown
    {
      public:
        virtual ~ISlaveDown()
        {
        }
        virtual int handleSlaveDown(const common::ObServer &server) = 0;
    };

    class ITransfer2Master
    {
      public:
        virtual ~ITransfer2Master()
        {
        }
        virtual int handleTransfer2Master() = 0;
    };

    class IMasterChanged
    {
      public:
        virtual ~IMasterChanged()
        {
        }
        virtual int handleMasterChanged() = 0;
    };

    class ILeaseTimeout
    {
      public:
        virtual ~ILeaseTimeout()
        {
        }
        virtual int handleLeaseTimeout() = 0;
    };
 
    /**************************************
     * Node功能接口
     **************************************/
    class ObNode : public tbnet::IPacketQueueHandler
    {
      public:
        ObNode();
        ~ObNode();
        int init(common::ObBaseServer *server, int32_t ipv4, 
                 int32_t port, common::ObClientManager *client_manager, 
                 const common::ObLease &lease, ObServer *monitor_server);
        int start(char* app_name, char *instance_name, char *hostname);
        int destroy_();
        bool handlePacketQueue(tbnet::Packet *packet, void *args);
        //负责将包放入node自己的队列中
        bool handlePacket(common::ObPacket *packet);

        //事件处理函数,//int heartbeat(const int64_t lease_time, const common::ObServer &master);
        int heartbeat(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
            const uint32_t channel_id, common::ObDataBuffer& out_buff);

        //通知本slave node下线,//int stop_slave();
        int stop_slave(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
            const uint32_t channel_id, common::ObDataBuffer& out_buff);
        
        //调用本函数，将slave nod的节点由slave not sync变成slave sync
        int transfer_state(const MMS_Status &new_state);

        //通知master，有slave下线 //int slave_down(const common::ObServer &slave);
        int slave_down(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
            const uint32_t channel_id, common::ObDataBuffer& out_buff);

        //通知本slave，你要转变成master
        int transfer_2_master(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
            const uint32_t channel_id, common::ObDataBuffer& out_buff);

        //主动跟monitor注册
        int register_to_monitor(const int64_t timeout, const int64_t retry_times, const ObServer &monitor, MMS_Status &status);

        //快过期时，主动去请求,实际上走重新注册的流程
        int renew_lease(const int64_t timeout, const common::ObServer &monitor);
        //如果租约超时,则需要做一些事情
        int handle_lease_timeout();
        //通知monitor，slave下线
        int report_slave_down(const int64_t timeout, const int64_t retry_times, 
                              const common::ObServer &monitor, const common::ObServer &slave);
        int get_rpc_buffer(common::ObDataBuffer &data_buffer)const;
        common::ObLease& get_lease()
        {
          return lease_;
        }
        common::ObServer& get_monitor_server()
        {
          return monitor_server_;
        }

        common::ObServer & get_self_server()
        {
          return self_server_;
        }

        MMS_Status& get_status()
        {
          return status_;
        }

      private:
        int heartbeat_(const ObLease &lease, const ObServer &master_server);
        int register_to_monitor_(const int64_t timeout, const ObServer &monitor, MMS_Status &status);
        int renew_lease_(const ObLease &lease); 
        int report_slave_down_(const int64_t timeout, const ObServer &monitor, const ObServer &slave);

      public:		
        //注册回调函数
        int set_mms_stop_slave_handler(IMMSStopSlave *mms_stop_slave_handler);
       // int set_transfer_state_handler(ITransferState *transfer_state_handler);
        int set_slave_down_handler(ISlaveDown *slave_down_handler);
        int set_transfer_to_master_handler(ITransfer2Master *transfer_2_master_handler);
        int set_master_changed_handler(IMasterChanged *master_changed_handler);
        int set_least_timeout_handler(ILeaseTimeout *lease_timeout_handler);

        static const int MY_VERSION = 1;
        static const int DEFAULT_QUEUE_THREADS_NUM = 1;
        static const int DEFAULT_QUEUE_SIZE = 100;
        static const int64_t DEFAULT_RESPONSE_TIME_OUT = 200 * 1000L;
        static const int64_t RESPONSE_PACKET_BUFFER_SIZE = 1000;
        static const int64_t DEFAULT_CHECK_LEASE_INTERVAL = 100 * 1000L; //100ms
        static const int64_t DEFAULT_LEASE_INTERVAL = 7000 * 1000L;
        static const int64_t DEFAULT_RENEW_INTERVAL = 4000 * 1000L;
      private:
        ObLease lease_;
        ObServer master_server_;
        ObServer self_server_;
        char* host_name_;
        ObServer monitor_server_;
        int32_t response_time_out_;
        MMS_Status status_;

        char* app_name_;
        char* instance_name_;

        common::ObBaseServer *base_server_;
        common::ObClientManager *client_manager_;
        common::ThreadSpecificBuffer rpc_buffer_;
        common::ThreadSpecificBuffer thread_buffer_;

        common::ObPacketQueueThread queue_thread_;
        int32_t queue_threads_num_;
        int32_t queue_size_;

        IMMSStopSlave *mms_stop_slave_handler_;
        //ITransferState *transfer_state_handler_;
        ISlaveDown *slave_down_handler_;
        ITransfer2Master *transfer_2_master_handler_;
        IMasterChanged *master_changed_handler_;
        ILeaseTimeout *lease_timeout_handler_;

        ObMMSCheckleaseTask check_lease_task_;
        common::ObTimer check_lease_timer_;
    };

  } // end namespace mms
} // end namespace oceanbase

#endif // OCEANBASE_MMS_OB_MONITOR_H_

