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

#ifndef OCEANBASE_MMS_OB_MONITOR_H_
#define OCEANBASE_MMS_OB_MONITOR_H_

#include <common/ob_packet.h>
#include <common/ob_string.h>
#include <common/ob_server.h>
#include "common/ob_base_server.h"
#include "common/ob_client_manager.h"
#include "common/ob_packet_queue_thread.h"
#include "common/ob_timer.h"
#include "common/ob_lease_common.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_list.h"
#include "common/ob_server_ext.h"
#include "ob_mms_heartbeat_task.h"

namespace oceanbase
{
  namespace mms
  {
    using oceanbase::common::ObServer;
    using oceanbase::common::ObLease;
    
    const int64_t DEFAULT_LEASE_TIEMOUT = 5000000L;
    /**************************************
     * Monitor事件通知接口
     **************************************/
    class INodeRegister
    {
      public:
        virtual ~INodeRegister()
        {
        }
        virtual int handleNodeRegister(const common::ObServerExt &node, const char* app_name, const char *inst_name, bool is_master) = 0;
    };

    class ISlaveFailure
    {
      public:
        virtual ~ISlaveFailure()
        {
        }
        virtual int handleSlaveFailure(const common::ObServer &slave) = 0;
    };

    class IChangeMaster
    {
      public:
        virtual ~IChangeMaster()
        {
        }
        virtual int handleChangeMaster(const common::ObServer &new_master) = 0;
    };

    /*
       class IAdminStopSlave
       {
       public:
       virtual int handleAdminStopSlave(const ObServer &slave) = 0;
       };

       class IAdminTransferMaster
       {
       public:
       virtual int handleAdminTransferMaster(const ObServer &old_master, const ObServer &new_master) = 0;
       };
     */

    /**************************************
     * Monitor功能接口
     **************************************/

    enum MMS_Status
    {
      UNKNOWN = 1,
      MASTER_ACTIVE = 2,
      SLAVE_NOT_SYNC = 3,
      SLAVE_SYNC = 4,
    };

    struct mms_key
    {
      mms_key(){app_name = NULL, instance_name = NULL;};
      char *app_name;
      char *instance_name;

      int64_t hash() const
      {
        int64_t ret = common::murmurhash2(app_name, static_cast<int32_t>(strlen(app_name)), 0 );
        ret += common::murmurhash2(instance_name, static_cast<int32_t>(strlen(instance_name)), 0);
        return ret;
      };

      bool operator == (const mms_key &other) const
      {
        return (0 == strcmp(app_name, other.app_name)
            && 0 == strcmp(instance_name, other.instance_name));
      };
    };

    struct node_info
    {
      ObServer node_server;
      MMS_Status status;
      bool is_valid; //如果心跳无回复，则置为false，如果在租约超时时间后还没重新注册，则t掉
      ObLease lease;
      node_info()
      {
        status = UNKNOWN;
        is_valid = false;
      }
    };

    typedef common::ObList<node_info*> node_list_type;

    struct mms_value
    {
      mms_value(){has_master = false;}
      node_list_type node_list;
      bool has_master;
      ObServer master;
    };

    static const int64_t NODE_HASH_SIZE = 128;
    typedef common::hash::ObHashMap<mms_key, mms_value*> node_map_t;

    class ObMonitor : public tbnet::IPacketQueueHandler
    {
      public:
        ObMonitor();
        ~ObMonitor();
        int init(common::ObBaseServer * server, common::ObClientManager *client_manager, int64_t lease_interval, int64_t renew_interval, int64_t timeout, int64_t retry_times);
        int start();
        int clear_();
        int destroy();

        //处理队列里的包 
        virtual bool handlePacketQueue(tbnet::Packet *packet, void *args);
        //将包放入队列
        bool handlePacket(common::ObPacket *packet);

        //input: app_name, instance_name, ObServer node
        //output: ObResultCode, Role
        int node_register(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
            const uint32_t channel_id, common::ObDataBuffer& out_buff);

        //接受到master的汇报，说某个slave下线
        //int slave_failure(const common::ObServer &slave);
        int slave_failure(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
            const uint32_t channel_id, common::ObDataBuffer& out_buff);

        //接受工具的命令，踢出某个slave
        //int stop_slave(const ObServer &slave);
        //接受工具的命令，将某个node切换为master
        //int transfer_master(const ObServer &master);

        //heartbeat 发现slave失效,通知给master
        int report_slave_failure(const int64_t timeout, const int64_t retry_times, const ObServer slave, const ObServer master);
        //通知new_master转变为主
        int change_master(const int64_t timeout, const int64_t retry_times, const ObServer &new_master);
        //发送heartbeat
        int heartbeat(const int64_t timeou, const ObServer &master, const ObServer &node, MMS_Status &status);

        int set_node_register_handler(INodeRegister *node_register_handler);
        int set_slave_failure_handler(ISlaveFailure *slave_failure_handler);
        int set_change_master_handler(IChangeMaster *change_master_handler);
        //int set_admin_stop_slave_handler(IAdminStopSlave *admin_stop_slave_handler);
        //int set_admin_transfer_master_handler(IAdminTransferState *admin_transfer_master_handler);

        int exist_master(const char *app_name, const char *instance_name, bool& is_exist);
        int select_master(node_list_type *pnodelist, ObServer &new_master);

        bool if_node_exist(node_list_type* pnodelist, 
                          const ObServer &node, 
                          node_list_type::iterator &it_server);
        
        bool if_node_exist(const ObServer &node, 
                           const char *app_name, 
                           const char *instance_name,
                           node_list_type* &pnodelist,
                           node_list_type::iterator &it_server);
        
        int add_register_node(const ObServer &node, 
                              const char *app_name, 
                              const char *instance_name, 
                              const MMS_Status status);
        int get_rpc_buffer(common::ObDataBuffer &data_buffer)const;
        int64_t get_timeout()
        {
          return timeout_;
        }

        int64_t get_retry_times()
        {
          return retry_times_;
        }
        node_map_t& get_node_map()
        {
          return node_map_;
        }

        static const int64_t MY_VERSION = 1;
      private:
        int node_register_(const common::ObServerExt &node, const char *app_name, const char *instance_name, MMS_Status &status);
        int slave_failure_(const ObServer &slave, const char *app_name, const char *instance_name);
        int report_slave_failure_(const int64_t timeout, const ObServer &slave, const ObServer &master);
        int change_master_(const int64_t timeout, const ObServer &new_master);

      public:
        static const int64_t RESPONSE_PACKET_BUFFER_SIZE = 1000;
        static const int64_t DEFAULT_HEARTBEAT_INTERVAL = 2000 * 1000L;
        static const int64_t DEFAULT_RESPONSE_TIME_OUT = 100 * 1000L;
        static const int64_t DEFAULT_QUEUE_SIZE = 100;
        static const int64_t DEFAULT_QUEUE_THREADS_NUM = 1;
        static const int64_t DEFAULT_RETRY_TIMES = 5;
      private:
        node_map_t node_map_;
        int64_t lease_interval_;
        int64_t renew_interval_;
        int64_t timeout_;
        int64_t retry_times_;

        common::ObBaseServer *base_server_;
        common::ObClientManager *client_manager_;
        common::ThreadSpecificBuffer rpc_buffer_;
        common::ThreadSpecificBuffer thread_buffer_;

        common::ObPacketQueueThread queue_thread_;
        int32_t queue_threads_num_;
        int32_t queue_size_;

        INodeRegister *node_register_handler_;
        ISlaveFailure *slave_failure_handler_;
        IChangeMaster *change_master_handler_;
        //IAdminStopSlave *admin_stop_slave_handler_;
        //IAdminTransferState *admin_transfer_master_handler_;

        ObMMSHeartbeatTask *heartbeat_task_;
        common::ObTimer heartbeat_timer_;
    };

  } // end namespace mms
} // end namespace oceanbase

#endif // OCEANBASE_MMS_OB_MONITOR_H_
