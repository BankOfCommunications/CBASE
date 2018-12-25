/*
 * Copyright (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Zhifeng YANG <zhifeng.yang@aliyun-inc.com>
 *     - some work details here
 */
/** 
 * @file ob_ocm_service.h
 * @author Zhifeng YANG <zhifeng.yang@aliyun-inc.com>
 * @date 2011-07-06
 * @brief 
 */

#ifndef _OB_OCM_SERVICE_H
#define _OB_OCM_SERVICE_H 1

#include "common/thread_buffer.h"
#include "common/data_buffer.h"
#include "tbnet.h"
#include "ob_ocm_meta_manager.h"
//#include "ob_ocm_event_handler.h"
#include "ob_ocm_param.h"
#include "ob_ocm_server.h"
#include "ob_ocm_broadcast_task.h"
#include "common/ob_client_manager.h"
#include "ob_ocm_rpc_stub.h"
#include "common/ob_timer.h"

namespace oceanbase
{
  namespace clustermanager
  {
    class OcmServer;
    class ObOcmParam;
    class ObOcmRpcStub;
    static const int64_t OB_BROADCAST_INTERVAL_TIME = 120000000L;
    class OcmService: public tbnet::IPacketQueueHandler
    {
      public:
        OcmService();
        virtual ~OcmService();

      public:
        int init(ObOcmParam *ocm_param, tbnet::Transport* transport, tbnet::IPacketStreamer* streamer);
        // for IPacketQueueHandler
        int start();
        ObTimer& get_timer();
        ThreadSpecificBuffer* get_rpc_buffer();
        ThreadSpecificBuffer* get_thread_buffer();
        const ObClientManager& get_client_manager() const;
	      int send_response(const int32_t pcode, const int32_t version, const ObDataBuffer& buffer,
                tbnet::Connection* connection, const int32_t channel_id);
        virtual bool handlePacketQueue(tbnet::Packet *packet, void *args);
		
        // on_xxx, packet handler functions
        int on_no_matching_handler(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, 
            const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int on_get_instance_role_handler(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
            const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int on_set_instance_role_handler(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
            const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int on_update_ocm_list_handler(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
            const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int on_ocm_change_status_handler(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
            const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int broad_cast_ocm_info();							   
      private:
        int dispatch(int pcode, const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, 
            const uint32_t channel_id, common::ObDataBuffer& out_buff);
      private:
        bool init_;
        ObOcmParam *ocm_param_;
        ObOcmRpcStub *rpc_stub_;
        ObOcmBroadcastTask broad_task_;
        //ObMonitor monitor_;
        //ObOcmEventHandle *event_handle_;
        ObOcmMetaManager ocm_meta_manager_;
        common::ObTimer task_timer_;
        common::ObClientManager client_manager_;
        common::ThreadSpecificBuffer rpc_buffer_;
        common::ThreadSpecificBuffer thread_buffer_;
    };
  } // end namespace clustermanager

} // end namespace oceanbase



#endif /* _OB_OCM_SERVICE_H */

