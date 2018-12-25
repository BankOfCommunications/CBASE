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
 * @file ob_ocm_server.h
 * @author Zhifeng YANG <zhifeng.yang@aliyun-inc.com>
 * @date 2011-07-06
 * @brief 
 */

#ifndef _OB_OCM_SERVER_H
#define _OB_OCM_SERVER_H 1

#include "common/ob_single_server.h"
#include "common/ob_packet_factory.h"
#include "ob_ocm_service.h"
using namespace oceanbase::common;

namespace oceanbase
{
  namespace clustermanager
  {
    class OcmService;
    static const int32_t RESPONSE_PACKET_BUFFER_SIZE = 1024*1024*2; //2MB
    class OcmServer: public common::ObBaseServer
    {
      public:
        OcmServer();
        virtual ~OcmServer();
      public:
        // called when the server starting
        virtual int initialize();
        // called when the server to be started
        virtual int start_service();
        virtual void destroy();
        OcmService* get_service();
        // for IServerAdaptor
        virtual tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet);
        // for IServerAdaptor
        virtual bool handleBatchPacket(tbnet::Connection *connection, tbnet::PacketQueue &packetQueue);
      private:
        common::ObPacketFactory *packet_factory_;
        common::ObPacketQueueThread *queue_thread_;
        int queue_max_size_;
        ObOcmParam ocm_param_;
        int queue_threads_num_;
        OcmService *service_;

      private:
        static const int DEFAULT_MAX_QUEUE_SIZE = 1000;
        static const int DEFAULT_QUEUE_THREADS_NUM = 3;
        static const int DEFAULT_LISTEN_PORT = 8000;
    };
  } // end namespace clustermanager
} // end namespace oceanbase


#endif /* _OB_OCM_SERVER_H */

