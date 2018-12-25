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
 * @file TEMPLATE.cpp.tpl
 * @author Zhifeng YANG <zhifeng.yang@aliyun-inc.com>
 * @date 2011-07-06
 * @brief 
 */

#include "ob_ocm_server.h"
#include "common/ob_define.h"
#include <time.h>

using namespace oceanbase::clustermanager;
using namespace oceanbase::common;

  OcmServer::OcmServer()
:packet_factory_(NULL), queue_thread_(NULL), 
  queue_max_size_(DEFAULT_MAX_QUEUE_SIZE), 
  queue_threads_num_(DEFAULT_QUEUE_THREADS_NUM), 
  service_(NULL)
{
}

OcmServer::~OcmServer()
{
}

int OcmServer::initialize()
{
  int ret = OB_SUCCESS;
  // @TODO load config first
  ret = ocm_param_.load_from_config();
  if(OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "load config file fail.ret=%d", ret);
  }
  if(OB_SUCCESS == ret)
  {
    packet_factory_ = new(std::nothrow) common::ObPacketFactory();
    queue_thread_ = new(std::nothrow) common::ObPacketQueueThread();
    service_ = new(std::nothrow) OcmService();
    if (NULL == packet_factory_
        || NULL == queue_thread_
        || NULL == service_)
    {
      TBSYS_LOG(ERROR, "no memory");
      ret = OB_ERROR;
    }
    else
    {
      this->set_packet_factory(packet_factory_);
      this->set_listen_port(static_cast<int32_t>(ocm_param_.get_port()));
      this->set_dev_name(ocm_param_.get_device_name());
      //this->
      ///todo
      ////
      // start queue and threads
      queue_thread_->setThreadParameter(queue_threads_num_, service_, NULL);
    }	
  }
  if(OB_SUCCESS == ret)
  {	
    TBSYS_LOG(INFO, "ocm server initted");
  }
  if(OB_SUCCESS == ret)
  {
    ret = service_->init(&ocm_param_, get_transport(), get_packet_streamer());
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "fail to init service, ret = %d", ret);
    }
  }
  return ret;
}

int OcmServer::start_service()
{
  int err = OB_SUCCESS;
  queue_thread_->start();
  service_->start();
  return err;
}

OcmService* OcmServer::get_service()
{
  return service_;
}

void OcmServer::destroy()
{
  service_->get_timer().destroy();
  if (NULL != queue_thread_)
  {
    queue_thread_->stop();
    queue_thread_->wait();
    TBSYS_LOG(INFO, "ocm server destroyed");
  }
  // delete members
  if (NULL != queue_thread_)
  {
    delete queue_thread_;
    queue_thread_ = NULL;
  }
  if (NULL != service_)
  {
    delete service_;
    service_ = NULL;
  }
  if (NULL != packet_factory_)
  {
    delete packet_factory_;
    packet_factory_ = NULL;
  }  
}

tbnet::IPacketHandler::HPRetCode OcmServer::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
{
  tbnet::IPacketHandler::HPRetCode ret = tbnet::IPacketHandler::FREE_CHANNEL;
  if (!packet->isRegularPacket())
  {
    TBSYS_LOG(WARN, "control packet, packet code: %d", ((tbnet::ControlPacket*)packet)->getCommand());
  } 
  else
  {
    ObPacket* req = static_cast<ObPacket*>(packet);
    req->set_connection(connection);
    // dispatch the packet
    if (!queue_thread_->push(req, 10, false))
    {
      TBSYS_LOG(WARN, "packet queue temporarily full");
      ret = tbnet::IPacketHandler::KEEP_CHANNEL;
    }
  }
  return ret;
}

bool OcmServer::handleBatchPacket(tbnet::Connection *connection, tbnet::PacketQueue &pq)
{
  bool ret = true;
  UNUSED(connection);
  UNUSED(pq);
  return ret;
}
