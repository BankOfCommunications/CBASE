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

#include "ob_ocm_service.h"
#include "common/ob_packet.h"
#include "common/ob_trace_log.h"
#include "common/utility.h"
#include "common/ob_packet.h"
#include "common/ob_result.h"
#include "common/ob_server_ext.h"
#include "ob_ocm_param.h"

using namespace oceanbase::clustermanager;
using namespace oceanbase::common;

class ObOcmParam;

OcmService::OcmService()
  :rpc_buffer_(RESPONSE_PACKET_BUFFER_SIZE),
  thread_buffer_(RESPONSE_PACKET_BUFFER_SIZE)
{
  init_ = false;
}

OcmService::~OcmService()
{
  if(NULL != rpc_stub_)
  {
    delete rpc_stub_;
    rpc_stub_ = NULL;
  }
  /*if(NULL != event_handle_)
    {
    delete event_handle_;
    event_handle_ = NULL;
    }*/
}

int OcmService::init(ObOcmParam *ocm_param, tbnet::Transport* transport, tbnet::IPacketStreamer* streamer)
{
  int err = OB_SUCCESS;
  if(true == init_)
  {
    TBSYS_LOG(WARN, "ocm service already inited. init_=%d", init_);
    err = OB_INIT_TWICE;
  }
  if(OB_SUCCESS == err)
  {
    if(ocm_param == NULL || transport == NULL || streamer == NULL)
    {
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(ERROR, "invalid argument to init ocmservice");
    }
  }
  if(OB_SUCCESS == err)
  {
    ocm_param_ = ocm_param;
    ObServerExt ocm_servExt;
    ocm_servExt.set_hostname(ocm_param_->get_hostname());
    ocm_servExt.get_server().set_ipv4_addr(static_cast<int32_t>(ocm_param_->get_ip()), 
        static_cast<int32_t>(ocm_param_->get_port()));
    err = ocm_meta_manager_.init_ocm_list(ocm_param_->get_ocm_location(), ocm_servExt);
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "init ocm meta manager fail, err=%d", err);
    }
  }
  /* if(OB_SUCCESS == err)
     {
     event_handle_ = new(std::nothrow)ObOcmEventHandle(ocm_meta_manager_.get_self(), this);
     if(NULL != event_handle_)
     {
     monitor.set_node_register_handler(event_handle_);
     monitor.set_slave_failure_handler(event_handle_);
     monitor.set_admin_stop_slave_handler(event_handle_);
     monitor.set_admin_transfer_master_handler(event_handle_);
     } 
     }*/
  if(OB_SUCCESS == err)
  {
    rpc_stub_ = new(std::nothrow)ObOcmRpcStub();
    if(NULL == rpc_stub_)
    {
      TBSYS_LOG(ERROR, "malloc rpc_stub fail. rpc_stub_=%p", rpc_stub_);
      err = OB_ALLOCATE_MEMORY_FAILED;
    }
  }
  if(OB_SUCCESS == err)
  {
    err = client_manager_.initialize(transport, streamer);
  }
  if(OB_SUCCESS == err)
  {
    err = task_timer_.init();
  }
  if(OB_SUCCESS == err)
  {
    err = rpc_stub_->init(&rpc_buffer_, const_cast<ObClientManager*>(&client_manager_));
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to init rpc_stub_, err=%d", err);
    }
  }
  if(OB_SUCCESS == err)
  {
    TBSYS_LOG(INFO, "ocm start init...");
  }
    //第一次启动后要发送一次消息给其他ocm
  return err;
}

int OcmService::start()
{
  int err = OB_SUCCESS;
  err = task_timer_.schedule(broad_task_, OB_BROADCAST_INTERVAL_TIME, true);
  if(OB_SUCCESS == err)
  {
    TBSYS_LOG(INFO, "start to schedule broadcast task succ!");
  }
  else
  {
    TBSYS_LOG(WARN, "fail to start schedule broadcast task");
  }
  if(OB_SUCCESS == err)
  {
    TBSYS_LOG(INFO, "ocm service start...");
    broad_cast_ocm_info();
  }
  return err;
}

ObTimer& OcmService::get_timer()
{
  return task_timer_;
}

ThreadSpecificBuffer* OcmService::get_rpc_buffer()
{
  return &rpc_buffer_;
}

ThreadSpecificBuffer* OcmService::get_thread_buffer()
{
    return &thread_buffer_;
}

const ObClientManager& OcmService::get_client_manager() const
{
  return client_manager_;
}

bool OcmService::handlePacketQueue(tbnet::Packet *packet, void *args)
{
  bool res = false;
  int ret = OB_SUCCESS;
  UNUSED(args);

  if (NULL == packet)
  {
    TBSYS_LOG(WARN, "invalid packet");
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ThreadSpecificBuffer::Buffer* thread_buff = NULL;
    ObPacket* ob_packet = reinterpret_cast<ObPacket*>(packet);
    int pcode = ob_packet->get_packet_code();
    tbnet::Connection* conn = ob_packet->get_connection();
    int api_version = ob_packet->get_api_version();
    uint32_t channel_id = ob_packet->getChannelId();//tbnet need this

    // check timeout
    int64_t now = tbsys::CTimeUtil::getTime();
    int64_t timeout = ob_packet->get_source_timeout();
    if (0 < timeout
        && now > timeout + ob_packet->get_receive_ts())
    {
      TBSYS_LOG(WARN, "packet timeout, now=%ld, timeout=%ld, recive_time=%ld", 
          now, timeout, ob_packet->get_receive_ts());
      ret = OB_RESPONSE_TIME_OUT;
    }
    // deserialize
    else if (OB_SUCCESS != (ret = ob_packet->deserialize()))
    {
      TBSYS_LOG(WARN, "packet deserialize error, err=%d", ret);
      ret = OB_INVALID_ARGUMENT;
    }
    else if (NULL == (thread_buff = thread_buffer_.get_buffer()))
    {
      TBSYS_LOG(ERROR, "no memory in thread buffer");
      ret = OB_ERROR;
    }
    else 
    {
      ObDataBuffer* in_buff = ob_packet->get_buffer();
      thread_buff->reset();
      ObDataBuffer out_buff(thread_buff->current(), thread_buff->remain());
      CLEAR_TRACE_LOG();
      FILL_TRACE_LOG("start handle, packet wait=%ld, start_time=%ld, src=%s, pcode=%ld, channel_id=%ld",
          tbsys::CTimeUtil::getTime() - ob_packet->get_receive_ts(), 
          ob_packet->get_receive_ts(), inet_ntoa_r(conn->getPeerId()), pcode, channel_id);
      PRINT_TRACE_LOG();
      ret = dispatch(pcode, api_version, *in_buff, conn, channel_id, out_buff);
    }
  }
  if (OB_SUCCESS == ret)
  {
    res = true;
  }
  return res;
}

int OcmService::dispatch(int pcode, const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, 
    const uint32_t channel_id, common::ObDataBuffer& out_buff)
{
  int err = OB_SUCCESS;
  switch(pcode)
  {
    case OB_GRANT_LEASE_REQUEST:
      //err = mms.on_grant_lease_handler(version, in_buff, conn, channel_id, out_buff);
      break;
    case OB_RENEW_LEASE_REQUEST:
     // err = mms.on_renew_lease_handler(version, in_buff, conn, channel_id, out_buff);
      break;
    case OB_SLAVE_QUIT:
     // err = mms.on_slave_quit_handler(version, in_buff, conn, channel_id, out_buff);
      break;
    case OB_SERVER_REGISTER:
     // err = mms.on_register_handler(version, in_buff, conn, channel_id, out_buff);
      break;

    case OB_GET_INSTANCE_ROLE:
      err = OcmService::on_get_instance_role_handler(version, in_buff, conn, channel_id, out_buff);
      break;
    case OB_SET_INSTANCE_ROLE:
      err = OcmService::on_set_instance_role_handler(version, in_buff, conn, channel_id, out_buff);
      break;

    case OB_UPDATE_OCM_REQUEST:
      err = OcmService::on_update_ocm_list_handler(version, in_buff, conn, channel_id, out_buff);
      break;

    case OB_OCM_CHANGE_STATUS:
      err = OcmService::on_ocm_change_status_handler(version, in_buff, conn, channel_id, out_buff);
      break;     

    default:
      err = on_no_matching_handler(version, in_buff, conn, channel_id, out_buff);
  }
  return err;
}

int OcmService::on_get_instance_role_handler(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
    const uint32_t channel_id, common::ObDataBuffer& out_buff)
{
  int err = OB_SUCCESS;
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  if(version != MY_VERSION)
  {
    TBSYS_LOG(WARN, "version not equal. version=%d, MY_VERSION=%d", version, MY_VERSION);
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }

  CLEAR_TRACE_LOG();
  FILL_TRACE_LOG("handle = get instance role");
  ObString app_name;
  ObString instance_name;
  if(OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
  {
    err = app_name.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "app_name deserialize fail. err=%d", err);
    }
  }
  FILL_TRACE_LOG("step1:deserialize app_name=%s, err=%d", app_name.ptr(), err);

  if(OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
  {
    err = instance_name.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "instance_name deserialize fail. err=%d", err);
    }
  }
  FILL_TRACE_LOG("step2:deserialize instance_name=%s, err=%d", instance_name.ptr(), err);

  Status status;
  if(OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
  {
    result_msg.result_code_ = ocm_meta_manager_.get_instance_role(app_name.ptr(), instance_name.ptr(), status);
  }
  FILL_TRACE_LOG("step3:get instance role = %d,result_msg.result_code_=%d", status, result_msg.result_code_);

  if(OB_SUCCESS == err)
  {
    err = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "result_msg.serialize error");
    }
  }
  FILL_TRACE_LOG("step4:serialize result_msg, err=%d", err);

  if(OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
  {
    err = serialization::encode_vi64(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(), status);
    if(err != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "status serialize error");
    }
  }
  FILL_TRACE_LOG("step5:serialize status, err=%d", err);

  if (OB_SUCCESS == err)
  {
    send_response(OB_GET_INSTANCE_ROLE_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  FILL_TRACE_LOG("step6:send response, src=%s, channel_id=%ld", inet_ntoa_r(conn->getPeerId()), channel_id);
  FILL_TRACE_LOG("end get instance role .err=%d", err);
  PRINT_TRACE_LOG();
  return err;
}

int OcmService::on_set_instance_role_handler(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
    const uint32_t channel_id, common::ObDataBuffer& out_buff)
{
  static const int MY_VERSION = 1;
  ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int err = OB_SUCCESS;
  if (version != MY_VERSION)
  {
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }
  CLEAR_TRACE_LOG();
  FILL_TRACE_LOG("handle = set instance role");

  Status status;
  ObString app_name;
  ObString instance_name;

  if(OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
  {
    err = app_name.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "app_name deserialize fail. err=%d", err);
    }
  }
  FILL_TRACE_LOG("step1: deserialize app_name =%s, err =%d", app_name.ptr(), err);

  if(OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
  {
    err = instance_name.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "instance_name deserialize fail. err=%d", err);
    }
  }
  FILL_TRACE_LOG("setp2: deserialize instance name=%s, err=%d", instance_name.ptr(), err);

  if(OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
  {
    err = serialization::decode_vi32(in_buff.get_data(),in_buff.get_capacity(),in_buff.get_position(),reinterpret_cast<int32_t*>(&status));
    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "decode status error");
    }
  }
  FILL_TRACE_LOG("step3: decode status =%d, err=%d", status, err);

  if(OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
  {
    result_msg.result_code_ = ocm_meta_manager_.set_instance_role(app_name.ptr(), instance_name.ptr(), status);
  }
  FILL_TRACE_LOG("step4: set instance role, app_name=%s, instance_name=%s, status=%d, result_code=%d", 
      app_name.ptr(), instance_name.ptr(), status, result_msg.result_code_);

  if(OB_SUCCESS == err)
  {
    err = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "result_msg.serialize error");
    }
  }
  FILL_TRACE_LOG("step5: serialize result_msg, err=%d", err);

  if(OB_SUCCESS == err)
  {
    send_response(OB_SET_INSTANCE_ROLE_RESPONSE,  MY_VERSION, out_buff, conn, channel_id);
  }
  FILL_TRACE_LOG("step6: send responst, src=%s, channel_id=%ld", inet_ntoa_r(conn->getPeerId()), channel_id);
  PRINT_TRACE_LOG(); 

  if(OB_SUCCESS == err) 
  {
    err = broad_cast_ocm_info();
  }
  FILL_TRACE_LOG("step7: tart to broadcast ocm info, err=%d", err);
  FILL_TRACE_LOG("end set instance role, err=%d", err);
  return err;
}

int OcmService::on_update_ocm_list_handler(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
    const uint32_t channel_id, common::ObDataBuffer& out_buff)
{
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int err = OB_SUCCESS;
  if (version != MY_VERSION)
  {
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }
  CLEAR_TRACE_LOG();
  FILL_TRACE_LOG("handle = update ocm list");

  ObOcmMeta ocm_meta;
  if(OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
  {
    err = ocm_meta.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "deserialize ocm meta fail. err=%d",err);
    }
  }
  FILL_TRACE_LOG("step1: deserialize ocm_meta, err=%d", err);
 
  if(OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
  {
    result_msg.result_code_ = ocm_meta_manager_.add_ocm_info(ocm_meta);
  }
  FILL_TRACE_LOG("step2: add ocm info, resulet_code=%ld", result_msg.result_code_);
  
  if(OB_SUCCESS ==err)
  {
    err = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "result_msg.serialize error");
    } 
  }
  FILL_TRACE_LOG("step3: serialize result_msg, err=%d", err);
  
  if(OB_SUCCESS == err)
  {
    send_response(OB_UPDATE_OCM_REQUEST_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  FILL_TRACE_LOG("step4: send response, src=%s, channel_id=%d", inet_ntoa_r(conn->getPeerId()), channel_id);
  FILL_TRACE_LOG("end update ocm list, err=%d", err);
  PRINT_TRACE_LOG();
  return err;
}

int OcmService::on_ocm_change_status_handler(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
    const uint32_t channel_id, common::ObDataBuffer& out_buff)
{
  static const int MY_VERSION = 1;
  common::ObResultCode result_msg;
  result_msg.result_code_ = OB_SUCCESS;
  int err = OB_SUCCESS;
  if (version != MY_VERSION)
  {
    result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
  }
  CLEAR_TRACE_LOG();
  FILL_TRACE_LOG("handler = ocm change status");
  
  ObString ip_addr;
  bool is_exist = false;
  if(OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
  {
    err = ip_addr.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "ip addr deserialize fail.ip_addr = %s, err=%d", ip_addr.ptr(), err);
    }
  }
  FILL_TRACE_LOG("step1: deserialize ip_addr, ip_addr=%s, err = %d", ip_addr.ptr(), err);
  
  if(OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
  {
    err = serialization::decode_bool(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &is_exist);
  }
  FILL_TRACE_LOG("step2: decode_bool, is_exist = %d, err = %d", is_exist, err);
  
  if(OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
  {
    result_msg.result_code_ = ocm_param_->set_ocm_status(ip_addr, is_exist);
  }
  FILL_TRACE_LOG("step3: set ocm status, result_code=%d", result_msg.result_code_);
  
  if(OB_SUCCESS == err)
  {
    err = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
    if(OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "result_msg.serialize error");
    }
  }
  FILL_TRACE_LOG("step4: serialize result_msg , err= %d", err);
  
  if(OB_SUCCESS == err)
  {
    send_response(OB_OCM_CHANGE_STATUS_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
  }
  FILL_TRACE_LOG("send response, src=%s, channel_id=%d", inet_ntoa_r(conn->getPeerId()), channel_id);
  FILL_TRACE_LOG("end ocm change status. err=%d", err);
  PRINT_TRACE_LOG();
  return err;  
}

int OcmService::on_no_matching_handler(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, 
    const uint32_t channel_id, common::ObDataBuffer& out_buff)
{
  int ret = OB_SUCCESS;
  UNUSED(version);
  UNUSED(in_buff);
  UNUSED(conn);
  UNUSED(channel_id);
  UNUSED(out_buff);
  TBSYS_LOG(WARN, "no matching handler");
  return ret;
}

int OcmService::send_response(const int32_t pcode, const int32_t version, const ObDataBuffer& buffer, 
    tbnet::Connection* connection, const int32_t channel_id)
{
  int rc = OB_SUCCESS;

  if (connection == NULL)
  {
    rc = OB_ERROR;
    TBSYS_LOG(WARN, "connection is NULL");
  }

  ObPacket* packet = new(std::nothrow) ObPacket();
  if (packet == NULL)
  {
    rc = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(ERROR, "create packet failed");
  }
  else
  {
    packet->set_packet_code(pcode);
    packet->setChannelId(channel_id);
    packet->set_api_version(version);
    packet->set_data(buffer);
  }

  if (rc == OB_SUCCESS)
  {
    rc = packet->serialize();
    if (rc != OB_SUCCESS)
      TBSYS_LOG(WARN, "packet serialize error, error: %d", rc);
  }
  if (rc == OB_SUCCESS)
  {
    if (!connection->postPacket(packet))
    {
      uint64_t peer_id = connection->getPeerId();
      TBSYS_LOG(WARN, "send packet to [%s] failed", tbsys::CNetUtil::addrToString(peer_id).c_str());
      rc = OB_ERROR;
    }
  }

  if (rc != OB_SUCCESS)
  {
    if (NULL != packet)
    {
      packet->free();
      packet = NULL;
    }
  }

  return rc;
}

int OcmService::broad_cast_ocm_info()
{
  int err = OB_SUCCESS;
  int64_t retry_times = ocm_param_->get_retry_times();
  int64_t timeout = ocm_param_->get_broadcast_timeout_us();
  CLEAR_TRACE_LOG();
  FILL_TRACE_LOG("broadcast ocm info begin");
  
  ObServer ocm_server;
  const ObClusterInfo *ocm_addr = NULL;
  for(int64_t i = 0; i < ocm_param_->get_cluster_manager_count(); i++)
  {
    if(ocm_param_->get_ocm_status(i))
    {
      err = OB_RESPONSE_TIME_OUT;
      FILL_TRACE_LOG("send to the %dth ocm", i);
      ocm_addr = ocm_param_->get_ocm_addr_array(i);
      bool res = ocm_server.set_ipv4_addr(ocm_addr->ip_addr, static_cast<int32_t>(ocm_addr->port));
      FILL_TRACE_LOG("ip_addr=%d, port=%d, res=%d", ocm_server.get_ipv4(), ocm_server.get_port(), res);
      if(!res)
      {
        TBSYS_LOG(ERROR, "fail to get ocm_server's addr. ip=%s, port=%ld", ocm_addr->ip_addr, ocm_addr->port);
      }
      else
      {  
        for(int64_t j = 0; OB_SUCCESS != err && j < retry_times; j++)
        {
          int64_t timeu = tbsys::CTimeUtil::getMonotonicTime();
          err = rpc_stub_->broad_cast_ocm_info(timeout, ocm_server, *ocm_meta_manager_.get_self());
          timeu = tbsys::CTimeUtil::getMonotonicTime() - timeu;
          if(OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "%ldth, broadcast fail. err =%d", j, err);
            if(timeu < timeout)
            {
              TBSYS_LOG(INFO, "timeu=%ld not match timeout=%ld,will sleep %ld", timeu, timeout, timeout-timeu);
              int sleep_ret = usleep(static_cast<useconds_t>(timeout-timeu));
              if(OB_SUCCESS != sleep_ret)
              {
                TBSYS_LOG(ERROR, "precise sleep, ret=%d", sleep_ret);
              }
            }
          }
          FILL_TRACE_LOG("err = %d", err);
        }
        if(OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "send ocm info to %s fail.", ocm_addr->ip_addr);
        }
      }
    }
  }
  PRINT_TRACE_LOG();
  return err;
}


