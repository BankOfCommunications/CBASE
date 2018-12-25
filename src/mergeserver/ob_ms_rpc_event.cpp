/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_rpc_event.h,v 0.1 2011/09/26 14:01:30 zhidong Exp $
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#include "common/ob_define.h"
#include "common/ob_packet.h"
#include "common/ob_result.h"
#include "common/ob_read_common_data.h"
#include "ob_merge_server_main.h"
#include "ob_ms_request.h"
#include "ob_ms_rpc_event.h"
#include "ob_ms_async_rpc.h"


using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerRpcEvent::ObMergerRpcEvent()
{
  client_request_id_ = OB_INVALID_ID;
  client_request_ = NULL;
  timeout_us_ = 0;
}

ObMergerRpcEvent::~ObMergerRpcEvent()
{
  reset();
}

void ObMergerRpcEvent::reset(void)
{
  // print debug info
  ObCommonRpcEvent::reset();
  client_request_id_ = OB_INVALID_ID;
  client_request_ = NULL;
}

uint64_t ObMergerRpcEvent::get_client_id(void) const
{
  return client_request_id_;
}

const ObMergerRequest * ObMergerRpcEvent::get_client_request(void) const
{
  return client_request_;
}

int ObMergerRpcEvent::init(const uint64_t client_id, ObMergerRequest * request)
{
  int ret = OB_SUCCESS;
  if ((OB_INVALID_ID == client_id) || (NULL == request))
  {
    ret = OB_INPUT_PARAM_ERROR;
    TBSYS_LOG(WARN, "check input failed:client[%lu], event[%lu], request[%p]",
        client_id, get_event_id(), request);
  }
  else
  {
    client_request_id_ = client_id;
    client_request_ = request;
  }
  return ret;
}

int ObMergerRpcEvent::parse_packet(ObPacket * packet, void * args)
{
  int ret = OB_SUCCESS;
  UNUSED(args);
  if (NULL == packet)
  {
    //ret = OB_INPUT_PARAM_ERROR;
    ret = OB_RESPONSE_TIME_OUT;
    TBSYS_LOG(WARN, "check packet is NULL:server[%s], client[%lu], request[%lu], event[%lu]",
        server_.to_cstring(), client_request_id_, client_request_->get_request_id(), get_event_id());
  }
  else
  {
    ret = deserialize_packet(*packet, ObCommonRpcEvent::get_result());
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "deserialize packet failed:server[%s], client[%lu], request[%lu], "
          "event[%lu], ret[%d]", server_.to_cstring(), client_request_id_, client_request_->get_request_id(),
          ObCommonRpcEvent::get_event_id(), ret);
    }
  }
  return ret;
}

int ObMergerRpcEvent::handle_packet(ObPacket * packet, void * args)
{
  int ret = OB_SUCCESS;
  if (false == check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat failed");
  }
  else if (ObMergeServerMain::get_instance()->get_merge_server().is_stoped())
  {
    TBSYS_LOG(WARN, "server stoped, cannot handle anything.");
    ret = OB_ERROR;
  }
  else
  {
    this->end();
    /// parse the packet for get result code and result scanner
    ret = parse_packet(packet, args);
    if (ret != OB_SUCCESS)
    {
      /// set result code, maybe timeout packet, connection errors.
      ObCommonRpcEvent::set_result_code(ret);
    }

    if (client_request_ != NULL)
    {
      /// no matter parse succ or failed push to finish queue
      /// not check the event valid only push to the finish queue
      ret = client_request_->signal(*this);
    }
  }
  return ret;
}

int ObMergerRpcEvent::deserialize_packet(ObPacket & packet, ObScanner & result)
{
  int64_t data_length = packet.get_data_length();
  ObDataBuffer * data_buff = NULL;
  int ret = packet.deserialize();
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "deserialize the packet failed:ret[%d]", ret);
  }
  else
  {
    data_buff = packet.get_buffer();
    if (NULL == data_buff)
    {
      ret = OB_INNER_STAT_ERROR;
      TBSYS_LOG(WARN, "check packet data buff failed:buff[%p]", data_buff);
    }
    else
    {
      data_length += data_buff->get_position();
    }
    if (packet.get_packet_code() == OB_SESSION_END)
    {
      /// when session end, set session id to 0
      set_session_end();
    }
    else
    {
      set_session_id(packet.get_session_id());
    }
  }
  ObResultCode code;
  if (OB_SUCCESS == ret)
  {
    ret = code.deserialize(data_buff->get_data(), data_length, data_buff->get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize result failed:pos[%ld], ret[%d]",
          data_buff->get_position(), ret);
    }
    else
    {
      ObCommonRpcEvent::set_result_code(code.result_code_);
    }
  }
  result.reset();
  if ((OB_SUCCESS == ret) && (OB_SUCCESS == code.result_code_))
  {
    ret = result.deserialize(data_buff->get_data(), data_length, data_buff->get_position());
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "deserialize scanner failed:pos[%ld], ret[%d]",
          data_buff->get_position(), ret);
    }
  }
  return ret;
}

void ObMergerRpcEvent::print_info(FILE * file) const
{
  if (NULL != file)
  {
    ObCommonRpcEvent::print_info(file);
    if (NULL == client_request_)
    {
      fprintf(file, "merger rpc event::clinet[%lu], request[%p]\n",
          client_request_id_, client_request_);
    }
    else
    {
      fprintf(file, "merger rpc event:client[%lu], request[%lu], ptr[%p]\n",
          client_request_id_, client_request_->get_request_id(), client_request_);
    }
    fflush(file);
  }
}

