/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ocm_info_manager.h,v 0.1 2010/09/28 13:39:26 rongxuan Exp $
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_ocm_rpc_stub.h"
#include "common/ob_result.h"

using namespace oceanbase;
using namespace oceanbase::clustermanager;

ObOcmRpcStub::ObOcmRpcStub()
{
  init_ = false;
}

ObOcmRpcStub::~ObOcmRpcStub()
{
}

bool ObOcmRpcStub::check_inner_stat() const
{
  return (init_ && (NULL != rpc_buffer_) && (NULL != rpc_frame_));
}

int ObOcmRpcStub::init(ThreadSpecificBuffer *rpc_buffer, ObClientManager *rpc_frame)
{
  int err = OB_SUCCESS;
  if (init_ || (NULL == rpc_buffer) || (NULL == rpc_frame))
  {
    TBSYS_LOG(ERROR, "already inited or check input failed:inited[%s], "
        "rpc_buffer[%p], rpc_frame[%p]", (init_? "ture": "false"), rpc_buffer, rpc_frame);
    err = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    rpc_buffer_ = rpc_buffer;
    rpc_frame_ = rpc_frame;
    init_ = true;
  }
  return err;
}

int ObOcmRpcStub::broad_cast_ocm_info(const int64_t timeout, const common::ObServer &ocm_server, const ObOcmMeta &ocm_meta) const
{
  int err = OB_SUCCESS;
  static const int32_t DEFAULT_VERSION = 1;
  ObDataBuffer data_buffer;
  err = get_rpc_buffer(data_buffer);
  if(OB_SUCCESS == err)
  {
    err = ocm_meta.serialize(data_buffer.get_data(), data_buffer.get_capacity(), data_buffer.get_position());
	if(OB_SUCCESS != err)
	{
	  TBSYS_LOG(WARN, "serialize ocm meta fail. err=%d", err);
	}
  }
  if(OB_SUCCESS == err)
  {
    err = rpc_frame_->send_request(ocm_server, OB_UPDATE_OCM_REQUEST, DEFAULT_VERSION, timeout, data_buffer);
	if(OB_SUCCESS != err)
	{
    char ip_addr[OB_IP_STR_BUFF];
    ocm_server.to_string(ip_addr, OB_IP_STR_BUFF);
	  TBSYS_LOG(WARN, "send request to ocm server fail.ip=%s, err=%d", ip_addr, err);
	}
  }
  int64_t pos = 0;
  if(OB_SUCCESS == err)
  {
    ObResultCode result_code;
	err = result_code.deserialize(data_buffer.get_data(), data_buffer.get_capacity(), pos);
	if(OB_SUCCESS != err)
	{
	  TBSYS_LOG(ERROR, "deserialize resule_code failed, err=%d", err);
	}
	else
	{
	  err = result_code.result_code_;
	}
  }
  return err;  
}

int ObOcmRpcStub::get_rpc_buffer(common::ObDataBuffer &data_buffer) const
{
  int err = OB_SUCCESS;
  if(!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "check inner stat failed");
    err = OB_INNER_STAT_ERROR;
  }
  else
  {
    common::ThreadSpecificBuffer::Buffer* rpc_buffer = rpc_buffer_->get_buffer();
    if(NULL == rpc_buffer)
    {
      TBSYS_LOG(ERROR, "get thread rpc buff fail.buffer[%p]", rpc_buffer);
      err = OB_INNER_STAT_ERROR;
    }
    else
    {
      rpc_buffer->reset();
      data_buffer.set_data(rpc_buffer->current(), rpc_buffer->remain());
    }
  }
  return err;  
}

