/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ups_rpc_stub.h,v 0.1 2010/09/27 16:59:49 chuanhui Exp $
 *
 * Authors:
 *   rizhao <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_common_rpc_stub.h"
#include "common/utility.h"
using namespace oceanbase::common;

const int32_t ObCommonRpcStub :: DEFAULT_VERSION = 1;
const int64_t ObCommonRpcStub :: DEFAULT_RPC_TIMEOUT_US = 1 * 1000 * 1000;
ObCommonRpcStub :: ObCommonRpcStub()
{
  client_mgr_ = NULL;
}

ObCommonRpcStub :: ~ObCommonRpcStub()
{
}

int ObCommonRpcStub :: init(const ObClientManager* client_mgr)
{
  int err = OB_SUCCESS;

  if (NULL == client_mgr)
  {
    TBSYS_LOG(WARN, "invalid param, client_mgr=%p", client_mgr);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    client_mgr_ = client_mgr;
  }

  return err;
}

int ObCommonRpcStub :: ups_report_slave_failure(const common::ObServer &slave_add, const int64_t timeout_us)
{
  int err = OB_SUCCESS;
  UNUSED(slave_add);
  UNUSED(timeout_us);
  return err;
}

int ObCommonRpcStub :: send_log(const ObServer& ups_slave, ObDataBuffer& log_data,
    const int64_t timeout_us)
{
  int err = OB_SUCCESS;
  ObDataBuffer out_buff;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
    err = OB_ERROR;
  }
  else
  {
    err = get_thread_buffer_(out_buff);
  }

  // step 1. send log data to slave
  if (OB_SUCCESS == err)
  {
    err = client_mgr_->send_request(ups_slave,
        OB_SEND_LOG, DEFAULT_VERSION, timeout_us, log_data, out_buff);
    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send log data to slave failed "
          "data_len[%ld] err[%d].", log_data.get_position(), err);
    }
  }

  // step 2. deserialize the response code
  int64_t pos = 0;
  if (OB_SUCCESS == err)
  {
    ObResultCode result_code;
    err = result_code.deserialize(out_buff.get_data(), out_buff.get_position(), pos);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
    }
    else
    {
      err = result_code.result_code_;
    }
  }

  return err;
}

int ObCommonRpcStub :: renew_lease(const common::ObServer& master,
    const common::ObServer& slave_addr, const int64_t timeout_us)
{
  int err = OB_SUCCESS;

  ObDataBuffer data_buff;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
    err = OB_ERROR;
  }
  else
  {
    err = get_thread_buffer_(data_buff);
  }

  // step 1. serialize slave addr
  if (OB_SUCCESS == err)
  {
    err = slave_addr.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to serialize slave addr, err[%d].", err);
    }
  }


  // step 2. send renew lease request
  if (OB_SUCCESS == err)
  {
    err = client_mgr_->send_request(master,
        OB_RENEW_LEASE_REQUEST, DEFAULT_VERSION, timeout_us, data_buff);
    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send renew_lease failed, err[%d].", err);
    }
  }

  // step 3. deserialize the response code
  int64_t pos = 0;
  if (OB_SUCCESS == err)
  {
    ObResultCode result_code;
    err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
    }
    else
    {
      err = result_code.result_code_;
    }
  }

  return err;
}

int ObCommonRpcStub :: grant_lease(const common::ObServer& slave,
    const ObLease& lease, const int64_t timeout_us)
{
  int err = OB_SUCCESS;

  ObDataBuffer data_buff;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
    err = OB_ERROR;
  }
  else
  {
    err = get_thread_buffer_(data_buff);
  }

  // step 1. serialize lease
  if (OB_SUCCESS == err)
  {
    err = lease.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to serialize lease, err[%d].", err);
    }
  }

  // step 2. send grant lease request
  if (OB_SUCCESS == err)
  {
    err = client_mgr_->send_request(slave,
        OB_GRANT_LEASE_REQUEST, DEFAULT_VERSION, timeout_us, data_buff);
    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send grant lease failed, err[%d].", err);
    }
  }

  // step 3. deserialize the response code
  int64_t pos = 0;
  if (OB_SUCCESS == err)
  {
    ObResultCode result_code;
    err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
    }
    else
    {
      err = result_code.result_code_;
    }
  }

  return err;
}

int ObCommonRpcStub::slave_quit(const common::ObServer& master, const common::ObServer& slave_addr,
    const int64_t timeout_us)
{
  int err = OB_SUCCESS;
  ObDataBuffer data_buff;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
    err = OB_ERROR;
  }
  else
  {
    err = get_thread_buffer_(data_buff);
  }

  // step 1. serialize slave addr
  if (OB_SUCCESS == err)
  {
    err = slave_addr.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
  }

  // step 2. send request to register
  if (OB_SUCCESS == err)
  {
    err = client_mgr_->send_request(master,
        OB_SLAVE_QUIT, DEFAULT_VERSION, timeout_us, data_buff);
    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send request to register failed"
          "err[%d].", err);
    }
  }

  // step 3. deserialize the response code
  int64_t pos = 0;
  if (OB_SUCCESS == err)
  {
    ObResultCode result_code;
    err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
    }
    else
    {
      err = result_code.result_code_;
    }
  }
  TBSYS_LOG(INFO, "send slave(%s) quit info to Master(%s), err[%d].", to_cstring(slave_addr), to_cstring(master), err);
  return err;
}
int ObCommonRpcStub :: get_master_ups_info(const ObServer& rs, ObServer &master_ups, const int64_t timeout_us)
{
  int err = OB_SUCCESS;
  ObDataBuffer data_buff;
  ObServer new_ups;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    err = get_thread_buffer_(data_buff);
  }
  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS != (err = client_mgr_->send_request(rs, OB_GET_UPDATE_SERVER_INFO, DEFAULT_VERSION, timeout_us, data_buff)))
    {
      TBSYS_LOG(WARN, "fail to get master ups. , rootserver=%s, err=%d", rs.to_cstring(), err);
    }
  }
  common::ObResultCode result_msg;
  int64_t pos = 0;
  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS != (err = result_msg.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
    {
      TBSYS_LOG(WARN, "fail to deserialize result_msg, err =%d", err);
    }
    else
    {
      err = result_msg.result_code_;
    }
  }
  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS != (err = new_ups.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
    {
      TBSYS_LOG(WARN, "fail to deserialize master ups. err=%d", err);
    }
    else if (!new_ups.is_valid())
    {
      err = OB_ENTRY_NOT_EXIST;
    }
    else
    {
      master_ups = new_ups;
    }
  }
  return err;
}
int ObCommonRpcStub :: get_obi_role(const common::ObServer& rs, common::ObiRole &obi_role, const int64_t timeout_us)
{
  int err = OB_SUCCESS;

  ObDataBuffer data_buff;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
    err = OB_ERROR;
  }
  else
  {
    err = get_thread_buffer_(data_buff);
  }

  // step 1. send get obi role request
  if (OB_SUCCESS == err)
  {
    err = client_mgr_->send_request(rs,
        OB_GET_OBI_ROLE, DEFAULT_VERSION, timeout_us, data_buff);
    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send get obi role failed, err[%d].", err);
    }
  }

  // step 2. deserialize the ObObiRole and response code
  int64_t pos = 0;
  if (OB_SUCCESS == err)
  {
    ObResultCode result_code;
    err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
    }
    else
    {
      err = result_code.result_code_;
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "get obi response error, err=%d", err);
      }
    }
  }

  if (OB_SUCCESS == err)
  {
    err = obi_role.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "deserialize ObObiRole failed:pos[%ld], err[%d].", pos, err);
    }
  }

  return err;
}

int ObCommonRpcStub :: get_thread_buffer_(ObDataBuffer& data_buff)
{
  int err = OB_SUCCESS;

  ThreadSpecificBuffer::Buffer* rpc_buffer = NULL;
  // get buffer for rpc send and receive
  rpc_buffer = thread_buffer_.get_buffer();
  if (NULL == rpc_buffer)
  {
    TBSYS_LOG(ERROR, "get thread rpc buff failed:buffer[%p].", rpc_buffer);
    err = OB_ERROR;
  }
  else
  {
    rpc_buffer->reset();
    data_buff.set_data(rpc_buffer->current(), rpc_buffer->remain());
  }

  return err;
}

int ObCommonRpcStub :: send_obi_role(const common::ObServer& slave, const common::ObiRole obi_role)
{
  int err = OB_SUCCESS;
  ObDataBuffer data_buff;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
    err = OB_ERROR;
  }
  else
  {
    err = get_thread_buffer_(data_buff);
  }

  if (OB_SUCCESS == err)
  {
    err = obi_role.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to serialize obi_role, err[%d].", err);
    }
  }

  if (OB_SUCCESS == err)
  {
    err = client_mgr_->send_request(slave, OB_SET_OBI_ROLE_TO_SLAVE, DEFAULT_VERSION,
        DEFAULT_RPC_TIMEOUT_US, data_buff);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "send send request failed, err[%d].", err);
    }
  }

  int64_t pos = 0;
  if (OB_SUCCESS == err)
  {
    ObResultCode result_code;
    err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
    }
    else
    {
      err = result_code.result_code_;
    }
  }

  return err;
}

int ObCommonRpcStub :: send_keep_alive(const common::ObServer &slave)
{
  UNUSED(slave);
  return OB_SUCCESS;
}

// copy from ob_client_helper
//mod hongchen [UNLIMIT_TABLE] 20161031:b
//int ObCommonRpcStub::scan(const ObServer& ms, const ObScanParam& scan_param, ObScanner& scanner, const int64_t timeout)
int ObCommonRpcStub::scan(const ObServer& ms, const ObScanParam& scan_param, ObScanner& scanner, const int64_t timeout, int64_t* session_id)
{
  int ret = OB_SUCCESS;
  static const int MY_VERSION = 1;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr is NULL");
    ret = OB_NOT_INIT;
  }

  if (OB_SUCCESS == ret)
  {
    ObDataBuffer data_buff;
    get_thread_buffer_(data_buff);
    ret = scan_param.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());

    if (OB_SUCCESS == ret)
    {
      //ret = client_mgr_->send_request(ms, OB_SCAN_REQUEST, MY_VERSION, timeout, data_buff);
      if (NULL == session_id)
      {
          ret = client_mgr_->send_request(ms, OB_SCAN_REQUEST, MY_VERSION, timeout, data_buff);
      }
      else
      {
          ret = client_mgr_->send_request(ms, OB_SCAN_REQUEST, MY_VERSION, timeout, data_buff, *session_id);
      }

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to send request to (%s), ret=%d", to_cstring(ms), ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      // deserialize the response code
      int64_t pos = 0;
      if (OB_SUCCESS == ret)
      {
        ObResultCode result_code;
        ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
        }
        else
        {
          ret = result_code.result_code_;
          if (OB_SUCCESS == ret
              && OB_SUCCESS != (ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
          {
            TBSYS_LOG(ERROR, "deserialize result data failed:pos[%ld], ret[%d]", pos, ret);
          }
        }
      }
    }
  }
  return ret;
}

//receive next packet
int ObCommonRpcStub::scan_for_next_packet(const common::ServerSession& ssession ,const int64_t timeout, ObScanner& scanner)
{
  int ret = OB_SUCCESS;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr is NULL");
    ret = OB_NOT_INIT;
  }

 if (OB_SUCCESS == ret)
  {
    ObDataBuffer data_buff;
    get_thread_buffer_(data_buff);
    TBSYS_LOG(INFO,"MS IS %s SESSION_ID %ld", to_cstring(ssession.server_),ssession.session_id_);
    ret = client_mgr_->get_next(ssession.server_, ssession.session_id_, timeout, data_buff, data_buff);

    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "failed to send request to (%s), ret=%d", to_cstring(ssession.server_), ret);
    }
    else
    {
      // deserialize the response code
      int64_t pos = 0;
      if (OB_SUCCESS == ret)
      {
        ObResultCode result_code;
        ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
        }
        else
        {
          ret = result_code.result_code_;
          if (OB_SUCCESS == ret
              && OB_SUCCESS != (ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
          {
            TBSYS_LOG(ERROR, "deserialize result data failed:pos[%ld], ret[%d]", pos, ret);
          }
        }
      }
    }
  }

  return ret;
}
//mod hongchen [UNLIMIT_TABLE] 20161031:e

int ObCommonRpcStub::mutate(const ObServer& update_server, const ObMutator& mutator, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  static const int MY_VERSION = 1;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr is NULL");
    ret = OB_NOT_INIT;
  }
  if (OB_SUCCESS == ret)
  {
    ObDataBuffer data_buff;
    get_thread_buffer_(data_buff);
    ret = mutator.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());

    if (OB_SUCCESS == ret)
    {
      ret = client_mgr_->send_request(update_server, OB_WRITE, MY_VERSION, timeout, data_buff);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to send mutate request to updateserver=%s, ret=%d", to_cstring(update_server),ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      // deserialize the response code
      int64_t pos = 0;
      if (OB_SUCCESS == ret)
      {
        ObResultCode result_code;
        ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
        }
        else if (result_code.result_code_ != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"apply failed, err=%d ups=%s",result_code.result_code_, to_cstring(update_server));
          ret = result_code.result_code_;
        }
      }
    }
  }
  return ret;
}

int ObCommonRpcStub :: renew_lease(const common::ObServer &rootserver)
{
  UNUSED(rootserver);
  return OB_SUCCESS;
}

const ObClientManager* ObCommonRpcStub::get_client_mgr() const
{
  return client_mgr_;
}
