#include "ob_ms_async_rpc.h"
#include "ob_ms_rpc_event.h"
#include "ob_ms_sql_rpc_event.h"
#include "common/ob_server.h"
#include "common/ob_result.h"
#include "common/thread_buffer.h"
#include "common/ob_read_common_data.h"
#include "common/ob_client_manager.h"
#include "sql/ob_sql_get_param.h"
#include "sql/ob_sql_scan_param.h"
#include "ob_ms_sql_get_request.h"
#include "ob_ms_async_rpc.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerAsyncRpcStub::ObMergerAsyncRpcStub()
{
  rpc_buffer_ = NULL;
  rpc_frame_ = NULL;
}

ObMergerAsyncRpcStub::~ObMergerAsyncRpcStub()
{
}

int ObMergerAsyncRpcStub::init(const ThreadSpecificBuffer * rpc_buffer, 
  const ObClientManager * rpc_frame) 
{
  int ret = OB_SUCCESS;
  if ((NULL == rpc_buffer) || (NULL == rpc_frame))
  {
    TBSYS_LOG(WARN, "check input failed:rpc_buffer[%p], rpc_frame[%p]",
      rpc_buffer, rpc_frame); 
    ret = OB_INPUT_PARAM_ERROR; 
  }
  else
  {
    rpc_buffer_ = rpc_buffer;
    rpc_frame_ = rpc_frame;
  }
  return ret;
}

int ObMergerAsyncRpcStub::get_rpc_buffer(ObDataBuffer & data_buffer) const
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(WARN, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    common::ThreadSpecificBuffer::Buffer* rpc_buffer = rpc_buffer_->get_buffer();
    if (NULL == rpc_buffer)
    {
      TBSYS_LOG(WARN, "get thread rpc buff failed:buffer[%p].", rpc_buffer);
      ret = OB_INNER_STAT_ERROR;
    }
    else
    {
      rpc_buffer->reset();
      data_buffer.set_data(rpc_buffer->current(), rpc_buffer->remain());
    }
  }
  return ret;
}

int ObMergerAsyncRpcStub::get(const int64_t timeout, const ObServer & server,
  const ObGetParam & get_param, ObMergerRpcEvent & result) const
{
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_rpc_buffer(data_buff);
  if (OB_SUCCESS == ret)
  {
    result.start();
    ret = get_param.serialize(data_buff.get_data(), data_buff.get_capacity(), 
      data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "serialize get_param failed:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    result.set_server(server);
    result.set_req_type(ObMergerRpcEvent::GET_RPC);
    ret = rpc_frame_->post_request(server, OB_GET_REQUEST, DEFAULT_VERSION, timeout, data_buff,
                                   result.get_handler(), &result);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send get request to server failed:client[%lu], event[%lu], "
          "server[%s], ret[%d]", result.get_client_id(), result.get_event_id(), to_cstring(server), ret);
    }
  }
  return ret;
}

int ObMergerAsyncRpcStub::get_session_next(const int64_t timeout, const ObServer & server, 
  const int64_t session_id, const int32_t req_type, ObMergerRpcEvent & result)const
{
  int err = OB_SUCCESS;
  ObDataBuffer data_buff;
  if ((OB_SUCCESS == err) && (OB_SUCCESS !=  (err = get_rpc_buffer(data_buff))))
  {
    TBSYS_LOG(WARN,"fail to get rpc buffer [err:%d]", err);
  }
  if ((OB_SUCCESS == err))
  {
    data_buff.get_position() = 0;
    result.start();
    result.set_server(server);
    result.set_session_id(session_id);
    result.set_req_type(req_type);
    result.set_timeout_us(timeout);
  }
  if ((OB_SUCCESS == err) && 
      (OB_SUCCESS  != (err = rpc_frame_->post_next(server, session_id,
                                                   timeout, data_buff, result.get_handler(), &result))))
  {
    TBSYS_LOG(WARN,"fail to post next request of session [err:%d,session_id:%ld]", err, 
      session_id);
  }
  return err;
}

int ObMergerAsyncRpcStub::scan(const int64_t timeout, const ObServer & server, 
  const ObScanParam & scan_param, ObMergerRpcEvent & result) const
{
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_rpc_buffer(data_buff);
  if (OB_SUCCESS == ret)
  {
    result.start();
    result.set_timeout_us(timeout);
    ret = scan_param.serialize(data_buff.get_data(), data_buff.get_capacity(), 
      data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "serialize scan param failed:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    result.set_server(server);
    result.set_req_type(ObMergerRpcEvent::SCAN_RPC);
    ret = rpc_frame_->post_request(server, OB_SCAN_REQUEST, DEFAULT_VERSION, timeout, data_buff,
                                   result.get_handler(), &result);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send scan request to server failed:client[%lu], event[%lu], "
        "server[%s], ret[%d]", result.get_client_id(), result.get_event_id(), to_cstring(server), ret);
    }
  }
  return ret;
}



//////////////// sql /////////////////////////


int ObMergerAsyncRpcStub::get_session_next(const int64_t timeout, const ObServer & server, 
  const int64_t session_id, const int32_t req_type, ObMsSqlRpcEvent & result)const
{
  int err = OB_SUCCESS;
  ObDataBuffer data_buff;
  if ((OB_SUCCESS == err) && (OB_SUCCESS !=  (err = get_rpc_buffer(data_buff))))
  {
    TBSYS_LOG(WARN,"fail to get rpc buffer [err:%d]", err);
  }
  if ((OB_SUCCESS == err))
  {
    data_buff.get_position() = 0;
    result.start();
    result.set_server(server);
    result.set_session_id(session_id);
    result.set_req_type(req_type);
    result.set_timeout_us(timeout);
  }
  if ((OB_SUCCESS == err) && (OB_SUCCESS  != (err = rpc_frame_->post_next(server, session_id,
            timeout, data_buff, result.get_handler(), &result))))
  {
    TBSYS_LOG(WARN,"fail to post next request of session [err:%d,session_id:%ld]", err, 
        session_id);
  }
  return err;
}

int ObMergerAsyncRpcStub::scan(const int64_t timeout, const ObServer & server, 
  const sql::ObSqlScanParam & scan_param, ObMsSqlRpcEvent & result) const
{
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_rpc_buffer(data_buff);
  if (OB_SUCCESS == ret)
  {
    result.start();
    result.set_timeout_us(timeout);
    ret = scan_param.serialize(data_buff.get_data(), data_buff.get_capacity(), 
      data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "serialize scan param failed:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    result.set_server(server);
    result.set_req_type(ObMsSqlRpcEvent::SCAN_RPC);
    ret = rpc_frame_->post_request(server, OB_SQL_SCAN_REQUEST, DEFAULT_VERSION, timeout, data_buff,
                                   result.get_handler(), &result);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send scan request to server failed:client[%lu], event[%lu], "
        "server[%s], ret[%d]", result.get_client_id(), result.get_event_id(), to_cstring(server), ret);
    }
  }
  return ret;
}

int ObMergerAsyncRpcStub::get(const int64_t timeout, const ObServer & server,
  const ObSqlGetParam & get_param, ObMsSqlRpcEvent & result) const
{
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_rpc_buffer(data_buff);
  if (OB_SUCCESS == ret)
  {
    result.start();
    result.set_timeout_us(timeout);
    ret = get_param.serialize(data_buff.get_data(), data_buff.get_capacity(), 
      data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "serialize get_param failed:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    result.set_server(server);
    result.set_req_type(ObMsSqlRpcEvent::GET_RPC);
    ret = rpc_frame_->post_request(server, OB_SQL_GET_REQUEST, DEFAULT_VERSION, timeout, data_buff,
                                   result.get_handler(), &result);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send get request to server failed:client[%lu], event[%lu], "
          "server[%s], ret[%d]", result.get_client_id(), result.get_event_id(), to_cstring(server), ret);
    }
  }
  return ret;
}


