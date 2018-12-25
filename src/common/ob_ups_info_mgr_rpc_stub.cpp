
#include "ob_ups_info_mgr_rpc_stub.h"

using namespace oceanbase;
using namespace common;

ObUpsInfoMgrRpcStub::ObUpsInfoMgrRpcStub()
  :client_mgr_(NULL)
{
}

ObUpsInfoMgrRpcStub::~ObUpsInfoMgrRpcStub()
{
}

int ObUpsInfoMgrRpcStub::init(const common::ObClientManager * client_mgr)
{
  int ret = OB_SUCCESS;
  if (NULL == client_mgr)
  {
    TBSYS_LOG(ERROR, "client_mgr[%p]", client_mgr); 
    ret = OB_INPUT_PARAM_ERROR; 
  }
  else
  {
    client_mgr_ = client_mgr;
  }
  return ret;
}

int ObUpsInfoMgrRpcStub::get_rpc_buffer(ObDataBuffer & data_buffer) const
{
  int ret = OB_SUCCESS;

  common::ThreadSpecificBuffer::Buffer* rpc_buffer = thread_buffer_.get_buffer();
  if (NULL == rpc_buffer)
  {
    TBSYS_LOG(ERROR, "get thread rpc buff failed:buffer[%p].", rpc_buffer);
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    rpc_buffer->reset();
    data_buffer.set_data(rpc_buffer->current(), rpc_buffer->remain());
  }

  return ret;
}

bool ObUpsInfoMgrRpcStub::check_inner_stat(void) const
{
  return (NULL != client_mgr_);
}

int ObUpsInfoMgrRpcStub::fetch_server_list(const int64_t timeout, const ObServer & root_server, ObUpsList & server_list) const
{
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;

  if(!check_inner_stat())
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "check inner status fail!");
  }

  // step 1. send get update server list info request
  if (OB_SUCCESS == ret)
  {
    ret = client_mgr_->send_request(root_server, OB_GET_UPS, DEFAULT_VERSION,
        timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send request to root server for find update server failed:ret[%d]", ret);
    }
  }

  // step 2. deserialize restult code
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
    }
  }

  // step 3. deserialize update server addr
  if (OB_SUCCESS == ret)
  {
    ret = server_list.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize server list failed:pos[%ld], ret[%d]", pos, ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "fetch update server list succ:count[%d]", server_list.ups_count_);
    }
  }
  return ret;
}
