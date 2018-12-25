#include "common/ob_result.h"
#include "rpc_stub.h"
#include "common/ob_mutator.h"
#include "common/ob_scanner.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

const char* ob_server_to_string(const ObServer& server)
{
  const int SIZE = 32;
  static char buf[SIZE];
  if (!server.to_string(buf, SIZE))
  {
    buf[0] = '\0';
    snprintf(buf, SIZE, "InvalidServerAddr");
  }
  return buf;
}
RpcStub::RpcStub(const ObClientManager * client, const ThreadSpecificBuffer * buffer)
{
  frame_ = client;
  buffer_ = buffer;
}

RpcStub::~RpcStub()
{
}

int RpcStub::get_rpc_buffer(ObDataBuffer & data_buffer) const
{
  int ret = OB_SUCCESS;
  if (false == check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_ERROR;
  }
  else
  {
    common::ThreadSpecificBuffer::Buffer * rpc_buffer = buffer_->get_buffer();
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
  }
  return ret;
}

int RpcStub::scan(const ObServer & server, const int64_t timeout, const ObScanParam & param,
    ObScanner & result)
{
  ObDataBuffer data_buff;
  int ret = get_rpc_buffer(data_buff);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "check get rpc buffer failed:ret[%d]", ret);
  }
  else
  {
    // step 1. serialize ObScanParam to the data_buff
    ret = param.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize scan param failed:ret[%d]", ret);
    }
  }

  int64_t pos = 0;
  // step 2. send request for scan data
  if (OB_SUCCESS == ret)
  {
    ObResultCode result_code;
    // merge server list
    ret = frame_->send_request(server, OB_SCAN_REQUEST, DEFAULT_VERSION,
        timeout, data_buff);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "send scan data failed, server = %s, ret[%d]", server.to_cstring(),ret);
    }
    else
    {
      // step 3. deserialize the response result
      pos = 0;
      ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "deserialize result failed:pos[%ld], ret[%d]", pos, ret);
      }
      else
      {
        ret = result_code.result_code_;
      }
    }
  }

  // step 4. deserialize the scanner
  if (OB_SUCCESS == ret)
  {
    result.clear();
    ret = result.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize scanner from buff failed:pos[%ld], ret[%d]", pos, ret);
    }
  }
  return ret;
}


int RpcStub::get_update_server(const ObServer & server, const int64_t timeout, ObServer & update_server)
{
  ObDataBuffer data_buff;
  int ret = get_rpc_buffer(data_buff);
  // step 1. send get update server info request
  if (OB_SUCCESS == ret)
  {
    ret = frame_->send_request(server, OB_GET_UPDATE_SERVER_INFO, DEFAULT_VERSION,
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
    ret = update_server.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize server failed:pos[%ld], ret[%d]", pos, ret);
    }
  }
  return ret;
}
int RpcStub::get_merge_server(const common::ObServer & server, const int64_t timeout,common::ObServer list[], const int64_t size, int32_t & count)
{
  ObDataBuffer data_buff;
  int ret = get_rpc_buffer(data_buff);
  if (OB_SUCCESS == ret)
  {
    ret = frame_->send_request(server, OB_GET_MS_LIST, DEFAULT_VERSION,timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send request to root server for find merge server failed:ret[%d]", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    data_buff.get_position() = 0;
    ObResultCode result_code;
    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", data_buff.get_position(), ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi32(data_buff.get_data(),
        data_buff.get_capacity(), data_buff.get_position(), &count);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize ms_num error, ret=%d", ret);
    }
    else
    {
      if (count == 0)
      {
        TBSYS_LOG(ERROR, "get mergeserver count = %d", count);
        ret = OB_ERROR;
      }
    }
    
  }
  if (OB_SUCCESS == ret)
  {
    for (int32_t i = 0; i < count && OB_SUCCESS == ret; i++)
    {
      ret = list[i].deserialize(data_buff.get_data(), data_buff.get_capacity(),
          data_buff.get_position());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "deserialize %dth MS error, ret=%d", i, ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "Get %dth Mergeserver %s", i, ob_server_to_string(list[i]));
        int64_t reserve;
        ret = serialization::decode_vi64(data_buff.get_data(), data_buff.get_capacity(),
            data_buff.get_position(), &reserve);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize reserve field error, ret=%d", ret);
        }
      }
    }
  }
  return ret;
}
int RpcStub::del(const common::ObServer & server, const int64_t timeout, const common::ObMutator & mutator)
{
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_rpc_buffer(data_buff);
  ObResultCode result_code;
  int64_t pos = 0;

  if (OB_SUCCESS != (ret = mutator.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position())))
  {
    TBSYS_LOG(ERROR, "serilaize ob mutator failed, ret = %d", ret);
  }
  else if (OB_SUCCESS != (ret = frame_->send_request(server, OB_WRITE, DEFAULT_VERSION, timeout, data_buff )))
  {
    TBSYS_LOG(ERROR , "failed to send request to updateserver[%s], ret = %d", ob_server_to_string(server), ret);

  }
  else if(OB_SUCCESS != (ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos )))
  {
    TBSYS_LOG(ERROR, "deserialize result code failed, ret = %d", ret);
  }
  else
  {
    ret = result_code.result_code_;
  }
  return ret;

}
int RpcStub::del(const common::ObServer & server, const int64_t timeout, common::ObScanner & scanner)
{
  bool is_row_changed = false;
  ObCellInfo *cell = NULL;
  int ret = OB_SUCCESS;
  ObMutator mutator;
  while (OB_SUCCESS == ret && 
      (OB_SUCCESS == (ret = scanner.next_cell())))
  {
    ret = scanner.get_cell(&cell, &is_row_changed);
    if (OB_SUCCESS != ret || NULL == cell)
    {
      TBSYS_LOG(ERROR, "failed to get cell, ret = %d", ret);
      ret = OB_ERROR;
    }
    if (OB_SUCCESS == ret)
    {
      if (is_row_changed == true)
      {
	ret = mutator.del_row(cell->table_name_, cell->row_key_);
	if (OB_SUCCESS != ret)
	{
	  TBSYS_LOG(WARN, "add del row operation into ob_mutator failed, ret = %d",ret);
	}
      }
      
    }
  }
  ret = del(server, timeout, mutator);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "delete mutator from updateserver: %s, failed, ret = %d", ob_server_to_string(server), ret);
  }
  return ret;

}


