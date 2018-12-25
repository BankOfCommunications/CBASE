#include "common/data_buffer.h"
#include "common/ob_result.h"
#include "load_packet.h"
#include "load_manager.h"
#include "load_rpc.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

LoadRpc::LoadRpc()
{
  frame_ = NULL;
}

LoadRpc::~LoadRpc()
{
}

void LoadRpc::init(const ObClientManager * client, const ThreadSpecificBuffer * buffer)
{
  frame_ = client;
  buffer_ = buffer;
}

int LoadRpc::get_rpc_buffer(ObDataBuffer & data_buffer) const
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


int LoadRpc::send_request(const ObServer & server, FIFOPacket * packet, ObScanner & result)
{
  int ret = OB_ERROR;
  if (true == check_inner_stat())
  {
    switch (packet->type)
    {
      case FIFO_PKT_REQUEST:
      {
        Request * request = reinterpret_cast<Request *> (packet->buf);
        if (NULL == request)
        {
          TBSYS_LOG(WARN, "check request failed:req[%p]", request);
        }
        else//if (true == filter(packet))
        {
          ret = send_request(server, request, result);
        }
        break;
      }
      default:
      {
        TBSYS_LOG(WARN, "not supported packet type:type[%d]", packet->type);
        break;
      }
    }
  }
  else
  {
    TBSYS_LOG(WARN, "check inner stat failed:ret[%d]", ret);
  }
  return ret;
}

int LoadRpc::fetch_query(const ObServer & server, const int64_t timeout, QueryInfo & query)
{
  int ret = OB_ERROR;
  int64_t pos = 0;
  ObDataBuffer output_buffer;
  if (true == check_inner_stat())
  {
    ret = get_rpc_buffer(output_buffer);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get rpc buffer failed:ret[%d]", ret);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "check inner stat failed:ret[%d]", ret);
  }
  // send request for get the packet
  if (OB_SUCCESS == ret)
  {
    ret = frame_->send_request(server, GET_PACKET_REQUEST, DEFAULT_VERSION, timeout, output_buffer);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send request to server for get request packet failed:ret[%d]", ret);
    }
    else
    {
      ObResultCode result_code;
      ret = result_code.deserialize(output_buffer.get_data(), output_buffer.get_position(), pos);
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
  // deserialize the packet
  if (OB_SUCCESS == ret)
  {
    // allocate new memory for fifopacket
    ret = query.deserialize(output_buffer.get_data(), output_buffer.get_position(), pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize query packet failed:pos[%ld], ret[%d]", pos, ret);
    }
  }
  return ret;
}

int LoadRpc::send_request(const ObServer & server, Request * request, ObScanner & result)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObResultCode result_code;
  ObDataBuffer data_buffer;
  data_buffer.set_data(request->buf, request->buf_size);
  ObDataBuffer output_buffer;
  ret = get_rpc_buffer(output_buffer);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "get rpc buffer failed:ret[%d]", ret);
  }
  else
  {
    ret = frame_->send_request(server, request->pcode, request->version, request->timeout, data_buffer, output_buffer);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "send request failed to server:code[%d], version[%d], ret[%d]",
          request->pcode, request->version, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = result_code.deserialize(output_buffer.get_data(), output_buffer.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize result failed:pos[%ld], ret[%d]", pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(INFO, "check result code:code[%d]", ret);
      }
    }
  }

  if ((OB_SUCCESS == ret)
    && ((request->pcode == OB_GET_REQUEST) || (request->pcode == OB_SCAN_REQUEST)))
  {
    int err = result.deserialize(output_buffer.get_data(), output_buffer.get_position(), pos);
    if (err != OB_SUCCESS)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "deserialize result failed:pos[%ld], ret[%d]", pos, err);
    }
  }

  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "check ret failed:ret[%d], code[%d], pos[%ld]", ret, result_code.result_code_, pos);
  }
  return ret;
}


