#include <tbnet.h>
#include "packet.h"
#include "ob_result.h"
#include "ob_scanner.h"
#include "ob_scan_param.h"
#include "ob_define.h"
#include "ob_rpc_client.h"
#include "ob_client_packet.h"

using namespace tbnet;
using namespace oceanbase::common;

ObRpcClient::ObRpcClient(const ObServer & server):init_(false), ready_(false)
{
  retry_times_ = 0;
  retry_interval_ = DEFAULT_RETRY_INTERVAL;
  server_ = server;
  param_buffer_ = NULL;
}

ObRpcClient::~ObRpcClient()
{
  if (param_buffer_ != NULL)
  {
    delete []param_buffer_;
    param_buffer_ = NULL;
  }
  close();
}

int ObRpcClient::init(void)
{
  int ret = OB_SUCCESS;
  param_buffer_ = new(std::nothrow) char[MAX_PARAM_LEN];
  if (NULL == param_buffer_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "alloc param temp buffer failed");
  }
  /// init receive buffer
  if (OB_SUCCESS == ret)
  {
    char hostname[MAX_ADDR_LEN] = "";
    int32_t port = server_.get_port();
    server_.set_port(0);
    server_.ip_to_string(hostname, sizeof(hostname));
    socket_.setAddress(hostname, port); 
    server_.set_port(port);
    init_ = true;
  }
  return ret;
}

int ObRpcClient::construct_request(const int64_t timeout, const ObScanParam & param)
{
  ObDataBuffer data;
  data.set_data(param_buffer_, MAX_PARAM_LEN);
  int64_t len = 0;
  int ret = param.serialize(data.get_data(), data.get_capacity(), len);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "param serialize failed:ret[%d]", ret);
  }
  else
  {
    rpc_packet_.set_data(data);
    rpc_packet_.set_packet_code(OB_SCAN_REQUEST);
    rpc_packet_.setChannelId(0);
    rpc_packet_.set_source_timeout(timeout);
    rpc_packet_.set_api_version(1);
    ret = rpc_packet_.serialize();
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "serialize packet failed:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret) 
  {
    request_buffer_.ensureFree(MAX_PARAM_LEN);
    if (!stream_.encode(&rpc_packet_, &request_buffer_))
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "encode packet failed:ret[%d]", ret);
    }
  }
  return ret;
}

int ObRpcClient::parse_response(ObScanner & result, int & code)
{
  int ret = OB_SUCCESS;
  PacketHeader header;
  bool broken = false;
  if (!stream_.getPacketInfo(&response_buffer_, &header, &broken))
  {
    TBSYS_LOG(WARN, "get packet info failed:ret[%d]", ret);
  }
  else
  {
    if (!response_packet_.decode(&response_buffer_, &header))
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "decode packet failed:ret[%d]", ret);
    }
    else
    {
      ret = response_packet_.deserialize();
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "deserialize packet failed:ret[%d]", ret);
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = parse_packet(result, code);
  }
  return ret;
}

int ObRpcClient::parse_packet(ObScanner & result, int & code)
{
  ObResultCode result_code;
  int64_t len = response_packet_.get_data_length();
  int64_t pos = 0;
  ObDataBuffer * data = response_packet_.get_buffer();
  assert(data != NULL);
  int ret = result_code.deserialize(data->get_data(), len, pos);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "deserialize result code failed:ret[%d]", ret);
  }
  else if (OB_SUCCESS == (code = result_code.result_code_))
  {
    ret = result.deserialize(data->get_data(), len, pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "deserialize result failed:ret[%d]", ret);
    }
  }
  return ret;
}

int ObRpcClient::scan(const ObScanParam & param, const int64_t timeout, ObScanner & result)
{
  int ret = OB_ERROR;
  if (init_)
  {
    ret = construct_request(timeout, param);
    if (OB_SUCCESS == ret)
    {
      ret = request(retry_times_, timeout);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "rquest failed:ret[%d]", ret);
      }
    }
    // parse response
    if (OB_SUCCESS == ret)
    {
      int code = OB_SUCCESS;
      ret = parse_response(result, code);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "parse response failed:server[%s], ret[%d]", to_cstring(server_), ret);
        close();
      }
      if (code != OB_SUCCESS)
      {
        ret = code;
      }
    }
  }
  else
  {
    TBSYS_LOG(WARN, "not init ready");
  }
  return ret;
}

int ObRpcClient::request(const int64_t retry_times, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i <= retry_times; ++i)
  {
    ret = connect();
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "connect failed");
      break;
    }
    else
    {
      ret = write();
      if (OB_SUCCESS == ret)
      {
        ret = read(timeout);
        if (OB_SUCCESS == ret)
        {
          break;
        }
      }
    }
    usleep(retry_interval_);
  }
  return ret;
}

int ObRpcClient::write(void)
{
  int ret = socket_.write(request_buffer_.getData(), request_buffer_.getDataLen());
  if (ret != OB_SUCCESS)
  {
    close();
    TBSYS_LOG(WARN, "socket write failed:server[%s], len[%d], ret[%d]",
        to_cstring(server_), request_buffer_.getDataLen(), ret);
  }
  return ret;
}

int ObRpcClient::read(const int64_t timeout)
{
  ////////////////for timeout check///////////////////////////////
  UNUSED(timeout);
  PacketHeader packet;
  int ret = socket_.read(&packet, sizeof(packet));
  if (OB_SUCCESS == ret) 
  {
    ret = socket_.read(response_buffer_.getData(), packet._dataLen);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "socket read failed:server[%s], ret[%d]", to_cstring(server_), ret);
      close();
    }
  }
  return ret;
}

int ObRpcClient::connect(void)
{
  int ret = OB_SUCCESS;
  if (!init_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "stauts error not init");
  }
  else if (!ready_)
  {
    if (true == socket_.connect())
    {
      ready_ = true;
    }
    else
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "connect failed:server[%s]", to_cstring(server_));
    }
  }
  return ret;
}

void ObRpcClient::close(void)
{
  if (init_ && ready_)
  {
    ready_ = false;
    socket_.close();
  }
}

