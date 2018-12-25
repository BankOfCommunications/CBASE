#include "common/ob_result.h"
#include "rpc_stub.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

RpcStub::RpcStub(ObClientManager * client, ThreadSpecificBuffer * buffer)
{
  server_ = NULL;
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

int RpcStub::fetch_task(const ObServer & server, const int64_t timeout,
    TaskCounter & count, TaskInfo & task)
{
  ObDataBuffer data_buff;
  int ret = get_rpc_buffer(data_buff);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "check get rpc buffer failed:ret[%d]", ret);
  }
  else
  {
    // step 1. send get update server info request
    ret = frame_->send_request(server, FETCH_TASK_REQUEST, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send request to task server for fetch new task failed:ret[%d]", ret);
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

  // step 3. deserialize task counter info
  if (OB_SUCCESS == ret)
  {
    ret = count.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize uncompleted task failed:pos[%ld], ret[%d]", pos, ret);
    }
  }

  // step 4. deserialize new task info
  if (OB_SUCCESS == ret)
  {
    ret = task.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize new task failed:pos[%ld], ret[%d]", pos, ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "fetch new task succ:id[%lu]", task.get_id());
    }
  }
  return ret;
}


int RpcStub::report_task(const ObServer & server, const int64_t timeout,
    const int64_t result_code, const char * file_name, const TaskInfo & task)
{
  ObDataBuffer data_buff;
  int ret = get_rpc_buffer(data_buff);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "check get rpc buffer failed:ret[%d]", ret);
  }
  else
  {
    // step 1. serialize result code to data_buff
    ret = serialization::encode_i64(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position(), result_code);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize result code failed:ret[%d]", ret);
    }
  }

  // step 2. serialize finish task info to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = task.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize task info failed:ret[%d]", ret);
    }
  }

  // step 3. serialize output file path
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS == result_code)
    {
      ret = serialization::encode_vstr(data_buff.get_data(), data_buff.get_capacity(),
          data_buff.get_position(), file_name, strlen(file_name));
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "serialize filename failed:ret[%d]", ret);
      }
    }
  }

  // step 4. send request for report task complete
  if (OB_SUCCESS == ret)
  {
    ret = frame_->send_request(server, REPORT_TASK_REQUEST, DEFAULT_VERSION, timeout,
        data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send report task info failed:ret[%d]", ret);
    }
  }

  // step 5. deserialize the response code
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
  return ret;
}


int RpcStub::get(const ObServer & server, const int64_t timeout,
    const ObGetParam & param, ObScanner & result)
{
  ObDataBuffer data_buff;
  int ret = get_rpc_buffer(data_buff);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "check get rpc buffer failed:ret[%d]", ret);
  }
  else
  {
    // step 1. serialize ObGetParam to the data_buff
    ret = param.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize get param failed:ret[%d]", ret);
    }
  }

  // step 2. send request for get data
  if (OB_SUCCESS == ret)
  {
    ret = frame_->send_request(server, OB_GET_REQUEST, DEFAULT_VERSION,
        timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get data failed from server:ret[%d]", ret);
    }
  }

  // step 3. deserialize the response result
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode result_code;
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

  // step 4. deserialize the scanner
  if (OB_SUCCESS == ret)
  {
    result.reset();
    ret = result.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize scanner from buff failed:pos[%ld], ret[%d]", pos, ret);
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
      TBSYS_LOG(WARN, "send scan data failed to server:ret[%d]", ret);
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

int RpcStub::scan(const int64_t index, const TabletLocation & list, const int64_t timeout,
    const ObScanParam & param, ObScanner & result)
{
  int ret = OB_SUCCESS;
  const int32_t MAX_SERVER_ADDR_SIZE = 128;
  char server_addr[MAX_SERVER_ADDR_SIZE];
  int64_t count = 0;
  int64_t size = list.size();
  int64_t version = list[index].tablet_version_;

  for (int64_t i = index; count < size; i = (i + 1) % size, ++count)
  {
    list[i].chunkserver_.to_string(server_addr, MAX_SERVER_ADDR_SIZE);
    /* 09/13/2011, add tablet version control */
    if (list[i].tablet_version_ != version &&
        list[i].tablet_version_ != (version + 1))
    {
      TBSYS_LOG(DEBUG, "skip version:index[%ld], size[%ld], server[%s], tablet_ver=%ld, version=%ld",
          i, size, server_addr, list[i].tablet_version_, version);
      continue;
    }

    ret = scan(list[i].chunkserver_, timeout, param, result);
    if (OB_SUCCESS == ret)
    {
      TBSYS_LOG(DEBUG, "scan from server succ:index[%ld], size[%ld], server[%s]",
          i, size, server_addr);
      break;
    }
    else
    {
      TBSYS_LOG(WARN, "scan from server failed:index[%ld], size[%ld], server[%s], ret[%d]",
          i, size, server_addr, ret);
    }
  }
  return ret;
}

int RpcStub::get_update_server(const common::ObServer & server, const int64_t timeout,
    common::ObServer & update_server)
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

int RpcStub::get_schema(const ObServer & server, const int64_t timeout, const int64_t version,
    ObSchemaManagerV2 & schema)
{
  ObDataBuffer data_buff;
  int ret = get_rpc_buffer(data_buff);
  // step 1. serialize timestamp to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(data_buff.get_data(),
        data_buff.get_capacity(), data_buff.get_position(), version);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize timestamp failed:version[%ld], ret[%d]",
          version, ret);
    }
  }

  // step 2. send request for fetch new schema
  if (OB_SUCCESS == ret)
  {
    ret = frame_->send_request(server, OB_FETCH_SCHEMA, DEFAULT_VERSION,
        timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send request to root server for fetch schema failed:"
          "version[%ld], ret[%d]", version, ret);
    }
  }

  // step 3. deserialize the response code
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

  // step 4. deserialize the table schema
  if (OB_SUCCESS == ret)
  {
    ret = schema.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize schema from buff failed:"
          "version[%ld], pos[%ld], ret[%d]", version, pos, ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "fetch schema succ:version[%ld]", schema.get_version());
    }
  }
  return ret;
}

int RpcStub::get_version(const ObServer & server, const int64_t timeout, int64_t & version)
{
  ObDataBuffer data_buff;
  int ret = get_rpc_buffer(data_buff);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "check get rpc buffer failed:ret[%d]", ret);
  }
  else
  {
    // step 1. send get memtable version request
    ret = frame_->send_request(server, OB_UPS_GET_LAST_FROZEN_VERSION, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send get memtable version request failed:ret[%d]", ret);
    }
  }

  // step 2. deserialize result code
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode result_code;
    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "result code deserialize failed:ret[%d]", ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }

  // step 3. deserialize memtable version
  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi64(data_buff.get_data(), data_buff.get_position(), pos, &version);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "decode last frozen memtable version failed:ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "get frozen version succ:version[%ld]", version);
    }
  }
  return ret;
}

int RpcStub::response_finish(const int ret_code, const ObPacket * packet)
{
  int ret = common::OB_SUCCESS;
  if ((NULL == packet) || (false == check_inner_stat()) || NULL == server_)
  {
    TBSYS_LOG(ERROR, "check packet or inner stat failed:packet[%p]", packet);
    ret = common::OB_ERROR;
  }
  else
  {
    common::ObDataBuffer out_buffer;
    ret = get_rpc_buffer(out_buffer);
    if (ret != common::OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "get rpc buffer failed:ret[%d]", ret);
    }
    else
    {
      // serialize result code
      ObResultCode result;
      result.result_code_ = ret_code;
      ret = result.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "serialize result message failed:ret[%d]", ret);
      }

      // send reponse
      if (OB_SUCCESS == ret)
      {
        ret = server_->send_response(REPORT_TASK_RESPONSE, DEFAULT_VERSION, out_buffer, packet->get_request(),
            packet->get_channel_id());
        if (ret != common::OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "send response packet failed:ret[%d]", ret);
        }
      }
    }
  }
  return ret;
}

int RpcStub::response_fetch(const int ret_code, const TaskCounter & couter, const TaskInfo & task, ObPacket * packet)
{
  int ret = OB_SUCCESS;
  if ((NULL == packet) || (false == check_inner_stat()) || NULL == server_)
  {
    TBSYS_LOG(ERROR, "check packet or inner stat failed:packet[%p]", packet);
    ret = OB_ERROR;
  }
  else
  {
    ObDataBuffer out_buffer;
    ret = get_rpc_buffer(out_buffer);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "get rpc buffer failed:ret[%d]", ret);
    }
    else
    {
      // step 1. serialize result code
      ObResultCode result;
      result.result_code_ = ret_code;
      ret = result.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "serialize result message failed:ret[%d]", ret);
      }
    }

    // step 2. serialize task couter and task content
    if ((OB_SUCCESS == ret_code) && (OB_SUCCESS == ret))
    {
      ret = couter.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "serialize counter failed:ret[%d]", ret);
      }
      else
      {
        ret = task.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "serialize task faile:ret[%d]", ret);
        }
      }
    }

    // step 3. send packet for response
    if (OB_SUCCESS == ret)
    {
      ret = server_->send_response(FETCH_TASK_RESPONSE, DEFAULT_VERSION, out_buffer, packet->get_request(),
          packet->get_channel_id());
      if (ret != common::OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "send response packet failed:ret[%d]", ret);
      }
    }
  }
  return ret;
}

