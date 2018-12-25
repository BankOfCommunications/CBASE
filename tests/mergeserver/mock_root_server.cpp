#include "common/ob_result.h"
#include "common/ob_scanner.h"
#include "common/ob_read_common_data.h"
#include "common/ob_ups_info.h"
#include "common/ob_schema.h"
#include "mock_root_server.h"
#include "mock_update_server.h"
#include "../common/test_rowkey_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace oceanbase::mergeserver::test;
static CharArena allocator_;

int MockRootServer::initialize()
{
  set_listen_port(ROOT_SERVER_PORT);
  return MockServer::initialize();
}


int MockRootServer::do_request(ObPacket* base_packet)
{
  int ret = OB_SUCCESS;
  ObPacket* ob_packet = base_packet;
  int32_t packet_code = ob_packet->get_packet_code();
  ret = ob_packet->deserialize();
  if (OB_SUCCESS == ret)
  {   
    switch (packet_code)
    {
    case OB_SERVER_REGISTER:
      {
        ret = handle_register_server(ob_packet);
        break;
      }
    case OB_FETCH_SCHEMA:
      {
        ret = handle_fetch_schema(ob_packet);
        break;
      }
    case OB_FETCH_SCHEMA_VERSION:
      {
        ret = handle_fetch_schema_version(ob_packet);
        break;
      }
    case OB_SCAN_REQUEST:
    case OB_GET_REQUEST:
      {
        ret = handle_get_root(ob_packet);
        break;
      }
    case OB_GET_UPS:
      {
        ret = handle_get_ups_list(ob_packet);
        break;
      }
    case OB_GET_UPDATE_SERVER_INFO:
      {
        ret = handle_get_updater(ob_packet);
        break;
      }
    default:
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "wrong packet code:%d", packet_code);
        break;
      }
    }
    //
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "check handle failed:ret[%d]", ret);
    }
  }
  return ret;
}

int MockRootServer::handle_get_ups_list(ObPacket * ob_packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer* data = ob_packet->get_buffer();
  if (NULL == data)
  {
    ret = OB_ERROR;
  }

  tbnet::Connection* connection = ob_packet->get_connection();
  ThreadSpecificBuffer::Buffer* thread_buffer = response_packet_buffer_.get_buffer();
  if (NULL == thread_buffer)
  {
    ret = OB_ERROR;
  }
  else
  {
    thread_buffer->reset();
    ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());

    ObResultCode result_msg;
    result_msg.result_code_ = ret;
    ret = result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());

    // new version
    int32_t channel_id = ob_packet->getChannelId();
    ObServer server;
    ObUpsList list;
    list.ups_count_ = 3;
    server.set_ipv4_addr("localhost", MockUpdateServer::UPDATE_SERVER_PORT);
    list.ups_array_[0].addr_ = server;
    server.set_ipv4_addr("localhost", static_cast<int32_t>(random() + MockUpdateServer::UPDATE_SERVER_PORT));
    list.ups_array_[1].addr_ = server;
    server.set_ipv4_addr("localhost", static_cast<int32_t>(random() + MockUpdateServer::UPDATE_SERVER_PORT));
    list.ups_array_[2].addr_ = server;

    ret = list.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    ret = send_response(OB_REPORT_TABLETS_RESPONSE, 1, out_buffer, connection, channel_id);
  }
  return ret;
}

int MockRootServer::handle_get_updater(ObPacket * ob_packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer* data = ob_packet->get_buffer();
  if (NULL == data)
  {
    ret = OB_ERROR;
  }
  
  tbnet::Connection* connection = ob_packet->get_connection();
  ThreadSpecificBuffer::Buffer* thread_buffer = response_packet_buffer_.get_buffer();
  if (NULL == thread_buffer)
  {
    ret = OB_ERROR;
  }
  else
  {
    thread_buffer->reset();
    ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
    
    ObResultCode result_msg;
    result_msg.result_code_ = ret;
    ret = result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    
    // new version
    int32_t channel_id = ob_packet->getChannelId();
    ObServer server;
    server.set_ipv4_addr("localhost", MockUpdateServer::UPDATE_SERVER_PORT); 
    ret = server.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    ret = send_response(OB_REPORT_TABLETS_RESPONSE, 1, out_buffer, connection, channel_id);
  }
  return ret;
}

int MockRootServer::handle_fetch_schema_version(ObPacket * ob_packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer* data = ob_packet->get_buffer();
  if (NULL == data)
  {
    ret = OB_ERROR;
  }
  tbnet::Connection* connection = ob_packet->get_connection();
  ThreadSpecificBuffer::Buffer* thread_buffer = response_packet_buffer_.get_buffer();
  if (NULL == thread_buffer)
  {
    ret = OB_ERROR;
  }
  else
  {
    thread_buffer->reset();
    ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
    
    ObResultCode result_msg;
    result_msg.result_code_ = ret;
    ret = result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    
    int64_t timestamp = 1025;
    ret = serialization::encode_vi64(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(), timestamp);
    
    int32_t channel_id = ob_packet->getChannelId();
    ret = send_response(OB_REPORT_TABLETS_RESPONSE, 1, out_buffer, connection, channel_id);
  }
  return ret;
}

int MockRootServer::handle_fetch_schema(ObPacket * ob_packet)
{
  int ret = OB_SUCCESS;
  int64_t timestamp = 0;
  bool only_core_tables = false;
  ObDataBuffer* data = ob_packet->get_buffer();
  if (NULL == data)
  {
    ret = OB_ERROR;
  }
  
  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi64(data->get_data(), data->get_capacity(), data->get_position(), &timestamp);
    if ((timestamp != 0) && (timestamp != 1024))
    {
      TBSYS_LOG(ERROR, "check timestamp failed:timestamp[%ld]", timestamp);
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_bool(data->get_data(), data->get_capacity(), data->get_position(), &only_core_tables);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "decode only_core_tables failed.");
    }
  }

  TBSYS_LOG(DEBUG, "handle_fetch_schema timestamp=%ld, only_core_tables=%d", timestamp, only_core_tables);
  
  tbnet::Connection* connection = ob_packet->get_connection();
  ThreadSpecificBuffer::Buffer* thread_buffer = response_packet_buffer_.get_buffer();
  if (NULL == thread_buffer)
  {
    ret = OB_ERROR;
  }
  else
  {
    ObSchemaManagerV2 schema(1025);
    tbsys::CConfig conf;
    if (!schema.parse_from_file("schema.ini", conf))
    {
      TBSYS_LOG(ERROR, "%s", "parse from file failed");
      ret = OB_ERROR;
    }

    int32_t channel_id = ob_packet->getChannelId();
    thread_buffer->reset();
    ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
    
    ObResultCode result_msg;
    result_msg.result_code_ = ret;
    if (OB_SUCCESS != (ret = result_msg.serialize(out_buffer.get_data(), 
            out_buffer.get_capacity(), out_buffer.get_position())))
    {
      TBSYS_LOG(ERROR, "serialize result_msg failed.");
    }
    else if (OB_SUCCESS != (ret = schema.serialize(out_buffer.get_data(), 
            out_buffer.get_capacity(), out_buffer.get_position())))
    {
      TBSYS_LOG(ERROR, "serialize schema failed.");
    }
    else if (OB_SUCCESS != (ret = send_response(OB_FETCH_SCHEMA_RESPONSE, 
            1, out_buffer, connection, channel_id)))
    {
      TBSYS_LOG(ERROR, "send response failed.");
    }
    else
    {
      TBSYS_LOG(DEBUG, "send_response succeed, pos=%ld", out_buffer.get_position());
    }
  }
  return ret;
}

int MockRootServer::handle_register_server(ObPacket * ob_packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer* data = ob_packet->get_buffer();
  if (NULL == data)
  {
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret)
  {
    ObServer server;
    ret = server.deserialize(data->get_data(), data->get_capacity(), data->get_position());
  }

  if (OB_SUCCESS == ret)
  {
    bool is_merger = false;
    ret = serialization::decode_bool(data->get_data(), data->get_capacity(), data->get_position(), &is_merger);
    if ((ret == OB_SUCCESS) && is_merger)
    {
      TBSYS_LOG(INFO, "%s", "merge server registered");
    }
  }
  
  // response
  tbnet::Connection* connection = ob_packet->get_connection();
  ThreadSpecificBuffer::Buffer* thread_buffer = response_packet_buffer_.get_buffer();
  if (NULL == thread_buffer)
  {
    ret = OB_ERROR;
  }
  else
  {
    thread_buffer->reset();
    ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
    
    ObResultCode result_msg;
    result_msg.result_code_ = ret;
    result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    
    int32_t channel_id = ob_packet->getChannelId();
    ret = send_response(OB_REPORT_TABLETS_RESPONSE, 1, out_buffer, connection, channel_id);
  }
  TBSYS_LOG(INFO, "handle register server result:ret[%d]", ret);
  return ret;
}


int MockRootServer::handle_get_root(ObPacket * ob_packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer* data = ob_packet->get_buffer();
  if (NULL == data)
  {
    ret = OB_ERROR;
  }
  
  tbnet::Connection* connection = ob_packet->get_connection();
  ThreadSpecificBuffer::Buffer* thread_buffer = response_packet_buffer_.get_buffer();
  if (NULL == thread_buffer)
  {
    ret = OB_ERROR;
  }
  else
  {
    thread_buffer->reset();
    ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
    
    ObResultCode result_msg;
    result_msg.result_code_ = ret;
    ret = result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());

    // fake location cell
    ObScanner scanner;
    char table[128] = "";
    char rowkey[128] = "";
    char temp[128] = "";
    ObCellInfo cell;
    
    // the first row
    sprintf(table, "root_table");
    cell.table_name_.assign(table, static_cast<int32_t>(strlen(table)));
    
    sprintf(rowkey, "row_100");
    cell.row_key_ = make_rowkey(rowkey, &allocator_);
    
    sprintf(temp, "2_ipv4");
    cell.column_name_.assign(temp, static_cast<int32_t>(strlen(temp)));
    cell.value_.set_int(16777343);
    scanner.add_cell(cell);
    
    sprintf(temp, "2_port");
    cell.column_name_.assign(temp, static_cast<int32_t>(strlen(temp)));
    cell.value_.set_int(10240);
    scanner.add_cell(cell);
    
    // the second row
    sprintf(rowkey, "row_200");
    cell.row_key_ = make_rowkey(rowkey, &allocator_);
    for (int i = 1; i <= 3; ++i)
    {
      sprintf(temp, "%d_ipv4", i);
      cell.column_name_.assign(temp, static_cast<int32_t>(strlen(temp)));
      cell.value_.set_int(16777343);
      scanner.add_cell(cell);
      
      sprintf(temp, "%d_port", i);
      cell.column_name_.assign(temp, static_cast<int32_t>(strlen(temp)));
      cell.value_.set_int(12341);
      scanner.add_cell(cell);
    }
    
    // the third row
    sprintf(rowkey, "row_999");
    cell.row_key_ = make_rowkey(rowkey, &allocator_);
    for (int i = 1; i <= 3; ++i)
    {
      sprintf(temp, "%d_ipv4", i);
      cell.column_name_.assign(temp, static_cast<int32_t>(strlen(temp)));
      cell.value_.set_int(16777343);
      scanner.add_cell(cell);
      
      sprintf(temp, "%d_port", i);
      cell.column_name_.assign(temp, static_cast<int32_t>(strlen(temp)));
      cell.value_.set_int(12341);
      scanner.add_cell(cell);
    }
    
    int32_t channel_id = ob_packet->getChannelId();
    ret = scanner.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    ret = send_response(OB_REPORT_TABLETS_RESPONSE, 1, out_buffer, connection, channel_id);
    TBSYS_LOG(INFO, "send get root response:ret[%d]", ret);
  }
  
  TBSYS_LOG(INFO, "handle get root table result:ret[%d]", ret);
  return ret;
}



