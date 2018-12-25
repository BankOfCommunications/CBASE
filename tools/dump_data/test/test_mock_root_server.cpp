#include "common/ob_result.h"
#include "common/ob_scanner.h"
#include "common/ob_read_common_data.h"
#include "common/ob_schema.h"
#include "test_mock_root_server.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

int MockRootServer::initialize()
{
  set_dev_name("bond0");
  set_batch_process(false);
  set_listen_port(ROOT_SERVER_PORT);
  int ret = set_default_queue_size(100);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "set default queue size failed:ret[%d]", ret);
  }
  else
  {
    ret = BaseServer::initialize();
  }
  return ret;
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
    case OB_FETCH_SCHEMA:
      {
        ret = handle_fetch_schema(ob_packet);
        break;
      }
    case OB_GET_REQUEST:
      {
        ret = handle_get_root(ob_packet);
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

int MockRootServer::handle_fetch_schema(ObPacket * ob_packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer* data = ob_packet->get_buffer();
  if (NULL == data)
  {
    ret = OB_ERROR;
  }
  
  if (OB_SUCCESS == ret)
  {
    int64_t timestamp = 0;
    ret = serialization::decode_vi64(data->get_data(), data->get_capacity(), data->get_position(), &timestamp);
    if ((timestamp != 0) && (timestamp != 1024))
    {
      TBSYS_LOG(ERROR, "check timestamp failed:timestamp[%ld]", timestamp);
      ret = OB_ERROR;
    }
  }
  
  tbnet::Connection* connection = ob_packet->get_connection();
  ThreadSpecificBuffer::Buffer* thread_buffer = rpc_buffer_.get_buffer();
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
    ObSchemaManager schema(1025);
    tbsys::CConfig conf;
    if (!schema.parse_from_file("schema.ini", conf))
    {
      TBSYS_LOG(ERROR, "%s", "parse from file failed");
    }
    
    int32_t channel_id = ob_packet->getChannelId();
    ret = schema.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    ret = send_response(OB_REPORT_TABLETS_RESPONSE, 1, out_buffer, connection, channel_id);
  }
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
  ThreadSpecificBuffer::Buffer* thread_buffer = rpc_buffer_.get_buffer();
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
    cell.table_name_.assign(table, strlen(table));
    
    sprintf(rowkey, "row_100");
    cell.row_key_.assign(rowkey, strlen(rowkey));
    
    sprintf(temp, "2_ipv4");
    cell.column_name_.assign(temp, strlen(temp));
    cell.value_.set_int(16777343);
    scanner.add_cell(cell);
    
    sprintf(temp, "2_ms_port");
    cell.column_name_.assign(temp, strlen(temp));
    cell.value_.set_int(10240);
    scanner.add_cell(cell);
    
    // the second row
    sprintf(rowkey, "row_200");
    cell.row_key_.assign(rowkey, strlen(rowkey));
    for (int i = 1; i <= 3; ++i)
    {
      sprintf(temp, "%d_ipv4", i);
      cell.column_name_.assign(temp, strlen(temp));
      cell.value_.set_int(16777343);
      scanner.add_cell(cell);
      
      sprintf(temp, "%d_ms_port", i);
      cell.column_name_.assign(temp, strlen(temp));
      cell.value_.set_int(12341);
      scanner.add_cell(cell);
    }
    
    // the third row
    sprintf(rowkey, "row_999");
    cell.row_key_.assign(rowkey, strlen(rowkey));
    for (int i = 1; i <= 3; ++i)
    {
      sprintf(temp, "%d_ipv4", i);
      cell.column_name_.assign(temp, strlen(temp));
      cell.value_.set_int(16777343);
      scanner.add_cell(cell);
      
      sprintf(temp, "%d_ms_port", i);
      cell.column_name_.assign(temp, strlen(temp));
      cell.value_.set_int(12341);
      scanner.add_cell(cell);
    }
    
    // last row
    for (int i = 0; i < 8; ++i)
    {
      rowkey[i] = 0xFF;
    }
    cell.row_key_.assign(rowkey, strlen(rowkey));
    for (int i = 1; i <= 3; ++i)
    {
      sprintf(temp, "%d_ipv4", i);
      cell.column_name_.assign(temp, strlen(temp));
      cell.value_.set_int(16777343);
      scanner.add_cell(cell);
      
      sprintf(temp, "%d_ms_port", i);
      cell.column_name_.assign(temp, strlen(temp));
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




