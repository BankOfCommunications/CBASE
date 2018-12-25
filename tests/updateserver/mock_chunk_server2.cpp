#include "mock_chunk_server2.h"
#include "mock_define.h"
#include "common/ob_result.h"
#include "common/ob_define.h"
#include "common/ob_scanner.h"
#include "common/ob_tablet_info.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;
using namespace oceanbase::updateserver::test;

int MockChunkServer::initialize()
{
  set_listen_port(CHUNK_SERVER_PORT);
  return MockServer::initialize();
}


int MockChunkServer::do_request(ObPacket* base_packet)
{
  int ret = OB_SUCCESS;
  ObPacket* ob_packet = base_packet;
  int32_t packet_code = ob_packet->get_packet_code();
  ret = ob_packet->deserialize();
  if (OB_SUCCESS == ret)
  {
    switch (packet_code)
    {
    case OB_GET_REQUEST:
      {
#if false
        ret = handle_mock_get(ob_packet);
#else
        ret = handle_get_table(ob_packet);
#endif
        break;
      }
    case OB_SCAN_REQUEST:
      {
#if false
        ret = handle_mock_scan(ob_packet);
#else
        ret = handle_scan_table(ob_packet);
#endif
        break;
      }
    default:
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "wrong packet code:packet[%d]", packet_code);
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

int MockChunkServer::handle_scan_table(ObPacket * ob_packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer* data = ob_packet->get_buffer();
  if (NULL == data)
  {
    ret = OB_ERROR;
  }

  ObScanParam param;
  if (OB_SUCCESS == ret)
  {
    ret = param.deserialize(data->get_data(), data->get_capacity(), data->get_position());
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "%s", "check param failed");
    }
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

    // fake cell
    ObCellInfo cell;
    ObScanner scanner;
    ObString row_key;
    ObString column_name;
    char temp[256] = "";
    cell.table_id_ = 101;
    for (uint64_t i = 0; i < 10; ++i)
    {
      snprintf(temp, 256, "chunk_%lu_scan_row_key:%lu", i, i);
      row_key.assign(temp, strlen(temp));
      printf("server:%.*s\n", row_key.length(), row_key.ptr());
      cell.row_key_ = row_key;
      cell.column_id_ = i + 1;
      cell.value_.set_int(2234 + i);
      scanner.add_cell(cell);
    }

    if (OB_SUCCESS == ret)
    {
      //scanner.set_timestamp(mock::schema_timestamp);
      ret = scanner.set_is_req_fullfilled(true,2);
    }
    int32_t channel_id = ob_packet->getChannelId();
    scanner.set_data_version(1);
    ret = scanner.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    ObScannerIterator iter;
    for (iter = scanner.begin(); iter != scanner.end(); ++iter)
    {
      iter.get_cell(cell);
      printf("server_temp:%.*s\n", cell.row_key_.length(), cell.row_key_.ptr());
    }//

    ret = send_response(OB_GET_RESPONSE, 1, out_buffer, connection, channel_id);
  }
  TBSYS_LOG(INFO, "handle scan root table result:ret[%d]", ret);
  return ret;
}

int MockChunkServer::handle_mock_get(ObPacket * ob_packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer* data = ob_packet->get_buffer();
  if (NULL == data)
  {
    ret = OB_ERROR;
  }

  ObGetParam get_param;
  if (OB_SUCCESS == ret)
  {
    ret = get_param.deserialize(data->get_data(), data->get_capacity(), data->get_position());
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "%s", "check param failed");
    }
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

    // fake cell
    ObCellInfo cell;
    ObScanner scanner;
    ObString row_key;
    ObString column_name;
    for (int32_t i = 0; i < get_param.get_cell_size(); i ++)
    {
      cell.table_id_ = get_param[i]->table_id_;
      cell.column_id_ = get_param[i]->column_id_;
      row_key.assign((char*)mock::join_rowkey,strlen(mock::join_rowkey));
      cell.row_key_ = row_key;
      cell.value_.set_int(mock::join_column1_cs_value);
      ret = scanner.add_cell(cell);
      if(OB_SUCCESS != ret)
      {
        break;
      }
    }

    if (OB_SUCCESS == ret)
    {
      //scanner.set_timestamp(mock::schema_timestamp);
      ret = scanner.set_is_req_fullfilled(true,2);
    }
    int32_t channel_id = ob_packet->getChannelId();
    scanner.set_data_version(1);
    ret = scanner.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    ret = send_response(OB_GET_RESPONSE, 1, out_buffer, connection, channel_id);
  }
  TBSYS_LOG(INFO, "handle scan root table result:ret[%d]", ret);
  return ret;
}

int MockChunkServer::handle_mock_scan(ObPacket * ob_packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer* data = ob_packet->get_buffer();
  if (NULL == data)
  {
    ret = OB_ERROR;
  }

  ObScanParam scan_param;
  if (OB_SUCCESS == ret)
  {
    ret = scan_param.deserialize(data->get_data(), data->get_capacity(), data->get_position());
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "%s", "check scan_param failed");
    }
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

    // fake cell
    ObCellInfo cell;
    ObScanner scanner;
    ObString row_key;
    ObString column_name;
    for (int32_t i = 0; i < 10; i ++)
    {
      cell.table_id_ = 123;
      cell.column_id_ = 345;
      row_key.assign((char*)mock::join_rowkey,strlen(mock::join_rowkey));
      cell.row_key_ = row_key;
      cell.value_.set_int(mock::join_column1_cs_value);
      ret = scanner.add_cell(cell);
      if(OB_SUCCESS != ret)
      {
        break;
      }
    }

    if (OB_SUCCESS == ret)
    {
      //scanner.set_timestamp(mock::schema_timestamp);
      ret = scanner.set_is_req_fullfilled(true,2);
    }
    int32_t channel_id = ob_packet->getChannelId();
    scanner.set_data_version(1);
    ret = scanner.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    ret = send_response(OB_GET_RESPONSE, 1, out_buffer, connection, channel_id);
  }
  TBSYS_LOG(INFO, "handle scan root table result:ret[%d]", ret);
  return ret;
}

int MockChunkServer::handle_get_table(ObPacket * ob_packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer* data = ob_packet->get_buffer();
  if (NULL == data)
  {
    ret = OB_ERROR;
  }

  ObGetParam param;
  if (OB_SUCCESS == ret)
  {
    ret = param.deserialize(data->get_data(), data->get_capacity(), data->get_position());
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "%s", "check param failed");
    }
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

    // fake data cell
    ObCellInfo *cell = new ObCellInfo[8];
    ObScanner scanner;
    ObString row_key;
    ObString column_name;
    scanner.reset();
    int64_t z = 0;
    for (int64_t i = 1; i < 9; ++i)
    //for (int64_t i = 1; i < 2; ++i)
    {
      char *temp = new char[256];
      snprintf(temp, 256, "row_key_%ld", i);
      ObString row_key;
      row_key.assign(temp, strlen(temp));
      cell[i-1].row_key_ = row_key;
     cell[i-1].table_id_ = 1001;
      cell[i-1].column_id_ = i;
      z = i+ 2232;
      cell[i-1].value_.set_int(z);

      bool found = false;
      for (int64_t j = 0; j < param.get_cell_size(); ++j)
      {
        ObCellInfo* get_cell_info = param[j];
        if (get_cell_info->table_id_ == cell[i-1].table_id_
            && get_cell_info->row_key_ == cell[i-1].row_key_)
        {
          found = true;
          break;
        }
      }

      if (found)
      {
        scanner.add_cell(cell[i-1]);
      }
    }
    delete [] cell;
    int32_t channel_id = ob_packet->getChannelId();
    scanner.set_data_version(1);
    scanner.set_is_req_fullfilled(true, 8);
   /* ObCellInfo *cell_info;
    scanner.next_cell();
    int64_t j = 0;
    while(OB_SUCCESS == (scanner.get_cell(&cell_info)))
    {
      cell_info->value_.get_int(j);
      TBSYS_LOG(INFO,"get_int=%d",j);
      scanner.next_cell();
    }
    scanner.reset_iter();
    */ret = scanner.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    TBSYS_LOG(INFO,"scanner size=%d",scanner.get_size()); 
    ret = send_response(OB_SCAN_RESPONSE, 1, out_buffer, connection, channel_id);
  }
  TBSYS_LOG(INFO, "==handle get  table result:ret[%d]", ret);
  return ret;
}



