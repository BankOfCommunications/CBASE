#include "mock_chunk_server.h"
#include "mock_define.h"
#include "common/ob_result.h"
#include "common/ob_define.h"
#include "common/ob_scanner.h"
#include "common/ob_tablet_info.h"
#include "common/ob_read_common_data.h"
#include "common/utility.h"
#include "../common/test_rowkey_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace oceanbase::mergeserver::test;
static CharArena allocator_;

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
    ObRowkey row_key;
    ObString column_name;
    char temp[256] = "";
#if 1
    cell.table_id_ = 101;
    for (uint64_t i = 0; i < 10; ++i)
    {
      snprintf(temp, 256, "chunk_%lu_scan_row_key:%lu", i, i);
      row_key = make_rowkey(temp, &allocator_);
      cell.row_key_ = row_key;
      cell.column_id_ = i + 1;
      cell.value_.set_int(2234 + i);
      scanner.add_cell(cell);
    }
#else
    cell.table_id_ = 123;
    for (uint64_t i = 100; i < 200; ++i)
    {
      snprintf(temp, 256, "row_%lu", i);
      row_key = make_rowkey(temp, &allocator_);
      cell.row_key_ = row_key;
      cell.column_id_ = 101;
      cell.value_.set_int(2234 + i);
      scanner.add_cell(cell);
    }
#endif

    /* begin add by xiaochu */
    //Scanner Range must be set other wise the ms client will report error
    ObNewRange range;
    /*
    /// This will cause rowkey mismatch
    //char *start= "chunk_0_scan_row_key:0";
    //char *end  = "chunk_9_scan_row_key:9";
    */
    char *start= (char*)"row_100";
    char *end  = (char*)"row_200";
    range.start_key_ = make_rowkey(start, &allocator_);;
    range.end_key_ = make_rowkey(end, &allocator_);
    range.table_id_ = 103;
    scanner.set_range(range);
    scanner.set_is_req_fullfilled(true, 10);
    /* end add by xiaochu */

    int32_t channel_id = ob_packet->getChannelId();
    ret = scanner.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    ObScannerIterator iter;
    for (iter = scanner.begin(); iter != scanner.end(); ++iter)
    {
      iter.get_cell(cell);
      printf("server_temp:%s\n", to_cstring(cell.row_key_));
    }//
    ret = send_response(OB_SCAN_RESPONSE, 1, out_buffer, connection, channel_id);
  }
  TBSYS_LOG(INFO, "handle scan table result:ret[%d]", ret);
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
    ObRowkey row_key;
    ObString column_name;
    for (int32_t i = 0; i < get_param.get_cell_size(); i ++)
    {
      cell.table_id_ = get_param[i]->table_id_;
      cell.column_id_ = get_param[i]->column_id_;
      if (mock::join_table_id == cell.table_id_)
      {
        if (mock::join_column1_id == cell.column_id_)
        {
          row_key = make_rowkey(mock::join_rowkey, &allocator_);
          cell.column_id_ = mock::join_column1_id;
          cell.row_key_ = row_key;
          cell.value_.set_int(mock::join_column1_cs_value);
          ret = scanner.add_cell(cell);
        }
        else if (mock::join_column2_id == cell.column_id_)
        {
          row_key = make_rowkey(mock::join_rowkey, &allocator_);
          cell.column_id_ = mock::join_column2_id;
          cell.row_key_ = row_key;
          cell.value_.set_int(mock::join_column2_cs_value);
          ret = scanner.add_cell(cell);
        }
        else
        {
          TBSYS_LOG(ERROR, "unepxected column id [tableid:%lu,columnid:%lu]", cell.table_id_,
                    cell.column_id_);
          ret = OB_ERR_UNEXPECTED;
        }
      }
      else if (mock::table_id == cell.table_id_)
      {
        if (mock::column1_id == cell.column_id_)
        {
          row_key = make_rowkey(mock::rowkey, &allocator_);
          cell.column_id_ = mock::column1_id;
          cell.row_key_ = row_key;
          cell.value_.set_int(mock::column1_cs_value);
          ret = scanner.add_cell(cell);
        }
        else if (mock::column2_id == cell.column_id_)
        {
          row_key = make_rowkey(mock::rowkey, &allocator_);
          cell.column_id_ = mock::column2_id;
          cell.row_key_ = row_key;
          cell.value_.set_int(mock::column2_cs_value);
          ret = scanner.add_cell(cell);
        }
        else
        {
          TBSYS_LOG(ERROR, "unepxected column id [tableid:%lu,columnid:%lu]", cell.table_id_,
                    cell.column_id_);
          ret = OB_ERR_UNEXPECTED;
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "unexpected table id [tableid:%lu]", cell.table_id_);
        ret = OB_ERR_UNEXPECTED;
      }
    }

    if (OB_SUCCESS == ret)
    {
      //scanner.set_timestamp(mock::schema_timestamp);
      ret = scanner.set_is_req_fullfilled(true,2);
    }
    int32_t channel_id = ob_packet->getChannelId();
    ret = scanner.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    ret = send_response(OB_GET_RESPONSE, 1, out_buffer, connection, channel_id);
  }
  TBSYS_LOG(INFO, "handle get table result:ret[%d]", ret);
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
    ObRowkey row_key;
    ObString column_name;
    cell.table_id_ = scan_param.get_table_id();
    if (mock::join_table_id == cell.table_id_)
    {
      for (int32_t i = 0; i < scan_param.get_column_id_size(); i++)
      {
        if (mock::join_column1_id == scan_param.get_column_id()[i])
        {
          row_key = make_rowkey(mock::join_rowkey, &allocator_);
          cell.column_id_ = mock::join_column1_id;
          cell.row_key_ = row_key;
          cell.value_.set_int(mock::join_column1_cs_value);
          ret = scanner.add_cell(cell);
        }
        else if (mock::join_column2_id == scan_param.get_column_id()[i])
        {
          row_key = make_rowkey(mock::join_rowkey, &allocator_);
          cell.column_id_ = mock::join_column2_id;
          cell.row_key_ = row_key;
          cell.value_.set_int(mock::join_column2_cs_value);
          ret = scanner.add_cell(cell);
        }
        else
        {
          TBSYS_LOG(ERROR, "unepxected column id [tableid:%lu,columnid:%lu]", cell.table_id_,
                    scan_param.get_column_id()[i]);
          ret = OB_ERR_UNEXPECTED;
        }
      }
    }
    else if (mock::table_id == cell.table_id_)
    {
      for (int32_t i = 0; i < scan_param.get_column_id_size(); i++)
      {
        if (mock::column1_id == scan_param.get_column_id()[i])
        {
          row_key = make_rowkey(mock::rowkey, &allocator_);
          cell.column_id_ = mock::column1_id;
          cell.row_key_ = row_key;
          cell.value_.set_int(mock::column1_cs_value);
          ret = scanner.add_cell(cell);
        }
        else if (mock::column2_id == scan_param.get_column_id()[i])
        {
          row_key = make_rowkey(mock::rowkey, &allocator_);
          cell.column_id_ = mock::column2_id;
          cell.row_key_ = row_key;
          cell.value_.set_int(mock::column2_cs_value);
          ret = scanner.add_cell(cell);
        }
        else
        {
          TBSYS_LOG(ERROR, "unepxected column id [tableid:%lu,columnid:%lu]", cell.table_id_,
                    scan_param.get_column_id()[i]);
          ret = OB_ERR_UNEXPECTED;
        }
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "unexpected table id [tableid:%lu]", cell.table_id_);
      ret = OB_ERR_UNEXPECTED;
    }

    if (OB_SUCCESS == ret)
    {
      //scanner.set_timestamp(mock::schema_timestamp);
      ret = scanner.set_is_req_fullfilled(true,1);
    }
    int64_t pos = 0;
    char range_buf[512];
    ObString range_str;
    if (OB_SUCCESS == ret)
    {
      ObRange range;
      range.border_flag_.set_min_value();
      range.border_flag_.set_max_value();
      ret = range.serialize(range_buf,sizeof(range_buf),pos);
      if (OB_SUCCESS == ret)
      {
        range_str.assign(range_buf,static_cast<int32_t>(pos));
        //ret = scanner.set_ext_info(range_str);
      }
      pos = 0;
      TBSYS_LOG(INFO, "pos:%ld,ret:%d",pos, 
                range.deserialize(range_str.ptr(),range_str.length(),pos));
    }
    int32_t channel_id = ob_packet->getChannelId();
    if (OB_SUCCESS == ret)
    {
      ret = scanner.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    }
    if (OB_SUCCESS == ret)
    {
      ret = send_response(OB_SCAN_RESPONSE, 1, out_buffer, connection, channel_id);
    }
  }
  TBSYS_LOG(INFO, "handle scan table result:ret[%d]", ret);
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
    ObCellInfo cell;
    ObScanner scanner;
    ObRowkey row_key;
    ObString column_name;
    char temp[256] = "";
    cell.table_id_ = 101; 
    for (uint64_t i = 0; i < 10; ++i)
    {
      snprintf(temp, 256, "chunk_%lu_get_row_key:%lu", i, i);
      row_key = make_rowkey(temp, &allocator_);
      cell.row_key_ = row_key;
      cell.column_id_ = i + 1;
      cell.value_.set_int(2234 + i);
      scanner.add_cell(cell);
    }
    scanner.set_is_req_fullfilled(true, 1);
    int32_t channel_id = ob_packet->getChannelId();
    ret = scanner.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    //
    ret = send_response(OB_GET_RESPONSE, 1, out_buffer, connection, channel_id);
  }
  TBSYS_LOG(INFO, "handle get table result:ret[%d]", ret);
  return ret;
}



