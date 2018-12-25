#include "mock_merge_server.h"
#include "common/ob_packet.h"
#include "common/ob_scanner.h"
#include "common/data_buffer.h"
using namespace oceanbase::mergeserver;
using namespace oceanbase::common;

MockMergeServer::MockMergeServer()
{
}
MockMergeServer::~MockMergeServer()
{
}
int MockMergeServer::initialize()
{
  int ret = OB_SUCCESS;
  /*
  if(TBSYS_CONFIG.load("./mock_merge_server.conf"))
  {
    fprintf(stderr, "load config file mock_merge_server.conf error");
    return EXIT_FAILURE;
  }
  ret = ms_params_.load_from_config(TBSYS_CONFIG);
  if (OB_SUCCESS == ret)
  {
    ms_params_.dump_config();
  }
  else
  {
    TBSYS_LOG(ERROR, "load config failed, ret=%d", ret);
  }
  */
  tbsys::CConfig config;
  if (EXIT_FAILURE == (ret = config.load("./mock_merge_server.conf")))
  {
    TBSYS_LOG(ERROR, "load config file ./mock_merge_server.conf error");
  }
  if (OB_SUCCESS != (ret = set_listen_port(config.getInt("merge_server", "port", 2800))))
  {
    TBSYS_LOG(ERROR, "set listen port failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = set_dev_name(config.getString("merge_server", "dev_name", NULL))))
  {
    TBSYS_LOG(ERROR, "set dev name failed, ret=%d",ret);
  }
  else if (OB_SUCCESS != (ret = set_self(config.getString("merge_server", "dev_name", NULL), config.getInt("merge_server", "port", 2800))))
  {
    TBSYS_LOG(ERROR, "failed to set self, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = set_thread_count(config.getInt("merge_server", "task_thread_count", 32))))
  {
    TBSYS_LOG(ERROR, "failed to set thread count,ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = set_packet_factory(&factory_)))
  {
    TBSYS_LOG(ERROR, "failed to set packet factory, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_manager_.initialize(get_transport(), get_packet_streamer())))
  {
    TBSYS_LOG(ERROR, "failed to initialize client manager,ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSingleServer::initialize()))
  {
    TBSYS_LOG(ERROR, "failed to start thread pool,ret=%d",ret);
  }

  /*
  if (OB_SUCCESS != (ret = set_listen_port(ms_params_.get_listen_port())))
  {
    TBSYS_LOG(ERROR, "set listen port failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = set_dev_name(ms_params_.get_device_name())))
  {
    TBSYS_LOG(ERROR, "set dev name failed, ret=%d",ret);
  }
  else if (OB_SUCCESS != (ret = set_self(ms_params_.get_device_name(), ms_params_.get_listen_port())))
  {
    TBSYS_LOG(ERROR, "failed to set self, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = set_thread_count(ms_params_.get_task_thread_size())))
  {
    TBSYS_LOG(ERROR, "failed to set thread count,ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = set_packet_factory(&factory_)))
  {
    TBSYS_LOG(ERROR, "failed to set packet factory, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_manager_.initialize(get_transport(), get_packet_streamer())))
  {
    TBSYS_LOG(ERROR, "failed to initialize client manager,ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSingleServer::initialize()))
  {
    TBSYS_LOG(ERROR, "failed to start thread pool,ret=%d",ret);
  }
  */
  return ret;
}
int MockMergeServer::do_request(ObPacket * base_packet)
{
  ObPacket * packet = base_packet;
  int32_t packet_code = packet->get_packet_code();
  int ret = OB_SUCCESS;
  ret = packet->deserialize();
  int64_t receive_time = packet->get_receive_ts();
  int32_t version = packet->get_api_version();
  int32_t channelId = packet->getChannelId();
  tbnet::Connection *connection = packet->get_connection();
  ObDataBuffer *in_buff = packet->get_buffer();
  ThreadSpecificBuffer::Buffer *thread_buff = response_packet_buffer_.get_buffer();
  //貌似刨去了网络时间？
  int64_t timeout_us = packet->get_source_timeout() - (tbsys::CTimeUtil::getTime() - packet->get_receive_ts());
  if (NULL != thread_buff)
  {
    thread_buff->reset();
    ObDataBuffer out_buff(thread_buff->current(), thread_buff->remain());
    if (OB_SUCCESS == ret)
    {
      switch(packet_code)
      {
        case OB_SCAN_REQUEST:
          ret = ms_scan(receive_time, version, channelId, connection, *in_buff, out_buff, timeout_us);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "ms_scan failed");
          }
          break;
      }
    }

  }
  return ret;
  
}
int MockMergeServer::ms_scan(const int64_t start_time,const int32_t version,const int32_t channel_id,tbnet::Connection* connection,common::ObDataBuffer& in_buffer,common::ObDataBuffer& out_buffer,const int64_t timeout_us)
{
  ObResultCode rc;
  rc.result_code_ = OB_SUCCESS;
  int ret = OB_SUCCESS;
  const int32_t MS_SCAN_VERSION = 1;
  ret = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
  if (OB_SUCCESS == ret)
  {
    ObScanner result_scanner;
    ObCellInfo cell;
    cell.table_name_.assign((char*)"table1", 6);
    //TODO
    char rowkey[30 * 1024];
    cell.row_key_.assign(rowkey, 30 * 1024);
    cell.column_name_.assign((char*)"column1", 7);
    cell.value_.set_int(0xff);
    ret = result_scanner.add_cell(cell);
    result_scanner.set_is_req_fullfilled(true, 1);
    if (OB_SUCCESS == ret)
    {
      ret = result_scanner.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
    }
    if (OB_SUCCESS == ret)
    {
      ret = this->send_response(OB_SCAN_RESPONSE, MS_SCAN_VERSION, out_buffer, connection, channel_id, 0);
    }

  }
  return ret;
}
