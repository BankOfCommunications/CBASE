
#include "ob_root_server_rpc.h"
#include "common/ob_client_manager.h"
#include "common/ob_result.h"
#include "ob_chunk_server.h"
#include "ob_chunk_server_main.h"
#include "common/ob_scan_param.h"
#include "common/ob_scanner.h"
#include "common/ob_version.h"

using namespace oceanbase::common;
using namespace oceanbase::chunkserver;

ObRootServerRpcStub::ObRootServerRpcStub()
{
  init_ = false;
  rpc_frame_ = NULL;
}

ObRootServerRpcStub::~ObRootServerRpcStub()
{

}


int ObRootServerRpcStub::init(const ObServer & root_server, const ObClientManager * rpc_frame)
{
  int ret = OB_SUCCESS;
  if (init_ || (NULL == rpc_frame))
  {
    TBSYS_LOG(ERROR, "already inited or check input failed:inited[%s], rpc_frame[%p]",
       (init_? "ture": "false"), rpc_frame);
    ret = OB_ERROR;
  }
  else
  {
    root_server_ = root_server;
    rpc_frame_ = rpc_frame;
    init_ = true;
  }
  return ret;
}


ThreadSpecificBuffer::Buffer* ObRootServerRpcStub::get_thread_buffer(void) const
{
  ThreadSpecificBuffer::Buffer* buffer = NULL;
  // get buffer for rpc send and receive
  ObChunkServerMain * obj = ObChunkServerMain::get_instance();
  if (NULL == obj)
  {
    TBSYS_LOG(ERROR, "get ObChunkServerMain instance failed.");
  }
  else
  {
    const ObChunkServer & server = obj->get_chunk_server();
    buffer = server.get_rpc_buffer();
  }
  return buffer;
}

int ObRootServerRpcStub::get_frame_buffer(common::ObDataBuffer & data_buffer) const
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "check inner stat failed.");
    ret = OB_ERROR;
  }

  common::ThreadSpecificBuffer::Buffer* rpc_buffer = get_thread_buffer();
  if (OB_SUCCESS == ret)
  {
    if (NULL == rpc_buffer)
    {
      TBSYS_LOG(ERROR, "get thread rpc buff failed:buffer[%p].", rpc_buffer);
      ret = OB_ERROR;
    }
    else
    {
      rpc_buffer->reset();
      data_buffer.set_data(rpc_buffer->current(), rpc_buffer->remain());
    }
  }
  return ret;
}


int ObRootServerRpcStub::fetch_schema(const int64_t timestamp, ObSchemaManagerV2 & schema)
{
  int ret = OB_SUCCESS;
  // send_request timeout us
  const int64_t timeout = ObChunkServerMain::get_instance()->get_chunk_server().get_config().get_network_timeout();
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  // step 1. serialize timestamp to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = common::serialization::encode_vi64(data_buff.get_data(),
        data_buff.get_capacity(), data_buff.get_position(), timestamp);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize timestamp failed:timestamp[%ld], ret[%d].",
          timestamp, ret);
    }
  }

  // step 2. send request for fetch new schema
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server_, OB_FETCH_SCHEMA, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send request to root server(%s) for fetch schema failed"
          ":timestamp[%ld], ret[%d].",to_cstring(root_server_), timestamp, ret);
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
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d].", pos, ret);
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
      TBSYS_LOG(ERROR, "deserialize schema from buff failed,"
          "timestamp[%ld], pos[%ld], ret[%d].", timestamp, pos, ret);
    }
  }

  return ret;
}

int ObRootServerRpcStub::report_tablets(const ObTabletReportInfoList& tablets, int64_t time_stamp, bool has_more)
{
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  const int64_t report_timeout = ObChunkServerMain::get_instance()->
    get_chunk_server().get_config().get_network_timeout() ;  // send_request timeout us
  const ObChunkServer& chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();
  ObServer client_server;

  if (OB_SUCCESS == ret)
  {
    client_server = chunk_server.get_self();
  }

  // serialize ObServer(chunkserver) + ObTabletReportInfoList + int64_t(timestamp)
  // 1. serialize ObServer(client_server)
  if (OB_SUCCESS == ret)
  {
    ret = client_server.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize ObServer(chunkserver) failed: ret[%d].", ret);
    }
  }

  // 2. serialize report Tablets
  if (OB_SUCCESS == ret)
  {
    ret = tablets.serialize(data_buff.get_data(),
        data_buff.get_capacity(), data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize TabletInfoList failed: ret[%d].", ret);
    }
  }

  // 3. serialize time_stamp
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "report_tablets, timestamp:%ld", time_stamp);
    ret = serialization::encode_vi64(data_buff.get_data(),
        data_buff.get_capacity(), data_buff.get_position(), time_stamp);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "encode_vi64 time_stamp failed: ret[%d].", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server_, OB_REPORT_TABLETS,
        DEFAULT_VERSION, report_timeout, data_buff);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "send_request(ReportTablets) to RootServer failed: ret[%d].", ret);
    }
  }

  int64_t return_start_pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode report_result;
    ret = report_result.deserialize(data_buff.get_data(),
        data_buff.get_position(), return_start_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize ObResultCode(report tablet result) failed: ret[%d].", ret);
    }
    else if (OB_SUCCESS != report_result.result_code_)
    {
      TBSYS_LOG(ERROR, "report Tablet failed, the RootServer returned error: ret[%d].",
          report_result.result_code_);
      ret = report_result.result_code_;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (!has_more)  // at the end of report process
    {
      TBSYS_LOG(INFO, "report tablets over, send OB_WAITING_JOB_DONE message.");
      // reset param buffer, for new remote procedure call process(OB_WAITING_JOB_DONE)
      data_buff.get_position() = 0;
      // serialize input param (const ObServer& client, const int64_t time_stamp)
      ret = client_server.serialize(data_buff.get_data(),
          data_buff.get_capacity(), data_buff.get_position());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "serialize ObServer(chunkserver) failed: ret[%d].", ret);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(data_buff.get_data(),
            data_buff.get_capacity(), data_buff.get_position(), time_stamp);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "encode_vi64 time_stamp failed: ret[%d].", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = rpc_frame_->send_request(root_server_, OB_WAITING_JOB_DONE,
            DEFAULT_VERSION, report_timeout, data_buff);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "ObClientManager::send_request(schema_changed) to "
              "RootServer failed: ret[%d].", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "schema_changed request is sent: "
            "dst[%s] code[%d] version[%d] data_length[%ld]",
            to_cstring(root_server_), OB_WAITING_JOB_DONE, DEFAULT_VERSION, data_buff.get_position());

        ObResultCode report_result;
        return_start_pos = 0;
        ret = report_result.deserialize(data_buff.get_data(),
            data_buff.get_position(), return_start_pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize ObResultCode(schema changed result)"
              "failed: ret[%d].", ret);
        }
        else if (OB_SUCCESS != report_result.result_code_)
        {
          TBSYS_LOG(ERROR, "report schema changed failed, "
              "the RootServer returned error: ret[%d].",
              report_result.result_code_);
          ret = report_result.result_code_;
        }
      }
    }
  }

  return ret;
}

int ObRootServerRpcStub::delete_tablets(const common::ObTabletReportInfoList& tablets)
{
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  const int64_t timeout = ObChunkServerMain::get_instance()->
    get_chunk_server().get_config().get_network_timeout() ;  // send_request timeout us
  const ObChunkServer& chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();
  ObServer client_server;

  if (OB_SUCCESS == ret)
  {
    client_server = chunk_server.get_self();
  }

  // serialize ObServer(chunkserver) + ObTabletReportInfoList + int64_t(timestamp)
  // 1. serialize ObServer(client_server)
  if (OB_SUCCESS == ret)
  {
    ret = client_server.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize ObServer(chunkserver) failed: ret[%d].", ret);
    }
  }

  // 2. serialize report Tablets
  if (OB_SUCCESS == ret)
  {
    ret = tablets.serialize(data_buff.get_data(),
        data_buff.get_capacity(), data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize TabletInfoList failed: ret[%d].", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server_, OB_CS_DELETE_TABLETS,
        DEFAULT_VERSION, timeout, data_buff);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "send_request(ReportTablets) to RootServer failed: ret[%d].", ret);
    }
  }

  int64_t return_start_pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode result;
    ret = result.deserialize(data_buff.get_data(),
        data_buff.get_position(), return_start_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize ObResultCode(report tablet result) failed: ret[%d].", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(ERROR, "report Tablet failed, the RootServer returned error: ret[%d].",
          result.result_code_);
      ret = result.result_code_;
    }
  }

  return ret;
}

int ObRootServerRpcStub::register_server(const common::ObServer & server,
    const bool is_merge_server, int32_t& status)
{
  const int64_t register_timeout = ObChunkServerMain::get_instance()->
    get_chunk_server().get_config().get_network_timeout() ;  // send_request timeout us
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  // step 1. serialize server to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = server.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize server failed[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_bool(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position(), is_merge_server);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize is_merge_server failed=[%d]", ret);
    }
  }

  // step 2. send request for fetch new schema
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server_,
        OB_SERVER_REGISTER, DEFAULT_VERSION, register_timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send request to root server for register failed"
          ",ret=[%d].", ret);
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
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d].", pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }
  // step 4. deserialize status field
  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi32(data_buff.get_data(), data_buff.get_position(), pos, &status);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize status from buff failed,"
          "pos[%ld], ret[%d].", pos, ret);
    }
  }

  return ret;
}


int ObRootServerRpcStub::report_capacity_info(const common::ObServer &server,
    const int64_t capacity, const int64_t used)
{
  const int64_t report_timeout = ObChunkServerMain::get_instance()->
    get_chunk_server().get_config().get_network_timeout() ;  // send_request timeout us
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  // step 1. serialize server to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = server.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize server failed[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position(), capacity);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize capacity failed=[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position(), used);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize used failed=[%d]", ret);
    }
  }

  // step 2. send request for fetch new schema
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server_,
        OB_REPORT_CAPACITY_INFO, DEFAULT_VERSION, report_timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send request to root server for report capacity failed"
          ",ret=[%d].", ret);
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
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d].", pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }

  return ret;
}

int ObRootServerRpcStub::dest_load_tablet(
    const common::ObServer &dest_server,
    const common::ObNewRange &range,
    const int32_t dest_disk_no,
    const int64_t tablet_version,
    const uint64_t crc_sum,
    const int64_t size,
    const char (*path)[common::OB_MAX_FILE_NAME_LENGTH])
{
  const int64_t timeout = ObChunkServerMain::get_instance()->
    get_chunk_server().get_config().get_network_timeout() ;  // send_request timeout us
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  // step 1. serialize range to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = range.serialize(data_buff.get_data(), data_buff.get_capacity(),
                          data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize range failed[%d]", ret);
    }
  }

  // dest_disk_no
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi32(data_buff.get_data(), data_buff.get_capacity(),
                                     data_buff.get_position(), dest_disk_no);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize dest_disk_no failed[%d]", ret);
    }
  }

  // tablet_version
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(data_buff.get_data(), data_buff.get_capacity(),
                                     data_buff.get_position(), tablet_version);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize tablet_version failed=[%d]", ret);
    }
  }

  // crc checksum
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(data_buff.get_data(), data_buff.get_capacity(),
                                     data_buff.get_position(), crc_sum);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize crc_sum failed=[%d]", ret);
    }
  }

  // number of sstable files
  if(OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(data_buff.get_data(), data_buff.get_capacity(),
                                     data_buff.get_position(), size);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize file num failed=[%d]", ret);
    }
  }

  // path
  if (OB_SUCCESS == ret)
  {
    for ( int64_t idx =0 ; idx < size; idx++)
    {
      ret = serialization::encode_vstr(data_buff.get_data(), data_buff.get_capacity(),
          data_buff.get_position(), path[idx]);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "serialize file path failed=[%d]", ret);
        break;
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(dest_server,
        OB_MIGRATE_OVER, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send request to chunkserver for dest load tablet"
          ",ret=[%d].", ret);
    }
    else
    {
      TBSYS_LOG(INFO,"send request to destination server success");
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
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d].", pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }
  return ret;
}


int ObRootServerRpcStub::migrate_over(
    const common::ObNewRange &range,
    const common::ObServer &src_server,
    const common::ObServer &dest_server,
    const bool keep_src,
    const int64_t tablet_version)
{
  const int64_t timeout = ObChunkServerMain::get_instance()->
    get_chunk_server().get_config().get_network_timeout() ;  // send_request timeout us
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  // step 1. serialize server to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = range.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize range failed[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = src_server.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize src_server failed[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = dest_server.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize dest_server failed=[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_bool(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position(), keep_src);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize keep_src failed=[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position(), tablet_version);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize tablet_version failed=[%d]", ret);
    }
  }

  // step 2. send request for report tablet migrate over.
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server_,
        OB_MIGRATE_OVER, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send request to root server for register failed"
          ",ret=[%d].", ret);
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
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d].", pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }

  return ret;
}

int ObRootServerRpcStub::merge_tablets_over(
  const ObTabletReportInfoList& tablet_list,
  const bool is_merge_succ)
{
  const int64_t timeout = ObChunkServerMain::get_instance()->
    get_chunk_server().get_config().get_network_timeout() ;  // send_request timeout us
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  // step 1. serialize server to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_bool(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position(), is_merge_succ);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize is_merge_succ failed=[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret && is_merge_succ)
  {
    ret = tablet_list.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize merged_tablet_list failed=[%d]", ret);
    }
  }

  // step 2. send request for report tablet merge over.
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server_,
        OB_CS_MERGE_TABLETS_DONE, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send request to root server for merge tablets over failed"
          ",ret=[%d].", ret);
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
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d].", pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }

  return ret;
}

int ObRootServerRpcStub::get_merge_delay_interval(int64_t &interval) const
{
  int ret = OB_SUCCESS;
  const int64_t timeout = ObChunkServerMain::get_instance()->
    get_chunk_server().get_config().get_network_timeout() ;  // send_request timeout us
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);
  // step 1. send get update server info request
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server_,
                                  OB_GET_MERGE_DELAY_INTERVAL,
                                  DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send request to root server for register server failed:ret[%d]", ret);
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

  // step 3. deserialize
  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi64(data_buff.get_data(), data_buff.get_position(), pos,&interval);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize failed:pos[%ld], ret[%d]", pos, ret);
    }
  }
  return ret;
}

int ObRootServerRpcStub::async_heartbeat(const ObServer & client)
{
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  // step 1. serialize client server addr to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = client.serialize(data_buff.get_data(),
        data_buff.get_capacity(), data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize client server addr failed:ret[%d].", ret);
    }
  }

  // step 2. rpc frame send request and receive the response
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->post_request(root_server_,
        OB_HEARTBEAT, DEFAULT_VERSION, data_buff);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "post request to root server "
          "for heartbeat failed:ret[%d].", ret);
    }
  }

  // step 3. dont care server's response packet.
  return ret;
}

int ObRootServerRpcStub::get_migrate_dest_location(
    const int64_t occupy_size, int32_t &dest_disk_no, ObString &dest_path)
{
  const int64_t timeout = ObChunkServerMain::get_instance()->
    get_chunk_server().get_config().get_network_timeout() ;  // send_request timeout us
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  // step 1. serialize server to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position(), occupy_size);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize occupy_size failed[%d]", ret);
    }
  }

  // step 2. send request for report tablet migrate over.
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server_,
        OB_CS_GET_MIGRATE_DEST_LOC, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send request to root server for register failed"
          ",ret=[%d].", ret);
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
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d].", pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }

  // step 4. deserialize dest_disk_no & dest_path
  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi32(data_buff.get_data(), data_buff.get_position(), pos, &dest_disk_no);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize dest_disk_no failed:pos[%ld], ret[%d]", pos, ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = dest_path.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize dest_path failed:pos[%ld], ret[%d]", pos, ret);
    }
  }

  return ret;
}



int ObRootServerRpcStub::get_last_frozen_memtable_version(int64_t &last_version)
{
  const int64_t timeout = ObChunkServerMain::get_instance()->
    get_chunk_server().get_config().get_network_timeout() ;  // send_request timeout us
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  // step 1. serialize server to data_buff

  // step 2. send request for report tablet migrate over.
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server_,
        OB_UPS_GET_LAST_FROZEN_VERSION, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send request to root server for register failed"
          ",ret=[%d].", ret);
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
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d].", pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }

  // step 4. deserialize dest_disk_no & dest_path
  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi64(data_buff.get_data(), data_buff.get_position(), pos, &last_version);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize last_version failed:pos[%ld], ret[%d]", pos, ret);
    }
  }

  return ret;
}

int ObRootServerRpcStub::scan(const ObServer & server, const int64_t timeout,
                             const ObScanParam & param, ObScanner & result)
{
  ObDataBuffer data_buff;
  int ret = get_frame_buffer(data_buff);
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
    ret = rpc_frame_->send_request(server, OB_SCAN_REQUEST, DEFAULT_VERSION,
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
    else if ((ret = result_code.result_code_) != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR,"scan failed,ret[%d]",ret);
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


int ObRootServerRpcStub::get_tablet_info(const common::ObSchemaManagerV2& schema,
    const uint64_t table_id, const ObNewRange& range, ObTabletLocation location [],int32_t& size)
{
  int ret = OB_SUCCESS;
  int32_t index = 0;
  const ObChunkServer& chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();
  const int64_t timeout = chunk_server.get_config().get_network_timeout();
  if (OB_INVALID_ID == table_id || size <= 0)
  {
    TBSYS_LOG(ERROR,"invalid table id");
    ret = OB_ERROR;
  }

  ObScanParam param;
  ObScanner scanner;
  ObString table_name;

  const ObTableSchema *table = schema.get_table_schema(table_id);

  if (NULL == table)
  {
    TBSYS_LOG(ERROR,"can not find table %lu",table_id);
    ret = OB_ERROR;
  }
  else
  {
    table_name.assign_ptr(const_cast<char *>(table->get_table_name()),static_cast<int32_t>(strlen(table->get_table_name())));
  }

  if (OB_SUCCESS == ret)
  {
    param.set(OB_INVALID_ID,table_name,range); //use table name
    param.set_is_read_consistency(false);
  }

  if ((OB_SUCCESS == ret) && ((ret = scan(root_server_,timeout,param,scanner)) != OB_SUCCESS) )
  {
    TBSYS_LOG(ERROR,"get tablet from rootserver failed:[%d]",ret);
  }

  ObServer server;
  char tmp_buf[32];
  ObCellInfo * cell = NULL;
  ObScannerIterator iter;
  bool row_change = false;

  if (OB_SUCCESS == ret)
  {
    int64_t ip = 0;
    int64_t port = 0;
    int64_t version = 0;
    iter = scanner.begin();
    ret = iter.get_cell(&cell, &row_change);
    row_change = false;

    while((OB_SUCCESS == ret) && index < size)
    {
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "get cell from scanner iterator failed:ret[%d]", ret);
      }
      else if (row_change)
      {
        TBSYS_LOG(DEBUG,"row :%s changed,ignore", to_cstring(cell->row_key_));
        break; //just get one row
      }
      else if (cell != NULL)
      {
        if ((cell->column_name_.compare("1_port") == 0)
            || (cell->column_name_.compare("2_port") == 0)
            || (cell->column_name_.compare("3_port") == 0)
            //add zhaoqiong[roottable tablet management]20160104:b
            || (cell->column_name_.compare("4_port") == 0)
            || (cell->column_name_.compare("5_port") == 0)
            || (cell->column_name_.compare("6_port") == 0))
            //add:e
        {
          ret = cell->value_.get_int(port);
          TBSYS_LOG(DEBUG,"port is %ld",port);
        }
        else if ((cell->column_name_.compare("1_ipv4") == 0)
                 || (cell->column_name_.compare("2_ipv4") == 0)
                 || (cell->column_name_.compare("3_ipv4") == 0)
                 //add zhaoqiong[roottable tablet management]20160104:b
                 || (cell->column_name_.compare("4_ipv4") == 0)
                 || (cell->column_name_.compare("5_ipv4") == 0)
                 || (cell->column_name_.compare("6_ipv4") == 0))
                 //add:e
        {
          ret = cell->value_.get_int(ip);
          TBSYS_LOG(DEBUG,"ip is %ld",ip);
        }
        else if ((cell->column_name_.compare("1_tablet_version") == 0)
                 || (cell->column_name_.compare("2_tablet_version") == 0)
                 || (cell->column_name_.compare("3_tablet_version") == 0)
                 //add zhaoqiong[roottable tablet management]20160104:b
                 || (cell->column_name_.compare("4_tablet_version") == 0)
                 || (cell->column_name_.compare("5_tablet_version") == 0)
                 || (cell->column_name_.compare("6_tablet_version") == 0))
                 //add:e
        {
          ret = cell->value_.get_int(version);
          TBSYS_LOG(DEBUG,"tablet_version is %ld, rowkey:%s",version, to_cstring(cell->row_key_));
        }
        if (OB_SUCCESS == ret)
        {
          if (0 != port && 0 != ip && 0 != version)
          {
            server.set_ipv4_addr(static_cast<int32_t>(ip), static_cast<int32_t>(port));
            TBSYS_LOG(INFO,"add tablet s:%s,%ld", to_cstring(server), version);
            ObTabletLocation addr(version, server);
            location[index++] = addr;
            ip = port = version = 0;
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "check get value failed:ret[%d]", ret);
        }

        if (++iter == scanner.end())
          break;
        ret = iter.get_cell(&cell, &row_change);
      }
      else
      {
        //impossible
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    size = index;
    TBSYS_LOG(INFO,"get %d tablets from rootserver",size);
  }
  return ret;
}
