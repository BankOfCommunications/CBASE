
#include "client_rpc.h"
#include "common/ob_result.h"
#include "base_client.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;

ObClientServerStub::ObClientServerStub()
  : frame_buffer_(FRAME_BUFFER_SIZE)
{
  init_ = false;
  rpc_frame_ = NULL;
}

ObClientServerStub::~ObClientServerStub()
{

}


int ObClientServerStub::initialize(const ObServer & root_server, 
    const ObClientManager * rpc_frame)
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

    ret = get_update_server(update_server_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "can not get update server");
      return ret;
    }

    ret = get_cs_and_ms();
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "can not get chunk/merge servers");
      return ret;
    }

  }
  return ret;
}

/*
int ObClientServerStub::initialize(const ObServer & root_server, 
    const ObClientManager * rpc_frame,
    const ObServer & update_server,
    const ObChunkServerManager & obcsm)
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

    update_server_ = update_server;
    obcs_manager_ = obcsm;
  }
  return ret;
}
*/


int ObClientServerStub::get_frame_buffer(ObDataBuffer & data_buffer) const
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "check inner stat failed.");
    ret = OB_ERROR;
  }

  ThreadSpecificBuffer::Buffer* rpc_buffer = frame_buffer_.get_buffer();
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


int ObClientServerStub::cs_scan(const ObScanParam& scan_param, ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = 1000000;  // send_request timeout millionseconds
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  // step 1. serialize timestamp to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = scan_param.serialize(data_buff.get_data(), 
        data_buff.get_capacity(), data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize scan_param failed:ret[%d].", ret);
    }
  }

  // step 2. send request for fetch new schema
  if (OB_SUCCESS == ret)
  { 
    uint32_t pos = random() % merge_server_list_.size();
    ObServer &cur_server = merge_server_list_.at(pos);

    ret = rpc_frame_->send_request(cur_server, 
        OB_SCAN_REQUEST, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send request to remote server for scan failed"
          " ret[%d].",  ret);
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
    ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize scanner from buff failed"
          "pos[%ld], ret[%d].", pos, ret);
    }
  }

  return ret;
}


int ObClientServerStub::cs_get(const ObGetParam& get_param, ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = 1000000;  // send_request timeout millionseconds
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  // step 1. serialize timestamp to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = get_param.serialize(data_buff.get_data(), 
        data_buff.get_capacity(), data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize get_param failed:ret[%d].", ret);
    }
  }

  // step 2. send request for fetch new schema
  if (OB_SUCCESS == ret)
  {
    uint32_t pos = random() % merge_server_list_.size();
    ObServer &cur_server = merge_server_list_.at(pos);

    ret = rpc_frame_->send_request(cur_server, 
        OB_GET_REQUEST, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send request to remote server for get failed"
          " ret[%d].",  ret);
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
    ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize scanner from buff failed"
          "pos[%ld], ret[%d].", pos, ret);
    }
  }

  return ret;
}

int ObClientServerStub::ups_apply(const ObMutator &mutator)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = 1000000;  // send_request timeout millionseconds
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  // step 1. serialize mutator to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = mutator.serialize(data_buff.get_data(), 
                            data_buff.get_capacity(), 
                            data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "serialize get_param failed, ret=%d.", ret);
    }
  }

  // step 2. send request for apply
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(update_server_, 
                                   OB_WRITE, DEFAULT_VERSION, 
                                   timeout, data_buff);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "send request to update server for apply failed, "
                      "timeout=%ld, ret=%d.", timeout, ret);
    }
  }

  // step 3. deserialize the response code
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode result_code;
    ret = result_code.deserialize(data_buff.get_data(), 
                                  data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "deserialize result_code failed, pos=%ld, ret=%d.", 
                pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }

  return ret;
}

int ObClientServerStub::get_cs_tablet_image(const oceanbase::common::ObServer & remote_server,
                                               const int32_t disk_no, 
                                               oceanbase::common::ObString &image_buf)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = 1000000;  // send_request timeout millionseconds
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  // step 1. serialize timestamp to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi32(data_buff.get_data(), 
        data_buff.get_capacity(), data_buff.get_position(), disk_no);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize disk_no failed:ret[%d].", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(data_buff.get_data(), 
        data_buff.get_capacity(), data_buff.get_position(), DEFAULT_VERSION);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize data_version failed:ret[%d].", ret);
    }
  }

  // step 2. send request for fetch new schema
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(remote_server, 
        OB_CS_DUMP_TABLET_IMAGE, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send request to remote server for cs_dump_tablet_image failed"
          " ret[%d].",  ret);
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
    ret = image_buf.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize image_buf from buff failed"
          "pos[%ld], ret[%d].", pos, ret);
    }
  }

  return ret;
}

int ObClientServerStub::rs_dump_cs_info(ObChunkServerManager &obcsm)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = 1000000;  // send_request timeout millionseconds
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  // step 1. serialize timestamp to data_buff

  // step 2. send request for fetch new schema
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server_, 
        OB_DUMP_CS_INFO, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send request to remote server for cs_dump_tablet_image failed"
          " ret[%d].",  ret);
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
    ret = obcsm.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize ObChunkServerManager from buff failed"
          "pos[%ld], ret[%d].", pos, ret);
    }
  }

  return ret;
}

int ObClientServerStub::get_cs_and_ms()
{
  int                   ret = OB_SUCCESS;
  ObChunkServerManager  obcsm;
  ObChunkServerManager::const_iterator it;
  ObServer              server;
  
  ret = rs_dump_cs_info(obcsm);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr, "Get chunk server manager error!\n");
    return ret;
  }

  for (it = obcsm.begin(); it != obcsm.end(); ++it)
  {
    ObServer node;
    node = it->server_;
    node.set_port(it->port_cs_);
    chunk_server_list_.push_back(node);
    node.set_port(it->port_ms_);
    merge_server_list_.push_back(node);
  }

  return ret;
}

int ObClientServerStub::fetch_stats(const oceanbase::common::ObServer & remote_server,
    oceanbase::common::ObStatManager &obsm)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = 1000000;  // send_request timeout millionseconds
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  // step 1. serialize timestamp to data_buff

  // step 2. send request for fetch new schema
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(remote_server, 
        OB_FETCH_STATS, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send request to remote server for cs_dump_tablet_image failed"
          " ret[%d].",  ret);
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
    ret = obsm.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize ObChunkServerManager from buff failed"
          "pos[%ld], ret[%d].", pos, ret);
    }
  }

  return ret;
}

int ObClientServerStub::get_update_server(ObServer &update_server)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = 1000000;
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);
  // step 1. send get update server info request
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server_, OB_GET_UPDATE_SERVER_INFO, DEFAULT_VERSION,
        timeout, data_buff);
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

int ObClientServerStub::fetch_schema(ObSchemaManagerV2 &schema)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = 1000000;
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  // step 1. serialize timestamp to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(data_buff.get_data(), 
        data_buff.get_capacity(), data_buff.get_position(), DEFAULT_VERSION);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize timestamp failed:version[%ld], ret[%d]", 
          DEFAULT_VERSION, ret);
    }
  }
    
  // step 2. send get update server info request
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server_, OB_FETCH_SCHEMA, DEFAULT_VERSION,
        timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send request to root server for getting schema");
    }
  }

  // step 3. deserialize restult code
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

  // step 4. deserialize update server addr
  if (OB_SUCCESS == ret)
  {
    ret = schema.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize server failed:pos[%ld], ret[%d]", pos, ret);
    }
  }
  return ret;
}


int ObClientServerStub::start_merge(const int64_t frozen_memtable_version, const int32_t init_flag)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = 1000000;  // send_request timeout millionseconds
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  // step 1. serialize frozen_memtable_version to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(data_buff.get_data(), 
        data_buff.get_capacity(), data_buff.get_position(), frozen_memtable_version);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize frozen_memtable_version failed:ret[%d].", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi32(data_buff.get_data(), 
        data_buff.get_capacity(), data_buff.get_position(), init_flag);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize init_flag failed:ret[%d].", ret);
    }
  }

  // step 2. send request for fetch new schema
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server_, 
        OB_START_MERGE, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send request to remote server for start_merge failed"
          " ret[%d].",  ret);
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

int ObClientServerStub::drop_tablets(const int64_t frozen_memtable_version)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = 1000000;  // send_request timeout millionseconds
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  // step 1. serialize frozen_memtable_version to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(data_buff.get_data(), 
        data_buff.get_capacity(), data_buff.get_position(), frozen_memtable_version);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize frozen_memtable_version failed:ret[%d].", ret);
    }
  }

  // step 2. send request for fetch new schema
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server_, 
        OB_START_MERGE, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send request to remote server for start_merge failed"
          " ret[%d].",  ret);
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

int ObClientServerStub::start_gc(const int32_t reserve)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = 1000000;  // send_request timeout millionseconds
  ObDataBuffer data_buff;
  ret = get_frame_buffer(data_buff);

  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi32(data_buff.get_data(), 
        data_buff.get_capacity(), data_buff.get_position(), reserve);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR,"encode failed ret[%d]",ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server_, 
        OB_CS_START_GC, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send request to remote server for cs_dump_tablet_image failed"
          " ret[%d].",  ret);
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
    TBSYS_LOG(INFO,"send start gc success");
  }
  
  return ret;
}

