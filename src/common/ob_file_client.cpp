#include "ob_file_client.h"
#include "ob_crc64.h"
#include "ob_trace_log.h"

using namespace oceanbase;
using namespace oceanbase::common;

ObFileClient::ObFileClient():
  inited_(false), client_(NULL),
  rpc_buffer_(NULL), block_size_(ObFileService::BLOCK_SIZE),band_limit_(0)
{
}

ObFileClient::~ObFileClient()
{
}

int ObFileClient::initialize(ThreadSpecificBuffer *rpc_buffer,
    ObClientManager *client, const int64_t band_limit)
{
  int ret = OB_SUCCESS;

  if(inited_)
  {
    TBSYS_LOG(WARN, "init twice");
    ret = OB_INIT_TWICE;
  }

  if(OB_SUCCESS == ret && NULL == rpc_buffer)
  {
    TBSYS_LOG(WARN, "rpc_buffer is null");
    ret = OB_INVALID_ARGUMENT;
  }

  if(OB_SUCCESS == ret && NULL == client)
  {
    TBSYS_LOG(WARN, "client is null");
    ret = OB_INVALID_ARGUMENT;
  }

  if(OB_SUCCESS == ret && band_limit < MIN_BAND_LIMIT)
  {
    TBSYS_LOG(WARN, "Ob file service band limit[%ld] should not less than %ld KB/s",
        band_limit, MIN_BAND_LIMIT);
    ret = OB_INVALID_ARGUMENT;
  }

  if(OB_SUCCESS == ret)
  {
    client_ = client;
    rpc_buffer_ = rpc_buffer;
    band_limit_ = band_limit;
    inited_ = true;
  }

  return ret;

}

int ObFileClient::get_rpc_buffer(ObDataBuffer& data_buffer) const
{
  int ret = OB_SUCCESS;

  common::ThreadSpecificBuffer::Buffer* rpc_buffer = rpc_buffer_->get_buffer();
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

  return ret;
}

int ObFileClient::send_file_pre(const int64_t timeout,
    const ObServer& dest_server, const int64_t file_size,
    const ObString& dest_dir, const ObString& dest_file_name,
    ObDataBuffer& in_buffer, ObDataBuffer& out_buffer,int64_t& session_id)
{
  int ret = OB_SUCCESS;

  // send file request to dest server
  in_buffer.get_position() = 0;
  out_buffer.get_position() = 0;
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_i64(in_buffer.get_data(),
                in_buffer.get_capacity(), in_buffer.get_position(), file_size);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "Serialize file info failed:ret=[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = dest_dir.serialize(in_buffer.get_data(), in_buffer.get_capacity(),
        in_buffer.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "Serialize dest_dir failed:ret=[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = dest_file_name.serialize(in_buffer.get_data(),
        in_buffer.get_capacity(), in_buffer.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "Serialize dest_file_name:ret=[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = client_->send_request(dest_server, OB_SEND_FILE_REQUEST,
        DEFAULT_VERSION, timeout, in_buffer, out_buffer, session_id);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "Send the send_file_request failed:ret=[%d]", ret);
    }
  }

  ObResultCode rc;
  int64_t pos = 0;

  if (OB_SUCCESS == ret)
  {
    ret = rc.deserialize(out_buffer.get_data(), out_buffer.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "Deserialize result code failed:ret = [%d]", ret);
    }
    else if (OB_SUCCESS != rc.result_code_)
    {
      ret = rc.result_code_;
      TBSYS_LOG(WARN, "failed to prepare for send file:ret =[%d].",
          rc.result_code_);// OB_EAGAIN means currency count limit reached.
    }
  }

  return ret;
}

int ObFileClient::send_file_block(const int64_t timeout,
    const ObServer& dest_server, ObFileReader& file_reader, char *buf,
    const int64_t offset, int64_t& read_size,
    ObDataBuffer& in_buffer, ObDataBuffer& out_buffer,const int64_t session_id)
{
  int err = OB_SUCCESS;

  //FILL_TRACE_LOG("Start send block");
  // read file data to buf
  err = file_reader.pread(buf, block_size_, offset, read_size);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "Read file block failed");
  }

  //FILL_TRACE_LOG("End file_reader.pread");

  ObResultCode rc;
  int ret = OB_SUCCESS;

  rc.result_code_ = err;
  in_buffer.get_position() = 0;
  out_buffer.get_position() = 0;

  if (OB_SUCCESS == ret)
  {
    ret = rc.serialize(in_buffer.get_data(), in_buffer.get_capacity(),
        in_buffer.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "Encode rc failed:rc.result_code_[%d], ret[%d]",
          rc.result_code_, ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_i64(in_buffer.get_data(),
        in_buffer.get_capacity(), in_buffer.get_position(), offset);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "Encode offset failed:offset[%ld], ret[%d]", offset, ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vstr(in_buffer.get_data(),
        in_buffer.get_capacity(), in_buffer.get_position(), buf, read_size);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "Encode block buf failed:ret[%d], "
          "offset[%ld] read_size[%ld]", ret, offset, read_size);
    }
  }

  // send block request and get response
  if (OB_SUCCESS == ret)
  {
    ret = client_->get_next(dest_server, session_id, timeout,
        in_buffer, out_buffer);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "Send the send_file_request failed:ret=[%d] timeout[%ld]",
          ret, timeout);
    }
  }

  // decode response
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ret = rc.deserialize(out_buffer.get_data(), out_buffer.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "Deserialize result code failed:ret = [%d]", ret);
    }
    else if (OB_SUCCESS != rc.result_code_)
    {
      ret = rc.result_code_;
      TBSYS_LOG(WARN, "Send file block response failed:result_code=[%d]",
          rc.result_code_);
    }
  }
  //FILL_TRACE_LOG("End decode");
  if (OB_SUCCESS != err)
  {
    ret = err;
  }

  return ret;
}

int ObFileClient::send_file_end(const int64_t timeout,
    const ObServer& dest_server, ObDataBuffer& in_buffer,
    ObDataBuffer& out_buffer,const int64_t session_id)
{
  int ret = OB_SUCCESS;
  ObResultCode rc;
  rc.result_code_ = OB_ITER_END;
  in_buffer.get_position() = 0;
  out_buffer.get_position() = 0;

  ret = rc.serialize(in_buffer.get_data(), in_buffer.get_capacity(),
      in_buffer.get_position());
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "Encode rc failed:rc.result_code_[%d], ret[%d]",
        rc.result_code_, ret);
  }

  if (OB_SUCCESS == ret)
  {
    ret = client_->get_next(dest_server, session_id, timeout, in_buffer,
        out_buffer);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "Send the send_file_end  failed:ret=[%d]", ret);
    }
  }
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ret = rc.deserialize(out_buffer.get_data(), out_buffer.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "Deserialize result code failed:ret=[%d]", ret);
    }
    else if (OB_SUCCESS != rc.result_code_)
    {
      ret = rc.result_code_;
      TBSYS_LOG(WARN, "Get response of sned-file_end failed:result code=[%d]",
          rc.result_code_);
    }
  }
  return ret;
}

int ObFileClient::send_file_loop(const int64_t timeout,
    const int64_t band_limit_in, const ObServer& dest_server,
    const ObString& local_path, const ObString& dest_dir,
    const ObString& dest_file_name)
{
  int64_t t1 = tbsys::CTimeUtil::getTime();

  ObFileReader file_reader;
  ObDataBuffer in_buffer;
  ObDataBuffer out_buffer;
  int64_t file_size = -1;
  char *block_buffer = NULL;
  int64_t session_id = 0;
  int ret = OB_SUCCESS;
  int64_t band_limit = band_limit_in;

  if (band_limit > band_limit_)
  {
    band_limit = band_limit_;
    TBSYS_LOG(WARN, "the band limit param of send file client[%ld] is"
        " larger than ob_file_service_band_limit[%ld]",
        band_limit, band_limit_);
  }

  if (band_limit <= 0)
  {
    TBSYS_LOG(ERROR, "the band limit param should not be [%ld]",
        band_limit);
    ret = OB_INVALID_ARGUMENT;
  }

  //FILL_TRACE_LOG("Start send file loop");
  // init buffer
  if (OB_SUCCESS == ret)
  {
    ret = get_rpc_buffer(in_buffer);
  }
  if (OB_SUCCESS == ret)
  {
    ret = get_rpc_buffer(out_buffer);
  }

  if (OB_SUCCESS == ret)
  {
    block_buffer = reinterpret_cast<char *>(ob_malloc(block_size_, ObModIds::OB_FILE_CLIENT));
    if (NULL == block_buffer)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "Allocate memory for block buffer failed:ret[%d]", ret);
    }
  }

  // get file size
  if (OB_SUCCESS == ret)
  {
    struct stat file_stat;
    if (stat(local_path.ptr(), &file_stat) != 0)
    {
      TBSYS_LOG(WARN, "get file size of %.*s failed: %s",
          local_path.length(), local_path.ptr(), strerror(errno));
      ret = OB_IO_ERROR;
    } else {
      file_size = file_stat.st_size;
    }
  }

  // open local file
  if (OB_SUCCESS == ret)
  {
    bool is_dio = true;
    ret = file_reader.open(local_path, is_dio);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "Open file for send_file failed. path=[%.*s] ret=[%d]",
          local_path.length(), local_path.ptr(), ret);
    }
  }

  //FILL_TRACE_LOG("Start send_file_pre");

  // open local file and send send_file_request
  if (OB_SUCCESS == ret)
  {
    ret = send_file_pre(timeout, dest_server, file_size, dest_dir, dest_file_name,
        in_buffer, out_buffer, session_id);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "Do send_file_pre failed: ret[%d]", ret);
    }
  }

  //FILL_TRACE_LOG("End send_file_pre");
  // do send file loop
  int64_t offset = 0;
  int64_t read_size = -1;
  int64_t start_time_us = 0;
  int64_t sleep_time_us = 0;
  int64_t cost_time_us = 0;
  int64_t total_sleep_time_us = 0;
  int64_t total_cost_time_us = 0;

  while(OB_SUCCESS == ret)
  {
    //FILL_TRACE_LOG("Send_file_block=%ld", offset);
    start_time_us = tbsys::CTimeUtil::getTime();
    ret = send_file_block(timeout, dest_server, file_reader, block_buffer,
        offset, read_size, in_buffer, out_buffer, session_id);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "failed to send file block to server[%s] offset[%ld] "
          "read_size[%ld] session_id[%ld]",
          dest_server.to_cstring(), offset, read_size, session_id);
    }
    else
    {
      offset += read_size;
      if (read_size > 0)
      {
        cost_time_us = tbsys::CTimeUtil::getTime() - start_time_us;
        sleep_time_us = 1000000L*read_size/1024/band_limit - cost_time_us;
        total_cost_time_us += cost_time_us;

        if (sleep_time_us > 0)
        {
          total_sleep_time_us += sleep_time_us;
          usleep(static_cast<useconds_t>(sleep_time_us));
        }
      }

      if (offset == file_size)
      {
        // send the finish notify to dest server
        //FILL_TRACE_LOG("Start send_file_end");
        ret = send_file_end(timeout, dest_server, in_buffer, out_buffer, session_id);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "failed to send_file_end:ret=[%d]", ret);
        }
        break; // end of send file
      }
      else if(offset > file_size)
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "Send file offset error [%ld] "
            "should not larger than [%ld].[%.*s]",
            offset, file_size, local_path.length(), local_path.ptr());
      }
    }
  }

  if (NULL != block_buffer)
  {
    ob_free(block_buffer);
  }

  if (file_reader.is_opened())
  {
    file_reader.close();
  }

  int64_t duration = tbsys::CTimeUtil::getTime() - t1;
  if (OB_SUCCESS == ret && duration == 0)
  {
    TBSYS_LOG(ERROR, "duration time of send file should not be zero:"
       " total_cost_time_us[%ld], total_sleep_time_us[%ld]",
       total_cost_time_us, total_sleep_time_us);
    ret = OB_ERR_UNEXPECTED;
  }

  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "Send file local_path[%.*s] to dest_server[%s] dest_dir[%.*s] "
        "dest_file_name[%.*s] filesize[%ld] time[%ld]us work_time[%ld] sleep_time[%ld] speed[%ld]KB/s",
        local_path.length(), local_path.ptr(), dest_server.to_cstring(),
        dest_dir.length(), dest_dir.ptr(),
        dest_file_name.length(), dest_file_name.ptr(),
        file_size, duration, total_cost_time_us, total_sleep_time_us,
        file_size*1000000L/1024/duration);
  }
  else
  {
    TBSYS_LOG(ERROR, "Send file local_path[%.*s] to dest_server[%s] dest_dir[%.*s] "
        "dest_file_name[%.*s] failed, filesize[%ld] cost time[%ld]us work_time[%ld] sleep_time[%ld]: ret[%d]",
        local_path.length(), local_path.ptr(), dest_server.to_cstring(),
        dest_dir.length(), dest_dir.ptr(),
        dest_file_name.length(), dest_file_name.ptr(),
        file_size, duration, total_cost_time_us, total_sleep_time_us, ret);
  }

  return ret;
}

int ObFileClient::send_file(const int64_t timeout, const int64_t band_limit,
    const ObServer& dest_server, const ObString& local_path,
    const ObString& dest_dir, const ObString& dest_file_name)
{
  int ret = OB_SUCCESS;

  //FILL_TRACE_LOG("send_file");
  if(!inited_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "ob file client is not inited yet.");
  }

  if (OB_SUCCESS == ret)
  {
    ret = send_file_loop(timeout, band_limit, dest_server, local_path,
        dest_dir, dest_file_name);
  }
  //PRINT_TRACE_LOG();

  return ret;
}
