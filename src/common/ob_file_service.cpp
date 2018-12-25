#include "ob_file_service.h"

using namespace oceanbase;
using namespace oceanbase::common;


ObFileService::ObFileService():server_(NULL),inited_(false),queue_thread_(NULL),
  max_concurrency_count_(0), concurrency_count_(0),network_timeout_(0),block_size_(BLOCK_SIZE)
{
}

ObFileService::~ObFileService()
{
}

int ObFileService::initialize(ObBaseServer* server,
    ObPacketQueueThread* queue_thread, const int64_t network_timeout,
    const uint32_t max_concurrency_count)
{
  int ret = OB_SUCCESS;

  if(inited_)
  {
    ret = OB_INIT_TWICE;
    TBSYS_LOG(WARN, "ob_file_service is inited twice");
  }

  if(OB_SUCCESS == ret && NULL == server)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "server is null");
  }

  if(OB_SUCCESS == ret && NULL == queue_thread)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "queue_thread is null");
  }

  if (OB_SUCCESS == ret && network_timeout < MIN_NETWORK_TIMEOUT)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "Network_timeout should not less than %ld",
        MIN_NETWORK_TIMEOUT);
  }

  if (OB_SUCCESS == ret && max_concurrency_count <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "max_concurrency_count for file_service should not less than 0");
  }

  if(OB_SUCCESS == ret)
  {
    server_ = server;
    inited_ = true;
    queue_thread_ = queue_thread;

    max_concurrency_count_ = max_concurrency_count;
    concurrency_count_ = 0;
    network_timeout_ = network_timeout;
  }

  return ret;
}

int ObFileService::handle_send_file_request(
  const int32_t version,
  const int32_t channel_id,
  easy_request_t* request,
  common::ObDataBuffer& in_buffer,
  common::ObDataBuffer& out_buffer)
{
  int ret = OB_SUCCESS;
  bool inc_concurrency_count_flag = false;
  int32_t response_cid = channel_id;
  int64_t session_id = queue_thread_->generate_session_id();
  const char* src_ip;

  //FILL_TRACE_LOG("handle_send_file_request");
  if (!inited_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "ob file client is not inited yet.");
  }

  if (OB_SUCCESS == ret && NULL == request)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "Connection param in receive_file_loop should not be null");
  }

  if (request != NULL && request->ms != NULL && request->ms->c != NULL)
  {
     src_ip  = get_peer_ip(request);
  }
  else
  {
    src_ip = "";
    TBSYS_LOG(WARN, "can't get src ip for send_file_request");
  }

  if (DEFAULT_VERSION != version)
  {
    ret = OB_ERROR_FUNC_VERSION;
    TBSYS_LOG(ERROR, "Can'd handle send_file_request in different version:"
        "Server version:%d, Packet version:%d", DEFAULT_VERSION, version);
    // send back error message
    ObResultCode rc;
    rc.result_code_ = OB_ERROR_FUNC_VERSION;
    int err = OB_SUCCESS;
    out_buffer.get_position() = 0;
    err = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
        out_buffer.get_position());
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "Encode result code failed:ret=[%d]", err);
    }
    else
    {
      err = server_->send_response(OB_SEND_FILE_REQUEST_RESPONSE,
          DEFAULT_VERSION, out_buffer, request, response_cid, session_id);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "Send error message for OB_SEND_FILE_REQUEST failed:"
            "ret[%d]", err);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = inc_concurrency_count();
    if(OB_SUCCESS == ret)
    {
      inc_concurrency_count_flag = true;
    }
    else
    {
      // send back error message
      ObResultCode rc;
      int err = OB_SUCCESS;
      rc.result_code_ = ret;
      out_buffer.get_position() = 0;
      err = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
          out_buffer.get_position());
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "Encode result code failed:ret=[%d]", err);
      }
      else
      {
        err = server_->send_response(OB_SEND_FILE_REQUEST_RESPONSE,
            DEFAULT_VERSION, out_buffer, request, response_cid, session_id);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "Send error message for OB_SEND_FILE_REQUEST failed:"
              "ret[%d]", err);
        }
      }
    }
  }

  // handle...
  ObFileAppender file_appender;
  int64_t file_size;
  ObString file_path;
  ObString tmp_file_path;
  char file_path_buf[OB_MAX_FILE_NAME_LENGTH];
  char tmp_file_path_buf[OB_MAX_FILE_NAME_LENGTH];
  int64_t t1 = tbsys::CTimeUtil::getTime();

  file_path.assign_buffer(file_path_buf, sizeof(file_path_buf));
  tmp_file_path.assign_buffer(tmp_file_path_buf, sizeof(tmp_file_path_buf));

  if (OB_SUCCESS == ret)
  {
    ret = receive_file_pre(file_appender, file_size, file_path, tmp_file_path,
        request, in_buffer, out_buffer, response_cid, session_id);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "Prepare for receive_file failed: ret=[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = receive_file_loop(file_path, tmp_file_path, file_size,
        file_appender, request, out_buffer,
        response_cid, session_id);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "Reveive_file_loop failed:ret=[%d]", ret);
    }
  }

  int64_t duration = tbsys::CTimeUtil::getTime() - t1;
  if (OB_SUCCESS == ret && duration == 0)
  {
    TBSYS_LOG(ERROR, "duration of recieve time should not be zero");
    ret = OB_ERR_UNEXPECTED;
  }

  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "Recieve file from server[%s] to file_path[%.*s] "
        "tmp_file_path[%.*s] filesize[%ld] time[%ld]us speed[%ld]KB/s",
        src_ip, file_path.length(), file_path.ptr(),
        tmp_file_path.length(), tmp_file_path.ptr(),
        file_size, duration, file_size*1000000/1024/duration);
  }
  else
  {
    TBSYS_LOG(ERROR, "Recieve file from server[%s] to file_path[%.*s] "
        "tmp_file_path[%.*s] failed, filesize[%ld] cost time[%ld]:ret[%d]",
        src_ip, file_path.length(), file_path.ptr(),
        tmp_file_path.length(), tmp_file_path.ptr(),
        file_size, duration, ret);
  }

  if (inc_concurrency_count_flag)
  {
    dec_concurrency_count();
  }

  int tmp_ret = queue_thread_->destroy_session(session_id);
  if (OB_SUCCESS == ret && OB_SUCCESS != tmp_ret)
  {
    ret = tmp_ret;
  }

  //PRINT_TRACE_LOG();
  return ret;
}

int ObFileService::inc_concurrency_count()
{
  int ret = OB_SUCCESS;
  int is_inc_concurrency_count = 0;
  uint32_t old_concurrency_count = concurrency_count_;

  while (old_concurrency_count < max_concurrency_count_)
  {
    uint32_t tmp = atomic_compare_exchange(&concurrency_count_, old_concurrency_count+1, old_concurrency_count);
    if (tmp == old_concurrency_count)
    {
      is_inc_concurrency_count = 1;
      break;
    }
    old_concurrency_count = concurrency_count_;
  }

  if (0 == is_inc_concurrency_count)
  {
    TBSYS_LOG(WARN, "Concurrency file trans count[%d] reach the threshold:[%d]. "
        "New send file request is refused.", old_concurrency_count, max_concurrency_count_);
    ret = OB_EAGAIN;
  }
  //TBSYS_LOG(INFO, "inc Concurrency file trans count[%d].", concurrency_count_);
  return ret;
}

void ObFileService::dec_concurrency_count()
{
  if (concurrency_count_ <= 0)
  {
    TBSYS_LOG(ERROR, "can't decrease concurrency count[%u]", concurrency_count_);
  }
  else
  {
    atomic_dec(&concurrency_count_);
  }
  //TBSYS_LOG(INFO, "dec Concurrency file trans count[%d].", concurrency_count_);
}

int ObFileService::receive_file_pre(ObFileAppender& file_appender,
    int64_t& file_size, ObString& file_path, ObString& tmp_file_path,
    easy_request_t* request, ObDataBuffer& in_buffer,
    ObDataBuffer& out_buffer, int32_t& response_cid, const int64_t session_id)
{
  int err = OB_SUCCESS;

  if (OB_SUCCESS == err)
  {
    err = serialization::decode_i64(in_buffer.get_data(),
                in_buffer.get_capacity(), in_buffer.get_position(), &file_size);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "Decode file_info failed: err=[%d]", err);
    }
  }

  char dest_dir_buf[OB_MAX_FILE_NAME_LENGTH];
  ObString dest_dir;
  dest_dir.assign_buffer(dest_dir_buf, sizeof(dest_dir_buf));
  if (OB_SUCCESS == err)
  {
    err = dest_dir.deserialize(in_buffer.get_data(), in_buffer.get_capacity(),
        in_buffer.get_position());
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "Decode dest_dir failed:err=[%d]", err);
    }
  }

  char dest_file_name_buf[OB_MAX_FILE_NAME_LENGTH];
  ObString dest_file_name;
  dest_file_name.assign_buffer(dest_file_name_buf, sizeof(dest_file_name_buf));
  if (OB_SUCCESS == err)
  {
    err = dest_file_name.deserialize(in_buffer.get_data(),
        in_buffer.get_capacity(), in_buffer.get_position());
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "Decode dest_file_name failed:err=[%d]", err);
    }
  }

  // check dir
  if (OB_SUCCESS == err)
  {
    err = check_dir(dest_dir, file_size);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "Check dir failed:err=[%d]", err);
    }
  }

  // generate tmp_file_path and file_path
  char file_path_buf[OB_MAX_FILE_NAME_LENGTH];
  char tmp_file_path_buf[OB_MAX_FILE_NAME_LENGTH];
  if (OB_SUCCESS == err)
  {
    const char tmp_file_prefix[]= "tmp_";
    int64_t count = 0;

    count = snprintf(file_path_buf, OB_MAX_FILE_NAME_LENGTH, "%.*s/%.*s",
        dest_dir.length(), dest_dir.ptr(), dest_file_name.length(), dest_file_name.ptr());

    if (count<0 || count >= OB_MAX_FILE_NAME_LENGTH)
    {
      err = OB_SIZE_OVERFLOW;
      TBSYS_LOG(WARN, "snprintf file name failed, return[%ld] [%.*s]/[%.*s]",
          count, dest_dir.length(), dest_dir.ptr(), dest_file_name.length(), dest_file_name.ptr());
    }
    count = file_path.write(file_path_buf,
        static_cast<ObString::obstr_size_t>(strlen(file_path_buf)));
    if (count != static_cast<int>(strlen(file_path_buf)))
    {
      err = OB_SIZE_OVERFLOW;
      TBSYS_LOG(WARN, "Write file_path_buf to ObString failed");
    }

    count = snprintf(tmp_file_path_buf, OB_MAX_FILE_NAME_LENGTH, "%.*s/%s%.*s",
        dest_dir.length(), dest_dir.ptr(), tmp_file_prefix, dest_file_name.length(), dest_file_name.ptr());
    if (count <0 || count >= OB_MAX_FILE_NAME_LENGTH)
    {
      err = OB_SIZE_OVERFLOW;
      TBSYS_LOG(WARN, "snprintf tmp file name failed, return[%ld] [%.*s]/[%s][%.*s]",
          count, dest_dir.length(), dest_dir.ptr(), tmp_file_prefix, dest_file_name.length(), dest_file_name.ptr());
    }

    count = tmp_file_path.write(tmp_file_path_buf,
        static_cast<ObString::obstr_size_t>(strlen(tmp_file_path_buf)));
    if (count != static_cast<int>(strlen(tmp_file_path_buf)))
    {
      err = OB_SIZE_OVERFLOW;
      TBSYS_LOG(WARN, "Write tmp_file_path_buf to ObString failed");
    }
  }

  // check if the tmp_file and the dest file exist already
  if (OB_SUCCESS == err)
  {
    if (0 == access(tmp_file_path_buf, F_OK))
    {
      TBSYS_LOG(WARN, "Tmp file [%s] already exists", tmp_file_path_buf);
      err = OB_FILE_ALREADY_EXIST;
    }
    if (0 == access(file_path_buf, F_OK))
    {
      TBSYS_LOG(WARN, "File [%s] already exists", file_path_buf);
      err = OB_FILE_ALREADY_EXIST;
    }
  }
  // log operation
  if (OB_SUCCESS == err)
  {
    TBSYS_LOG(INFO, "start receive file: dir[%.*s] file_name[%.*s] "
        "tmp_file_path[%.*s] from server [%s]",
        dest_dir.length(), dest_dir.ptr(),
        dest_file_name.length(), dest_file_name.ptr(),
        tmp_file_path.length(), tmp_file_path.ptr(),
              get_peer_ip(request));
  }

  // open tmp file
  if (OB_SUCCESS == err)
  {
    bool is_dio = true;
    bool is_create = true;
    bool is_trunc = true;
    err = file_appender.open(tmp_file_path, is_dio, is_create, is_trunc);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "open file_appender failed:tmp_file[%.*s] err[%d]",
          tmp_file_path.length(), tmp_file_path.ptr(), err);
    }
  }

  // send response
  ObResultCode rc;
  int ret = OB_SUCCESS;
  rc.result_code_ = err;
  out_buffer.get_position() = 0;

  if (OB_SUCCESS == ret)
  {
    ret = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
        out_buffer.get_position());
    if(OB_SUCCESS != ret)
    {
     TBSYS_LOG(WARN, "Serialize result code failed:ret=[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = queue_thread_->prepare_for_next_request(session_id);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "prepare for next request fail:ret=[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = server_->send_response(OB_SEND_FILE_REQUEST_RESPONSE, DEFAULT_VERSION,
        out_buffer, request, response_cid, session_id);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "Send send_file_request_response failed:ret=[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret && OB_SUCCESS != err)
  {
    ret = err;
  }
  return ret;
}

int ObFileService::receive_file_block(ObFileAppender& file_appender,
    char* block_buf, easy_request_t* request, ObDataBuffer& in_buffer,
    ObDataBuffer& out_buffer, int32_t & response_cid, const int64_t session_id)
{
  int err = OB_SUCCESS;
  int64_t offset;
  int64_t read_size = -1;
  //FILL_TRACE_LOG("Start receive_file_block");
  if (OB_SUCCESS == err)
  {
    err = serialization::decode_i64(in_buffer.get_data(),
        in_buffer.get_capacity(), in_buffer.get_position(), &offset);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "Decode offset failed: err=[%d]", err);
    }
  }
  if (OB_SUCCESS == err)
  {
    if (NULL == serialization::decode_vstr(in_buffer.get_data(),
          in_buffer.get_capacity(), in_buffer.get_position(),
          block_buf, block_size_, &read_size))
    {
      err = OB_DESERIALIZE_ERROR;
      TBSYS_LOG(WARN, "Decode block failed");
    }
  }
  //FILL_TRACE_LOG("decode end");
  // write file block
  if (OB_SUCCESS == err)
  {
    const bool is_fsync = false;
    err = file_appender.append(block_buf, read_size, is_fsync);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "Appender file block failed:err=[%d]", err);
    }
  }
  //FILL_TRACE_LOG("append end");
  // send response
  ObResultCode rc;
  int ret = OB_SUCCESS;
  rc.result_code_ = err;
  if (OB_SUCCESS == ret)
  {
    ret = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
        out_buffer.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "Encode result code failed:ret=[%d]", ret);
    }
  }
  //FILL_TRACE_LOG("encode end");
  if (OB_SUCCESS == ret)
  {
    ret = queue_thread_->prepare_for_next_request(session_id);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "prepare for next request fail:ret[%d]", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = server_->send_response(OB_SEND_FILE_REQUEST_RESPONSE, DEFAULT_VERSION,
        out_buffer, request, response_cid, session_id);

    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "Send send_file_end_response failed:ret[%d]", ret);
    }
  }

  //FILL_TRACE_LOG("send response end");
  if (OB_SUCCESS == ret && OB_SUCCESS != err)
  {
    ret = err;
  }
  return ret;
}


int ObFileService::receive_file_end(ObString& file_path, ObString& tmp_file_path,
    const int64_t file_size, easy_request_t* request, ObDataBuffer& out_buffer,
    int32_t& response_cid, const int64_t session_id)
{
  int ret = OB_SUCCESS;

  struct stat file_stat;
  char tmp_path_buf[OB_MAX_FILE_NAME_LENGTH];
  char path_buf[OB_MAX_FILE_NAME_LENGTH];
  int n = snprintf(tmp_path_buf, OB_MAX_FILE_NAME_LENGTH, "%.*s",
      tmp_file_path.length(), tmp_file_path.ptr());
  if (n<0 || n>=OB_MAX_FILE_NAME_LENGTH)
  {
    TBSYS_LOG(ERROR, "failed to get tmp_file_path length[%d] [%.*s]",
        n, tmp_file_path.length(), tmp_file_path.ptr());
    ret = OB_SIZE_OVERFLOW;
  }
  else if (stat(tmp_path_buf, &file_stat) != 0)
  {
    TBSYS_LOG(ERROR, "stat tmp_file_path[%s] failed: %s",
        tmp_path_buf, strerror(errno));
    ret = OB_IO_ERROR;
  }

  n = snprintf(path_buf, OB_MAX_FILE_NAME_LENGTH, "%.*s",
      file_path.length(), file_path.ptr());
  if (OB_SUCCESS == ret && (n<0 || n>=OB_MAX_FILE_NAME_LENGTH))
  {
    TBSYS_LOG(ERROR, "failed to get path_buf length[%d] [%.*s]",
        n, file_path.length(), file_path.ptr());
    ret = OB_SIZE_OVERFLOW;
  }

  if (OB_SUCCESS == ret && file_stat.st_size != file_size)
  {
    TBSYS_LOG(ERROR, "The size of received tmp file size[%ld], "
        "remote file size[%ld]", file_stat.st_size, file_size);
    ret = OB_INVALID_DATA;
  }
  if (OB_SUCCESS == ret && 0 != rename(tmp_path_buf, path_buf))
  {
    TBSYS_LOG(ERROR, "Rename [%s] to path[%s] failed: errno[%d] %s",
        tmp_path_buf, path_buf, errno, strerror(errno));
    ret = OB_IO_ERROR;
  }

  int err = OB_SUCCESS;
  ObResultCode rc;
  rc.result_code_ = ret;
  err = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
      out_buffer.get_position());
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "Encode result code failed:ret=[%d]", err);
  }
  if (OB_SUCCESS == err)
  {
    err = server_->send_response(OB_SEND_FILE_REQUEST_RESPONSE, DEFAULT_VERSION,
        out_buffer, request, response_cid, session_id);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "Send send_file_end_response failed:ret[%d]", err);
    }
  }

  if (OB_SUCCESS == ret && OB_SUCCESS != err)
  {
    ret = err;
  }

  return ret;
}

int ObFileService::receive_file_loop(ObString& file_path, ObString& tmp_file_path,
    const int64_t file_size, ObFileAppender& file_appender,
    easy_request_t* request, ObDataBuffer& out_buffer,
    int32_t& response_cid, const int64_t session_id)
{
  int ret = OB_SUCCESS;
  ObResultCode rc;
  ObPacket *next_request = NULL;

  if (!file_appender.is_opened())
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "File appender is not opened yet in receive_file_loop");
  }

  //generate block_buf
  char *block_buf = NULL;
  if (OB_SUCCESS == ret)
  {
    block_buf = reinterpret_cast<char *>(ob_malloc(block_size_, ObModIds::OB_FILE_CLIENT));
    if(NULL == block_buf)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "Allocate memory for block buffer in receive_file_loop"
          " failed:err[%d]", ret);
    }
  }
  // do receive file loop
  ObDataBuffer *in_buffer = NULL;
  while(OB_SUCCESS == ret)
  {
    ret = queue_thread_->wait_for_next_request(session_id, next_request,
        network_timeout_);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "wait for next request fail:ret[%d] "
          "network_timeout_[%ld]", ret, network_timeout_);
    }

    if (OB_SUCCESS == ret)
    {
      response_cid = next_request->get_channel_id();
      request = next_request->get_request();
      ret = next_request->deserialize();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "Deserialize next request failed:ret[%d]", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      in_buffer = next_request->get_buffer();
      if (NULL == in_buffer)
      {
        ret = OB_INNER_STAT_ERROR;
        TBSYS_LOG(WARN, "Get buffer for next request failed");
      }
    }

    if (OB_SUCCESS == ret)
    {
      ret = rc.deserialize(in_buffer->get_data(), in_buffer->get_capacity(),
          in_buffer->get_position());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "Decode result code failed: ret=[%d]", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS == rc.result_code_) // receive file block
      {
        ret = receive_file_block(file_appender, block_buf, request,
            *in_buffer, out_buffer, response_cid, session_id);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "failed to receive_file_block");
        }
        //FILL_TRACE_LOG("receive_file_block ");
      }
      else if (OB_ITER_END == rc.result_code_) // the whole file is received
      {
        if (file_appender.is_opened())
        {
          file_appender.close();
        }
        //FILL_TRACE_LOG("receive_file_end");
        ret = receive_file_end(file_path, tmp_file_path, file_size,
            request, out_buffer, response_cid, session_id);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "failed to send receive_file_end");
        }
        break;// end the receive loop
      }
    }
  }
  if (NULL != block_buf)
  {
    ob_free(block_buf);
  }
  if (file_appender.is_opened())
  {
    file_appender.close();
  }
  return ret;
}

int ObFileService::check_dir(const ObString& local_dir, const int64_t file_size)
{
  int ret = OB_SUCCESS;
  char path_buf[OB_MAX_FILE_NAME_LENGTH];
  if (OB_SUCCESS == ret)
  {
    // check if the dir is allowed to write
    struct stat dir_stat;
    int n = snprintf(path_buf, OB_MAX_FILE_NAME_LENGTH, "%.*s",
        local_dir.length(), local_dir.ptr());
    if (n<0 || n>=OB_MAX_FILE_NAME_LENGTH)
    {
      ret = OB_SIZE_OVERFLOW;
      TBSYS_LOG(ERROR, "local_dir length is overflow, length[%d] str[%.*s]",
          local_dir.length(), local_dir.length(), local_dir.ptr());
    }
    else if (stat(path_buf, &dir_stat) != 0)
    {
      TBSYS_LOG(WARN, "get local directory[%s] stat fail: %s",
          path_buf, strerror(errno));
      ret = OB_IO_ERROR;
    }
    else if (!(S_IWUSR & dir_stat.st_mode))
    {
      TBSYS_LOG(WARN, "local dir[%s] cannot be written", path_buf);
      ret = OB_IO_ERROR;
    }
  }

  if (OB_SUCCESS == ret)
  {
    // check the free space
    struct statvfs dir_statvfs;
    if (statvfs(path_buf, &dir_statvfs) != 0)
    {
      TBSYS_LOG(WARN, "get disk free space of %s info fail: %s",
          path_buf, strerror(errno));
      ret = OB_IO_ERROR;
    }
    else
    {
      int64_t free_space =  dir_statvfs.f_bavail * dir_statvfs.f_bsize;
      if(file_size > free_space)
      {
        TBSYS_LOG(WARN, "free disk space of %s is not enough: file_size[%ld]"
             " free_space[%ld]", path_buf, file_size, free_space);
        ret = OB_CS_OUTOF_DISK_SPACE;
      }
    }
  }
  return ret;
}
