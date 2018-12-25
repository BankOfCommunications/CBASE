
#include "common/ob_define.h"
#include "common/ob_packet.h"
#include "common/ob_result.h"
#include "common/ob_read_common_data.h"
#include "ob_merge_server_main.h"
#include "ob_ms_sql_request.h"
#include "ob_ms_sql_rpc_event.h"
#include "ob_ms_async_rpc.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMsSqlRpcEvent::ObMsSqlRpcEvent()
  :magic_(0x1234abcd)
{
  client_request_id_ = OB_INVALID_ID;
  client_request_ = NULL;
  timeout_us_ = 0;
  channel_id_ = 0;
  timestamp_ = 0;
}

ObMsSqlRpcEvent::~ObMsSqlRpcEvent()
{
  //TBSYS_LOG(INFO, "destroy event[%p] client_request_id_[%ld] client_request_[%p]"
  //    "start_ts[%ld] end_ts[%ld] elapsed[%ld]",
  //    this, client_request_id_, client_request_, start_time_us_, end_time_us_,
  //    end_time_us_ - start_time_us_);
  reset();
}

void ObMsSqlRpcEvent::reset(void)
{
  // print debug info
  ObCommonSqlRpcEvent::reset();
  client_request_id_ = OB_INVALID_ID;
  client_request_ = NULL;
}

uint64_t ObMsSqlRpcEvent::get_client_id(void) const
{
  return client_request_id_;
}

const ObMsSqlRequest * ObMsSqlRpcEvent::get_client_request(void) const
{
  return client_request_;
}

int ObMsSqlRpcEvent::init(const uint64_t client_id, ObMsSqlRequest * request)
{
  int ret = OB_SUCCESS;
  if ((OB_INVALID_ID == client_id) || (NULL == request))
  {
    ret = OB_INPUT_PARAM_ERROR;
    TBSYS_LOG(WARN, "check input failed:client[%lu], event[%lu], request[%p]",
        client_id, get_event_id(), request);
  }
  else
  {
    client_request_id_ = client_id;
    client_request_ = request;
    TBSYS_LOG(DEBUG, "init rpc event succ:client[%lu], event[%lu], request[%p]",
        client_id, get_event_id(), request);
  }
  return ret;
}

int ObMsSqlRpcEvent::parse_packet(ObPacket * packet, void * args)
{
  int ret = OB_SUCCESS;
  UNUSED(args);
  if (NULL == packet)
  {
    ret = OB_RESPONSE_TIME_OUT;
    TBSYS_LOG(WARN, "message timeout, packet from chunkserver[%s] is NULL, request_id[%lu], event_id[%lu]",
        server_.to_cstring(), client_request_id_, get_event_id());
  }
  else
  {
    ret = deserialize_packet(*packet, ObCommonSqlRpcEvent::get_result());
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "deserialize packet from server[%s] failed, request_id[%lu], event_id[%lu], ret[%d]",
          server_.to_cstring(), client_request_id_, ObCommonSqlRpcEvent::get_event_id(), ret);
    }
  }
  return ret;
}

int ObMsSqlRpcEvent::handle_packet(ObPacket * packet, void * args)
{
  int ret = OB_SUCCESS;
  if (ObMergeServerMain::get_instance()->get_merge_server().is_stoped())
  {
    TBSYS_LOG(WARN, "server stoped, cannot handle anything.");
    ret = OB_ERROR;
  }
  else
  {
    /// stat event process time
    this->end();
    /// parse the packet for get result code and result scanner
    ret = parse_packet(packet, args);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "parse packet from server[%s] failed, request_id[%lu], event_id[%lu], rpc_event[%p]",
          server_.to_cstring(), client_request_id_, get_event_id(), this);
      /// set result code, maybe timeout packet, connection errors.
      if (NULL != args && 1 == reinterpret_cast<easy_connection_t*>(args)->conn_has_error)
      {
        ObCommonSqlRpcEvent::set_result_code(OB_CONN_ERROR);
      }
      else
      {
        ObCommonSqlRpcEvent::set_result_code(ret);
      }
    }

    ObPacket* obpacket = packet;
    if (NULL != obpacket)
    {
      TBSYS_LOG(DEBUG, "get packet from server[%s] success, event_id[%lu], time_used[%ld],"
          "result code=%d, packet code=%d, session_id=%ld",
          server_.to_cstring(), get_event_id(), get_time_used(), get_result_code(),
          obpacket->get_packet_code(), obpacket->get_session_id());
    }
    //lock_.lock();
    if (client_request_ == NULL)
    {
      TBSYS_LOG(WARN, "rpc event(%p) unknown status, destroy myself, event_id[%lu] "
          "request_id[%lu], req_type[%d]",
          this, this->get_event_id(), this->get_client_id(), this->get_req_type());
      this->~ObMsSqlRpcEvent();
      ob_tc_free(this);
    }
    else
    {
      /// no matter parse succ or failed push to finish queue
      /// not check the event valid only push to the finish queue
      if (OB_SUCCESS != (ret = client_request_->signal(*this)))
      {
        TBSYS_LOG(WARN, "failed to push event to finish queue, destroy myself, req_type[%s], request_id[%lu], ret=%d",
            (this->get_req_type() == SCAN_RPC) ? "SCAN":"GET", this->get_client_id(), ret);
        OB_ASSERT(magic_ == 0x1234abcd);
        this->~ObMsSqlRpcEvent();
        ob_tc_free(this);
      }
      else
      {
        //OB_ASSERT(magic_ == 0x1234abcd);
        //lock_.unlock();
      }
    }
  }
  return ret;
}

int ObMsSqlRpcEvent::deserialize_packet(ObPacket & packet, ObNewScanner & result)
{
  ObDataBuffer * data_buff = NULL;
  int ret = packet.deserialize();
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "deserialize the packet failed:ret[%d]", ret);
  }
  else
  {
    data_buff = packet.get_buffer();
    if (NULL == data_buff)
    {
      ret = OB_INNER_STAT_ERROR;
      TBSYS_LOG(WARN, "check packet data buff failed:buff[%p]", data_buff);
    }
    if (packet.get_packet_code() == OB_SESSION_END)
    {
      /// when session end, set session id to 0
      set_session_end();
    }
    else
    {
      set_session_id(packet.get_session_id());
    }
  }

  ObResultCode code;
  if (OB_SUCCESS == ret)
  {
    ret = code.deserialize(data_buff->get_data(), data_buff->get_capacity(),
        data_buff->get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize result failed:pos[%ld], ret[%d]",
          data_buff->get_position(), ret);
    }
    else
    {
      ObCommonSqlRpcEvent::set_result_code(code.result_code_);
    }
  }
  ///
  result.clear();
  if ((OB_SUCCESS == ret) && (OB_SUCCESS == code.result_code_))
  {
    ret = result.deserialize(data_buff->get_data(), data_buff->get_capacity(),
        data_buff->get_position());
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "deserialize scanner failed:pos[%ld], ret[%d]",
          data_buff->get_position(), ret);
    }
  }
  return ret;
}

void ObMsSqlRpcEvent::print_info(FILE * file) const
{
  if (NULL != file)
  {
    ObCommonSqlRpcEvent::print_info(file);
    if (NULL == client_request_)
    {
      fprintf(file, "merger rpc event::clinet[%lu], request[%p]\n",
          client_request_id_, client_request_);
    }
    else
    {
      fprintf(file, "merger rpc event:client[%lu], request[%lu], ptr[%p]\n",
          client_request_id_, client_request_->get_request_id(), client_request_);
    }
    fflush(file);
  }
}
