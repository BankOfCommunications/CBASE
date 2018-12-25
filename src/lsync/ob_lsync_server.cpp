/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_lsync_server.cpp
 *
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *
 */

#include "common/ob_repeated_log_reader.h"
#include "ob_lsync_server.h"
#include "common/ob_tbnet_callback.h"
#include "ob_lsync_callback.h"

namespace oceanbase
{
#ifndef NO_STAT
  namespace sstable
  {
    void set_stat(const uint64_t table_id, const int32_t index, const int64_t value)
    {
      UNUSED(table_id);
      UNUSED(index);
      UNUSED(value);
    }

    void inc_stat(const uint64_t table_id, const int32_t index, const int64_t inc_value)
    {
      UNUSED(table_id);
      UNUSED(index);
      UNUSED(inc_value);
    }
  }
#endif

  namespace lsync
  {
    const int64_t HANDLE_REQUEST_TIMEOUT_DELTA = 10 * 1000;

    void free_data_buffer(ObDataBuffer& buf)
    {
      if(buf.get_data())
      {
        delete [] buf.get_data();
        buf.set_data(NULL, 0);
      }
    }

    ObLsyncServer:: ObLsyncServer()
    {}

    ObLsyncServer:: ~ObLsyncServer()
    {
      free_data_buffer(log_buffer_);
    }

    static char* net_addr_format(uint64_t id)
    {
      static char addr[64];
      int ip = (int) (id & 0xffffffff);
      int port = (int) ((id >> 32) & 0xffffffff);
      ip = ntohl(ip);
      sprintf(addr, "%d.%d.%d.%d:%d",
              (ip >> 24) & 0xff, (ip >> 16) & 0xff, (ip >> 8) & 0xff, ip & 0xff,  port);
      return addr;
    }

    static int get_thread_buffer(ThreadSpecificBuffer& thread_buffer, ObDataBuffer& data_buff)
    {
      int err = OB_SUCCESS;

      ThreadSpecificBuffer::Buffer* my_buffer = NULL;
      my_buffer = thread_buffer.get_buffer();
      if (NULL == my_buffer)
      {
        TBSYS_LOG(ERROR, "get thread buff failed:buffer[%p].", my_buffer);
        err = OB_ERROR;
      }
      else
      {
        my_buffer->reset();
        data_buff.set_data(my_buffer->current(), my_buffer->remain());
      }

      return err;
    }

    static int new_data_buffer(ObDataBuffer& buf, int64_t len)
    {
      int err = OB_SUCCESS;
      char* pbuf = new(std::nothrow) char[len];
      if (OB_SUCCESS != (err = pbuf? OB_SUCCESS: OB_ALLOCATE_MEMORY_FAILED))
      {
        TBSYS_LOG(ERROR, "new_data_buffer: new char[%ld]=>NULL", len);
      }
      else
      {
        buf.set_data(pbuf, len);
        buf.get_position() = 0;
      }
      return err;
    }

    int ObLsyncServer:: get_log(ObFetchLogRequest& req, char* buf, int64_t limit, int64_t& pos, int64_t timeout) //TODO:
    {
      int err = OB_SUCCESS;
      int64_t read_count = 0;
      uint64_t log_file_id = (req.log_file_id == 0? origin_log_file_id_: req.log_file_id);
      uint64_t log_seq_id = req.log_seq_id;
      TBSYS_LOG(DEBUG, "get_log(log_file_id=%ld, log_seq_id=%ld)", log_file_id, log_seq_id);
      if (OB_SUCCESS != (err = reader_.get_log(log_file_id, log_seq_id, buf + pos, limit - pos, read_count, timeout))
          && OB_READ_NOTHING != err)
      {
        TBSYS_LOG(ERROR, "get_log(file_id=%ld, seq_id=%ld)=>%d", log_file_id, log_seq_id, err);
      }
      else if (OB_READ_NOTHING == err)
      {
        err = OB_SUCCESS;
        TBSYS_LOG(DEBUG, "read_nothing, reset errno to OB_SUCCESS.");
      }
      else
      {
        pos += read_count;
      }
      return err;
    }

    int ObLsyncServer::initialize(const char* log_dir, uint64_t log_start, const char* dev, int port, int64_t timeout, int64_t convert_switch_log, int64_t lsync_retry_wait_time_us)
    {
      int err = OB_SUCCESS;
      UNUSED(timeout);
      state_ = WAIT_REGISTER;
      log_dir_ = log_dir;
      origin_log_file_id_ = log_start;
      set_dev_name(dev);
      //set_packet_factory(&my_packet_factory_);
      set_listen_port(port);

      if (OB_SUCCESS == err)
      {
        memset(&server_handler_, 0, sizeof(easy_io_handler_pt));
        server_handler_.encode = ObTbnetCallback::encode;
        server_handler_.decode = ObTbnetCallback::decode;
        server_handler_.process = ObLsyncCallback::process;
        server_handler_.get_packet_id = ObTbnetCallback::get_packet_id;
        server_handler_.on_disconnect = ObTbnetCallback::on_disconnect;
        server_handler_.user_data = this;
      }

      //if (OB_SUCCESS == err)
      //{
      //  err = set_io_thread_count(1);//defalut
      //}

      if (OB_SUCCESS != (err = new_data_buffer(log_buffer_, OB_MAX_LOG_BUFFER_SIZE)))
      {
        TBSYS_LOG(ERROR, "new_data_buffer(log_buffer, len=%ld)=>%d", OB_MAX_LOG_BUFFER_SIZE, err); 
     }
      else if (OB_SUCCESS != (err = reader_.init(log_dir, 0 == convert_switch_log, lsync_retry_wait_time_us)))
      {
        TBSYS_LOG(ERROR, "reader_.initialize(log_dir='%s', retry_wait_time_us=%ld)=>%d", log_dir, lsync_retry_wait_time_us, err);
      }

      return err;
    }

    void ObLsyncServer::destroy()
    {
      reader_.stop();
    }

    int ObLsyncServer::ups_slave_register(easy_request_t* req, const uint32_t channel_id, int packet_code,
                                            ObDataBuffer* buf)
    {
      int ret = OB_SUCCESS;
      ObLogEntry entry;
      ObSlaveRegisterRequest reg_packet;
      int64_t pos = buf->get_position();

      if (OB_LSYNC_FETCH_LOG == packet_code)
      {
        ret = OB_NOT_REGISTERED;
      }
      else if (OB_SLAVE_REG != packet_code)
      {
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        ret = reg_packet.deserialize(buf->get_data(), buf->get_capacity(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "RegisterPacket deserialize failed: %d", ret);
        }
      }
      TBSYS_LOG(DEBUG, "SlaveRegisterRequest{file_id: %ld, log_id: %ld, slave: %s}",
                reg_packet.log_file_id, reg_packet.log_seq_id, net_addr_format(slave_id_));

      if (OB_SUCCESS == ret)
      {
        if (reg_packet.log_file_id == 0)
        {
          reg_packet.log_file_id = origin_log_file_id_;
          reg_packet.log_seq_id = 0;
        }
        slave_id_ = convert_addr_to_server(req->ms->c->addr);
      }
      else
      {
        reg_packet.log_file_id = 0;
        reg_packet.log_seq_id = 0;
      }

      ObSlaveRegisterResponse response(ret, reg_packet);

      if (OB_SUCCESS != (ret = send_response_packet(OB_SLAVE_REG_RES, MY_VERSION, &response, req, channel_id)))
      {
        TBSYS_LOG(DEBUG, "ups_slave_register:send_packet()=>%d", ret);
      }
      return ret;
    }

    bool ObLsyncServer::is_registered(uint64_t id)
    {
      return state_ == WAIT_LOG_REQ
        && (id & 0xffffffff) == (slave_id_ & 0xffffffff);
    }

    int ObLsyncServer::send_log(easy_request_t* request, const uint32_t channel_id,
                                int packet_code, ObDataBuffer* buf, int64_t timeout)
    {
      int ret = OB_SUCCESS;
      ObFetchLogRequest req;
      int64_t pos = buf->get_position();

      if (packet_code == OB_SLAVE_REG)
      {
        TBSYS_LOG(WARN, "receive reg packet when expected FETCH_LOG.");
        ret = OB_NEED_RETRY;
      }
      else if (packet_code != OB_LSYNC_FETCH_LOG)
      {
        TBSYS_LOG(WARN, "unexpected packet: %d", packet_code);
        ret = OB_ERR_UNEXPECTED;
      }
      else if(!is_registered(convert_addr_to_server(request->ms->c->addr)))
      {
        TBSYS_LOG(WARN, "not registered: %s", get_peer_ip(request));
        ret = OB_DATA_NOT_SERVE; // not served by me
      }

      if (OB_SUCCESS != ret) {}
      else if (OB_SUCCESS != (ret = req.deserialize(buf->get_data(), buf->get_capacity(), pos)))
      {
        TBSYS_LOG(WARN, "ObRequestLogPacket.deserialize() error: %d.", ret);
      }
      else
      {
        ret = send_log_(req, request, channel_id, timeout);
        if(OB_SUCCESS != ret && OB_READ_NOTHING != ret)
          TBSYS_LOG(WARN, "send_log:send_packet=>%d", ret);
      }
      return ret;
    }

    int ObLsyncServer::send_log_(ObFetchLogRequest& req, easy_request_t* request, const uint32_t channel_id, int64_t timeout)
    {
      int ret = OB_SUCCESS;
      int64_t start_us = tbsys::CTimeUtil::getTime();

      log_buffer_.get_position() = 0;
      if (OB_SUCCESS != (ret = log_buffer_.get_data()? OB_SUCCESS: OB_NOT_INIT))
      {
        TBSYS_LOG(DEBUG, "log_buffer_.get_data()==NULL");
      }
      else if (OB_SUCCESS != (ret = get_log(req, log_buffer_.get_data(), log_buffer_.get_capacity(), log_buffer_.get_position(), timeout)))
      {
        TBSYS_LOG(DEBUG, "ObLsyncServer.get_log()=>%d.", ret);
      }

      ObFetchLogResponse response(ret, log_buffer_);
      if (start_us + timeout < tbsys::CTimeUtil::getTime())
      {} // timeout
      else
      {
        if (OB_SUCCESS != ret) // 其他的错误重设为OB_NEED_RETRY
        {
          log_buffer_.get_position() = 0;
          if (OB_NEED_RETRY != ret)
          {
            ret = OB_NEED_RETRY;
            TBSYS_LOG(ERROR, "get_log(file_id=%ld, seq_id=%ld)=>%d", req.log_file_id, req.log_seq_id, ret);
          }
        }
        ObFetchLogResponse response(ret, log_buffer_);
        if (OB_SUCCESS != (ret = send_response_packet(OB_SEND_LOG, MY_VERSION, &response, request, channel_id)))
        {
          TBSYS_LOG(WARN, "send_log:send_packet=>%d", ret);
        }
      }
      return ret;
    }

    // make sure only one thread call handlePacket()
    //tbnet::IPacketHandler::HPRetCode ObLsyncServer::handlePacket(tbnet::Connection *conn, tbnet::Packet *packet)
    //{
    //  tbnet::IPacketHandler::HPRetCode rc = tbnet::IPacketHandler::KEEP_CHANNEL;
    //  int err = OB_SUCCESS;
    //
    //  if (!packet->isRegularPacket()) // Maybe caused by `Timeout', so reset state => WAIT_REGISTER
    //  {
    //    rc = tbnet::IPacketHandler::FREE_CHANNEL;
    //    state_ = WAIT_REGISTER;
    //    err = OB_ERR_UNEXPECTED;
    //    TBSYS_LOG(WARN, "control packet, packet code: %d", ((tbnet::ControlPacket*)packet)->getCommand());
    //  }
    //
    //  if (OB_SUCCESS == err)
    //  {
    //    ObPacket* req = (ObPacket*) packet;
    //    req->set_connection(conn);
    //    if (OB_SUCCESS != (err = req->deserialize()))
    //    {
    //      TBSYS_LOG(WARN, "packet deserialize error: packet_code=%d, err=%d", req->get_packet_code(), err);
    //    }
    //    else if (OB_SUCCESS != (err = (req->get_api_version() == MY_VERSION)? OB_SUCCESS: OB_ERROR_FUNC_VERSION))
    //    {
    //      TBSYS_LOG(ERROR, "api version mismatch: request version %d, support version %d", req->get_api_version(), MY_VERSION);
    //    }
    //    else if (OB_SUCCESS != (err = handleRequest(conn, req->getChannelId(), req->get_packet_code(), req->get_buffer(),
    //                                                req->get_source_timeout())))
    //    {
    //      TBSYS_LOG(DEBUG, "handleRequest(packet_code=%d)=>%d", req->get_packet_code(), err);
    //    }
    //  }
    //  return rc;
    //}

    int ObLsyncServer::handlePacket(ObPacket *packet)
    {
      int err = OB_SUCCESS;
      ObPacket* req = (ObPacket*) packet;
      int64_t remain_time = 0;
      if (0 >= (remain_time = req->get_receive_ts() + req->get_source_timeout() - tbsys::CTimeUtil::getTime()))
      {
        TBSYS_LOG(WARN, "packet wait too long time: receive[%ld] + timeout[%ld] < now[%ld]",
                  req->get_receive_ts(), req->get_source_timeout(), tbsys::CTimeUtil::getTime());
      }
      else if (OB_SUCCESS != (err = req->deserialize()))
      {
        TBSYS_LOG(WARN, "packet deserialize error: packet_code=%d, err=%d", req->get_packet_code(), err);
      }
      else if (OB_SUCCESS != (err = (req->get_api_version() == MY_VERSION)? OB_SUCCESS: OB_ERROR_FUNC_VERSION))
      {
        TBSYS_LOG(ERROR, "api version mismatch: request version %d, support version %d", req->get_api_version(), MY_VERSION);
      }
      else if (OB_SUCCESS != (err = handleRequest(req->get_request(), req->get_channel_id(), req->get_packet_code(),
                                                  req->get_buffer(), req->get_source_timeout())))
      {
        TBSYS_LOG(DEBUG, "handleRequest(packet_code=%d)=>%d", req->get_packet_code(), err);
      }

      return err;
    }

    int ObLsyncServer::handleBatchPacket(ObPacketQueue& packetQueue)
    {
      UNUSED(packetQueue);
      return OB_SUCCESS;
    }

    int ObLsyncServer::handleRequest(easy_request_t* req, const uint32_t channel_id, int packet_code, ObDataBuffer* buf, int64_t timeout)
    {
      int err = handleRequestMayNeedRetry(req, channel_id, packet_code, buf, timeout);
      if (OB_NEED_RETRY == err)
      {
        err = handleRequestMayNeedRetry(req, channel_id, packet_code, buf, timeout);
      }
      return err;
    }

    int ObLsyncServer::handleRequestMayNeedRetry(easy_request_t* req, const uint32_t channel_id, int packet_code, ObDataBuffer* buf, int64_t timeout)
    {
      int err = OB_SUCCESS;
      uint64_t id = 0;
      easy_connection_t* conn = req->ms->c;
      id = convert_addr_to_server(conn->addr);

      switch(state_)
      {
      case WAIT_REGISTER:
        err = ups_slave_register(req, channel_id, packet_code, buf);
        if (OB_SUCCESS == err)
        {
          TBSYS_LOG(INFO, "slave register ok:%s", inet_ntoa_r((conn->addr)));
          state_ = WAIT_LOG_REQ;
        }
        else
        {
          TBSYS_LOG(WARN, "slave register failed.");
          state_ = WAIT_REGISTER;
        }
        break;
      case WAIT_LOG_REQ:
        err = send_log(req, channel_id, packet_code, buf, timeout);
        if (OB_SUCCESS == err || OB_READ_NOTHING == err)
        {
          state_ = WAIT_LOG_REQ;
        }
        else
        {
          state_ = WAIT_REGISTER;
        }
        break;
      case STOP:
      default:
        TBSYS_LOG(ERROR, "unexpected state: %d", state_);
        err = OB_ERR_UNEXPECTED;
        break;
      }

      return err;
    }

    /*const char *inet_ntoa_r(const uint64_t ipport)
    {
      static const int64_t BUFFER_SIZE = 32;
      static __thread char buffers[2][BUFFER_SIZE];
      static __thread int64_t i = 0;
      char *buffer = buffers[i++ % 2];
      buffer[0] = '\0';

      uint32_t ip = (uint32_t)(ipport & 0xffffffff);
      int port = (int)((ipport >> 32 ) & 0xffff);
      unsigned char *bytes = (unsigned char *) &ip;
      if (port > 0)
      {
        snprintf(buffer, BUFFER_SIZE, "%d.%d.%d.%d:%d", bytes[0], bytes[1], bytes[2], bytes[3], port);
      }
      else
      {
        snprintf(buffer, BUFFER_SIZE, "%d.%d.%d.%d:-1", bytes[0], bytes[1], bytes[2], bytes[3]);
      }

      return buffer;
      }*/

    int ObLsyncServer::send_response_packet(int packet_code, int version, ObLsyncPacket* packet,
                                   easy_request_t* req, const uint32_t channel_id)
    {
      ObDataBuffer out_buff;
      int err = OB_SUCCESS;
      char cbuf[OB_LSYNC_MAX_STR_LEN];
      TBSYS_LOG(DEBUG, "send_response(%s)", packet->str(cbuf, sizeof(cbuf)));
      UNUSED(packet_code);
      easy_connection_t* conn = req->ms->c;
      if (OB_SUCCESS != (err = get_thread_buffer(thread_buffer_, out_buff)))
      {
        TBSYS_LOG(WARN, "get_buffer_()=>%d", err);
      }
      else if(OB_SUCCESS != (err = packet->serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
      {
        TBSYS_LOG(WARN, "packet serialize failed: buf=%p, len=%ld, pos=%ld",
                  out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      }

      if (OB_SUCCESS != (err = send_response(OB_SEND_LOG, version, out_buff, req, channel_id)))
      {
        TBSYS_LOG(WARN, "send_response()=>%d", err);
      }
      else
      {
        TBSYS_LOG(DEBUG, "send response succ size=%ld src=[%s]", out_buff.get_position(), inet_ntoa_r(conn->addr));
      }
      return err;
    }

  } // end namespace lsync
} // end namespace oceanbase

