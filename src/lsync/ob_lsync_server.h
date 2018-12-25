/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_lsync_server.h
 *
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *
 */

#ifndef __OCEANBASE_LSYNC_OB_LSYNC_SERVER_H__
#define __OCEANBASE_LSYNC_OB_LSYNC_SERVER_H__

#include "common/ob_define.h"
#include "ob_seekable_log_reader2.h"
#include "common/ob_base_server.h"
#include "common/ob_packet_factory.h"
#include "common/ob_server.h"
#include "common/ob_result.h"
#include "common/serialization.h"
#include "common/utility.h"
using namespace oceanbase::common;

namespace oceanbase
{
  namespace lsync
  {
    const int OB_LSYNC_MAX_STR_LEN = (1<<10);
    /*
     *  ObBaseServer                        ObClientMgr
     * -------------                       ------------
     *  LsyncServer                            UPS
     * -------------                       ------------
     *
     *                    Register
     * ------------------------------------------------------
     * WAIT_REGISTER                          Init
     *               <-- RegisterPacket --
     *                --    RegisterOk  -->
     * WAIT_LOG_REQ                          Can_SEND_LOG_REQ
     * ------------------------------------------------------
     *
     *                       SendLog
     * ------------------------------------------------------
     * WAIT_LOG_REQ                          Can_SEND_LOG_REQ
     *               <--      LogReq    --
     *                                       Wait_Log
     *                --  LogResponse   -->
     * ------------------------------------------------------
     *
     *                       SendLog ...
     * ------------------------------------------------------
     * WAIT_LOG_REQ                          Can_SEND_LOG_REQ
     *               <--      LogReq    --
     *                          .
     *                          .
     *                          .
     *                          .
     */
    enum ObLogSyncServerState
    {
      WAIT_REGISTER = 0,
      WAIT_LOG_REQ = 1,
      STOP = 2,
    };

    struct ObLsyncPacket
    {
      virtual ~ObLsyncPacket(){}
      virtual int serialize(char* buf, int64_t len, int64_t& pos) = 0;
      virtual int deserialize(char* buf, int64_t len, int64_t& pos) = 0;
      virtual char* str(char* buf, int len) = 0;
    };

    struct ObLogSyncPoint: public ObLsyncPacket
    {
      uint64_t log_file_id;
      uint64_t log_seq_id;
      ObLogSyncPoint(){}
      ObLogSyncPoint(ObLogSyncPoint& sync_point) : ObLsyncPacket()
      {
        log_file_id = sync_point.log_file_id;
        log_seq_id = sync_point.log_seq_id;
      }
      ObLogSyncPoint(uint64_t log_file_id, uint64_t log_seq_id): log_file_id(log_file_id), log_seq_id(log_seq_id){}
      virtual ~ObLogSyncPoint(){}
      virtual int serialize(char* buf, int64_t len, int64_t& pos)
      {
        int rc = OB_SUCCESS;
        if(!(OB_SUCCESS == serialization::encode_i64(buf, len, pos, log_file_id)
             && OB_SUCCESS == serialization::encode_i64(buf, len, pos, log_seq_id)))
        {
          rc = OB_SERIALIZE_ERROR;
          TBSYS_LOG(WARN, "ObLogSyncPoint.serialize(buf=%p, len=%ld, pos=%ld)=>%d", buf, len, pos, rc);
        }
        return rc;
      }

      virtual int deserialize(char* buf, int64_t len, int64_t& pos)
      {
        int rc = OB_SUCCESS;
        if(! (OB_SUCCESS == serialization::decode_i64(buf, len, pos, (int64_t*)&log_file_id)
              && OB_SUCCESS == serialization::decode_i64(buf, len, pos, (int64_t*)&log_seq_id)))
        {
          rc = OB_DESERIALIZE_ERROR;
          TBSYS_LOG(WARN, "ObLogSyncPoint.deserialize(buf=%p, len=%ld, pos=%ld)=>%d", buf, len, pos, rc);
        }
        return rc;
      }
      virtual char* str(char* buf, int len)
      {
        snprintf(buf, len, "ObLogSyncPoint{log_file_id=%ld, log_seq_id=%ld}", log_file_id, log_seq_id);
        return buf;
      }
    };

    typedef ObLogSyncPoint ObSlaveRegisterRequest;
    typedef ObLogSyncPoint ObFetchLogRequest;

    struct ObSlaveRegisterResponse: public ObLsyncPacket
    {
      ObResultCode result_code;
      ObLogSyncPoint sync_point;
      ObSlaveRegisterResponse(int err, ObLogSyncPoint& sync_point)
      {
        result_code.result_code_ = err;
        this->sync_point = sync_point;
      }
      virtual ~ObSlaveRegisterResponse(){}
      virtual int serialize(char* buf, int64_t len, int64_t& pos)
      {
        int rc = OB_SUCCESS;
        if(!(OB_SUCCESS == result_code.serialize(buf, len, pos)
             && OB_SUCCESS == sync_point.serialize(buf, len, pos)))
        {
          rc = OB_SERIALIZE_ERROR;
          TBSYS_LOG(WARN, "ObSlaveRegisterResponse.serialize(buf=%p, len=%ld, pos=%ld)=>%d", buf, len, pos, rc);
        }
        return rc;
      }

      virtual int deserialize(char* buf, int64_t len, int64_t& pos)
      {
        int rc = OB_SUCCESS;
        if(! (OB_SUCCESS == result_code.deserialize(buf, len, pos)
              && OB_SUCCESS == sync_point.deserialize(buf, len, pos)))
        {
          TBSYS_LOG(WARN, "ObSlaveRegisterResponse.deserialize(buf=%p, len=%ld, pos=%ld)=>%d", buf, len, pos, rc);
          rc = OB_DESERIALIZE_ERROR;
        }
        return rc;
      }

      virtual char* str(char* buf, int len)
      {
        char sync_point_buf[OB_LSYNC_MAX_STR_LEN];
        sync_point.str(sync_point_buf, sizeof(sync_point_buf));
        snprintf(buf, len, "ObSlaveRegisterResponse{err=%d, sync_point=%s}", result_code.result_code_, sync_point_buf);
        return buf;
      }
    };


    static int mem_chunk_serialize(char* buf, int64_t len, int64_t& pos, char* pmem, int64_t mem_len)
    {
      int rc = OB_SUCCESS;
      if (len - pos >= mem_len)
      {
        memcpy(buf + pos, pmem, mem_len);
        pos += mem_len;
        rc = OB_SUCCESS;
      }
      else
      {
        rc = OB_BUF_NOT_ENOUGH;
      }
      return rc;
    }

    static int ob_data_buffer_serialize(char* buf, int64_t len, int64_t& pos, ObDataBuffer& data_buf)
    {
      int rc = OB_SUCCESS;
      if(! (OB_SUCCESS == serialization::encode_i64(buf, len, pos, data_buf.get_position())
            && OB_SUCCESS == mem_chunk_serialize(buf, len, pos, data_buf.get_data(), data_buf.get_position())))
      {
        rc = OB_SERIALIZE_ERROR;
      }
      return rc;
    }

    static int ob_data_buffer_deserialize(char* buf, int64_t len, int64_t& pos, ObDataBuffer& data_buf)
    {
      int rc = OB_SUCCESS;
      int64_t data_len = 0;
      if(OB_SUCCESS == (rc = serialization::decode_i64(buf, len, pos, &data_len)))
      {
        data_buf.set_data(buf + pos, data_len);
      }
      return rc;
    }

    struct ObFetchLogResponse: public ObLsyncPacket
    {
      ObResultCode result_code;
      ObDataBuffer data;
      ObFetchLogResponse(int64_t err, ObDataBuffer& data)
      {
        result_code.result_code_ = static_cast<int32_t>(err);
        this->data.set_data(data.get_data(), data.get_capacity());
        this->data.get_position()  = data.get_position();
      }
      virtual ~ObFetchLogResponse(){}
      virtual int serialize(char* buf, int64_t len, int64_t& pos)
      {
        int rc = OB_SUCCESS;
        if(!(OB_SUCCESS == result_code.serialize(buf, len, pos)
             && OB_SUCCESS == ob_data_buffer_serialize(buf, len, pos, data)))
        {
          rc = OB_SERIALIZE_ERROR;
          TBSYS_LOG(WARN, "ObFetchLogResponse.serialize(buf=%p, len=%ld, pos=%ld)=>%d", buf, len, pos, rc);
        }
        return rc;
      }

      virtual int deserialize(char* buf, int64_t len, int64_t& pos)
      {
        int rc = OB_SUCCESS;
        if(! (OB_SUCCESS == result_code.deserialize(buf, len, pos)
              && OB_SUCCESS == ob_data_buffer_deserialize(buf, len, pos, data)))
        {
          TBSYS_LOG(WARN, "ObFetchLogResponse.deserialize(buf=%p, len=%ld, pos=%ld)=>%d", buf, len, pos, rc);
          rc = OB_DESERIALIZE_ERROR;
        }
        return rc;
      }

      virtual char* str(char* buf, int len)
      {
        snprintf(buf, len, "ObFetchLogResponse{err=%d, data_len=%ld}", result_code.result_code_, data.get_position());
        return buf;
      }
    };

    class ObLsyncServer: public ObBaseServer
    {
      public:
        ObLsyncServer();
        virtual ~ObLsyncServer();

        int initialize(const char* log_dir, uint64_t start_id, const char* dev, int port, int64_t timeout, int64_t convert_switch_log, int64_t lsync_retry_wait_time);
        virtual void destroy();

        //tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet);
        //bool handleBatchPacket(tbnet::Connection *connection, ObPacketQueue &packetQueue);

        int handlePacket(ObPacket *packet);
        int handleBatchPacket(ObPacketQueue& packetQueue);

      private:
        int send_response_packet(int packet_code, int version, ObLsyncPacket* packet,
                                 easy_request_t* req, const uint32_t channel_id);
        int handleRequest(easy_request_t* req, const uint32_t channel_id, int packet_code, ObDataBuffer* buf, int64_t timeout);
        int handleRequestMayNeedRetry(easy_request_t* req, const uint32_t channel_id, int packet_code, ObDataBuffer* buf, int64_t timeout);
        int ups_slave_register(easy_request_t* req, const uint32_t channel_id, int packet_code, ObDataBuffer* buf);
        int send_log(easy_request_t* req, const uint32_t channel_id, int packet_code, ObDataBuffer* buf, int64_t timeout);
        int send_log_(ObFetchLogRequest& req, easy_request_t* request, const uint32_t channel_id, int64_t timeout);
        int get_log(ObFetchLogRequest& req, char* buf, int64_t limit, int64_t& pos, int64_t timeout);
        bool is_registered(uint64_t id);
        int get_thread_buffer_(ObDataBuffer& data_buff);

        static const int MY_VERSION = 1;
        const char* log_dir_;
        uint64_t origin_log_file_id_;
        ObSeekableLogReader reader_;
        int state_;
        uint64_t slave_id_;
        //ObPacketFactory my_packet_factory_;
        ObDataBuffer log_buffer_;
        ThreadSpecificBuffer thread_buffer_;
    };
  } // end namespace lsync
} // end namespace oceanbase

#endif // __OCEANBASE_LSYNC_OB_LSYNC_SERVER_H__

