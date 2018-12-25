/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ????.cpp is for what ...
 *
 *  Version: $Id: ipvsadm.c,v 1.27 2005/12/10 16:00:07 wensong Exp $
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 *        - some work details if you want
 *  Modify:
 *     fangji <fangji.hcm@taobao.com>
 */
#ifndef OCEANBASE_COMMON_CLIENT_MANAGER_H_
#define OCEANBASE_COMMON_CLIENT_MANAGER_H_

#include "easy_io_struct.h"

#include "data_buffer.h"
#include "ob_server.h"
#include "ob_packet.h"
namespace oceanbase
{

  namespace common
  {
    class ObClientManager
    {
      public:
        ObClientManager();
        ~ObClientManager();
      public:
        int initialize(easy_io_t *eio, easy_io_handler_pt* handler, const int64_t max_request_timeout = 5000000);

        int set_dedicate_thread_num(const int n);
        void set_error(const int err);
        int post_request_using_dedicate_thread(const ObServer& server, const int32_t pcode, const int32_t version,
                                               const int64_t timeout, const ObDataBuffer& in_buffer,
                                               easy_io_process_pt handler, void* args,
                                               //add wangjiahao [Paxos ups_replication_tmplog] 20150609 :b
                                               const int64_t cmt_log_seq,
                                               //add:e
                                               int thread_idx=0) const;
        /**
         * post request (%in_buffer) to %server, and do not care repsonse from %server.
         */
        int post_request(const ObServer& server,  const int32_t pcode, const int32_t version,
            const ObDataBuffer& in_buffer) const;

        /**
         * post request (%in_buffer) to %server
         */
        int post_request(const ObServer& server, const int32_t pcode, const int32_t version,
            const int64_t timeout, const ObDataBuffer& in_buffer, easy_io_process_pt handler, void* args) const;

        /**
         * send request (%in_buffer) to %server, and wait until %server response
         * (parse to %out_buffer) or timeout.
         */
        int send_request(const ObServer& server, const int32_t pcode, const int32_t version,
            const int64_t timeout, ObDataBuffer& in_buffer, ObDataBuffer& out_buffer) const;

        /**
         * send request (%in_out_buffer) to %server, and wait until %server response
         * (parse to %in_out_buffer) or timeout.
         * use one buffer %in_out_buffer for store request and response packet;
         */
        int send_request(const ObServer& server, const int32_t pcode, const int32_t version,
            const int64_t timeout, ObDataBuffer& in_out_buffer) const;

        /**
         * same as above send_packet, but server may response several times.
         * client can get following response by use get_next on this %session_id
         */
        int send_request(const ObServer& server, const int32_t pcode, const int32_t version,
            const int64_t timeout, ObDataBuffer& in_buffer, ObDataBuffer& out_buffer,
            int64_t& session_id) const;
        /**
         * use one buffer %in_out_buffer for store request and response packet;
         */
        int send_request(const ObServer& server, const int32_t pcode, const int32_t version,
            const int64_t timeout, ObDataBuffer& in_out_buffer, int64_t& session_id) const;

        /**
         * send a special NEXT packet to server for get following response on %session_id
         */
        int get_next(const ObServer& server, const int64_t session_id,
            const int64_t timeout, ObDataBuffer& in_buffer, ObDataBuffer& out_buffer) const;

        int post_next(const ObServer& server, const int64_t session_id, const int64_t timeout,
            ObDataBuffer& in_buffer, easy_io_process_pt handler, void* args) const;
        int post_end_next(const ObServer& server, const int64_t session_id, const int64_t timeout,
            ObDataBuffer& in_buffer, easy_io_process_pt handler, void* args) const;

      private:
        /**
         * send request async
         * handler callback will called when response received or timeout
         * This function will return OB_PACKET_NOT_SEND due to libeasy internal error
         * such as too many doing request
         *
         * @param server       dest server
         * @param pcode        request type
         * @param version      packet version always 1 now
         * @param session_id   session id always 0
         * @param timeout      timeout in microsecond(10^-6s)
         * @param inbuffer     data to send
         * @param handler      callback function
         * @param args         user data
         *
         * @return int OB_SUCCESS/OB_ERROR  OB_PACKET_NOT_SEND
         * @warning
         */
        int do_post_request(const ObServer& server,
                            const int32_t pcode, const int32_t version,
                            const int64_t session_id, const int64_t timeout,
                            const ObDataBuffer& in_buffer,
                            easy_io_process_pt handler, void* args) const;

        /**
         * send request to server and wait for response
         *
         * @param server             dest server
         * @param pcode              request type
         * @param version            packet version alwasy 1 now
         * @param timeout            timeout in microsecond(10^-6s)
         * @param in_buffer          data to send
         * @param response[in/out]   response packet pointer
         * @return int               OB_SUCCESS/OB_ERROR
         */
        int do_send_request(const ObServer& server,
                            const int32_t pcode, const int32_t version,
                            const int64_t timeout, ObDataBuffer& in_buffer,ObDataBuffer &out_buffer, int64_t &session_id) const;

        /**
         * Created session to send
         * @param server             dest server
         * @param pcode              request type
         * @param version            packet version alwasy 1 now
         * @param timeout            timeout in microsecond(10^-6s)
         * @param in_buffer          data to send
         * @param size               packet size
         * @param easy_session_t*&   session created
         */
        int create_session(const ObServer& server,
                           const int32_t pcode, const int32_t version,
                           const int64_t timeout, const ObDataBuffer& in_buffer,
                           int64_t size, easy_session_t *& session) const;
        //add wangjiahao [Paxos ups_replication_tmplog] 20150609 :b
        int create_session(const ObServer& server,
                           const int32_t pcode, const int32_t version,
                           const int64_t timeout, const ObDataBuffer& in_buffer,
                           int64_t size, easy_session_t *& session, const int64_t cmt_log_seq) const;
        //add :e
        int post_session_using_dedicate_thread(easy_session_t* s, int thread_idx = 0) const;
        /**
         * post packet in session
         * to addr not wait for response
         *
         * @param s  session to be post
         */
        int post_session(easy_session_t* s) const;

        /**
         * send packet in session and wait for response
         *
         * @param s  session to be send
         * @return   pointer to response buffer
         */
        void* send_session(easy_session_t *s) const;

        void destroy();
      private:
        int error_;
        int32_t inited_;
        int dedicate_thread_num_;
        mutable int64_t max_request_timeout_;
        easy_io_t* eio_;
        easy_io_handler_pt* handler_;
    };

  } // end namespace chunkserver
} // end namespace oceanbase


#endif //OCEANBASE_COMMON_CLIENT_MANAGER_H_
