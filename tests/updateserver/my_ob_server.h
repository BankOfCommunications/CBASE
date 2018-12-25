#ifndef __OB_UPDATESERVER_MY_OB_SERVER_H__
#define __OB_UPDATESERVER_MY_OB_SERVER_H__
#include "common/ob_packet_factory.h"
#include "common/ob_base_server.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace test
  {
    ObDataBuffer& __get_thread_buffer(ThreadSpecificBuffer& thread_buf, ObDataBuffer& buf)
    {
      ThreadSpecificBuffer::Buffer* my_buffer = thread_buf.get_buffer();
      my_buffer->reset();
      buf.set_data(my_buffer->current(), my_buffer->remain());
      return buf;
    }

    class MyObServer : public ObBaseServer
    {
      public:
        MyObServer() {}
        virtual ~MyObServer() {}
      public:
        virtual int initialize() {
          ObBaseServer::initialize();
          return 0;
        }

        virtual int do_request(int pcode, ObDataBuffer& inbuf,  int& ret_pcode, ObDataBuffer& outbuf) = 0;
        virtual int handlePacket(ObPacket *packet){
          int err = OB_SUCCESS;
          int handle_err = OB_SUCCESS;
          int32_t ret_pcode = 0;
          ObPacket* req = (ObPacket*) packet;

          ObDataBuffer handle_buf;
          ObDataBuffer outbuf;
          __get_thread_buffer(handle_buffer_, handle_buf);
          __get_thread_buffer(response_buffer_, outbuf);

          TBSYS_LOG(DEBUG,"get packet code is %d, priority=%d", req->get_packet_code(), req->get_packet_priority());
          if (!packet)
          {
            err = OB_ERR_UNEXPECTED;
          }
          else if (OB_SUCCESS != (err = req->deserialize()))
          {
            TBSYS_LOG(ERROR, "req->deserialize()=>%d", err);
          }
          else if (OB_SUCCESS != (handle_err = do_request(req->get_packet_code(), *req->get_buffer(), ret_pcode, handle_buf)))
          {
            TBSYS_LOG(ERROR, "do_request(pcode=%d)=>%d", req->get_packet_code(), handle_err);
          }

          if (OB_SUCCESS != err)
          {}
          else if (OB_SUCCESS != (err = serialize_result(handle_err, handle_buf, outbuf)))
          {
            TBSYS_LOG(ERROR, "serialize_result()=>%d", err);
          }
          else if (OB_SUCCESS != (err = send_response(ret_pcode, req->get_api_version(), outbuf, req->get_request(), req->get_channel_id())))
          {
            TBSYS_LOG(ERROR, "send_response(pcode=%d)=>%d", ret_pcode, err);
          }

          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "packet %d can not be distribute to queue", req->get_packet_code());
            send_response(ret_pcode, req->get_api_version(), outbuf, req->get_request(), req->get_channel_id());
          }
          return ret_pcode;
        }

        int serialize_result(int err_code, ObDataBuffer& buf, ObDataBuffer& outbuf) {
          int err = OB_SUCCESS;
          common::ObResultCode result_msg;
          result_msg.result_code_ = err_code;
          if (OB_SUCCESS != (err = result_msg.serialize(outbuf.get_data(), outbuf.get_capacity(), outbuf.get_position())))
          {
            TBSYS_LOG(ERROR, "result_mgr.serialize()=>%d", err);
          }
          else if (outbuf.get_remain() < buf.get_position())
          {
            err = OB_BUF_NOT_ENOUGH;
            TBSYS_LOG(ERROR, "outbuf.get_remain()[%ld] < buf.get_position()[%ld], err=%d", outbuf.get_remain(), buf.get_position(), err);
          }
          else if (NULL == memcpy(outbuf.get_data() + outbuf.get_position(), buf.get_data(), buf.get_position()))
          {
            err = OB_ERR_UNEXPECTED;
          }
          else
          {
            outbuf.get_position() += buf.get_position();
          }
          return err;
        }
        virtual int handleBatchPacket(ObPacketQueue &packetQueue) {
          UNUSED(packetQueue);
          TBSYS_LOG(WARN, "You should not reach here!");
          return 0;
        }
      private:
        ThreadSpecificBuffer handle_buffer_;
        ThreadSpecificBuffer response_buffer_;
    };
  }; // end namespace test
}; // end namespace oceanbase
#endif /* __OB_UPDATESERVER_MY_OB_SERVER_H__ */
