
#include <gtest/gtest.h>

#include "updateserver/ob_ups_rpc_stub.h"

#include "common/thread_buffer.h"
#include "common/ob_tablet_info.h"
#include "common/ob_packet_factory.h"
#include "common/ob_base_server.h"
#include "common/ob_single_server.h"
#include "common/ob_malloc.h"
#include "common/ob_result.h"

#include "tbnet.h"
#include "tbsys.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

static const int MOCK_SERVER_LISTEN_PORT = 8888;

namespace oceanbase
{
  namespace tests
  {
    namespace updateserver
    {
      class MockServer : public ObSingleServer
      {
        public:
          MockServer()
          {
          }
          virtual ~MockServer()
          {
          }
          virtual int initialize()
          {
            set_batch_process(false);
            set_listen_port(MOCK_SERVER_LISTEN_PORT);
            set_dev_name("bond0");
            set_packet_factory(&factory_);
            set_default_queue_size(100);
            set_thread_count(1);
            set_packet_factory(&factory_);
            ObSingleServer::initialize();

            return OB_SUCCESS;
          }

          virtual int do_request(ObPacket* base_packet)
          {
            int ret = OB_SUCCESS;
            ObPacket* ob_packet = base_packet;
            int32_t packet_code = ob_packet->get_packet_code();
            int32_t version = ob_packet->get_api_version();
            int32_t channel_id = ob_packet->getChannelId();
            ret = ob_packet->deserialize();

            TBSYS_LOG(INFO, "recv packet with packet_code[%d] version[%d] channel_id[%d]",
                packet_code, version, channel_id);

            if (OB_SUCCESS == ret)
            {
              switch (packet_code)
              {
                case OB_SEND_LOG:
                  handle_send_log(ob_packet);
                  break;
                case OB_RENEW_LEASE_REQUEST:
                  handle_renew_lease(ob_packet);
                  break;
                case OB_GRANT_LEASE_REQUEST:
                  handle_grant_lease(ob_packet);
                  break;
                default:
                  break;
              };
            }

            return NULL;
          }

          int handle_send_log(ObPacket* ob_packet)
          {
            int ret = OB_SUCCESS;

            ObDataBuffer* data_buffer = ob_packet->get_buffer();
            if (NULL == data_buffer)
            {
              TBSYS_LOG(ERROR, "data_buffer is NUll should not reach this");
              ret = OB_ERROR;
            }
            else
            {
              tbnet::Connection* connection = ob_packet->get_connection();
              char buf[10 * 1024];
              ObDataBuffer out_buffer(buf, sizeof(buf));

              ObResultCode response;
              response.result_code_ = OB_SUCCESS;
              response.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                  out_buffer.get_position());
              TBSYS_LOG(DEBUG, "recv send log request, ret=%d", ret);

              int32_t version = 1;
              int32_t channel_id = ob_packet->getChannelId();
              ret = send_response(OB_SEND_LOG_RES, version, out_buffer, connection, channel_id);
              TBSYS_LOG(DEBUG, "send log response, ret=%d", ret);
            }

            return ret;
          }

          int handle_renew_lease(ObPacket* ob_packet)
          {
            int ret = OB_SUCCESS;

            ObDataBuffer* data_buffer = ob_packet->get_buffer();
            if (NULL == data_buffer)
            {
              TBSYS_LOG(ERROR, "data_buffer is NUll should not reach this");
              ret = OB_ERROR;
            }
            else
            {
              ObServer slave_addr;
              char addr_buf[1024];
              ret = slave_addr.deserialize(data_buffer->get_data(), data_buffer->get_capacity(),
                 data_buffer->get_position());
              slave_addr.to_string(addr_buf, sizeof(addr_buf));
              addr_buf[sizeof(addr_buf) - 1] = '\0';
              TBSYS_LOG(DEBUG, "recv renew lease request, slave_addr=%s, ret=%d", addr_buf, ret);
              tbnet::Connection* connection = ob_packet->get_connection();
              char buf[10 * 1024];
              ObDataBuffer out_buffer(buf, sizeof(buf));

              ObResultCode response;
              response.result_code_ = OB_SUCCESS;
              response.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                  out_buffer.get_position());

              int32_t version = 1;
              int32_t channel_id = ob_packet->getChannelId();
              ret = send_response(OB_RENEW_LEASE_RESPONSE, version, out_buffer, connection, channel_id);
              TBSYS_LOG(DEBUG, "send renew lease response, ret=%d", ret);
            }

            return ret;
          }

          int handle_grant_lease(ObPacket* ob_packet)
          {
            int ret = OB_SUCCESS;

            ObDataBuffer* data_buffer = ob_packet->get_buffer();
            if (NULL == data_buffer)
            {
              TBSYS_LOG(ERROR, "data_buffer is NUll should not reach this");
              ret = OB_ERROR;
            }
            else
            {
              tbnet::Connection* connection = ob_packet->get_connection();
              char buf[10 * 1024];
              ObDataBuffer out_buffer(buf, sizeof(buf));
              ObLease lease;
              ret = lease.deserialize(data_buffer->get_data(), data_buffer->get_capacity(),
                 data_buffer->get_position());
              TBSYS_LOG(DEBUG, "recv grant lease request, lease_time=%ld, lease_interval=%ld, "
                  "ret=%d", lease.lease_time, lease.lease_interval, ret);

              ObResultCode response;
              response.result_code_ = OB_SUCCESS;
              response.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                  out_buffer.get_position());

              int32_t version = 1;
              int32_t channel_id = ob_packet->getChannelId();
              ret = send_response(OB_GRANT_LEASE_RESPONSE, version, out_buffer, connection, channel_id);
              TBSYS_LOG(DEBUG, "send grant lease response, ret=%d", ret);
            }

            return ret;
          }

        private:
          ObPacketFactory factory_;
          ThreadSpecificBuffer response_packet_buffer_;
      };
    }
  }
}

int main(int argc, char** argv)
{
  using namespace oceanbase::tests::updateserver;
  ob_init_memory_pool();
  MockServer mock_server;
  mock_server.start();

  return 0;
}

