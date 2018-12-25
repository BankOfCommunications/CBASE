
#include <gtest/gtest.h>

#include "ob_slave_mgr.h"
#include "ob_packet_factory.h"
#include "ob_single_server.h"
#include "ob_packet.h"
#include "ob_log_entry.h"

#include "tbsys.h"

using namespace oceanbase::common;
using namespace tbsys;

namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      class TestObSlaveMgr: public ::testing::Test
      {
      public:
        static const int MOCK_SERVER_LISTEN_PORT = 33248;
        static const int64_t log_sync_timeout = 1000000;
        static const int64_t lease_interval = 15000000;
        static const int64_t lease_reserved_time = 12000000;
        static const int64_t send_retry_times = 1;
      public:
        virtual void SetUp()
        {
          memset(log_data, 'a', BUFSIZ);
        }

        virtual void TearDown()
        {
        }

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
              set_default_queue_size(100);
              set_thread_count(1);
              ObSingleServer::initialize();

              return OB_SUCCESS;
            }

            virtual int do_request(ObPacket* base_packet)
            {
              int ret = OB_SUCCESS;
              ObPacket* ob_packet = base_packet;
              int32_t packet_code = ob_packet->get_packet_code();
              int32_t version = ob_packet->get_api_version();
              int32_t channel_id = ob_packet->get_channel_id();
              ret = ob_packet->deserialize();

              TBSYS_LOG(INFO, "recv packet with packet_code[%d] version[%d] channel_id[%d]",
                  packet_code, version, channel_id);

              if (OB_SUCCESS == ret)
              {
                switch (packet_code)
                {
                  case OB_SEND_LOG:
                    handle_sendlog(ob_packet);
                    break;
                };
              }

              return ret;
            }

            int handle_sendlog(ObPacket* ob_packet)
            {
              int ret = OB_SUCCESS;

              ObDataBuffer* data = ob_packet->get_buffer();
              if (NULL == data)
              {
                TBSYS_LOG(ERROR, "data is NUll should not reach this");
                ret = OB_ERROR;
              }
              else
              {
                EXPECT_EQ(0, memcmp(data->get_data() + data->get_position(), log_data, BUFSIZ));
                ObResultCode result_msg;
                easy_request_t* connection = ob_packet->get_request();
                ThreadSpecificBuffer::Buffer* thread_buffer = response_packet_buffer_.get_buffer();
                if (NULL != thread_buffer)
                {
                  thread_buffer->reset();
                  ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());

                  result_msg.result_code_ = OB_SUCCESS;
                  result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                      out_buffer.get_position());

                  TBSYS_LOG(DEBUG, "handle tablets report packet");

                  int32_t version = 1;
                  int32_t channel_id = ob_packet->get_channel_id();
                  ret = send_response(OB_SEND_LOG_RES, version, out_buffer, connection, channel_id);
                }
                else
                {
                  TBSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
                  ret = OB_ERROR;
                }
              }

              return OB_SUCCESS;
            }

          private:
            ObPacketFactory factory_;
            ThreadSpecificBuffer response_packet_buffer_;
        };

        class MockServerRunner : public tbsys::Runnable
        {
          public:
            virtual void run(tbsys::CThread *thread, void *arg)
            {
              UNUSED(thread);
              UNUSED(arg);
              MockServer mock_server;
              mock_server.start();
            }
        };

        static char log_data[BUFSIZ];
      };

      char TestObSlaveMgr::log_data[BUFSIZ];

      TEST_F(TestObSlaveMgr, test_init)
      {
      }

      TEST_F(TestObSlaveMgr, test_add_server)
      {
        ObSlaveMgr slave_mgr;
        ObRoleMgr role_mgr;
        role_mgr.set_state(ObRoleMgr::ACTIVE);
        EXPECT_EQ(OB_INVALID_ARGUMENT, slave_mgr.init(&role_mgr, 0, NULL, log_sync_timeout, lease_interval, lease_reserved_time, send_retry_times));
        ObServer server1(ObServer::IPV4, "localhost", MOCK_SERVER_LISTEN_PORT);
        ObServer server2(ObServer::IPV4, "localhost", MOCK_SERVER_LISTEN_PORT);
        ObServer server3(ObServer::IPV4, "10.10.10.10", MOCK_SERVER_LISTEN_PORT);
        EXPECT_EQ(OB_SUCCESS, slave_mgr.add_server(server1));
        EXPECT_EQ(1, slave_mgr.get_num());
        EXPECT_EQ(OB_SUCCESS, slave_mgr.delete_server(server1));
        EXPECT_EQ(0, slave_mgr.get_num());
        EXPECT_EQ(OB_ERROR, slave_mgr.delete_server(server2));
        EXPECT_EQ(OB_SUCCESS, slave_mgr.add_server(server1));
        EXPECT_EQ(OB_SUCCESS, slave_mgr.add_server(server2));
        EXPECT_EQ(OB_SUCCESS, slave_mgr.add_server(server3));
        EXPECT_EQ(2, slave_mgr.get_num());
        EXPECT_EQ(OB_SUCCESS, slave_mgr.delete_server(server3));
        EXPECT_EQ(OB_SUCCESS, slave_mgr.delete_server(server1));
        EXPECT_EQ(OB_ERROR, slave_mgr.delete_server(server2));
      }

      TEST_F(TestObSlaveMgr, test_delete_server)
      {
      }

/*
      TEST_F(TestObSlaveMgr, test_send_data)
      {
        ObSlaveMgr slave_mgr;
        ObRoleMgr role_mgr;
        role_mgr.set_state(ObRoleMgr::ACTIVE);
        ObServer server1(ObServer::IPV4, "localhost", MOCK_SERVER_LISTEN_PORT);
        ObServer server2(ObServer::IPV4, "10.32.22.130", MOCK_SERVER_LISTEN_PORT);
        //ObServer server1(ObServer::IPV4, "localhost", 8888);

        ObClientManager client_mgr;
        ObPacketFactory factory;
        tbnet::Transport transport;
        tbnet::DefaultPacketStreamer streamer;
        streamer.setPacketFactory(&factory);
        EXPECT_EQ(OB_SUCCESS, client_mgr.initialize(&transport, &streamer));

        transport.start();

        ObCommonRpcStub rpc_stub;
        EXPECT_EQ(OB_SUCCESS, rpc_stub.init(&client_mgr));
        EXPECT_EQ(OB_SUCCESS, slave_mgr.init(&role_mgr, ObServer::convert_ipv4_addr("10.232.35.40"), &rpc_stub, log_sync_timeout, lease_interval, lease_reserved_time, send_retry_times));
        EXPECT_EQ(OB_INIT_TWICE, slave_mgr.init(&role_mgr, 0, &rpc_stub, log_sync_timeout, lease_interval, lease_reserved_time, send_retry_times));

        EXPECT_EQ(OB_SUCCESS, slave_mgr.add_server(server1));

        tbsys::CThread test_slave_thread;
        MockServerRunner test_slave;
        test_slave_thread.start(&test_slave, NULL);
        usleep(20000);

        EXPECT_EQ(OB_SUCCESS, slave_mgr.send_data(log_data, BUFSIZ));

        EXPECT_EQ(OB_SUCCESS, slave_mgr.add_server(server2));
        EXPECT_EQ(2, slave_mgr.get_num());
        EXPECT_EQ(OB_PARTIAL_FAILED, slave_mgr.send_data(log_data, BUFSIZ));
        EXPECT_EQ(1, slave_mgr.get_num());
        EXPECT_EQ(OB_SUCCESS, slave_mgr.send_data(log_data, BUFSIZ));
        EXPECT_EQ(1, slave_mgr.get_num());

        transport.stop();
        transport.wait();
      }
*/

      TEST_F(TestObSlaveMgr, test_server_node)
      {
        ObSlaveMgr::ServerNode* item = (ObSlaveMgr::ServerNode*)ob_malloc(sizeof(ObSlaveMgr::ServerNode), ObModIds::TEST);
        item->reset();
        ASSERT_EQ(item->lease.lease_time, 0);
        ASSERT_EQ(item->lease.lease_interval, 0);
        ASSERT_EQ(item->lease.renew_interval, 0);
      }

/*
      TEST_F(TestObSlaveMgr, test_server_race_condition)
      {
        ObSlaveMgr slave_mgr;
        ObRoleMgr role_mgr;
        role_mgr.set_state(ObRoleMgr::ACTIVE);
        ObServer server1(ObServer::IPV4, "localhost", 3391);

        ObClientManager client_mgr;
        ObPacketFactory factory;
        tbnet::Transport transport;
        tbnet::DefaultPacketStreamer streamer;
        streamer.setPacketFactory(&factory);
        EXPECT_EQ(OB_SUCCESS, client_mgr.initialize(&transport, &streamer));

        ObCommonRpcStub rpc_stub;
        EXPECT_EQ(OB_SUCCESS, rpc_stub.init(&client_mgr));
        EXPECT_EQ(OB_SUCCESS, slave_mgr.init(&role_mgr, ObServer::convert_ipv4_addr("10.232.35.40"), &rpc_stub, log_sync_timeout, lease_interval, lease_reserved_time, send_retry_times));

        EXPECT_EQ(OB_SUCCESS, slave_mgr.add_server(server1));

        transport.start();

        ObSlaveMgr::ServerNode* node = slave_mgr.find_server_(server1);

        node->lease.lease_time = tbsys::CTimeUtil::getTime();
        node->lease.lease_interval = lease_interval;
        node->lease.renew_interval = lease_reserved_time;

        int64_t flag = 0;

        class ExtendLeaseThread : public tbsys::CDefaultRunnable
        {
        public:
          ObSlaveMgr *slave_mgr_;
          ObServer *server1_;
          int64_t *flag_;
          virtual void run(CThread *thread, void *arg)
          {
            sleep(1);
            ObLease lease;
            slave_mgr_->extend_lease(*server1_, lease);
            sleep(1);
            *flag_ = 1;
          }
        } el_thread;

        el_thread.slave_mgr_ = &slave_mgr;
        el_thread.server1_ = &server1;
        el_thread.flag_ = &flag;
        el_thread.start();

        EXPECT_EQ(OB_PARTIAL_FAILED, slave_mgr.send_data(log_data, BUFSIZ));
        EXPECT_EQ(flag, 1);

        transport.stop();
        transport.wait();
      }
*/

    }
  }
}

int main(int argc, char** argv)
{
  TBSYS_LOGGER.setLogLevel("DEBUG");
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
