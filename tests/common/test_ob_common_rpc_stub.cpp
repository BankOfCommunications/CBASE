/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: test_rpc_stub.cpp,v 0.1 2010/09/28 11:49:27 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include <iostream>
#include <sstream>
#include <algorithm>
#include <gtest/gtest.h>
#include "tblog.h"
//#include "test_helper.h"
//#include "test_init.h"
#include "ob_packet_factory.h"
#include "ob_common_rpc_stub.h"

using namespace std;
using namespace oceanbase::common;

const int32_t MOCK_SERVER_LISTEN_PORT = 8888;

namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      class TesObCommontRpcStub : public ::testing::Test
      {
        public:
          virtual void SetUp()
          {

          }

          virtual void TearDown()
          {

          }
      };

      TEST_F(TesObCommontRpcStub, test_send_log)
      {
        int err = OB_SUCCESS;
        ObCommonRpcStub rpc_stub;

        ObServer dst_host;
        const char* dst_addr = "localhost";
        dst_host.set_ipv4_addr(dst_addr, MOCK_SERVER_LISTEN_PORT);

        // init client mgr
        ObClientManager client_mgr;
        ObPacketFactory factory;
        tbnet::Transport transport;
        tbnet::DefaultPacketStreamer streamer;
        streamer.setPacketFactory(&factory);
        EXPECT_EQ(OB_SUCCESS, client_mgr.initialize(&transport, &streamer));

        err = rpc_stub.init(&client_mgr);
        ASSERT_EQ(0, err);

        transport.start();
        //usleep(100 * 1000);

        ObDataBuffer log_data;
        char data[1024];
        strcpy(data, "log_data");
        log_data.set_data(data, sizeof(data));
        log_data.get_position() = (int64_t) strlen(data);

        err = rpc_stub.send_log(dst_host, log_data, 1000 * 10);
        ASSERT_EQ(0, err);
        transport.stop();
        transport.wait();
      }

      TEST_F(TesObCommontRpcStub, test_renew_lease)
      {
        int err = OB_SUCCESS;
        ObCommonRpcStub rpc_stub;

        ObServer dst_host;
        const char* dst_addr = "localhost";
        dst_host.set_ipv4_addr(dst_addr, MOCK_SERVER_LISTEN_PORT);

        // init client mgr
        ObClientManager client_mgr;
        ObPacketFactory factory;
        tbnet::Transport transport;
        tbnet::DefaultPacketStreamer streamer;
        streamer.setPacketFactory(&factory);
        EXPECT_EQ(OB_SUCCESS, client_mgr.initialize(&transport, &streamer));

        err = rpc_stub.init(&client_mgr);
        ASSERT_EQ(0, err);

        transport.start();
        //usleep(100 * 1000);

        ObServer slave;
        const char* slave_addr = "localhost";
        slave.set_ipv4_addr(slave_addr, 7777);
        err = rpc_stub.renew_lease(dst_host, slave, 1000 * 10);
        ASSERT_EQ(0, err);
        transport.stop();
        transport.wait();
      }

      TEST_F(TesObCommontRpcStub, test_grant_lease)
      {
        int err = OB_SUCCESS;
        ObCommonRpcStub rpc_stub;

        ObServer dst_host;
        const char* dst_addr = "localhost";
        dst_host.set_ipv4_addr(dst_addr, MOCK_SERVER_LISTEN_PORT);

        // init client mgr
        ObClientManager client_mgr;
        ObPacketFactory factory;
        tbnet::Transport transport;
        tbnet::DefaultPacketStreamer streamer;
        streamer.setPacketFactory(&factory);
        EXPECT_EQ(OB_SUCCESS, client_mgr.initialize(&transport, &streamer));

        err = rpc_stub.init(&client_mgr);
        ASSERT_EQ(0, err);

        transport.start();
        //usleep(100 * 1000);

        ObLease lease;
        lease.lease_time = 10000;
        lease.lease_interval = 10000000;
        err = rpc_stub.grant_lease(dst_host, lease, 1000 * 10);
        ASSERT_EQ(0, err);
        transport.stop();
        transport.wait();
      }

    } // end namespace updateserver
  } // end namespace tests
} // end namespace oceanbase


int main(int argc, char** argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

