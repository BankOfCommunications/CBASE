/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 */


#include "tbnet.h"

#include "ob_define.h"
#include "ob_packet_factory.h"
#include "ob_client_manager.h"
#include "ob_packet.h"
#include "thread_buffer.h"
#include "ob_server.h"
#include "serialization.h"
#include "data_buffer.h"
#include "ob_result.h"
#include "ob_root_server_rpc.h"

using namespace oceanbase::common;
using namespace oceanbase::chunkserver;
using namespace oceanbase::common::serialization;

class BaseClient
{
  public:
    int initialize();
    int destory();
    int wait();
    
    ObClientManager * get_rpc()
    {
      return &client_;
    }

  public:
    tbnet::DefaultPacketStreamer streamer_;
    tbnet::Transport transport_;
    ObPacketFactory factory_;
    ObClientManager client_;
};

int BaseClient::initialize()
{
  ob_init_memory_pool();
  streamer_.setPacketFactory(&factory_);
  client_.initialize(&transport_, &streamer_);
  return transport_.start();
}

int BaseClient::destory()
{
  transport_.stop();
  return transport_.wait();
}

int BaseClient::wait()
{
  return transport_.wait();
}

int main(int argc, char* argv[])
{
  ObServer cs;
  cs.set_ipv4_addr("localhost", 3500);

  BaseClient client;
  client.initialize();
  
  ObServer root;
  root.set_ipv4_addr("10.232.35.40", 2500);

  ThreadSpecificBuffer buffer;
  ObRootServerRpcStub server;
  
  do
  {
    int ret = server.init(root, client.get_rpc());
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "init failed");
      break;
    } 

    int64_t timeout = 2000;
    int64_t timestamp = 0;
    // heartbeat 
    ret = server.renew_heartbeat(timeout, cs, timestamp); 
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "renew heartbeat failed:ret[%d]", ret);
      break;
    }
    else
    {
      printf("get new lease time[%ld]\n", timestamp);
    }

    sleep(1);
    
    // fetch schema
    timestamp = 1282900503726484;
    ObSchemaManager schema;
    ret = server.fetch_schema(timeout, timestamp, schema);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "fetch schema failed:ret[%d]", ret);
      break;
    }
    else
    {
      printf("fetch schema succ:appname[%s]\n", schema.get_app_name());
      schema.print_info();
    }
  } while(0);
  client.destory();
  return 0;
}

