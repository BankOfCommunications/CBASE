/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: mock_client.h,v 0.1 2010/10/08 16:09:06 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_MOCK_CLIENT_H__
#define __OCEANBASE_CHUNKSERVER_MOCK_CLIENT_H__

#include "tbnet.h"
#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "common/ob_server.h"
#include "common/ob_client_manager.h"
#include "common/ob_schema.h"
#include "common/ob_result.h"
#include "common/ob_packet_factory.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_mutator.h"

using namespace oceanbase::common;

class BaseClient
{
  public:
    BaseClient()
    {
    }
    virtual ~BaseClient()
    {
    }
  public:
    virtual int initialize();
    virtual int destroy();
    virtual int wait();
    
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

int BaseClient::destroy()
{
  transport_.stop();
  return transport_.wait();
}

int BaseClient::wait()
{
  return transport_.wait();
}

class MockClient : public BaseClient
{
  private:
    static const int64_t BUF_SIZE = 2 * 1024 * 1024;

  public:
    MockClient()
    {
      buffer_ = new char[BUF_SIZE];
    }

    ~MockClient()
    {
      delete[] buffer_;
    }

    int init(ObServer& server)
    {
      int err = OB_SUCCESS;

      initialize();
      server_ = server;

      return err;
    }


  public:
    int ups_apply(ObMutator &mutator, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      ObDataBuffer data_buff;
      data_buff.set_data(buffer_, BUF_SIZE);

      ObClientManager* client_mgr = get_rpc();
      ret = mutator.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
      if (OB_SUCCESS == ret)
      {
        ret = client_mgr->send_request(server_, OB_WRITE, MY_VERSION, timeout, data_buff);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        // deserialize the response code
        int64_t pos = 0;
        if (OB_SUCCESS == ret)
        {
          ObResultCode result_code;
          ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d].", pos, ret);
          }
          else
          {
            ret = result_code.result_code_;
          }
        }
      }
      return ret;
    }

    //int ups_scan(ObDataBuffer& in_data, ObDataBuffer& out_data, const int64_t timeout)
    int ups_scan(ObScanParam& scan_param, ObScanner& scanner, const int64_t timeout)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      ObDataBuffer data_buff;
      data_buff.set_data(buffer_, BUF_SIZE);

      ObClientManager* client_mgr = get_rpc();
      
      err = scan_param.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
      if (OB_SUCCESS == err)
      {
        err = client_mgr->send_request(server_, OB_SCAN_REQUEST, MY_VERSION, timeout, data_buff);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to send request, err=%d", err);
        }
      }

      if (OB_SUCCESS == err)
      {
        // deserialize the response code
        int64_t pos = 0;
        if (OB_SUCCESS == err)
        {
          ObResultCode result_code;
          err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
          }
          else
          {
            err = result_code.result_code_;
          }
        }
        // deserialize scanner
        if (OB_SUCCESS == err)
        {
          err = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(ERROR, "deserialize scanner from buff failed, "
                "pos[%ld], err[%s].", pos, err);
          }
        }
      }

      return err;
    }

  private:
    ObServer server_;
    char* buffer_;
};


#endif //__MOCK_CLIENT_H__

