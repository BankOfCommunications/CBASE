/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: mock_client.h,v 0.1 2011/12/23 16:26:01 zhidong Exp $
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef OCEANBASE_MOCK_CLIENT_H_
#define OCEANBASE_MOCK_CLIENT_H_

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
#include "common/thread_buffer.h"

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

inline int BaseClient::initialize()
{
  ob_init_memory_pool();
  streamer_.setPacketFactory(&factory_);
  client_.initialize(&transport_, &streamer_);
  return transport_.start();
}

inline int BaseClient::destroy()
{
  transport_.stop();
  return transport_.wait();
}

inline int BaseClient::wait()
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
    }

    ~MockClient()
    {
    }

    int init(ObServer& server)
    {
      int err = OB_SUCCESS;
      initialize();
      server_ = server;
      return err;
    }
  public:
    template <class Input>
    int send_request(const int pcode, const Input & param, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      ObDataBuffer data_buff;
      ret = get_thread_buffer(data_buff);
      ObClientManager* client_mgr = get_rpc();
      ret = param.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
      if (OB_SUCCESS == ret)
      {
        ret = client_mgr->send_request(server_, pcode, MY_VERSION, timeout, data_buff);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        int64_t pos = 0;
        if (OB_SUCCESS == ret)
        {
          ObResultCode result_code;
          ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
          }
          else
          {
            ret = result_code.result_code_;
          }
        }
      }
      return ret;
    }

    template <class Input, class Output>
    int send_request(const int pcode, const Input & param, Output & result, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      ObDataBuffer data_buff;
      ret = get_thread_buffer(data_buff);
      ObClientManager* client_mgr = get_rpc();
      ret = param.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
      if (OB_SUCCESS == ret)
      {
        ret = client_mgr->send_request(server_, pcode, MY_VERSION, timeout, data_buff);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        int64_t pos = 0;
        if (OB_SUCCESS == ret)
        {
          ObResultCode result_code;
          ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
          }
          else
          {
            ret = result_code.result_code_;
            if (OB_SUCCESS == ret
                && OB_SUCCESS != (ret = result.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
            {
              TBSYS_LOG(ERROR, "deserialize result data failed:pos[%ld], ret[%d]", pos, ret);
            }
          }
        }
      }
      return ret;
    }
  private:
    int get_thread_buffer(ObDataBuffer& data_buff)
    {
      int err = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer * buffer = rpc_buffer_.get_buffer();
      if (NULL == buffer)
      {
        err = OB_ERROR;
        TBSYS_LOG(WARN, "check get buffer failed");
      }
      else
      {
        buffer->reset();
        data_buff.set_data(buffer->current(), buffer->remain());
      }
      return err;
    }
  private:
    ObServer server_;
    ThreadSpecificBuffer rpc_buffer_;
};


#endif // OCEANBASE_MOCK_CLIENT_H_

