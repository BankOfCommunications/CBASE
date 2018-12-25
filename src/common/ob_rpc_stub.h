/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 *         ob_rpc_stub.h is for what ...
 *
 *  Version: $Id: ob_rpc_stub.h 11/14/2012 04:29:53 PM qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */

#ifndef OCEANBASE_COMMON_RPC_STUB_H_
#define OCEANBASE_COMMON_RPC_STUB_H_

#include "ob_server.h"
#include "data_buffer.h"
#include "serialization.h"
#include "ob_result.h"
#include "utility.h"
#include "ob_rpc_macros.h"
#include "ob_client_manager.h"



namespace oceanbase
{
  namespace common
  {
    class ThreadSpecificBuffer;
    class ObClientManager;
    class ObGetParam;
    class ObScanParam;
    class ObScanner;

    // -----------------------------------------------------------
    // serialization parameter for simple types(bool, int, string, float, double).
    int serialize_param(ObDataBuffer & data_buffer, const bool arg0);
    int serialize_param(ObDataBuffer & data_buffer, const int8_t arg0);
    int serialize_param(ObDataBuffer & data_buffer, const int16_t arg0);
    int serialize_param(ObDataBuffer & data_buffer, const int32_t arg0);
    int serialize_param(ObDataBuffer & data_buffer, const int64_t arg0);
    int serialize_param(ObDataBuffer & data_buffer, const float arg0);
    int serialize_param(ObDataBuffer & data_buffer, const double arg0);
    int serialize_param(ObDataBuffer & data_buffer, const char* arg0);

    // serialization parameter for general types.
    template <typename Arg0>
      int serialize_param(ObDataBuffer & data_buffer, const Arg0 & arg0)
      {
        return arg0.serialize( data_buffer.get_data(),
            data_buffer.get_capacity(), data_buffer.get_position());
      }

    template <typename Arg0>
      int serialize_param_1(ObDataBuffer & data_buffer, const Arg0 & arg0)
      {
        int ret = OB_SUCCESS;
        if (OB_SUCCESS != (ret = serialize_param(data_buffer, arg0)))
        {
          TBSYS_LOG(WARN, "serialize_param arg0 failed. ret=%d, buffer cap:%ld, pos:%ld",
              ret, data_buffer.get_capacity(), data_buffer.get_position());
        }
        return ret;
      }

    SERIALIZE_PARAM_DEFINE(2);
    SERIALIZE_PARAM_DEFINE(3);
    SERIALIZE_PARAM_DEFINE(4);
    SERIALIZE_PARAM_DEFINE(5);
    SERIALIZE_PARAM_DEFINE(6);

    // -----------------------------------------------------------
    // serialization parameter for simple types(bool, int, string, float, double).
    int deserialize_result(const ObDataBuffer & data_buffer, int64_t & pos, bool &arg0);
    int deserialize_result(const ObDataBuffer & data_buffer, int64_t & pos, int8_t &arg0);
    int deserialize_result(const ObDataBuffer & data_buffer, int64_t & pos, int16_t &arg0);
    int deserialize_result(const ObDataBuffer & data_buffer, int64_t & pos, int32_t &arg0);
    int deserialize_result(const ObDataBuffer & data_buffer, int64_t & pos, int64_t &arg0);
    int deserialize_result(const ObDataBuffer & data_buffer, int64_t & pos, float &arg0);
    int deserialize_result(const ObDataBuffer & data_buffer, int64_t & pos, double &arg0);

    template <typename Arg0>
      int deserialize_result(const ObDataBuffer & data_buffer, int64_t & pos, Arg0 & arg0)
      {
        return arg0.deserialize(data_buffer.get_data(), data_buffer.get_position(), pos);
      }

    int deserialize_result_0(const ObDataBuffer & data_buffer, int64_t & pos, ObResultCode & rc);

    template <typename Arg0>
    int deserialize_result_1(const ObDataBuffer & data_buffer, int64_t & pos, ObResultCode & rc, Arg0 & arg0)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = deserialize_result_0(data_buffer, pos, rc))
          && is_errno_sensitive(ret))
      {
        TBSYS_LOG(WARN, "deserialize_result_0 failed. ret:%d, buffer length:%ld, pos:%ld",
            ret, data_buffer.get_position(), pos);
      }
      else if (OB_SUCCESS != (ret = deserialize_result(data_buffer, pos, arg0)))
      {
        TBSYS_LOG(WARN, "deserialize arg0 failed. ret:%d, buffer length:%ld, pos:%ld",
            ret, data_buffer.get_position(), pos);
      }
      return ret;
    }

    DESERIALIZE_RESULT_DEFINE(2);
    DESERIALIZE_RESULT_DEFINE(3);
    DESERIALIZE_RESULT_DEFINE(4);
    DESERIALIZE_RESULT_DEFINE(5);
    DESERIALIZE_RESULT_DEFINE(6);

    // -----------------------------------------------------------
    // this class encapsulates network rpc interface as bottom layer,
    // and it only take charge of "one" rpc call.
    // if u need other operational work, please use rpc_proxy for interaction
    class ObRpcStub
    {
      public:
        // default cmd version
        static const int32_t DEFAULT_VERSION = 1;

      public:
        ObRpcStub();
        virtual ~ObRpcStub();

      public:
        // warning: rpc_buff should be only used by rpc stub for reset
        // param  @rpc_buff rpc send and response buff
        //        @rpc_frame client manger for network interaction
        int init(const ThreadSpecificBuffer * rpc_buffer, const ObClientManager * rpc_frame);


      protected:
        // check inner stat
        inline bool check_inner_stat(void) const
        {
          // check server and packet version
          return (init_ && (NULL != rpc_buffer_) && (NULL != rpc_frame_));
        }

        // get frame rpc data buffer from the thread buffer
        // waring:reset the buffer before serialize any packet
        int get_rpc_buffer(ObDataBuffer & data_buffer) const;

        // serialize parameters in thread specific data buffer one by one.
        /* template <typename Arg0>
          int send_param_1(ObDataBuffer & data_buffer, const ObServer& server,
              const int64_t timeout, const int32_t pcode, const int32_t version,
              const Arg0 & arg0) const;*/

        SEND_PARAM_DECLARE_0;
        SEND_PARAM_DECLARE(1);
        SEND_PARAM_DECLARE(2);
        SEND_PARAM_DECLARE(3);
        SEND_PARAM_DECLARE(4);
        SEND_PARAM_DECLARE(5);
        SEND_PARAM_DECLARE(6);

        POST_REQUEST_DECLARE(1);
        POST_REQUEST_DECLARE(2);
        POST_REQUEST_DECLARE(3);
        POST_REQUEST_DECLARE(4);
        POST_REQUEST_DECLARE(5);
        POST_REQUEST_DECLARE(6);


        // rpc call for frequently-used prototype
        /* template <typename Arg0, typename Arg1, ...>
          int send_n_return_m(const ObServer& server, const int64_t timeout,
              const int32_t pcode, const int32_t version, const Arg0 & arg0, ...) const; */
        int send_0_return_0(const ObServer& server,
            const int64_t timeout, const int32_t pcode, const int32_t version) const;

        // 1. send_0_return_(1~6)
        RPC_CALL_DECLARE_SND_0(1);
        RPC_CALL_DECLARE_SND_0(2);
        RPC_CALL_DECLARE_SND_0(3);
        RPC_CALL_DECLARE_SND_0(4);
        RPC_CALL_DECLARE_SND_0(5);
        RPC_CALL_DECLARE_SND_0(6);

        // 2. send_(1~6)_return_0
        RPC_CALL_DECLARE_RT_0(1);
        RPC_CALL_DECLARE_RT_0(2);
        RPC_CALL_DECLARE_RT_0(3);
        RPC_CALL_DECLARE_RT_0(4);
        RPC_CALL_DECLARE_RT_0(5);
        RPC_CALL_DECLARE_RT_0(6);

        // 3. send_(1~6)_return_1
        RPC_CALL_DECLARE(1, 1);
        RPC_CALL_DECLARE(2, 1);
        RPC_CALL_DECLARE(3, 1);
        RPC_CALL_DECLARE(4, 1);
        RPC_CALL_DECLARE(5, 1);
        RPC_CALL_DECLARE(6, 1);

        // 4. send_(1~6)_return_2
        RPC_CALL_DECLARE(1, 2);
        RPC_CALL_DECLARE(2, 2);
        RPC_CALL_DECLARE(3, 2);
        RPC_CALL_DECLARE(4, 2);
        RPC_CALL_DECLARE(5, 2);
        RPC_CALL_DECLARE(6, 2);

        // 4. send_(1~6)_return_3
        RPC_CALL_DECLARE(1, 3);
        RPC_CALL_DECLARE(2, 3);
        RPC_CALL_DECLARE(3, 3);
        RPC_CALL_DECLARE(4, 3);
        RPC_CALL_DECLARE(5, 3);
        RPC_CALL_DECLARE(6, 3);


      protected:
        bool init_;                                         // init stat for inner check
        const ThreadSpecificBuffer * rpc_buffer_;   // rpc thread buffer
        const ObClientManager * rpc_frame_;         // rpc frame for send request
    };

    /* template <typename Arg0>
      int ObRpcStub::send_param_1(ObDataBuffer & data_buffer, const ObServer& server,
          const int64_t timeout, const int32_t pcode, const int32_t version, const Arg0 & arg0) const
      {
        int ret = OB_SUCCESS;

        if (OB_SUCCESS != (ret = get_rpc_buffer(data_buffer)))
        {
          TBSYS_LOG(WARN, "get_rpc_buffer error with rpc call, ret =%d", ret);
        }
        else if (OB_SUCCESS != (ret = serialize_param_1(data_buffer, arg0)))
        {
          TBSYS_LOG(WARN, "serialize_param_1 error.");
        }
        else if (OB_SUCCESS != (ret = rpc_frame_->send_request(
                server, pcode, version, timeout, data_buffer)))
        {
          TBSYS_LOG(WARN, "send_request error, server=%s, pcode=%d, version=%d, timeout=%ld",
              to_cstring(server), pcode, version, timeout);
        }
        return ret;
      } */
    SEND_PARAM_DEFINE(1);
    SEND_PARAM_DEFINE(2);
    SEND_PARAM_DEFINE(3);
    SEND_PARAM_DEFINE(4);
    SEND_PARAM_DEFINE(5);
    SEND_PARAM_DEFINE(6);

    POST_REQUEST_DEFINE(1);
    POST_REQUEST_DEFINE(2);
    POST_REQUEST_DEFINE(3);
    POST_REQUEST_DEFINE(4);
    POST_REQUEST_DEFINE(5);
    POST_REQUEST_DEFINE(6);


    /*
    template <typename Arg0>
      int ObRpcStub::send_1_return_0(const ObServer& server, const int64_t timeout,
          const int32_t pcode, const int32_t version, const Arg0 & arg0) const
      {
        int ret = OB_SUCCESS;
        int64_t pos = 0;
        ObResultCode rc;
        ObDataBuffer data_buffer;

        if (OB_SUCCESS != (ret = send_param_1(data_buffer, server, timeout, pcode, version, arg0)))
        {
        }
        else if (OB_SUCCESS != (ret = deserialize_result_0(data_buffer, rc, pos)))
        {
        }

        return ret;
      } */

    // 1. send_0_return_(1~6)
    RPC_CALL_DEFINE_SND_0(1);
    RPC_CALL_DEFINE_SND_0(2);
    RPC_CALL_DEFINE_SND_0(3);
    RPC_CALL_DEFINE_SND_0(4);
    RPC_CALL_DEFINE_SND_0(5);
    RPC_CALL_DEFINE_SND_0(6);

    // 2. send_(1~6)_return_0
    RPC_CALL_DEFINE_RT_0(1);
    RPC_CALL_DEFINE_RT_0(2);
    RPC_CALL_DEFINE_RT_0(3);
    RPC_CALL_DEFINE_RT_0(4);
    RPC_CALL_DEFINE_RT_0(5);
    RPC_CALL_DEFINE_RT_0(6);

    // 3. send_(1~6)_return_1
    RPC_CALL_DEFINE(1, 1);
    RPC_CALL_DEFINE(2, 1);
    RPC_CALL_DEFINE(3, 1);
    RPC_CALL_DEFINE(4, 1);
    RPC_CALL_DEFINE(5, 1);
    RPC_CALL_DEFINE(6, 1);

    // 4. send_(1~6)_return_2
    RPC_CALL_DEFINE(1, 2);
    RPC_CALL_DEFINE(2, 2);
    RPC_CALL_DEFINE(3, 2);
    RPC_CALL_DEFINE(4, 2);
    RPC_CALL_DEFINE(5, 2);
    RPC_CALL_DEFINE(6, 2);

    // 5. send_(1~6)_return_3
    RPC_CALL_DEFINE(1, 3);
    RPC_CALL_DEFINE(2, 3);
    RPC_CALL_DEFINE(3, 3);
    RPC_CALL_DEFINE(4, 3);
    RPC_CALL_DEFINE(5, 3);
    RPC_CALL_DEFINE(6, 3);

    // -----------------------------------------------------------
    // class ObDataRpcStub
    class ObDataRpcStub : public ObRpcStub
    {
      public:
        ObDataRpcStub();
        ~ObDataRpcStub();
      public:
        int get(const int64_t timeout, const ObServer & server,
            const ObGetParam & get_param, ObScanner & scanner
                //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
                , const int32_t cluster_id
                //add:e
                ) const;
        int scan(const int64_t timeout, const common::ObServer & server,
            const ObScanParam & scan_param, ObScanner & scanner) const;
    };


  }
}


#endif // OCEANBASE_COMMON_RPC_STUB_H_
