/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 *         ob_rpc_stub.cpp is for what ...
 *
 *  Version: $Id: ob_rpc_stub.cpp 11/14/2012 04:34:16 PM qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#include "thread_buffer.h"
#include "ob_rpc_stub.h"
#include "ob_get_param.h"
#include "ob_scan_param.h"

errno_sensitive_pt &tc_errno_sensitive_func()
{
  static __thread errno_sensitive_pt func = NULL;
  return func;
}

bool is_errno_sensitive(const int error)
{
  bool bret = true;
  errno_sensitive_pt func = tc_errno_sensitive_func();
  if (NULL != func)
  {
    bret = func(error);
  }
  return bret;
}

namespace oceanbase
{
  namespace common
  {
    // -----------------------------------------------------------
    // serialization parameter for simple types(bool, int, string, float, double).
    int serialize_param(ObDataBuffer & data_buffer, const bool arg0)
    {
      return  serialization::encode_bool(
          data_buffer.get_data(), data_buffer.get_capacity(),
          data_buffer.get_position(), arg0);
    }

    int serialize_param(ObDataBuffer & data_buffer, const int8_t arg0)
    {
      return  serialization::encode_i8(
          data_buffer.get_data(), data_buffer.get_capacity(),
          data_buffer.get_position(), arg0);
    }

    int serialize_param(ObDataBuffer & data_buffer, const int16_t arg0)
    {
      return  serialization::encode_i16(
          data_buffer.get_data(), data_buffer.get_capacity(),
          data_buffer.get_position(), arg0);
    }

    int serialize_param(ObDataBuffer & data_buffer, const int32_t arg0)
    {
      return  serialization::encode_vi32(
          data_buffer.get_data(), data_buffer.get_capacity(),
          data_buffer.get_position(), arg0);
    }

    int serialize_param(ObDataBuffer & data_buffer, const int64_t arg0)
    {
      return  serialization::encode_vi64(
          data_buffer.get_data(), data_buffer.get_capacity(),
          data_buffer.get_position(), arg0);
    }

    int serialize_param(ObDataBuffer & data_buffer, const float arg0)
    {
      return  serialization::encode_float(
          data_buffer.get_data(), data_buffer.get_capacity(),
          data_buffer.get_position(), arg0);
    }

    int serialize_param(ObDataBuffer & data_buffer, const double arg0)
    {
      return  serialization::encode_double(
          data_buffer.get_data(), data_buffer.get_capacity(),
          data_buffer.get_position(), arg0);
    }

    int serialize_param(ObDataBuffer & data_buffer, const char* arg0)
    {
      return  serialization::encode_vstr(
          data_buffer.get_data(), data_buffer.get_capacity(),
          data_buffer.get_position(), arg0);
    }

    // -----------------------------------------------------------
    // deserialization result for simple types(bool, int, string, float, double).
    int deserialize_result(const ObDataBuffer & data_buffer, int64_t & pos, bool &arg0)
    {
      return  serialization::decode_bool(
          data_buffer.get_data(), data_buffer.get_position(), pos, &arg0);
    }

    int deserialize_result(const ObDataBuffer & data_buffer, int64_t & pos, int8_t &arg0)
    {
      return  serialization::decode_i8(
          data_buffer.get_data(), data_buffer.get_position(), pos, &arg0);
    }

    int deserialize_result(const ObDataBuffer & data_buffer, int64_t & pos, int16_t &arg0)
    {
      return  serialization::decode_i16(
          data_buffer.get_data(), data_buffer.get_position(), pos, &arg0);
    }

    int deserialize_result(const ObDataBuffer & data_buffer, int64_t & pos, int32_t &arg0)
    {
      return  serialization::decode_vi32(
          data_buffer.get_data(), data_buffer.get_position(), pos, &arg0);
    }

    int deserialize_result(const ObDataBuffer & data_buffer, int64_t & pos, int64_t &arg0)
    {
      return  serialization::decode_vi64(
          data_buffer.get_data(), data_buffer.get_position(), pos, &arg0);
    }

    int deserialize_result(const ObDataBuffer & data_buffer, int64_t & pos, float &arg0)
    {
      return  serialization::decode_float(
          data_buffer.get_data(), data_buffer.get_position(), pos, &arg0);
    }

    int deserialize_result(const ObDataBuffer & data_buffer, int64_t & pos, double &arg0)
    {
      return  serialization::decode_double(
          data_buffer.get_data(), data_buffer.get_position(), pos, &arg0);
    }

    int deserialize_result_0(const ObDataBuffer & data_buffer, int64_t & pos, ObResultCode & rc)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = rc.deserialize(data_buffer.get_data(), data_buffer.get_position(), pos)))
      {
        TBSYS_LOG(WARN, "deserialize result code failed. ret:%d, buffer length:%ld, pos:%ld",
            ret, data_buffer.get_position(), pos);
      }
      else
      {
        ret = rc.result_code_;
      }
      return ret;
    }

    // -----------------------------------------------------------
    //  class ObRpcStub
    ObRpcStub::ObRpcStub()
    {
      init_ = false;
      rpc_buffer_ = NULL;
      rpc_frame_ = NULL;
    }

    ObRpcStub::~ObRpcStub()
    {
    }

    int ObRpcStub::init(const ThreadSpecificBuffer * rpc_buffer, const ObClientManager * rpc_frame)
    {
      int ret = OB_SUCCESS;
      if (init_ || (NULL == rpc_buffer) || (NULL == rpc_frame))
      {
        TBSYS_LOG(WARN, "already inited or check input failed:inited[%s], "
            "rpc_buffer[%p], rpc_frame[%p]", (init_? "ture": "false"), rpc_buffer, rpc_frame);
        ret = OB_INPUT_PARAM_ERROR;
      }
      else
      {
        rpc_buffer_ = rpc_buffer;
        rpc_frame_ = rpc_frame;
        init_ = true;
      }
      return ret;
    }

    int ObRpcStub::get_rpc_buffer(ObDataBuffer & data_buffer) const
    {
      int ret = OB_SUCCESS;
      common::ThreadSpecificBuffer::Buffer* rpc_buffer = NULL;
      if (!check_inner_stat())
      {
        TBSYS_LOG(WARN, "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else if (NULL == (rpc_buffer = rpc_buffer_->get_buffer()))
      {
        TBSYS_LOG(ERROR, "get thread rpc buff failed:buffer[%p].", rpc_buffer);
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        rpc_buffer->reset();
        data_buffer.set_data(rpc_buffer->current(), rpc_buffer->remain());
      }
      return ret;
    }

    SEND_PARAM_DEFINE_0;

    int ObRpcStub::send_0_return_0(const ObServer& server,
        const int64_t timeout, const int32_t pcode, const int32_t version) const
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      ObResultCode rc;
      ObDataBuffer data_buffer;
      if (OB_SUCCESS != (ret = get_rpc_buffer(data_buffer)))
      {
        TBSYS_LOG(WARN, "get_rpc_buffer failed with rpc call, ret =%d", ret);
      }
      else if (OB_SUCCESS != (ret = rpc_frame_->send_request(
              server, pcode, version, timeout, data_buffer)))
      {
        TBSYS_LOG(WARN, "send_request failed, ret=%d, server=%s, pcode=%d, version=%d, timeout=%ld",
            ret, to_cstring(server), pcode, version, timeout);
      }
      else if (OB_SUCCESS != (ret = deserialize_result_0(data_buffer, pos, rc)))
      {
        TBSYS_LOG(WARN, "deserialize_result failed, ret=%d, server=%s, pcode=%d, version=%d, timeout=%ld",
            ret, to_cstring(server), pcode, version, timeout);
      }
      return ret;
    }

    // -----------------------------------------------------------
    // class ObDataRpcStub
    ObDataRpcStub::ObDataRpcStub()
    {
    }

    ObDataRpcStub::~ObDataRpcStub()
    {
    }

    int ObDataRpcStub::get(const int64_t timeout, const ObServer & server,
        const ObGetParam & get_param, ObScanner & scanner
         //add lbzhong [Paxos Cluster.Flow.CS] 201607026:b
         , const int32_t cluster_id
         //add:e
         ) const
    {
      //mod lbzhong [Paxos Cluster.Flow.CS] 201607026:b
      //return send_1_return_1(server, timeout, OB_GET_REQUEST, DEFAULT_VERSION, get_param, scanner);
      return send_2_return_1(server, timeout, OB_GET_REQUEST, DEFAULT_VERSION, get_param, cluster_id, scanner);
      //mod:e
    }

    int ObDataRpcStub::scan(const int64_t timeout, const common::ObServer & server,
        const ObScanParam & scan_param, ObScanner & scanner) const
    {
      return send_1_return_1(server, timeout, OB_SCAN_REQUEST, DEFAULT_VERSION, scan_param, scanner);
    }
  }
}
