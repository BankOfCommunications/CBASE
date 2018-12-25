/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rs_ups_message.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_rs_ups_message.h"
#include <tbsys.h>

using namespace oceanbase::common;

int ObMsgUpsHeartbeat::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ups_master_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, self_lease_)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  //add chujiajia [Paxos rs_election] 20151229:b
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, ups_quorum_)))
  {
      TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  //add:e
  else if (OB_SUCCESS != (ret = obi_role_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, schema_version_)))
  {
    TBSYS_LOG(ERROR, "serailize schema_version fail, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, config_version_)))
  {
    TBSYS_LOG(ERROR, "serailize config_version fail, err=%d", ret);
  }
  return ret;
}

int ObMsgUpsHeartbeat::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ups_master_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &self_lease_)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  //add chujiajia [Paxos rs_election] 20151229:b
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &ups_quorum_)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  //add:e
  else if (OB_SUCCESS != (ret = obi_role_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &schema_version_)))
  {
    TBSYS_LOG(ERROR, "deserialize schema_version fail, ret: [%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &config_version_)))
  {
    TBSYS_LOG(ERROR, "deserialize config_version fail, ret: [%d]", ret);
  }
  return ret;
}

int ObMsgUpsHeartbeatResp::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len,
                                                           pos, (int32_t)status_)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = obi_role_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  //add chujiajia [Paxos rs_election] 20151229:b
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, quorum_scale_)))
  {
    TBSYS_LOG(ERROR, "failed to serialize quorum_scale_, err=%d", ret);
  }
  //add:e
  return ret;
}

int ObMsgUpsHeartbeatResp::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int32_t status = -1;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &status)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = obi_role_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize, err=%d", ret);
  }
  //add chujiajia [Paxos rs_election] 20151229:b
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &quorum_scale_)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize quorum_scale_, err=%d", ret);
  }
  //add:e
  else
  {
    if (SYNC == status)
    {
      status_ = SYNC;
    }
    else if (NOTSYNC == status)
    {
      status_ = NOTSYNC;
    }
    else
    {
      TBSYS_LOG(ERROR, "unknown status=%d", status);
      ret = OB_INVALID_ARGUMENT;
    }
  }
  return ret;
}

int ObMsgUpsRegister::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len,
                                                           pos, inner_port_)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len,
                                                           pos, log_seq_num_)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len,
                                                           pos, lease_)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vstr(buf, buf_len,
                                                           pos, server_version_)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  return ret;
}

int ObMsgUpsRegister::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t server_version_length = 0;
  const char * server_version_temp = NULL;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &inner_port_)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &log_seq_num_)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &lease_)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (NULL == (server_version_temp = serialization::decode_vstr(buf, data_len, pos, &server_version_length)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    memcpy(server_version_, server_version_temp, server_version_length);
  }
  return ret;
}

int ObMsgUpsSlaveFailure::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = my_addr_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = slave_addr_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  return ret;
}

int ObMsgUpsSlaveFailure::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = my_addr_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = slave_addr_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ObMsgRevokeLease::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, lease_)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ups_master_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  return ret;
}

int ObMsgRevokeLease::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &lease_)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = ups_master_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

