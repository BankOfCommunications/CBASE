/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#include "clog_status.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

ClogStatus::ClogStatus()
{
  reset();
}

int ClogStatus::serialize(char* buf, int64_t len, int64_t& pos) const
{
  int err = OB_SUCCESS;
  if (NULL == buf || pos < 0 || pos > len)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if(OB_SUCCESS != serialization::encode_i32(buf, len, pos, self_ip_)
      || OB_SUCCESS != serialization::encode_i32(buf, len, pos, self_port_)
      || OB_SUCCESS != serialization::encode_i32(buf, len, pos, static_cast<int32_t>(state_))
      || OB_SUCCESS != serialization::encode_i32(buf, len, pos, master_rs_ip_)
      || OB_SUCCESS != serialization::encode_i32(buf, len, pos, master_rs_port_)
      || OB_SUCCESS != serialization::encode_i32(buf, len, pos, master_ups_ip_)
      || OB_SUCCESS != serialization::encode_i32(buf, len, pos, master_ups_port_)
      || OB_SUCCESS != serialization::encode_i64(buf, len, pos, lease_)
      || OB_SUCCESS != serialization::encode_i64(buf, len, pos, lease_remain_)
      || OB_SUCCESS != serialization::encode_i32(buf, len, pos, static_cast<int32_t>(role_))
      || OB_SUCCESS != serialization::encode_i64(buf, len, pos, send_receive_)
      || OB_SUCCESS != serialization::encode_i64(buf, len, pos, commit_replay_)
      || OB_SUCCESS != serialization::encode_i64(buf, len, pos, log_term_)
      || OB_SUCCESS != serialization::encode_i64(buf, len, pos, flush_)
      || OB_SUCCESS != serialization::encode_i64(buf, len, pos, commit_point_))
  {
    err = OB_SERIALIZE_ERROR;
    TBSYS_LOG(ERROR, "clog_status.serialize(buf=%p[%ld], pos=%ld)=>%d", buf, len, pos, err);
  }
  return err;
}

int ClogStatus::deserialize(const char* buf, int64_t len, int64_t& pos)
{
  int err = OB_SUCCESS;
  if (NULL == buf || pos < 0 || pos > len)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if(OB_SUCCESS != serialization::decode_i32(buf, len, pos, &self_ip_)
      || OB_SUCCESS != serialization::decode_i32(buf, len, pos, &self_port_)
      || OB_SUCCESS != serialization::decode_i32(buf, len, pos, (int32_t*)&state_)
      || OB_SUCCESS != serialization::decode_i32(buf, len, pos, &master_rs_ip_)
      || OB_SUCCESS != serialization::decode_i32(buf, len, pos, &master_rs_port_)
      || OB_SUCCESS != serialization::decode_i32(buf, len, pos, &master_ups_ip_)
      || OB_SUCCESS != serialization::decode_i32(buf, len, pos, &master_ups_port_)
      || OB_SUCCESS != serialization::decode_i64(buf, len, pos, &lease_)
      || OB_SUCCESS != serialization::decode_i64(buf, len, pos, &lease_remain_)
      || OB_SUCCESS != serialization::decode_i32(buf, len, pos, (int32_t*)&role_)
      || OB_SUCCESS != serialization::decode_i64(buf, len, pos, &send_receive_)
      || OB_SUCCESS != serialization::decode_i64(buf, len, pos, &commit_replay_)
      || OB_SUCCESS != serialization::decode_i64(buf, len, pos, &log_term_)
      || OB_SUCCESS != serialization::decode_i64(buf, len, pos, &flush_)
      || OB_SUCCESS != serialization::decode_i64(buf, len, pos, &commit_point_))
  {
    err = OB_DESERIALIZE_ERROR;
    TBSYS_LOG(ERROR, "clog_status.deserialize(buf=%p[%ld], pos=%ld)=>%d", buf, len, pos, err);
  }
  return err;
}

void ClogStatus::reset()
{
  self_ip_ = 0;
  self_port_ = 0;
  state_ = ObUpsRoleMgr::STOP;
  master_rs_ip_ = 0;
  master_rs_port_ = 0;
  master_ups_ip_ = 0;
  master_ups_port_ = 0;
  lease_ = 0;
  lease_remain_ = 0;

  role_ = ObUpsRoleMgr::SLAVE;
  send_receive_ = 0;
  commit_replay_ = 0;
  log_term_ = 0;
  flush_ = 0;
  commit_point_ = 0;
}

int ClogStatus::to_string(char* buf, const int64_t len) const
{
  int err = OB_SUCCESS;
  int64_t count = 0;

  char host_self[22];
  get_host_self(host_self, sizeof(host_self));

  char host_rs[22];
  get_host_rs(host_rs, sizeof(host_rs));

  char host_ups[22];
  get_host_ups(host_ups, sizeof(host_ups));

  if (0 >= (count = snprintf(buf, len,
    "ClogStatus<self=[%s], master_ups=[%s], master_ups=[%s], lease=[%ld]\n"
    "role=%d, log(send/receive:%ld, commit/replay:%ld, log_term:%ld, flush:%ld, commit_point:%ld)>\n"
    , host_self, host_rs, host_ups, lease_, role_, send_receive_, commit_replay_, log_term_, flush_, commit_point_))
     || count > len)
  {
    err = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(WARN, "snprintf(buf=%p[%ld]): count=%ld", buf, len, count);
  }
  return err;
}

