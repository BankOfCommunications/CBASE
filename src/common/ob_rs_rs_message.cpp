/*
* Version: $ObPaxos V0.1$
*
* Authors:
*  pangtianze <pangtianze@ecnu.cn>
*
* Date:
*  20150609
*
*  - message struct from rs to rs
*
*/
#include "ob_rs_rs_message.h"
#include <tbsys.h>

using namespace oceanbase::common;

int ObMsgRsLeaderBroadcast::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, lease_)))
  {
    TBSYS_LOG(ERROR, "failed to lease, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, term_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize rs term, err=%d", ret);
  }
  return ret;
}
int ObMsgRsLeaderBroadcast::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &lease_)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize lease, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &term_)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize rs term, err=%d", ret);
  }
  return ret;
}

int ObMsgRsLeaderBroadcastResp::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, term_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize rs term, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, is_granted_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize is_granted, err=%d", ret);
  }
  return ret;
}
int ObMsgRsLeaderBroadcastResp::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &term_)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize rs term, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &is_granted_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize is_granted, err=%d", ret);
  }
  return ret;
}

int ObMsgRsRequestVote::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, lease_)))
  {
    TBSYS_LOG(ERROR, "failed to lease, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, term_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize rs term, err=%d", ret);
  }
  return ret;
}
int ObMsgRsRequestVote::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &lease_)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize lease, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &term_)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize rs term, err=%d", ret);
  }
  return ret;
}

int ObMsgRsRequestVoteResp::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, term_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize rs term, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, is_granted_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize is_granted, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, need_regist_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize need_regist, err=%d", ret);
  }
  return ret;
}
int ObMsgRsRequestVoteResp::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &term_)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize rs term, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &is_granted_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize is_granted, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &need_regist_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize need_regist, err=%d", ret);
  }
  return ret;
}

int ObMsgRsHeartbeat::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, lease_)))
  {
    TBSYS_LOG(ERROR, "failed to lease, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, current_term_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize rs term, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, config_version_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize config version, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, paxos_num_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize paxos num, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, quorum_scale_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize quorum scale, err=%d", ret);
  }
  //add wangdonghui [paxos daily merge] 20170510 :b
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, last_frozen_time_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize last frozen time, err=%d", ret);
  }
  //add :e
  return ret;
}
int ObMsgRsHeartbeat::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &lease_)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize lease, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &current_term_)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize rs term, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &config_version_)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize config version, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &paxos_num_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize paxos num, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &quorum_scale_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize quorum scale, err=%d", ret);
  }
  //add wangdonghui [paxos daily merge] 20170510 :b
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &last_frozen_time_)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize last frozen time, err=%d", ret);
  }
  //add :e
  return ret;
}

int ObMsgRsHeartbeatResp::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, current_term_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize rs term, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, paxos_num_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize paxos num, err=%d", ret);
  }
  //add pangtianze [Paxos rs_election] 20161010:b
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, quorum_scale_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize quorum scale, err=%d", ret);
  }
  //add:e
  return ret;
}
int ObMsgRsHeartbeatResp::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &current_term_)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize rs term, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &paxos_num_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize paxos num, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &quorum_scale_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize quorum scale, err=%d", ret);
  }
  return ret;
}

//add chujiajia [Paxos rs_election] 20151030:b
int ObMsgRsChangePaxosNum::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to serialize addr_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, new_paxos_num_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize rs new_paxos_num_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, term_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize rs term, err=%d", ret);
  }
  return ret;
}

int ObMsgRsChangePaxosNum::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize addr_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &new_paxos_num_)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize rs new_paxos_num_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &term_)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize rs term, err=%d", ret);
  }
  return ret;
}

int ObMsgRsNewQuorumScale::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to serialize addr_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, new_ups_quorum_scale_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize rs new_paxos_num_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, term_)))
  {
    TBSYS_LOG(ERROR, "failed to serailize rs term, err=%d", ret);
  }
  return ret;
}

int ObMsgRsNewQuorumScale::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize addr_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &new_ups_quorum_scale_)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize rs new_paxos_num_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &term_)))
  {
    TBSYS_LOG(ERROR, "failed to deserialize rs term, err=%d", ret);
  }
  return ret;
}

