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
#ifndef OB_RS_RS_MESSAGE_H
#define OB_RS_RS_MESSAGE_H

#include "ob_server.h"
#include "data_buffer.h"
#include "ob_result.h"
#include "ob_obi_role.h"
namespace oceanbase
{
  namespace common
  {
    /// rs to rs, leader broadcast message
    struct ObMsgRsLeaderBroadcast
    {
      static const int MY_VERSION = 1;
      ObServer addr_;       // 消息发出端serever的IP、端口号
      int64_t lease_;       // 消息发出端授予接收端的租约(now + during_time)
      int64_t term_;        // 消息发出端的term值
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
    };
    /// rs to rs, vote request message response
    struct ObMsgRsLeaderBroadcastResp
    {
      static const int MY_VERSION = 1;
      ObServer addr_;       // 消息发出端serever的IP、端口号
      int64_t term_;        // 消息发出端的term值
      bool is_granted_;         // 是否同意广播请求
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
    };
  /// rs to rs, vote request message
    struct ObMsgRsRequestVote
    {
      static const int MY_VERSION = 1;
      ObServer addr_;       // 消息发出端serever的IP、端口号
      int64_t lease_;       // 消息发出端授予接收端的租约(now + during_time)
      int64_t term_;        // 消息发出端的term值
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
    };
    /// rs to rs, vote request message response
    struct ObMsgRsRequestVoteResp
    {
      static const int MY_VERSION = 1;
      ObServer addr_;       // 消息发出端serever的IP、端口号
      int64_t term_;        // 消息发出端的term值
      bool is_granted_;         // 是否同意投票请求
      bool need_regist_;      //是否需要向消息发出端重新注册
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
    };
    ///rs to rs, heartbeat message
    struct ObMsgRsHeartbeat
    {
      static const int MY_VERSION = 1;
      ObServer addr_; // 心跳发送端server的ip, port
      int64_t lease_; //消息发出端授予接收端的租约(now + during_time)
      int64_t current_term_; // 心跳发送端的term
      int64_t config_version_;
      //add pangtianze [Paxos rs_election] 20161020:b
      int32_t paxos_num_;
      int32_t quorum_scale_;
      //add:e
      //add wangdonghui [paxos daily merge] 20170510 :b
      int64_t last_frozen_time_;
      //add :e
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
    };

    /// rs to rs, heartbeat message response
    struct ObMsgRsHeartbeatResp {
      static const int MY_VERSION = 1;
      ObServer addr_; // 心跳发送端server的ip, port
      int64_t current_term_;  // 心跳发送端的term
      int32_t paxos_num_;
      //add pangtianze [Paxos rs_election] 20161010:b
      int32_t quorum_scale_;
      //add:e
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
    };
    //add chujiajia [Paxos rs_election] 20151030:b

    /// rs to rs, change paxos num request message
    struct ObMsgRsChangePaxosNum
    {
      static const int MY_VERSION = 1;
      ObServer addr_;       // 消息发出端serever的IP、端口号
      int32_t new_paxos_num_;   //用户设定的paxos_num的新值
      int64_t term_;        // 消息发出端的term值
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
    };
    /// rs to rs, send new quorum scale request message
    struct ObMsgRsNewQuorumScale
    {
      static const int MY_VERSION = 1;
      ObServer addr_;       // 消息发出端serever的IP、端口号
      int32_t new_ups_quorum_scale_;   //用户设定的quorum_scale的新值
      int64_t term_;        // 消息发出端的term值
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
    };
    //add:e
  } // end namespace common
} // end namespace oceanbase

#endif // OB_RS_RS_MESSAGE_H
