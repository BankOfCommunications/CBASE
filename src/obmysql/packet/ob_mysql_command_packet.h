/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_command_packet.h is for what ...
 *
 * Version: ***: ob_mysql_command_packet.h  Wed Jul 18 10:46:14 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */
#ifndef OB_MYSQL_COMMAND_PACKET_H_
#define OB_MYSQL_COMMAND_PACKET_H_

#include "common/data_buffer.h"
#include "ob_mysql_packet_header.h"
#include "easy_io_struct.h"
#include "common/ob_define.h"
#include "common/ob_string.h"

namespace oceanbase
{
  namespace tests
  {
    class TestObMySQLCommandPacket;
  }

  namespace obmysql
  {
    class ObMySQLCommandPacket
    {
      friend class ObMySQLCommandQueue;
      friend class tests::TestObMySQLCommandPacket;
    public:
      ObMySQLCommandPacket();
      ~ObMySQLCommandPacket();

      common::ObString& get_command()
      {
        return command_;
      }

      const common::ObString& get_command() const
      {
        return command_;
      }

      const ObMySQLPacketHeader& get_packet_header() const
      {
        return header_;
      }

      inline void set_header(uint32_t pkt_length, uint8_t pkt_seq)
      {
        header_.length_ = pkt_length;
        header_.seq_ = pkt_seq;
      }

      inline void set_type(uint8_t type)
      {
        type_ = type;
      }

      inline uint8_t get_type() const
      {
        return type_;
      }

      int set_request(easy_request_t* req);

      void set_command(char* command, const int32_t length);

      inline easy_request_t* get_request() const
      {
        return req_;
      }

      int32_t get_command_length() const;

      void set_receive_ts(const int64_t &now);
      int64_t get_receive_ts() const;

    private:
      ObMySQLPacketHeader header_;
      uint8_t type_;
      common::ObString command_;
      easy_request_t* req_;                 //request pointer for send response
      ObMySQLCommandPacket* next_;
      int64_t receive_ts_;
    };

    enum ObMySQLCommandType
    {
      OBMYSQL_LOGIN,
      OBMYSQL_LOGOUT,
      OBMYSQL_PREPARE,
      OBMYSQL_EXECUTE,
      OBMYSQL_OTHER
    };

    // used by packet recorder only
    struct ObMySQLCommandPacketRecord
    {
      uint32_t version_;        // version of this struct
      int socket_fd_;
      int cseq_;
      easy_addr_t addr_;
      uint32_t pkt_length_;         /* MySQL packet length not include packet header */
      uint8_t  pkt_seq_;            /* MySQL packet sequence */
      uint8_t cmd_type_;
      uint8_t obmysql_type_;    /* command type defined by ourself */
      int8_t reserved1_;
      int32_t reserved2_;
      int32_t reserved3_;
      uint64_t stmt_id_;//recode stmt_id when do_com_prepare
      ObMySQLCommandPacketRecord()
      :version_(1),socket_fd_(0),
         cseq_(0), pkt_length_(0),
         pkt_seq_(0), cmd_type_(30), //30 equals COM_END
         obmysql_type_(OBMYSQL_OTHER),
         reserved1_(0),
         reserved2_(0),reserved3_(0),
         stmt_id_(0)
      {
      }
    };
  }
}
#endif
