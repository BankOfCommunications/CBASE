/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_eof_packet.h is for what ...
 *
 * Version: ***: ob_mysql_eof_packet.h  Fri Aug  3 14:06:09 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */
#ifndef OB_MYSQL_EOF_PACKET_H_
#define OB_MYSQL_EOF_PACKET_H_

#include "ob_mysql_packet_header.h"
#include "ob_mysql_packet.h"
#include "common/ob_define.h"
#include "tblog.h"

namespace oceanbase
{
  namespace obmysql
  {
    class ObMySQLEofPacket : public ObMySQLPacket
    {
      public:
        ObMySQLEofPacket();
        ~ObMySQLEofPacket();

        int serialize(char* buffer, int64_t len, int64_t& pos);

        inline void set_warning_count(uint16_t count)
        {
          warning_count_ = count;
        }

        inline void set_server_status(uint16_t status)
        {
          server_status_ = status;
        }

        virtual uint64_t get_serialize_size();

        //for test
        inline uint8_t get_field_count() const
        {
          return field_count_;
        }

        inline uint16_t get_warning_count() const
        {
          return warning_count_;
        }

        inline uint16_t get_server_status() const
        {
          return server_status_;
        }

      private:
        DISALLOW_COPY_AND_ASSIGN(ObMySQLEofPacket);

        uint8_t field_count_;
        uint16_t warning_count_;
        uint16_t server_status_;
    };
  }
}
#endif
