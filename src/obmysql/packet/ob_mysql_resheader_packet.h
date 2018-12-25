/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_resheader_packet.h is for what ...
 *
 * Version: ***: ob_mysql_resheader_packet.h  Fri Aug  3 16:25:31 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */
#ifndef OB_MYSQL_RESHEADER_PACKET_H_
#define OB_MYSQL_RESHEADER_PACKET_H_

#include "ob_mysql_packet_header.h"
#include "common/ob_define.h"
#include "ob_mysql_packet.h"

namespace oceanbase
{
  namespace tests
  {
    class TestObMySQLResheaderPacket;
  }
  namespace obmysql
  {
    class ObMySQLResheaderPacket : public ObMySQLPacket
    {
      friend class tests::TestObMySQLResheaderPacket;
      public:
        ObMySQLResheaderPacket();
        ~ObMySQLResheaderPacket();

        int serialize(char* buffer, int64_t len, int64_t& pos);
        uint64_t get_serialize_size();
        inline void set_field_count(uint64_t count)
        {
          field_count_ = count;
        }

        //for test
        inline uint64_t get_field_count() const
        {
          return field_count_;
        }

    private:
        DISALLOW_COPY_AND_ASSIGN(ObMySQLResheaderPacket);

        uint64_t field_count_;
    };
  }
}
#endif
