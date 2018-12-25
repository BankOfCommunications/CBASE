/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_row_packet.h is for what ...
 *
 * Version: ***: ob_mysql_row_packet.h  Fri Aug 17 17:25:23 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */
#ifndef OB_MYSQL_ROW_PACKET_H_
#define OB_MYSQL_ROW_PACKET_H_

#include "ob_mysql_packet.h"
#include "../ob_mysql_row.h"

namespace oceanbase
{
  namespace obmysql
  {
    class ObMySQLRowPacket : public ObMySQLPacket
    {
      public:
        ObMySQLRowPacket(const ObMySQLRow* row);
        int serialize(char* buffer, int64_t length, int64_t& pos);
        virtual uint64_t get_serialize_size();
      private:
        DISALLOW_COPY_AND_ASSIGN(ObMySQLRowPacket);
        const ObMySQLRow* row_;
    };
  }
}
#endif
