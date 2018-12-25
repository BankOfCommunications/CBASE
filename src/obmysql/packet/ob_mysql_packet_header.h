/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_packet_header.h is for what ...
 *
 * Version: ***: ob_mysql_packet_header.h  Wed Jul 18 10:48:24 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */
#ifndef OB_MYSQL_PACKET_HEADER_H_
#define OB_MYSQL_PACKET_HEADER_H_

#include <stdint.h>
#include "common/ob_define.h"
#include "tblog.h"

namespace oceanbase
{
  namespace obmysql
  {
    class ObMySQLPacketHeader
    {
      public:
        ObMySQLPacketHeader(): length_(0), seq_(0)
        {
        }
      public:
        uint32_t length_;         /* MySQL packet length not include packet header */
        uint8_t  seq_;            /* MySQL packet sequence */
    };
  }
}
#endif
