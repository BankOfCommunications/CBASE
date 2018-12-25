/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_packet_queue_handler.h is for what ...
 *
 * Version: ***: ob_mysql_packet_queue_handler.h  Fri Jul 27 15:45:42 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */
#ifndef OB_MYSQL_PACKET_QUEUE_HANDLER_H
#define OB_MYSQL_PACKET_QUEUE_HANDLER_H

#include "packet/ob_mysql_command_packet.h"

namespace oceanbase
{
  namespace obmysql
  {
    class ObMySQLPacketQueueHandler
    {
      public:
        ObMySQLPacketQueueHandler() { }
        virtual ~ObMySQLPacketQueueHandler() { }
      public:
        virtual int handle_packet_queue(ObMySQLCommandPacket* packet, void* arg) = 0;
    };
  }
}
#endif
