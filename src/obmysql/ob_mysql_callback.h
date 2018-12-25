/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_callback.h is for what ...
 *
 * Version: ***: ob_mysql_callback.h  Tue Jul 17 11:19:14 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */

#ifndef OB_MYSQL_CALLBACK_H_
#define OB_MYSQL_CALLBACK_H_

#include "easy_io_struct.h"

namespace oceanbase
{
  namespace obmysql
  {
    class ObMySQLCallback
    {
      public:

        static int encode(easy_request_t* r, void* packet);

        static void* decode(easy_message_t* m);

        //handler login
        static int on_connect(easy_connection_t* c);

        static int on_disconnect(easy_connection_t* c);

        static int process(easy_request_t* r);

        static uint64_t get_packet_id(easy_connection_t* c, void* packet);

        static int clean_up(easy_request_t *r, void *apacket);
    };
  }
}

#endif
