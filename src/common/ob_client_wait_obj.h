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
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#ifndef __OB_COMMON_OB_CLIENT_WAIT_OBJ_H__
#define __OB_COMMON_OB_CLIENT_WAIT_OBJ_H__
#include "ob_define.h"
#include "easy_io_struct.h"
#include "data_buffer.h"
#include "ob_packet.h"
#include "semaphore.h"

namespace oceanbase
{
  namespace common
  {
    struct ObClientWaitObj
    {
      ObClientWaitObj();
      virtual ~ObClientWaitObj();
      void before_post();
      void after_post(const int post_err);
      virtual void handle_response(ObPacket* packet){ UNUSED(packet); }
      int receive_packet(ObPacket* packet);
      static int on_receive_response(easy_request_t* r);
      int wait(ObDataBuffer& response, const int64_t timeout_us);
      int err_;
      int64_t done_count_;
      tbsys::CThreadCond cond_;
      ObDataBuffer response_;
      char buf_[OB_MAX_PACKET_LENGTH];
    };
  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_CLIENT_WAIT_OBJ_H__ */

