/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: load_client.h,v 0.1 2012/03/15 14:00:00 zhidong Exp $
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef LOAD_CLIENT_H_
#define LOAD_CLIENT_H_

#include "tbnet.h"
#include "common/ob_define.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_packet_factory.h"

namespace oceanbase
{
  namespace tools
  {
    class LoadClient
    {
    public:
      LoadClient()
      {
      }

      virtual ~LoadClient()
      {
        destroy();
      }

      common::ObClientManager * get_client(void)
      {
        return &client_;
      }

      common::ThreadSpecificBuffer * get_buffer(void)
      {
        return &rpc_buffer_;
      }

      int init();

    private:
      virtual int destroy();

    private:
      tbnet::DefaultPacketStreamer streamer_;
      tbnet::Transport transport_;
      common::ObPacketFactory factory_;

    protected:
      common::ThreadSpecificBuffer rpc_buffer_;
      common::ObClientManager client_;
    };
  }
}

#endif // LOAD_CLIENT_H_

