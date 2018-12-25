/*
 * This program is free software; you can redistribute it and/or modify 
 * it under the terms of the GNU General Public License version 2 as 
 * published by the Free Software Foundation.
 *
 * ObPacketQueueHandler.h is for what ...
 *
 * Version: ***: ObPacketQueueHandler.h  Thu May 17 13:33:51 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want 
 *
 */

#ifndef OB_PACKET_QUEUE_HANDLER_H_
#define OB_PACKET_QUEUE_HANDLER_H_

#include "ob_packet.h"
namespace oceanbase
{
  namespace common
  {
    class ObPacketQueueHandler
    {
      public:
        virtual ~ObPacketQueueHandler(){};
        virtual bool handlePacketQueue(ObPacket *packet, void* args) = 0;
    };
  }
}
#endif
