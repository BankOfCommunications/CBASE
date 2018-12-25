/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_read_ups_banlance.h for read load balance stratege utility class. 
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_READ_UPS_BALANCE_H_
#define OCEANBASE_CHUNKSERVER_READ_UPS_BALANCE_H_

#include "common/ob_define.h"
#include "common/ob_ups_info.h"

namespace oceanbase
{
  namespace chunkserver
  {
    // read load balance stratege utility class
    class ObReadUpsBalance
    {
    public:
      ObReadUpsBalance();
      ~ObReadUpsBalance();

    public:
      // default sum percent 100
      const static int32_t DEFAULT_PERCENT = 100;
      
      // select a server from server list according the read flow throughput access ratio
      static int32_t select_server(const common::ObUpsList & list,  
          const common::ObServerType type = common::CHUNK_SERVER);
    };
  }
}

#endif //OCEANBASE_CHUNKSERVER_READ_UPS_BALANCE_H_
