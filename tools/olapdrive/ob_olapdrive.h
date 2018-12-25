/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_olapdrive.h for define olapdrive worker. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_H
#define OCEANBASE_OLAPDRIVE_H

#include "client/ob_client.h"
#include "ob_olapdrive_param.h"
#include "ob_write_worker.h"
#include "ob_read_worker.h"
#include "ob_olapdrive_stat.h"
#include "ob_olapdrive_schema.h"
#include "ob_olapdrive_meta.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    class ObOlapdrive
    {
    public:
      ObOlapdrive();
      ~ObOlapdrive();

      int init();
      int start();
      int stop();
      int wait();

    private:
      int init_servers_manager();

    private:
      DISALLOW_COPY_AND_ASSIGN(ObOlapdrive);

      ObOlapdriveParam param_;
      client::ObServerManager servers_mgr_;
      ObOlapdriveSchema olapdrive_schema_;
      client::ObClient client_;
      ObOlapdriveMeta meta_;
      ObOlapdriveStat stat_;
      ObWriteWorker write_worker_;
      ObReadWorker read_worker_;
    };
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_H
