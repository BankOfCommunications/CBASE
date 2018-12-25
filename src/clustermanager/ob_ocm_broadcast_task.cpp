/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ocm_info_manager.h,v 0.1 2010/09/28 13:39:26 rongxuan Exp $
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_ocm_main.h"
#include "ob_ocm_broadcast_task.h"

namespace oceanbase
{
  namespace clustermanager
  {
    ObOcmBroadcastTask::ObOcmBroadcastTask()
    {
    }
    ObOcmBroadcastTask::~ObOcmBroadcastTask()
    {
    }
    void ObOcmBroadcastTask::runTimerTask(void)
    {
      OcmMain *ocm_main = OcmMain::get_instance();
      if(NULL == ocm_main)
      {
        TBSYS_LOG(ERROR, "get ocm main fail.");
      }
      else
      {
        OcmServer *ocm_server = ocm_main->get_ocm_server();
        if(NULL == ocm_server)
        {
          TBSYS_LOG(ERROR, "get ocm_server fail.");
        }
        else
        {
          OcmService *service = ocm_server->get_service();
          if(NULL == service)
          {
            TBSYS_LOG(ERROR, "get service fail.");
          }
          else
          {
            service->broad_cast_ocm_info();
          }
        }
      }
    }
  }
}

