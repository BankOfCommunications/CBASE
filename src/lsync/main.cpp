/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * main.cpp
 *
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *
 */
#include "common/ob_malloc.h"
#include "ob_lsync_server_main.h"


using namespace oceanbase::common;
using namespace oceanbase::lsync;

int main(int argc, char** argv)
{
  int ret = OB_SUCCESS;
  TBSYS_LOGGER.setLogLevel("INFO");
  ObLsyncServerMain* lsync_server = ObLsyncServerMain::get_instance();
  if (OB_SUCCESS != (ret = ob_init_memory_pool()))
  {
    TBSYS_LOG(ERROR, "ob_init_memory_pool()=>%d", ret);
  }
  else if (NULL == lsync_server)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "new ObLsyncServerMain failed");
  }
  else if (OB_SUCCESS != (ret = lsync_server->start(argc, argv)))
  {
    TBSYS_LOG(ERROR, "lsync_server->start()=>%d", ret);
  }
  else
  {
    lsync_server->destroy();
  }

  return ret;
}

