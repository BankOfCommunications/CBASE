/*
 * Copyright (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *     - some work details here
 */

#include "ob_root_admin2.h"
#include "common/ob_define.h"
#include "common/ob_malloc.h"
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

int main(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  ob_init_memory_pool();
  TBSYS_LOGGER.setFileName("rs_admin.log", true);
  Arguments args;

  if (OB_SUCCESS != (ret = parse_cmd_line(argc, argv, args)))
  {
    printf("parse cmd line error, err=%d\n", ret);
  }
  else
  {
    // init
    ObServer server(ObServer::IPV4, args.rs_host, args.rs_port);
    ObBaseClient client;
    if (OB_SUCCESS != (ret = client.initialize(server)))
    {
      printf("failed to init client, err=%d\n", ret);
    }
    else
    {
      printf("server[%s], timeout=%ld\n", server.to_cstring(), args.request_timeout_us);
      ret = args.command.handler(client, args);
    }
    // destroy
    //client.destroy();
  }
  return ret == OB_SUCCESS ? 0 : -1;
}

