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


#include <getopt.h>
#include "ob_msync_client_main.h"

using namespace oceanbase::common;
using namespace oceanbase::msync;

namespace
{
  const char* PUBLIC_SECTION_NAME = "public";
}

int main(int argc, char* argv[])
{
  int err = OB_SUCCESS;
  ObMsyncClientMain* msync;
  
  TBSYS_LOGGER.setLogLevel("DEBUG");
  
  if (OB_SUCCESS != (err = ob_init_memory_pool()))
  {
    TBSYS_LOG(ERROR, "ob_init_memory_pool()=>%d", err);
  }
  else if (OB_SUCCESS != (err = (msync = ObMsyncClientMain::get_instance())? OB_SUCCESS: OB_ERROR))
  {
    TBSYS_LOG(ERROR, "ObMsyncClientMain::get_instance()=>NULL");
  }
  else if (OB_SUCCESS != (err = msync->start(argc, argv, PUBLIC_SECTION_NAME)))
  {
    TBSYS_LOG(ERROR, "msync->start()=>%d", err);
  }

  return err;
}
