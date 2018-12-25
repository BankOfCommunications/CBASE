/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: main.cpp,v 0.1 2010/09/14 10:11:02 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */

#include "common/ob_malloc.h"
#include "ob_update_server_main.h"
#include <malloc.h>
#include "easy_pool.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

namespace
{
  static const int DEFAULT_MMAP_MAX_VAL = 655360;
}

int main(int argc, char** argv)
{
  mallopt(M_MMAP_MAX, DEFAULT_MMAP_MAX_VAL);
  ob_init_memory_pool();
  tbsys::WarningBuffer::set_warn_log_on(true);
  //easy_pool_set_allocator(ob_malloc);
  ObUpdateServerMain* ups = ObUpdateServerMain::get_instance();
  int ret = OB_SUCCESS;
  if (NULL == ups)
  {
    fprintf(stderr, "cannot start updateserver, new ObUpdateServerMain failed\n");
    ret = OB_ERROR;
  }
  else
  {
    ret = ups->start(argc, argv);
    if (OB_SUCCESS == ret)
    {
      ups->destroy();
    }
    BaseMain::restart_server(argc, argv);
  }

  return ret;
}
