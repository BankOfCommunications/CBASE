/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * main.cpp for define olapdrive main function. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/ob_define.h"
#include "ob_olapdrive_main.h"

using namespace oceanbase::common;
using namespace oceanbase::olapdrive;

namespace
{
  const char* PUBLIC_SECTION_NAME = "public";
}

int main(int argc, char* argv[])
{
  int ret              = OB_SUCCESS;
  ObOlapdriveMain* sm  = ObOlapdriveMain::get_instance();
  
  if (NULL == sm) 
  {
    fprintf(stderr, "cannot start olapdrive, new ObOlapdriveMain failed\n");
    ret = OB_ERROR;
  }
  else 
  {
    ret = sm->start(argc, argv, PUBLIC_SECTION_NAME);
  }

  return ret;
}
