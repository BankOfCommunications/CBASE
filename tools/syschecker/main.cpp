/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * main.cpp for define syschecker main function. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/ob_define.h"
#include "ob_syschecker_main.h"

using namespace oceanbase::common;
using namespace oceanbase::syschecker;

int main(int argc, char* argv[])
{
  int ret               = OB_SUCCESS;
  ObSyscheckerMain* sm  = ObSyscheckerMain::get_instance();
  
  if (NULL == sm) 
  {
    fprintf(stderr, "cannot start syschecker, new ObSyscheckerMain failed\n");
    ret = OB_ERROR;
  }
  else 
  {
    ret = sm->start(argc, argv);
  }

  return ret;
}
