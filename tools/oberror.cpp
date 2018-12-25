/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * oberror.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "common/ob_errno.h"
#include "common/ob_define.h"
#include <cstdio>
#include <cstdlib>
using namespace oceanbase::common;

int main(int argc, char *argv[])
{
  if (2 != argc)
  {
    printf("Usage: oberror <errno>\n");
    exit(1);
  }
  int error_no = atoi(argv[1]);
  if (0 > error_no)
  {
    error_no = -error_no;
  }
  if (error_no > OB_MAX_ERROR_CODE)
  {
    printf("Invalid OB error code, errno=%d\n", error_no);
  }
  else
  {
    printf("OB-%d: %s\n", error_no, ob_strerror(-error_no));
  }
  return 0;
}
