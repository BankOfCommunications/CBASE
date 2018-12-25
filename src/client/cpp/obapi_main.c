/**
 * (C) 2011 Alibaba Group Holding Limited.
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 * 
 * Version: $Id$
 * 
 * oceanbase.h : API of Oceanbase
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 * 
 */

#include <stdio.h>
#include <stdlib.h>

const char my_interp[] __attribute__((section(".interp")))
    = "/lib64/ld-linux-x86-64.so.2";

#define INTRODUCTION \
    "This library is OceanBase C API,\n" \
    "containing interfaces for developing\n" \
    "applications accessing OceanBase.\n"

#define SO_VERSION_FMT       "SO Version: "
#define SVN_REVISION_FMT     "SVN Revision: "
#define BUILD_TIME_FMT       "SO Build Time: "
#define GCC_VERSION_FMT      "GCC Version: "
#define BUILD_ENV_FMT        ""

const char* svn_version();
const char* build_date();
const char* build_time();

void print_intro()
{
  printf(INTRODUCTION);
}

void print_so_version()
{
#ifdef OBAPI_SO_VERSION
  printf(SO_VERSION_FMT "%s\n", OBAPI_SO_VERSION);
#endif
}

void print_svn_revision()
{
  printf(SVN_REVISION_FMT "%s\n", svn_version());
}

void print_build_time()
{
  printf(BUILD_TIME_FMT "%s %s\n", build_date(), build_time());
}

void print_gcc_version()
{
  printf(GCC_VERSION_FMT "%s\n", __VERSION__);
}

void print_build_env()
{
}

int so_main()
{
  print_intro();
  print_so_version();
  print_svn_revision();
  print_build_time();
  print_gcc_version();
  print_build_env();
  printf("\n");
  exit(0);
}

