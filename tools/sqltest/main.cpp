/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: main.cpp,v 0.1 2012/02/27 16:53:05 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include <getopt.h>
#include "util.h"
#include "key_generator.h"
#include "sqltest_main.h"

int main(int argc, char** argv)
{
  SqlTestMain* sm = SqlTestMain::get_instance();

  int ret = sm->start(argc, argv, "public");

  return ret;
}

