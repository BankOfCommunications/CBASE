/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: test_init.h,v 0.1 2010/09/21 11:50:49 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_TEST_INIT_H__
#define __OCEANBASE_CHUNKSERVER_TEST_INIT_H__

#include "common/ob_malloc.h"

struct init_mem_pool
{
  init_mem_pool()
  {
    ob_init_memory_pool(2 * 1024L * 1024L);
  }
};

static init_mem_pool init_mem_pool_helper;

#endif //__TEST_INIT_H__
