/* (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 * Version: 0.1
 *
 * Authors:
 *    Wu Di <lide.wd@alipay.com>
 */


#include "common/ob_libeasy_mem_pool.h"

using namespace oceanbase;
using namespace common;


void* common::ob_easy_realloc(void *ptr, size_t size)
{
  void *ret = NULL;
  if (size)
  {
    ret = ob_tc_realloc(ptr, size, ObModIds::LIBEASY);
    if (ret == NULL)
    {
      TBSYS_LOG(WARN, "ob_tc_realloc failed, ptr:%p, size:%lu", ptr, size);
    }
  }
  else if (ptr)
  {
    ob_tc_free(ptr, ObModIds::LIBEASY);
  }
  return ret;
}
