/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_clist2str.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "common/ob_malloc.h"
#include "ob_chunk_server_manager.h"
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

int main(int argc, char *argv[])
{
  int ret = 0;
  if (2 != argc)
  {
    printf("clist2str <clist_file>\n");
  }
  else
  {
    ob_init_memory_pool();
    ObChunkServerManager csmgr;
    int32_t cs_num = 0;
    int32_t ms_num = 0;
    const char* filename = argv[1];
    printf("load clist from %s\n", filename);
    if (0 != (ret = csmgr.read_from_file(filename, cs_num, ms_num)))
    {
      printf("failed to load server list, err=%d\n", ret);
    }
    else
    {
      printf("load succ, cs_num=%d ms_num=%d\n", cs_num, ms_num);
      ObChunkServerManager::const_iterator it = csmgr.begin();
      int32_t index = 0;
      for (; it != csmgr.end(); ++it)
      {
        it->dump(index++);
      }
    }
  }
  return ret;
}
