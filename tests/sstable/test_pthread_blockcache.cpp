/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * test_pthread_blockcache.cpp for stress test of block cache 
 *
 * Authors: 
 *   yubai< yubai.lk@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <new>
#include <pthread.h>
#include "sstable/ob_blockcache.h"
#include "chunkserver/ob_fileinfo_cache.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace chunkserver;
using namespace sstable;
using namespace oceanbase::common;

// block cache压力测试代码，请先使用prepare参数运行一次生成测试数据

const uint32_t READ_BLOCK_SIZE = 62 * 1024 + 1;
uint32_t g_file_num = 128;
uint32_t g_file_block_num = 128;
uint32_t g_thread_num = 10;
uint32_t g_times_per_thread = 1024;
static const int64_t block_cache_size = 64 * 1024 * 256;
static const int64_t block_index_cache_size = 32 * 1024 * 1024;
static const int64_t ficache_max_num = 1024;
static FileInfoCache fic;
static ObBlockCache bc(fic);

union BlockData
{
  struct
  {
    uint64_t file_num;
    uint64_t block_num;
  };
  char data[READ_BLOCK_SIZE];
};

void prepare_file(const char *dir_name)
{
  BlockData data;
  memset(&data, 0, sizeof(data));
  mkdir(dir_name, S_IRWXU);
  for (uint32_t i = 0; i < g_file_num; i++)
  {
    char fname[256];
    sprintf(fname, "%s/sst_%u", dir_name, i);
    int fd = open(fname, O_WRONLY | O_APPEND | O_CREAT | O_TRUNC, S_IRWXU);
    for (uint32_t j = 0; j < g_file_block_num; j++)
    {
      data.file_num = i;
      data.block_num = j;
      write(fd, &data, sizeof(data));
      //fprintf(stderr, "sizeof data=%lu\n", sizeof(data));
    }
    close(fd);
  }
}

void *thread_func(void *data)
{
  int64_t pid = 0;
  uint32_t file_num;
  uint32_t block_num;
  ObBufferHandle *buffer_handle = new ObBufferHandle();
  ObBufferHandle handle_tmp;
  BlockData *bd = NULL;
  int32_t ret = 0;
  pid = pthread_self();
  UNUSED(data);

  for (uint32_t i = 0; i < g_times_per_thread; i++)
  {
    file_num = rand() % g_file_num;
    block_num = rand() % g_file_block_num;
    *buffer_handle = handle_tmp;
    if (sizeof(BlockData) != (ret = bc.get_block(file_num, 
                                                 block_num * sizeof(BlockData), 
                                                 sizeof(BlockData), 
                                                 *buffer_handle, 1024)))
    {
      abort();
    }
    else
    {
      bd = (BlockData*)(buffer_handle->get_buffer());
      if (file_num != bd->file_num
          || block_num != bd->block_num)
      {
        abort();
      }
    }
  }

  delete buffer_handle;
  return NULL;
}

void thread_test()
{
  pthread_t *pd = new pthread_t[g_thread_num];
  for (uint32_t i = 0; i < g_thread_num; i++)
  {
    pthread_create(pd + i, NULL, thread_func, NULL);
  }
  for (uint32_t i = 0; i < g_thread_num; i++)
  {
    pthread_join(pd[i], NULL);
  }

  delete[] pd;
}

int main(int argc, char **argv)
{
  TBSYS_LOGGER.setLogLevel("ERROR");
  TBSYS_LOGGER.setFileName("./log/thread_test.log");
  if (1 >= argc)
  {
    fprintf(stderr, "handle_type must be set\n");
    exit(-1);
  }

  if (0 == strcmp(argv[1], "prepare"))
  {
    prepare_file("./data");
  }
  else if (0 == strcmp(argv[1], "test"))
  {
    if (3 >= argc)
    {
      fprintf(stderr, "thread num and times per thread must be set\n");
      exit(-1);
    }
    fic.init(ficache_max_num);
    bc.init(block_cache_size);
    g_thread_num = atoi(argv[2]);
    g_times_per_thread = atoi(argv[3]);
    srand(static_cast<int32_t>(time(NULL)));
    thread_test();
    bc.destroy();
  }
  else
  {
    fprintf(stderr, "invalid handle_type must be [preapre | test]\n");
    exit(-1);
  }
}

