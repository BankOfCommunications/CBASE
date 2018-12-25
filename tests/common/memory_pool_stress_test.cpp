/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * memory_pool_stress_test.cpp is for memory pool stress test
 *
 * Version: $id: memory_pool_stress_test.cpp,v 0.1 8/31/2010 10:02a wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#include <stdlib.h>
#include <pthread.h>
#include "ob_malloc.h"
#include "ob_memory_pool.h"
#include "signal.h"
/// @property maximum size allocated from memory pool
const int64_t MAX_BUFFER_SIZE = 128*1024;
/// @property thread number 
const int64_t THREAD_NUM = 16;
/// @property buffer array of each thread
const int64_t BUFFER_ARRAY_SIZE = 2048;

/// @struct  BufInfo
struct BufInfo
{
  BufInfo()
  {
    ptr_ = NULL;
    size_ = 0;
  }
  void *ptr_;
  int64_t size_;
};

/// @property is program stopped
bool g_stopped = false;

oceanbase::common::ObVarMemPool g_var_memory_pool(64*1024);

/// @property fixed memory allocating thread
void *fixed_malloc_thread(void *arg)
{
  BufInfo memory_buffer[BUFFER_ARRAY_SIZE];
  memset(memory_buffer, 0, sizeof(memory_buffer));
  for (int64_t i = 0; i < BUFFER_ARRAY_SIZE; i ++)
  {
    memory_buffer[i].size_ = random()%MAX_BUFFER_SIZE + 1;
    memory_buffer[i].ptr_ = oceanbase::common::ob_malloc(memory_buffer[i].size_);
    assert(memory_buffer[i].ptr_ != NULL);
    memset(memory_buffer[i].ptr_, 0, memory_buffer[i].size_);
  }
  while (!g_stopped)
  {
    int64_t idx = random()%BUFFER_ARRAY_SIZE;
    memset(memory_buffer[idx].ptr_, 0, memory_buffer[idx].size_);
    /// free an reallocate
    oceanbase::common::ob_safe_free(memory_buffer[idx].ptr_);
    memory_buffer[idx].size_ = 0;
    memory_buffer[idx].size_ = random()%MAX_BUFFER_SIZE + 1;
    memory_buffer[idx].ptr_ = oceanbase::common::ob_malloc(memory_buffer[idx].size_);
    assert(memory_buffer[idx].ptr_ != NULL);
    memset(memory_buffer[idx].ptr_, 0, memory_buffer[idx].size_);
  }
  for (int64_t i = 0; i < BUFFER_ARRAY_SIZE; i ++)
  {
    oceanbase::common::ob_safe_free(memory_buffer[i].ptr_);
    memory_buffer[i].size_ = 0;
  }
  return NULL;
}

/// @property fixed memory allocating thread
void *var_malloc_thread(void *arg)
{
  BufInfo memory_buffer[BUFFER_ARRAY_SIZE];
  memset(memory_buffer, 0, sizeof(memory_buffer));
  for (int64_t i = 0; i < BUFFER_ARRAY_SIZE; i ++)
  {
    memory_buffer[i].size_ = random()%MAX_BUFFER_SIZE + 1;
    memory_buffer[i].ptr_ = g_var_memory_pool.malloc(memory_buffer[i].size_);
    assert(memory_buffer[i].ptr_ != NULL);
    memset(memory_buffer[i].ptr_, 0, memory_buffer[i].size_);
  }
  while (!g_stopped)
  {
    int64_t idx = random()%BUFFER_ARRAY_SIZE;
    memset(memory_buffer[idx].ptr_, 0, memory_buffer[idx].size_);
    /// free an reallocate
    g_var_memory_pool.free(memory_buffer[idx].ptr_);
    memory_buffer[idx].ptr_ = NULL;
    memory_buffer[idx].size_ = 0;
    memory_buffer[idx].size_ = random()%MAX_BUFFER_SIZE + 1;
    memory_buffer[idx].ptr_ = g_var_memory_pool.malloc(memory_buffer[idx].size_);
    assert(memory_buffer[idx].ptr_ != NULL);
    memset(memory_buffer[idx].ptr_, 0, memory_buffer[idx].size_);
  }
  for (int64_t i = 0; i < BUFFER_ARRAY_SIZE; i ++)
  {
    g_var_memory_pool.free(memory_buffer[i].ptr_);
    memory_buffer[i].ptr_ = NULL;
    memory_buffer[i].size_ = 0;
  }
  return NULL;
}

void signal_handler(int )
{
  g_stopped = true;
}

int main(int argc, char **argv)
{
  pthread_t threads[THREAD_NUM];
  void *result = NULL;
  int64_t thread_num = 0;
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
  assert(oceanbase::common::ob_init_memory_pool() == 0);
  for (thread_num = 0;thread_num < THREAD_NUM/2; thread_num ++)
  {
    pthread_create(threads + thread_num, NULL, fixed_malloc_thread, NULL);
  }
  for (;thread_num < THREAD_NUM; thread_num ++)
  {
    pthread_create(threads + thread_num, NULL, var_malloc_thread, NULL);
  }
  for(thread_num = 0; thread_num < THREAD_NUM; thread_num ++)
  {
    pthread_join(threads[thread_num], &result);
  }
  return 0;
}
