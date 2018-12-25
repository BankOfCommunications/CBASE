/**
 * (C) 2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * test_pthread_upscache.cpp for stress test of ups cache 
 *
 * Authors: 
 *   rongxuan<rongxuan.lc@taobao.com>
 *  
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <new>
#include <pthread.h>
#include "updateserver/ob_ups_cache.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace updateserver;
using namespace oceanbase::common;

const uint64_t g_pthread_num = 10;
const uint64_t g_times_per_thread = 1024;
const uint64_t g_key_num = 127;
ObUpsCacheKey g_key_array[g_key_num];
ObUpsCacheValue g_value_array[g_key_num];
ObUpsCacheValue g_value_result[g_key_num];
char *row_key = "hello1234";
const uint64_t g_nbyte = 10;
common::ObObj g_obj_int;
common::ObObj g_obj_var;
ObUpsCache uc;
common::ObString rowkey(g_nbyte, g_nbyte, row_key); 


void prepare_date()
{
  uint64_t i = 0;
  common::ObString str(g_nbyte, g_nbyte, row_key);
  g_obj_var.set_varchar(str);

  for(; i<g_key_num; i++)
  {
    g_key_array[i].table_id = i;
    g_key_array[i].column_id = i+1;
    g_key_array[i].nbyte = g_nbyte;
    g_key_array[i].buffer = row_key;
    
    g_value_array[i].version = i;
    g_obj_int.set_int(i);
    g_value_array[i].value = g_obj_int;
  }
}

void *thread_func(void *arg)
{
  int64_t int_val= 0;
  int j = 1;
  bool is_exist = false;
  for(uint64_t i = 1; i < g_times_per_thread + 1; i++)
  {
    j = i % g_key_num;
    uc.put(g_key_array[j].table_id,rowkey,g_key_array[j].column_id,g_value_array[j]);

    ObBufferHandle *handle = new ObBufferHandle();
    common::ObObj obj;
    if(OB_SUCCESS == uc.get(g_key_array[j].table_id,rowkey,*handle,g_key_array[j].column_id,g_value_result[j]))
    {
      if(j != g_value_result[j].version)
      {
        TBSYS_LOG(ERROR,"the error version");
        TBSYS_LOG(INFO,"j=%d,version=%ld",j,g_value_result[j].version);
      }
      g_value_result[j].value.get_int(int_val);
      if(j != int_val)
      {
        TBSYS_LOG(ERROR,"the error value");
        TBSYS_LOG(INFO,"j=%d,int_val=%d",j,int_val);
      }
    }
    else
    {
      TBSYS_LOG(WARN,"get failed");
    }
    if(NULL != handle)
      delete handle;
    
    if(i/2 == 0)
    {
      is_exist = false;
    }
    else
    {
      is_exist = true;
    }
    uc.set_row_exist(g_key_array[j].table_id,rowkey,is_exist);

    bool is_result = false;
    ObBufferHandle *handle2 = new ObBufferHandle();
    uc.is_row_exist(g_key_array[j].table_id,rowkey,is_result,*handle2);
    if(is_result != is_exist)
    {
      TBSYS_LOG(WARN,"is_row_exist failed");
    }

    if(NULL != handle2)
      delete handle2;

  }
  return NULL;
}

void thread_test()
{
  pthread_t *pd = new pthread_t[g_pthread_num];
  for(uint32_t i = 0; i < g_pthread_num; i++)
  {
    pthread_create(pd+i, NULL,thread_func,NULL);
  }
  for(uint32_t i = 0; i < g_pthread_num; i++)
  {
    pthread_join(pd[i],NULL);
  }
  delete[] pd;
}
int main(int argc, char **argv)
{
  int ret = OB_SUCCESS;
  uint i = 0;
  prepare_date();
  for(; i < g_key_num; i++)
  {
 //   TBSYS_LOG(INFO,"%d",g_value_array[i].version);
  }
  
  ob_init_memory_pool();
  uc.init(1024L*1024L*800L,2048L*1024L*1024L);
  thread_test();
  uc.destroy();
  return ret;
}
