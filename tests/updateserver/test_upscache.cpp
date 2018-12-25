/**
 * (C) 2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * test_upscache.cpp for test ups cache 
 *
 * Authors: 
 *   rongxuan< rongxuan.lc@taobao.com>
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <new>
#include "updateserver/ob_ups_cache.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace updateserver;
using namespace common;

namespace oceanbase
{
  namespace updateserver
  {
    namespace test
    {
      class TestBlockCacheHelper : public ObUpsCache
      {
        public:

          void set_cache_size(const int64_t cache_size)
          {
            ObUpsCache::set_cache_size(cache_size);
          }

          void set_cache_size_limit(const int64_t cache_size_limit)
          {
            ObUpsCache::set_cache_size_limit(cache_size_limit);
          }

      };
      class TestBlockCache : public ::testing::Test
      {
        public:
          static void SetUpTestCase()
          {
          }
          static void TearDownTestCase()
          {
          }

          virtual void SetUp()
          {
          }

          virtual void TearDown()
          {
          }
        public:
          TestBlockCacheHelper blockcache;
      };

      TEST_F(TestBlockCache,set_cache_size)
      {
        blockcache.set_cache_size(0);
        blockcache.set_cache_size(1);
        blockcache.set_cache_size(1024L*1024L*1024L*5);
        blockcache.set_cache_size(1024*2);
      }

      TEST_F(TestBlockCache,get_cache_size)
      {
        int64_t size_limit = 1024L*1024L*800L;
        blockcache.set_cache_size_limit(size_limit);
        EXPECT_EQ(size_limit, blockcache.get_cache_size_limit());

        int64_t cache_size = 1024L*1024L*800L;
        blockcache.set_cache_size(cache_size);
        EXPECT_EQ(size_limit,blockcache.get_cache_size());
        

        cache_size = 1024L*1024L*500L;
        blockcache.set_cache_size(0);
        EXPECT_EQ(cache_size,blockcache.get_cache_size());

        blockcache.set_cache_size(1);
        EXPECT_EQ(cache_size, blockcache.get_cache_size());

        blockcache.set_cache_size(cache_size);
        EXPECT_EQ(cache_size,blockcache.get_cache_size());
        
        cache_size = 1024L*1024L*900L;
        blockcache.set_cache_size(cache_size);

        int64_t size = 1024L*1024L*800L;
        EXPECT_EQ(size,blockcache.get_cache_size());
      }

      TEST_F(TestBlockCache, set_cache_size_limit)
      {
        int64_t size_limit = 0;
        blockcache.set_cache_size_limit(size_limit);
        size_limit = 1;
        blockcache.set_cache_size_limit(size_limit);
        size_limit = 1024L*1024L*1024L;
        blockcache.set_cache_size_limit(size_limit);
        size_limit = 1024*2;
        blockcache.set_cache_size_limit(size_limit);
      }

      TEST_F(TestBlockCache, get_cache_size_limit)
      {
        int64_t cache_size_limit = 0;

        blockcache.set_cache_size_limit(0);
        cache_size_limit = blockcache.get_cache_size_limit();
        printf("cache_size_limit=%ld\n",cache_size_limit); 

        blockcache.set_cache_size_limit(1);
        cache_size_limit = blockcache.get_cache_size_limit();
        printf("cache_size_limit=%ld\n",cache_size_limit);

        blockcache.set_cache_size_limit(1024L*1024L*1024L*5L);
        cache_size_limit = blockcache.get_cache_size_limit();
        printf("cache_size_limit=%ld\n",cache_size_limit);

        blockcache.set_cache_size_limit(1024*2);
        cache_size_limit = blockcache.get_cache_size_limit();
        printf("cache_size_limit=%ld\n",cache_size_limit);
      }

      TEST_F(TestBlockCache,init)
      {
        int64_t cache_size_limit = 0;
        int64_t cache_size = 0;
        EXPECT_EQ(OB_SUCCESS,blockcache.init(cache_size,cache_size_limit));
        EXPECT_EQ(1024L*1024L*500L,blockcache.get_cache_size());
        EXPECT_EQ(1024L*1024L*500L,blockcache.get_cache_size_limit());
        EXPECT_EQ(OB_INIT_TWICE,blockcache.init(cache_size,cache_size_limit));
        blockcache.destroy();

        cache_size_limit = 1;
        cache_size = 1;
        EXPECT_EQ(OB_SUCCESS,blockcache.init(cache_size,cache_size_limit));
        EXPECT_EQ(1024L*1024L*500L,blockcache.get_cache_size());
        EXPECT_EQ(1024L*1024L*500L,blockcache.get_cache_size_limit());
        blockcache.destroy();

        cache_size_limit = 1024*1024;
        cache_size = 1024*1024;
        EXPECT_EQ(OB_SUCCESS,blockcache.init(cache_size,cache_size_limit));
        EXPECT_EQ(1024L*1024L*500L*1024L,blockcache.get_cache_size());
        EXPECT_EQ(1024L*1024L*500L,blockcache.get_cache_size_limit());
        blockcache.destroy();

        cache_size_limit = 1024;
        cache_size = 1024*2;
        EXPECT_EQ(OB_SUCCESS,blockcache.init(cache_size,cache_size_limit));
        EXPECT_EQ(1024*1024*500,blockcache.get_cache_size());
        EXPECT_EQ(1024*1024*500,blockcache.get_cache_size_limit());
        blockcache.destroy();

        cache_size_limit = 1024*1024*1024;
        cache_size = 1024*1024*1024;
        EXPECT_EQ(OB_SUCCESS,blockcache.init(cache_size,cache_size_limit));
        EXPECT_EQ(1024*1024*1024,blockcache.get_cache_size());
        EXPECT_EQ(1024*1024*1024,blockcache.get_cache_size_limit());
        blockcache.destroy();

        cache_size_limit = 800*1024*1024;
        cache_size = 1024*1024*1024;
        EXPECT_EQ(OB_SUCCESS,blockcache.init(cache_size,cache_size_limit));
        EXPECT_EQ(1024*1024*800,blockcache.get_cache_size());
        EXPECT_EQ(1024*1024*800,blockcache.get_cache_size_limit());
        blockcache.destroy();

        cache_size_limit = 1024;
        cache_size = 1;
        EXPECT_EQ(OB_SUCCESS,blockcache.init(cache_size,cache_size_limit));
        EXPECT_EQ(1024*1024*500,blockcache.get_cache_size());
        EXPECT_EQ(1024*1024*500,blockcache.get_cache_size_limit());
        blockcache.destroy();

        cache_size_limit = 1024*1024*1024;
        cache_size = 1024*1024;
        EXPECT_EQ(OB_SUCCESS,blockcache.init(cache_size,cache_size_limit));
        EXPECT_EQ(1024*1024*500,blockcache.get_cache_size());
        EXPECT_EQ(1024*1024*1024,blockcache.get_cache_size_limit());
        blockcache.destroy();

        cache_size_limit = 1;
        cache_size = 1024*2;
        EXPECT_EQ(OB_SUCCESS,blockcache.init(cache_size,cache_size_limit));
        EXPECT_EQ(1024*1024*500,blockcache.get_cache_size());
        EXPECT_EQ(1024*1024*500,blockcache.get_cache_size_limit());
        blockcache.destroy();
      }

      TEST_F(TestBlockCache, put)
      {
        uint64_t table_id = -1;
        uint64_t column_id = 2;
        char buffer[20] = "145abc";                      
        ObString row_key(20,7,buffer);                   
        //ObBufferHandle handle;                         
        ObUpsCacheValue value; 
        EXPECT_EQ(OB_ERROR,blockcache.put(table_id,row_key,column_id,value) );
        
        blockcache.init(1024*1024*1024,1024*1024*1024);
        EXPECT_EQ(OB_ERROR,blockcache.put(table_id,row_key,column_id,value) );
        
        table_id = 1001;
        column_id = -1;
        EXPECT_EQ(OB_ERROR,blockcache.put(table_id,row_key,column_id,value) );
        
        column_id = 2;
        ObString valid_row_key;
        EXPECT_EQ(OB_ERROR,blockcache.put(table_id,valid_row_key,column_id,value) );
     
        EXPECT_EQ(OB_ERROR,blockcache.put(table_id,row_key,column_id,value) );
        
        //未初始化得obj;
        common::ObObj obj;
        int64_t int_val = 10;
        ObUpsCacheValue val1(1,obj);
        EXPECT_EQ(OB_ERROR,blockcache.put(table_id,row_key,column_id,val1) );
        
        //int的obj对象
        obj.set_int(int_val);
        ObUpsCacheValue val(1,obj);
        EXPECT_EQ(OB_SUCCESS,blockcache.put(table_id,row_key,column_id,val) );
        
        //int的obj对象，成功覆盖
        common::ObObj obj6;
        int_val = 100;
        obj6.set_int(int_val);
        ObUpsCacheValue val6(2,obj6);
        EXPECT_EQ(OB_SUCCESS,blockcache.put(table_id,row_key,column_id,val6) );
        
        //变长的obj对象，成功
        ObString obj_string = row_key;
        obj.set_varchar(obj_string);
        ObUpsCacheValue val2(1,obj);
        table_id = 11;
        //EXPECT_EQ(OB_ENTRY_EXIST,blockcache.put(table_id,row_key,column_id,val2) );
        EXPECT_EQ(OB_SUCCESS,blockcache.put(table_id,row_key,column_id,val2) );
        
       //用未初始化的obstring来建立obj对象
        ObString obj_string2;
        obj.set_varchar(obj_string2);
        ObUpsCacheValue val3(1,obj);
        EXPECT_EQ(OB_ERROR,blockcache.put(table_id,row_key,column_id,val3) );
        blockcache.clear();
        blockcache.destroy();
      }

      TEST_F(TestBlockCache, ObUpsCacheKey)
      {
        ObUpsCacheKey key_a;
        key_a.table_id = 1001;
        key_a.column_id = 10;
        char *buffer = "123456";
        key_a.nbyte = 7;
        key_a.buffer = buffer;

        ObUpsCacheKey key_b;
        key_b.table_id = 1001;
        key_b.column_id = 10;
        char *buf = "124456";
        key_b.nbyte = 7;
        key_b.buffer = buf;
        EXPECT_EQ(false,(key_a == key_b));
      }

      TEST_F(TestBlockCache, get)
      {
        uint64_t table_id = -1;
        uint64_t column_id = 10;
        char *buffer = "145abc";
        ObString row_key(7,7,buffer);
        ObBufferHandle *handle = new ObBufferHandle();
        ObString obj_string = row_key;
        common::ObObj obj;
        obj.set_varchar(obj_string);
        ObUpsCacheValue val2;
        val2.version = 1;
        val2.value = obj;
        EXPECT_EQ(OB_ERROR,blockcache.put(table_id,row_key,column_id,val2));
      
        blockcache.init(1024,2048);
        table_id = 1001;
        EXPECT_EQ(OB_SUCCESS,blockcache.put(table_id,row_key,column_id,val2));        
        

        ObUpsCacheValue val3;
        EXPECT_EQ(OB_SUCCESS,blockcache.get(table_id,row_key,*handle,column_id,val3));
        EXPECT_EQ(1,val3.version);
        EXPECT_EQ(6,val3.value.get_type());    
        delete handle;
        
       /* ObBufferHandle *handle5;
        common::ObObj obj4;
        const int64_t inint = 12;
        obj4.set_int(inint);
        ObUpsCacheValue val4;
        val4.version = 4;
        val4.value = obj4;
        EXPECT_EQ(OB_SUCCESS,blockcache.put(table_id,row_key,column_id,val4));
        ObUpsCacheValue val5;
        EXPECT_EQ(OB_SUCCESS,blockcache.get(table_id,row_key,*handle5,column_id,val5));
        EXPECT_EQ(4,val5.version);
        int64_t retvalue = 0;
        val5.value.get_int(retvalue);
        EXPECT_EQ(12,retvalue);
        delete handle5;*/

        ObBufferHandle *handle2 = new ObBufferHandle(); 
        table_id = 1002;
        EXPECT_NE(OB_SUCCESS,blockcache.get(table_id,row_key,*handle2,column_id,val3)); 
        delete handle2;

        ObBufferHandle *handle3 = new ObBufferHandle();
        table_id = 1001;
        column_id = -1;
        EXPECT_EQ(OB_ERROR,blockcache.get(table_id,row_key,*handle3,column_id,val2));
        delete handle3;

        ObBufferHandle *handle4 = new ObBufferHandle();
        column_id = 1;
        ObString valid_row_key;
        EXPECT_EQ(OB_ERROR,blockcache.get(table_id,valid_row_key,*handle4,column_id,val2));
        delete handle4;

        blockcache.clear();
        blockcache.destroy();
      }

      TEST_F(TestBlockCache,set_row_exist)
      {
        uint64_t table_id = 1001;
        char *buf = "123456";
        ObString row_key(strlen(buf),strlen(buf),buf);
        bool is_exist = false;
        EXPECT_EQ(OB_ERROR,blockcache.set_row_exist(table_id,row_key,is_exist));
        
        blockcache.init(1024,2048);
        EXPECT_EQ(OB_SUCCESS,blockcache.set_row_exist(table_id,row_key,is_exist));

        table_id = -1;
        EXPECT_EQ(OB_ERROR,blockcache.set_row_exist(table_id,row_key,is_exist));

        table_id = 1001;
        ObString rowkey2;
        EXPECT_EQ(OB_ERROR,blockcache.set_row_exist(table_id,rowkey2,is_exist));
     
        ObString rowkey3(2,2,NULL);
        EXPECT_EQ(OB_ERROR,blockcache.set_row_exist(table_id,rowkey3,is_exist));

        is_exist = true;
        //table_id = 111;
        EXPECT_EQ(OB_SUCCESS,blockcache.set_row_exist(table_id,row_key,is_exist));
        //EXPECT_EQ(OB_ENTRY_EXIST,blockcache.set_row_exist(table_id,row_key,is_exist));
       
        is_exist = false;
        EXPECT_EQ(OB_SUCCESS,blockcache.set_row_exist(table_id,row_key,is_exist));

        table_id = 1003;
        EXPECT_EQ(OB_SUCCESS,blockcache.set_row_exist(table_id,row_key,is_exist));
        
        blockcache.clear();
        blockcache.destroy();
      }

      TEST_F(TestBlockCache,is_row_exist)
      {
        uint64_t table_id = 1001;
        char *buf = "123456";
        bool is_exist = false;
        ObString row_key(strlen(buf),strlen(buf),buf);
        ObBufferHandle *handle = new ObBufferHandle();
        EXPECT_EQ(OB_ERROR,blockcache.is_row_exist(table_id,row_key,is_exist,*handle));

        blockcache.init(1024,2048);
        
        EXPECT_EQ(OB_UPS_NOT_EXIST,blockcache.is_row_exist(table_id,row_key,is_exist,*handle));
        delete handle;

        handle = new ObBufferHandle();
        EXPECT_EQ(OB_SUCCESS,blockcache.set_row_exist(table_id,row_key,true));
        EXPECT_EQ(OB_SUCCESS,blockcache.is_row_exist(table_id,row_key,is_exist,*handle));
        EXPECT_EQ(true,is_exist);
        delete handle;
        
        ObBufferHandle *handle2 = new ObBufferHandle();
        EXPECT_EQ(OB_SUCCESS,blockcache.set_row_exist(table_id,row_key,false));
       // EXPECT_EQ(OB_ENTRY_EXIST,blockcache.set_row_exist(table_id,row_key,false));
        EXPECT_EQ(OB_SUCCESS,blockcache.is_row_exist(table_id,row_key,is_exist,*handle2));
        EXPECT_EQ(false,is_exist);
        //EXPECT_EQ(true,is_exist);
        delete handle2;


        ObBufferHandle *handle3 = new ObBufferHandle();
        table_id = -1;
        EXPECT_EQ(OB_ERROR,blockcache.is_row_exist(table_id,row_key,is_exist,*handle3));
        delete handle3;
        
        ObBufferHandle *handle4 = new ObBufferHandle();
        table_id = 1001;
        ObString rowkey2(2,2,NULL);
        EXPECT_EQ(OB_ERROR,blockcache.is_row_exist(table_id,rowkey2,is_exist,*handle4));                 
        delete handle4;

        blockcache.destroy();
      }

      TEST_F(TestBlockCache,destroy)
      {
        EXPECT_EQ(OB_ERROR,blockcache.destroy());
        blockcache.init(1024,1024);
        EXPECT_EQ(OB_SUCCESS,blockcache.destroy());
        EXPECT_EQ(OB_ERROR,blockcache.clear());
      }

      TEST_F(TestBlockCache,clear)
      {
        EXPECT_EQ(OB_ERROR,blockcache.clear());
        
        blockcache.init(1024,2048);
        EXPECT_EQ(OB_SUCCESS,blockcache.clear());

        uint64_t table_id = 1001;
        char *buf = "123456";
        bool is_exist = false;
        ObString row_key(strlen(buf),strlen(buf),buf);
        ObBufferHandle *handle = new ObBufferHandle();
        EXPECT_EQ(OB_SUCCESS,blockcache.set_row_exist(table_id,row_key,true));
        EXPECT_EQ(OB_SUCCESS,blockcache.is_row_exist(table_id,row_key,is_exist,*handle));
        EXPECT_EQ(true,is_exist);
        blockcache.clear();
        delete handle;
        blockcache.clear();

        ObBufferHandle *handle2 = new ObBufferHandle(); 
        EXPECT_EQ(OB_UPS_NOT_EXIST,blockcache.is_row_exist(table_id,row_key,is_exist,*handle2));
        delete handle2;

        blockcache.destroy();
      }

    }
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("ERROR");
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}


