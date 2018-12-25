/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * test_bloom_filter.cpp for test bloom filter 
 *
 * Authors: 
 *   fanggang< fanggang@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */

#include <gtest/gtest.h>
#include <stdint.h>
#include <string.h>
#include <iostream>
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/murmur_hash.h"
#include "common/ob_bloomfilter.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace std;

//generate the key string
char* random_str(uint len)
{
  ::srandom(1);
  char *str = new char[len];
  long rand = 0;
  uint offset = 0;

  while(offset < len)
  {
    rand = ::random() % 256;
    str[offset] = (char)rand;
    offset ++;
  }
  return str;
}

struct ObStringHash
{
  uint64_t operator()(const ObString& key, uint64_t hash)
  {
    return murmurhash64A(key.ptr(), key.length(), hash);
  }
};

TEST(BLOOM_FILTER, test)
{
  //element count is about 32k  = 256m/ 0.3k
  uint32_t num = 1024 * 32;
  uint key_len = 17;
  ObBloomFilter<ObString, ObStringHash, ObTCMalloc> bfilter;
  uint i = 0;
  char *array[num];
  int ret = bfilter.init(3, 1024*1024LL*1);
  EXPECT_TRUE(OB_SUCCESS == ret);
  while(i < num) {
    char *key_str = random_str(key_len);
    array[i] = key_str;
    ObString key(0, key_len, key_str);
    bfilter.insert(key);
    i++;
  }

  i = 0;
  bool is_contain = false;
  while(i < num)
  {
    ObString key(0, key_len, array[i]);
    is_contain = bfilter.may_contain(key);
    EXPECT_EQ(true, is_contain);
    delete [] array[i];
    i++;
  }

  //test not contain key
  char* out_key = new char[key_len];
  memset(out_key, 0, key_len);
  ObString key(0, key_len, out_key);
  is_contain = bfilter.may_contain(key);
  EXPECT_EQ(is_contain, false);
  memset(out_key, 1, key_len);
  key.assign_ptr(out_key, key_len);
  is_contain = bfilter.may_contain(key);
  EXPECT_EQ(is_contain, false);
  delete [] out_key;
}

int main(int argc, char **argv)
{
  TBSYS_LOGGER.setLogLevel("INFO");
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
