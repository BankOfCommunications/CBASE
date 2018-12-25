/*
* (C) 2007-2011 TaoBao Inc.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
* test_blockmem.cpp is for what ...
*
* Version: $id$
*
* Authors:
*   MaoQi maoqi@taobao.com
*
*/
#include <gtest/gtest.h>
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "compactsstable/ob_block_membuf.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::compactsstable;


TEST(ObBlockMemBuf,test_basic)
{
  int64_t block_size = 2L * 1024 * 1024;
  ObBlockMembuf mem_buf(block_size);

  char *buf = new char [block_size];
  int64_t len = 0;
  int64_t total_len = 0;

  for(int i=0; i < 20; ++i)
  {
    len = labs( random() % (block_size - 2) ) + 1;
    ASSERT_EQ(0,mem_buf.write(buf,len));
    total_len += len;
  }
  ASSERT_EQ(total_len,mem_buf.get_data_size());
  mem_buf.clear();
}

TEST(ObBlockMemBuf,test_read)
{
  int64_t block_size = 2L * 1024 * 1024;
  ObBlockMembuf mem_buf(block_size);

  char *buf = new char [block_size];
  int64_t len = 0;
  int64_t total_len = 0;
  int value = 0;

  for(int i=1; i < 21; ++i)
  {
    len = i * 20;
    value = 96 + i;
    memset(buf,value,len);
    ASSERT_EQ(0,mem_buf.write(buf,len));
    total_len += len;
  }
  ASSERT_EQ(total_len,mem_buf.get_data_size());

  value = 0;
  const char* res = NULL;
  int64_t offset = 0;
  for(int i=1; i<21; ++i)
  {
    len = i * 20;
    value = 96 + i;
    memset(buf,value,len);
    res = mem_buf.read(offset,len);
    ASSERT_TRUE(res != NULL);
    ASSERT_EQ(0,memcmp(res,buf,len));
    offset += len;
  }
  mem_buf.clear();
}

int main(int argc, char **argv)
{
  common::ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
