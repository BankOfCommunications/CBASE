/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_desc_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "common/ob_row_desc.h"
#include "common/ob_malloc.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;

class ObRowDescTest: public ::testing::Test
{
  public:
    ObRowDescTest();
    virtual ~ObRowDescTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObRowDescTest(const ObRowDescTest &other);
    ObRowDescTest& operator=(const ObRowDescTest &other);
  private:
    // data members
};

ObRowDescTest::ObRowDescTest()
{
}

ObRowDescTest::~ObRowDescTest()
{
}

void ObRowDescTest::SetUp()
{
}

void ObRowDescTest::TearDown()
{
}

TEST_F(ObRowDescTest, basic_test)
{
  ObRowDesc row_desc;
  ASSERT_EQ(0, row_desc.get_column_num());
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1000, 16));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1000, 17));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1000, 18));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1000, 19));
  ASSERT_NE(OB_SUCCESS, row_desc.add_column_desc(1000, 19));

  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 16));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 17));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 18));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 19));
  ASSERT_EQ(8, row_desc.get_column_num());
  int64_t cidx = OB_INVALID_INDEX;
  int64_t count = 0;
  for (uint64_t tid = 1000; tid <= 1001; ++tid)
  {
    for (uint64_t cid = 16; cid <= 19; ++cid)
    {
      cidx = row_desc.get_idx(tid, cid);
      ASSERT_NE(OB_INVALID_INDEX, cidx);
      ASSERT_EQ(count, cidx);
      TBSYS_LOG(DEBUG, "get_idx tid=%lu cid=%lu index=%ld", tid, cid, count);
      ++count;
    }
  }
  uint64_t out_tid = OB_INVALID_ID;
  uint64_t out_cid = OB_INVALID_ID;
  count = 0;
  for (uint64_t tid = 1000; tid <= 1001; ++tid)
  {
    for (uint64_t cid = 16; cid <= 19; ++cid)
    {
      ASSERT_EQ(OB_SUCCESS, row_desc.get_tid_cid(count, out_tid, out_cid));
      ASSERT_EQ(tid, out_tid);
      ASSERT_EQ(cid, out_cid);
      TBSYS_LOG(DEBUG, "get_tid_cid tid=%lu cid=%lu index=%ld", out_tid, out_cid, count);
      ++count;
    }
  }
  ASSERT_EQ(0U, row_desc.get_hash_collisions_count());
}

TEST_F(ObRowDescTest, perf_test)
{
  ObRowDesc row_desc;
  // build
  for (int i = 0; i < OB_ROW_MAX_COLUMNS_COUNT; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 16+i));
  }
  ASSERT_EQ(OB_ROW_MAX_COLUMNS_COUNT, row_desc.get_column_num());
  TBSYS_LOG(WARN, "hash_collisions=%lu", row_desc.get_hash_collisions_count());
  // get idx
  int64_t start_time = tbsys::CTimeUtil::getTime();
  for (int round = 0; round < 1000; ++round)
  {
    for (int i = OB_ROW_MAX_COLUMNS_COUNT-1; i >= 0; --i)
    {
      int64_t cidx = row_desc.get_idx(1001, 16+i);
      ASSERT_EQ(i, cidx);
    }
  }
  int64_t finish_time = tbsys::CTimeUtil::getTime();
  int64_t elapsed_ms = (finish_time-start_time)/1000;
  TBSYS_LOG(INFO, "ops=%ld elapsed_ms=%ld ops/ms=%ld",
            1000*OB_ROW_MAX_COLUMNS_COUNT, elapsed_ms,
            elapsed_ms == 0 ? 0 : 1000*OB_ROW_MAX_COLUMNS_COUNT/elapsed_ms);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
