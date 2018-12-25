/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * dlist_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "common/dlist.h"
#include "common/ob_malloc.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;

class DListTest: public ::testing::Test
{
  public:
    DListTest();
    virtual ~DListTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    DListTest(const DListTest &other);
    DListTest& operator=(const DListTest &other);
  protected:
    // data members
};

DListTest::DListTest()
{
}

DListTest::~DListTest()
{
}

void DListTest::SetUp()
{
}

void DListTest::TearDown()
{
}

struct Node: public DLink
{
  int data_;
};

void test(int total, int pop_n)
{
  printf("total=%d, pop_n=%d\n", total, pop_n);
  DList dlist;
  for (int i = 0; i < total; ++i)
  {
    Node *node = new Node();
    node->data_ = i;
    ASSERT_TRUE(NULL != node);
    ASSERT_TRUE(dlist.add_last(node));
  }
  ASSERT_EQ(total, dlist.get_size());

  DList range;
  dlist.pop_range(pop_n, range);
  int real_pop_n = std::min(pop_n, total);

  ASSERT_EQ(total - real_pop_n, dlist.get_size());
  ASSERT_EQ(real_pop_n, range.get_size());
  Node *node = dynamic_cast<Node*>(range.get_first());
  for (int i = 0; i < real_pop_n; ++i)
  {
    ASSERT_EQ(i, node->data_);
    node = dynamic_cast<Node*>(node->get_next());
  } // end for
  dlist.push_range(range);
  ASSERT_EQ(total, dlist.get_size());
  ASSERT_EQ(0, range.get_size());
}

TEST_F(DListTest, pop_range_test)
{
  for (int j = 0; j <= 3; ++j)
  {
    for (int i = 0; i < 5; ++i)
    {
      test(j, i);
    }
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
