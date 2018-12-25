#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>

#include "common/ob_schema.h"
#include "common/ob_malloc.h"
#include "common/location/ob_btree_map.h"

using namespace std;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestBtreeMap: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};

TEST_F(TestBtreeMap, test_create)
{
  ObBtreeMap<int, int> tree;
  EXPECT_TRUE(OB_SUCCESS != tree.create(0));
  EXPECT_TRUE(OB_SUCCESS == tree.create(10));
  EXPECT_TRUE(OB_SUCCESS != tree.create(10));
}

TEST_F(TestBtreeMap, test_set)
{
  int old = 0;
  ObBtreeMap<int, int> tree;
  EXPECT_TRUE(OB_SUCCESS != tree.set(1, 3, old));
  EXPECT_TRUE(OB_SUCCESS == tree.create(10));

  int MAX_COUNT = 100;
  for (int i = 0; i < MAX_COUNT; ++i)
  {
    old = 0;
    EXPECT_TRUE(OB_SUCCESS == tree.set(i, i * 2, old));
    EXPECT_TRUE(0 == old);

    EXPECT_TRUE(OB_SUCCESS == tree.set(i, i * 3, old));
    EXPECT_TRUE(i * 2 == old);
  }
}

TEST_F(TestBtreeMap, test_get)
{
  ObBtreeMap<int, int> tree;
  int value = 0;
  EXPECT_TRUE(OB_SUCCESS != tree.get(1, value));
  EXPECT_TRUE(OB_SUCCESS == tree.create(10));

  int MAX_COUNT = 100;
  int old = 0;
  for (int i = 0; i < MAX_COUNT; ++i)
  {
    old = 0;
    EXPECT_TRUE(OB_SUCCESS == tree.set(i, i * 2, old));
    EXPECT_TRUE(0 == old);

    EXPECT_TRUE(OB_SUCCESS == tree.set(i, i * 3, old));
    EXPECT_TRUE(i * 2 == old);

    EXPECT_TRUE(OB_SUCCESS == tree.get(i, value));
    EXPECT_TRUE(i * 3 == value);
  }

  for (int i = MAX_COUNT; i < MAX_COUNT + 10; ++i)
  {
    EXPECT_TRUE(OB_SUCCESS != tree.get(i, value));
  }
}

TEST_F(TestBtreeMap, test_erase)
{
  ObBtreeMap<int, int> tree;
  int old = 0;
  int value = 0;
  EXPECT_TRUE(OB_SUCCESS != tree.erase(1, value));
  EXPECT_TRUE(OB_SUCCESS == tree.create(10));
  int MAX_COUNT = 100;
  for (int i = 0; i < MAX_COUNT; ++i)
  {
    old = 0;
    EXPECT_TRUE(OB_SUCCESS == tree.set(i, i * 2, old));
    EXPECT_TRUE(0 == old);

    EXPECT_TRUE(OB_SUCCESS == tree.set(i, i * 3, old));
    EXPECT_TRUE(i * 2 == old);

    EXPECT_TRUE(OB_SUCCESS == tree.get(i, value));
    EXPECT_TRUE(i * 3 == value);
  }


  for (int i = MAX_COUNT; i < MAX_COUNT + 10; ++i)
  {
    EXPECT_TRUE(OB_SUCCESS != tree.erase(i, value));
    EXPECT_TRUE(OB_SUCCESS != tree.get(i, value));
  }

  for (int i = 0; i < MAX_COUNT; ++i)
  {
    EXPECT_TRUE(OB_SUCCESS == tree.erase(i, value));
    EXPECT_TRUE(value == i * 3);
    EXPECT_TRUE(OB_SUCCESS != tree.erase(i, value));
  }

  for (int i = 0; i < MAX_COUNT; ++i)
  {
    EXPECT_TRUE(OB_SUCCESS != tree.get(i, value));
  }
}

TEST_F(TestBtreeMap, test_size)
{
  ObBtreeMap<int, int> tree;
  EXPECT_TRUE(0 == tree.size());
  EXPECT_TRUE(OB_SUCCESS == tree.create(10));
  EXPECT_TRUE(0 == tree.size());
  int MAX_COUNT = 100;
  int old = 0;
  int value = 0;
  for (int i = 0; i < MAX_COUNT; ++i)
  {
    old = 0;
    EXPECT_TRUE(OB_SUCCESS == tree.set(i, i * 2, old));
    EXPECT_TRUE(0 == old);

    EXPECT_TRUE(OB_SUCCESS == tree.set(i, i * 3, old));
    EXPECT_TRUE(i * 2 == old);

    EXPECT_TRUE(OB_SUCCESS == tree.get(i, value));
    EXPECT_TRUE(i * 3 == value);
    EXPECT_TRUE(i + 1 == tree.size());
  }

  for (int i = MAX_COUNT; i < MAX_COUNT + 10; ++i)
  {
    EXPECT_TRUE(OB_SUCCESS != tree.erase(i, value));
    EXPECT_TRUE(OB_SUCCESS != tree.get(i, value));
    EXPECT_TRUE(MAX_COUNT == tree.size());
  }

  for (int i = 0; i < MAX_COUNT; ++i)
  {
    EXPECT_TRUE(OB_SUCCESS == tree.erase(i, value));
    EXPECT_TRUE(OB_SUCCESS != tree.erase(i, value));
    EXPECT_TRUE(MAX_COUNT - i - 1 == tree.size());
  }

  for (int i = 0; i < MAX_COUNT; ++i)
  {
    EXPECT_TRUE(OB_SUCCESS != tree.get(i, value));
    EXPECT_TRUE(0 == tree.size());
  }
}

