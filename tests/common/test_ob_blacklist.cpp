#include "ob_define.h"
#include "gtest/gtest.h"
#include "ob_blacklist.h"

using namespace oceanbase::common;

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestBlackList: public ::testing::Test, public ObBlackList
{
public:
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
};


TEST_F(TestBlackList, init)
{
  ObBlackList list;
  EXPECT_TRUE(OB_SUCCESS != list.init(MAX_LIST_COUNT + 1, 0, 0, 10, 0));
  EXPECT_TRUE(OB_SUCCESS != list.init(MAX_LIST_COUNT - 1, 1, 0, 10, 0));
  EXPECT_TRUE(OB_SUCCESS != list.init(MAX_LIST_COUNT - 1, 10, 3, 10, 1));
  EXPECT_TRUE(OB_SUCCESS != list.init(MAX_LIST_COUNT - 1, 10, 30, 10, 0));
  EXPECT_TRUE(OB_SUCCESS == list.init(MAX_LIST_COUNT - 1, 10, 30, 10, 30));
  EXPECT_TRUE(OB_SUCCESS == list.init(MAX_LIST_COUNT - 1, 10, 30, 40, 30));
}

TEST_F(TestBlackList, check)
{
  ObBlackList list;
  for (int32_t i = 0; i < MAX_LIST_COUNT; ++i)
  {
    EXPECT_TRUE(false == list.check(i));
  }

  list.init(MAX_LIST_COUNT, 9, 10, 10, 1);
  for (int32_t i = 0; i < 2 * MAX_LIST_COUNT; ++i)
  {
    if (i < MAX_LIST_COUNT)
    {
      EXPECT_TRUE(list.check(i) == true);
    }
    else
    {
      EXPECT_TRUE(list.check(i) == false);
    }
  }

  for (int32_t i = 0; i < MAX_LIST_COUNT; ++i)
  {
    for (int32_t j = 1; j < DEFAULT_THRESHOLD; ++j)
    {
      list.update(false, i);
      EXPECT_TRUE(list.check(i) == true);
    }
    list.update(false, i);
    EXPECT_TRUE(list.check(i) == false);
    // succ again
    list.update(true, i);
    EXPECT_TRUE(list.check(i) == true);
  }
}

TEST_F(TestBlackList, update)
{
  ObBlackList list;
  int32_t count = 6;
  list.init(count, 9, 10, 10, 1);
  for (int32_t i = 0; i < count; ++i)
  {
    EXPECT_TRUE(false == list.update(false, i));
  }
  EXPECT_TRUE(OB_SUCCESS != list.update(false, count));
  for (int32_t i = 0; i < count; ++i)
  {
    for (int32_t j = 0; j < 10; ++j)
    {
      EXPECT_TRUE(OB_SUCCESS == list.update(false, i));
    }
  }
  for (int32_t i = 0; i < count; ++i)
  {
    EXPECT_TRUE(false == list.check(i));
  }
  for (int32_t i = 0; i < count; ++i)
  {
    EXPECT_TRUE(OB_SUCCESS == list.update(true, i));
    EXPECT_TRUE(true == list.check(i));
  }
}

TEST_F(TestBlackList, reset)
{
  ObBlackList list;
  for (int32_t i = 0; i < MAX_LIST_COUNT; ++i)
  {
    EXPECT_TRUE(false == list.check(i));
  }
  list.init(MAX_LIST_COUNT, 9, 10, 10, 1);
  for (int32_t i = 0; i < 2 * MAX_LIST_COUNT; ++i)
  {
    if (i < MAX_LIST_COUNT)
    {
      EXPECT_TRUE(list.check(i) == true);
    }
    else
    {
      EXPECT_TRUE(list.check(i) == false);
    }
  }

  for (int32_t i = 0; i < MAX_LIST_COUNT; ++i)
  {
    for (int32_t j = 1; j < DEFAULT_THRESHOLD; ++j)
    {
      list.update(false, i);
      EXPECT_TRUE(list.check(i) == true);
    }
    list.update(false, i);
    EXPECT_TRUE(list.check(i) == false);
    // succ again
    list.update(true, i);
    EXPECT_TRUE(list.check(i) == true);
    list.update(false, i);
    EXPECT_TRUE(list.check(i) == false);
    list.update(false, i);
    EXPECT_TRUE(list.check(i) == false);
  }

  for (int32_t i = 0; i < MAX_LIST_COUNT; ++i)
  {
    EXPECT_TRUE(list.check(i) == false);
  }

  list.reset();
  for (int32_t i = 0; i < MAX_LIST_COUNT; ++i)
  {
    EXPECT_TRUE(list.check(i) == true);
  }
}
