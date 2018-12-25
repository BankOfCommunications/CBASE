#include "ob_lrucache.h"
#include "ob_malloc.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace common;

struct value_t
{
  uint64_t value;
  value_t(uint64_t v)
  {
    value = v;
  };
  void release()
  {
  };
  bool operator == (const value_t &other) const
  {
    return (other.value == value);
  };
};

TEST(TestObLruCache, init)
{
  ObLruCache<uint64_t, value_t> lc;
  
  // 参数错
  EXPECT_EQ(OB_ERROR, lc.init(0, 0));
  EXPECT_EQ(OB_ERROR, lc.init(-1, -1));
  // 正常初始化
  EXPECT_EQ(OB_SUCCESS, lc.init(1, 1000));
  // 重复初始化
  EXPECT_EQ(OB_ERROR, lc.init(1, 1000));
  EXPECT_EQ(OB_SUCCESS, lc.destroy());
}

TEST(TestObLruCache, destroy)
{
  ObLruCache<uint64_t, value_t> lc;

  EXPECT_EQ(OB_SUCCESS, lc.destroy());
  EXPECT_EQ(OB_SUCCESS, lc.init(1, 1000));
  EXPECT_EQ(OB_SUCCESS, lc.destroy());
}

TEST(TestObLruCache, set)
{
  ObLruCache<uint64_t, value_t> lc;
  uint64_t key = 1;
  value_t data(1000);

  // 没有初始化
  EXPECT_EQ(OB_ERROR, lc.set(key, &data));

  lc.init(1, 1000);

  // 正常插入
  EXPECT_EQ(OB_SUCCESS, lc.set(key, &data));
  // 参数错
  EXPECT_EQ(OB_ERROR, lc.set(key, NULL));
  // 插入已存在
  EXPECT_EQ(OB_ERROR, lc.set(key, &data));
  // 已满
  EXPECT_EQ(OB_ERROR, lc.set(key + 1, &data));
  // 已满 但是时间满足淘汰要求
  usleep(1000);
  EXPECT_EQ(OB_SUCCESS, lc.set(key + 1, &data));

  fprintf(stderr, "==================================================\n");
  EXPECT_EQ(OB_SUCCESS, lc.clear());
}

TEST(TestObLruCache, get)
{
  ObLruCache<uint64_t, value_t> lc;
  uint64_t key = 1;
  value_t data(1000);
  value_t *nil = NULL;

  // 没有初始化
  EXPECT_EQ(nil, lc.get(key));

  lc.init(10, 1000);

  // 没有hit
  EXPECT_EQ(nil, lc.get(key));
  // 正常命中
  lc.set(1, &data);
  EXPECT_EQ(&data, lc.get(key)); 
 
  // 测试get加锁 
  EXPECT_EQ(OB_ERROR, lc.clear());
  lc.revert(key);
  fprintf(stderr, "==================================================\n");
  EXPECT_EQ(OB_SUCCESS, lc.clear());
}

TEST(TestObLruCache, revert)
{
  ObLruCache<uint64_t, value_t> lc;
  uint64_t key = 1;
  value_t data(1000);

  // 没有初始化
  EXPECT_EQ(OB_ERROR, lc.revert(key));

  lc.init(10, 1000);

  // 没有hit
  EXPECT_EQ(OB_ERROR, lc.revert(key));
  lc.set(key, &data);
  // 没有获取过
  EXPECT_EQ(OB_ERROR, lc.revert(key));
  
  // 测试revert解锁
  lc.get(key);
  EXPECT_EQ(OB_SUCCESS, lc.revert(key));
  fprintf(stderr, "==================================================\n");
  EXPECT_EQ(OB_SUCCESS, lc.clear());
}

TEST(TestObLruCache, free)
{
  ObLruCache<uint64_t, value_t> lc;
  uint64_t key = 1;
  value_t data(1000);
  value_t *nil = NULL;

  // 没有初始化
  EXPECT_EQ(OB_ERROR, lc.free(key, LC_RELEASE_IMMED));
  EXPECT_EQ(OB_ERROR, lc.free(key, LC_RELEASE_DELAY));

  lc.init(10, 1000);

  // free不存在的
  EXPECT_EQ(OB_SUCCESS, lc.free(key, LC_RELEASE_IMMED));
  EXPECT_EQ(OB_SUCCESS, lc.free(key, LC_RELEASE_DELAY));
  // 正常
  lc.set(key, &data);
  EXPECT_EQ(OB_SUCCESS, lc.free(key, LC_RELEASE_IMMED));
  // free之后可以重新插入
  EXPECT_EQ(OB_SUCCESS, lc.set(key, &data));
  // 测试锁被占用
  lc.get(key);
  EXPECT_EQ(OB_SUCCESS, lc.free(key, LC_RELEASE_IMMED));
  lc.revert(key);
  EXPECT_EQ(&data, lc.get(key)); 
  EXPECT_EQ(OB_SUCCESS, lc.free(key, LC_RELEASE_DELAY));
  lc.revert(key);
  EXPECT_EQ(nil, lc.get(key));

  lc.revert(key);
  fprintf(stderr, "==================================================\n");
  EXPECT_EQ(OB_SUCCESS, lc.clear());
}

TEST(TestObLruCache, clear)
{
  ObLruCache<uint64_t, value_t> lc;
  uint64_t key = 1;
  value_t data(1000);

  // 没有初始化
  EXPECT_EQ(OB_ERROR, lc.clear());

  lc.init(10, 1000);

  // 空cache
  EXPECT_EQ(OB_SUCCESS, lc.clear());
  // 有数据
  lc.set(key, &data);
  EXPECT_EQ(OB_SUCCESS, lc.clear());
  // 锁被占用
  lc.set(key, &data);
  lc.get(key);
  EXPECT_EQ(OB_ERROR, lc.clear());
  lc.revert(key);
  EXPECT_EQ(OB_SUCCESS, lc.clear());
  // 数据被free掉
  lc.set(key, &data);
  lc.free(key, 0);
  fprintf(stderr, "==================================================\n");
  EXPECT_EQ(OB_SUCCESS, lc.clear());
}

TEST(TestObLruCache, gc)
{
  ObLruCache<uint64_t, value_t> lc;
  uint64_t key = 1;
  value_t data(1000);
  value_t *nil = NULL;

  // 没有初始化
  lc.gc();
  lc.init(10, 1000);

  // 有数据
  lc.set(key, &data);
  lc.gc();
  EXPECT_EQ(&data, lc.get(key));
  lc.revert(key);
  usleep(1000);
  lc.gc();
  EXPECT_EQ(nil, lc.get(key));

  // 锁被占用
  lc.set(key, &data);
  lc.get(key);
  usleep(1000);
  lc.gc();
  EXPECT_EQ(&data, lc.get(key));
  lc.revert(key);
  lc.revert(key);
  EXPECT_EQ(&data, lc.get(key));
  lc.revert(key);
  lc.gc();
  EXPECT_EQ(nil, lc.get(key));
  fprintf(stderr, "==================================================\n");
  EXPECT_EQ(OB_SUCCESS, lc.clear());
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

