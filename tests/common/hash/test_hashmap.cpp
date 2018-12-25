#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include "ob_hashmap.h"
#include "ob_hashutils.h"
#include "ob_malloc.h"

#include "gtest/gtest.h"

using namespace oceanbase;
using namespace common;
using namespace hash;

uint32_t gHashItemNum = 128;
typedef uint64_t HashKey;
typedef uint64_t HashValue;

class CallBack
{
  public:
    void operator () (HashMapPair<HashKey, HashValue> &v)
    {
      v.second = v_;
    };
    void set_v(HashValue v)
    {
      v_ = v;
    };
  private:
    HashValue v_;
};

TEST(TestObHashMap, create)
{
  ObHashMap<HashKey, HashValue> hm;
  // 参数错误
  EXPECT_EQ(-1, hm.create(0));
  // 正常create
  EXPECT_EQ(0, hm.create(cal_next_prime(gHashItemNum)));
  // 重复create
  EXPECT_EQ(-1, hm.create(cal_next_prime(gHashItemNum)));
}

TEST(TestObHashMap, set)
{
  ObHashMap<HashKey, HashValue> hm;
  uint64_t key[4] = {1, 2, 1, 1 + cal_next_prime(gHashItemNum)};
  uint64_t value[4] = {100, 200, 300, 301};

  // 没有create
  EXPECT_EQ(-1, hm.set(key[0], value[0], 0));

  hm.create(cal_next_prime(gHashItemNum));
  // 正常插入
  EXPECT_EQ(HASH_INSERT_SUCC, hm.set(key[0], value[0], 0));
  // 正常插入不同bucket的key
  EXPECT_EQ(HASH_INSERT_SUCC, hm.set(key[1], value[1], 0));
  // 正常插入相同bucket的key
  EXPECT_EQ(HASH_INSERT_SUCC, hm.set(key[3], value[3], 0));
  // key存在但不覆盖
  EXPECT_EQ(HASH_EXIST, hm.set(key[2], value[2], 0));
  // key存在覆盖
  EXPECT_EQ(HASH_OVERWRITE_SUCC, hm.set(key[2], value[2], 1));
}

TEST(TestObHashMap, get)
{
  ObHashMap<HashKey, HashValue> hm;
  uint64_t key[4] = {1, 2, 1, 1 + cal_next_prime(gHashItemNum)};
  uint64_t value[4] = {100, 200, 300, 301};
  int fail = -1;
  HashValue value_tmp;

  // 没有create
  EXPECT_EQ(fail, hm.get(key[0], value_tmp));

  hm.create(cal_next_prime(gHashItemNum));
  // 查询已存在的数据
  hm.set(key[0], value[0], 0);
  hm.set(key[1], value[1], 0);
  hm.set(key[3], value[3], 0);
  EXPECT_EQ(HASH_EXIST, hm.get(key[0], value_tmp));
  EXPECT_EQ(value[0], value_tmp);
  EXPECT_EQ(HASH_EXIST, hm.get(key[1], value_tmp));
  EXPECT_EQ(value[1], value_tmp);
  EXPECT_EQ(HASH_EXIST, hm.get(key[3], value_tmp));
  EXPECT_EQ(value[3], value_tmp);
  // 查询更新后的数据
  hm.set(key[0], value[2], 1);
  EXPECT_EQ(HASH_EXIST, hm.get(key[0], value_tmp));
  EXPECT_EQ(value[2], value_tmp);
  // 查询不存在的数据
  EXPECT_EQ(HASH_NOT_EXIST, hm.get(-1, value_tmp));
}

TEST(TestObHashMap, erase)
{
  ObHashMap<HashKey, HashValue> hm;
  uint64_t key[4] = {1, 2, 1, 1 + cal_next_prime(gHashItemNum)};
  uint64_t value[4] = {100, 200, 300, 301};

  // 没有create
  EXPECT_EQ(-1, hm.erase(key[0]));

  hm.create(cal_next_prime(gHashItemNum));
  // 删除已存在的数据
  hm.set(key[0], value[0], 0);
  hm.set(key[1], value[1], 0);
  hm.set(key[3], value[3], 0);
  EXPECT_EQ(HASH_EXIST, hm.erase(key[0]));
  EXPECT_EQ(HASH_EXIST, hm.erase(key[1]));
  uint64_t value_ret = 0;
  EXPECT_EQ(HASH_EXIST, hm.erase(key[3], &value_ret));
  EXPECT_EQ(value[3], value_ret);
  // 删除不存在的数据
  EXPECT_EQ(HASH_NOT_EXIST, hm.erase(-1));
}

TEST(TestObHashMap, clear)
{
  ObHashMap<HashKey, HashValue> hm;
  uint64_t key[4] = {1, 2, 1, 1 + cal_next_prime(gHashItemNum)};
  uint64_t value[4] = {100, 200, 300, 301};

  // 没有create
  EXPECT_EQ(-1, hm.clear());
  hm.create(cal_next_prime(gHashItemNum));
  EXPECT_EQ(0, hm.clear());
  hm.set(key[0], value[0], 0);
  hm.set(key[1], value[1], 0);
  hm.set(key[3], value[3], 0);
  EXPECT_EQ(3, hm.size());
  EXPECT_EQ(0, hm.clear());
  EXPECT_EQ(0, hm.size());
}

TEST(TestObHashMap, destroy)
{
  ObHashMap<HashKey, HashValue> hm;

  // 没有create
  EXPECT_EQ(0, hm.destroy());
  hm.create(cal_next_prime(gHashItemNum));
  EXPECT_EQ(0, hm.destroy());
  EXPECT_EQ(0, hm.create(gHashItemNum));
}

TEST(TestObHashMap, iterator)
{
  ObHashMap<HashKey, HashValue> hm;
  const ObHashMap<HashKey, HashValue>& chm = hm;
  ObHashMap<HashKey, HashValue>::iterator iter;
  ObHashMap<HashKey, HashValue>::const_iterator citer;

  // 没有create
  EXPECT_EQ(true, hm.begin() == hm.end());
  iter = hm.begin();
  citer = chm.begin();
  EXPECT_EQ(true, iter == hm.end());
  EXPECT_EQ(true, citer == chm.end());
  EXPECT_EQ(true, (++iter) == hm.end());
  EXPECT_EQ(true, (++citer) == chm.end());

  // 没有数据
  hm.create(cal_next_prime(gHashItemNum));
  EXPECT_EQ(true, hm.begin() == hm.end());
  iter = hm.begin();
  citer = hm.begin();
  EXPECT_EQ(true, iter == hm.end());
  EXPECT_EQ(true, citer == hm.end());
  EXPECT_EQ(true, (++iter) == hm.end());
  EXPECT_EQ(true, (++citer) == hm.end());

  uint64_t key[4] = {1, 2, 5, 5 + cal_next_prime(gHashItemNum)};
  uint64_t value[4] = {100, 200, 500, 501};
  for (int32_t i = 3; i >= 0; i--)
  {
    hm.set(key[i], value[i], 0);
  }
  iter = hm.begin();
  citer = chm.begin();
  for (uint32_t i = 0; iter != hm.end(); iter++, i++)
  {
    EXPECT_EQ(value[i], iter->second);
  }
  for (uint32_t i = 0; citer != chm.end(); citer++, i++)
  {
    EXPECT_EQ(value[i], citer->second);
  }
} 

TEST(TestObHashMap, serialization)
{
  ObHashMap<HashKey, HashValue> hm;
  SimpleArchive arw, arr;
  arw.init("./hash.data", SimpleArchive::FILE_OPEN_WFLAG);
  arr.init("./hash.data", SimpleArchive::FILE_OPEN_RFLAG);
  SimpleArchive arw_nil, arr_nil;

  // 没有create
  EXPECT_EQ(-1, hm.serialization(arw));
  hm.create(cal_next_prime(gHashItemNum));
  // 没有元素
  EXPECT_EQ(0, hm.serialization(arw));
  EXPECT_EQ(0, hm.deserialization(arr));

  uint64_t key[4] = {1, 2, 1, 1 + cal_next_prime(gHashItemNum)};
  uint64_t value[4] = {100, 200, 300, 301};
  for (uint32_t i = 0; i < 4; i++)
  {
    hm.set(key[i], value[i], 0);
  }
  EXPECT_EQ(-1, hm.serialization(arw_nil));

  arw.destroy();
  arr.destroy();
  arw.init("./hash.data", SimpleArchive::FILE_OPEN_WFLAG);
  arr.init("./hash.data", SimpleArchive::FILE_OPEN_RFLAG);
  EXPECT_EQ(0, hm.serialization(arw));
  hm.destroy();
  EXPECT_EQ(0, hm.deserialization(arr));
  EXPECT_EQ(-1, hm.deserialization(arr));
  EXPECT_EQ(3, hm.size());

  arr_nil.init("./hash.data.nil", SimpleArchive::FILE_OPEN_RFLAG);
  EXPECT_EQ(-1, hm.deserialization(arr_nil));

  remove("./hash.data");
  remove("./hash.data.nil");
}

TEST(TestObHashMap, atomic)
{
  ObHashMap<HashKey, HashValue> hm;
  uint64_t key = 1;
  uint64_t value = 100;
  uint64_t value_update = 3000;
  CallBack callback;
  callback.set_v(value_update);
  HashValue value_tmp;

  // 没有create
  EXPECT_EQ(-1, hm.atomic(key, callback));

  hm.create(cal_next_prime(gHashItemNum));

  hm.set(key, value, 0);
  EXPECT_EQ(HASH_EXIST, hm.get(key, value_tmp));
  EXPECT_EQ(value, value_tmp);

  EXPECT_EQ(HASH_EXIST, hm.atomic(key, callback));
  EXPECT_EQ(HASH_EXIST, hm.get(key, value_tmp));
  EXPECT_EQ(value_update, value_tmp);

  EXPECT_EQ(HASH_NOT_EXIST, hm.atomic(key + 1, callback));
  EXPECT_EQ(HASH_EXIST, hm.get(key, value_tmp));
  EXPECT_EQ(value_update, value_tmp);
}

struct GAllocator
{
  void *alloc(const int64_t sz)
  {
    fprintf(stdout, "::malloc\n");
    return ::malloc(sz);
  }
  void free(void *p)
  { 
    fprintf(stdout, "::free\n");
    ::free(p);
  }
};

template <class T>
class GAllocBigArray : public BigArrayTemp<T, GAllocator>
{
};

TEST(TestObHashMap, use_gallocator)
{
  ObHashMap<HashKey,
            HashValue,
            ReadWriteDefendMode,
            hash_func<HashKey>,
            equal_to<HashKey>,
            SimpleAllocer<HashMapTypes<HashKey, HashValue>::AllocType, 1024, SpinMutexDefendMode, GAllocator>,
            GAllocBigArray> hm;

  uint64_t key[4] = {1, 2, 1, 1 + cal_next_prime(gHashItemNum)};
  uint64_t value[4] = {100, 200, 300, 301};

  // 没有create
  EXPECT_EQ(-1, hm.set(key[0], value[0], 0));

  hm.create(cal_next_prime(gHashItemNum));
  // 正常插入
  EXPECT_EQ(HASH_INSERT_SUCC, hm.set(key[0], value[0], 0));
  // 正常插入不同bucket的key
  EXPECT_EQ(HASH_INSERT_SUCC, hm.set(key[1], value[1], 0));
  // 正常插入相同bucket的key
  EXPECT_EQ(HASH_INSERT_SUCC, hm.set(key[3], value[3], 0));
  // key存在但不覆盖
  EXPECT_EQ(HASH_EXIST, hm.set(key[2], value[2], 0));
  // key存在覆盖
  EXPECT_EQ(HASH_OVERWRITE_SUCC, hm.set(key[2], value[2], 1));
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv); 
  return RUN_ALL_TESTS();
}

