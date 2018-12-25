#include "gtest/gtest.h"
#include "ob_placement_hashmap.h"

using namespace oceanbase;
using namespace common;
using namespace hash;

TEST(TestObPlacementHashMap, single_bucket)
{
  ObPlacementHashMap<int64_t, int64_t, 1> hashmap;
  int64_t v;
  ASSERT_EQ(HASH_INSERT_SUCC, hashmap.set(1, 1));
  ASSERT_EQ(HASH_EXIST, hashmap.set(1, 1));
  ASSERT_EQ(HASH_OVERWRITE_SUCC, hashmap.set(1, 1, 1));
  ASSERT_EQ(HASH_OVERWRITE_SUCC, hashmap.set(1, 1, 1, 1));
  ASSERT_EQ(HASH_EXIST, hashmap.get(1, v));
  ASSERT_EQ(1, v);
  ASSERT_EQ(1, *hashmap.get(1));
}

TEST(TestObPlacementHashMap, many_buckets)
{
  const uint64_t N = 10345;
  int64_t v = 0;
  ObPlacementHashMap<int64_t, int64_t, N> hashmap;
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(HASH_NOT_EXIST, hashmap.get(i, v));
    ASSERT_EQ(NULL, hashmap.get(i));
  }
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(HASH_INSERT_SUCC, hashmap.set(i, i * i));
  }
  ASSERT_EQ(OB_ERROR, hashmap.set(N, N * N));
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(HASH_EXIST, hashmap.get(i, v));
    ASSERT_EQ(static_cast<int64_t>(i * i) , v);
    ASSERT_EQ(static_cast<int64_t>(i * i), *hashmap.get(i));
  }
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(HASH_OVERWRITE_SUCC, hashmap.set(i, i * i, 1));
    ASSERT_EQ(HASH_OVERWRITE_SUCC, hashmap.set(i, i * i, 1, 1));
  }
  ASSERT_EQ(OB_ERROR, hashmap.set(N, N * N));
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(HASH_EXIST, hashmap.get(i, v));
    ASSERT_EQ(static_cast<int64_t>(i * i) , v);
    ASSERT_EQ(static_cast<int64_t>(i * i), *hashmap.get(i));
  }
}

TEST(TestObPlacementHashMap, many_buckets2)
{
  const uint64_t N = 10345;
  int64_t v = 0;
  ObPlacementHashMap<int64_t, int64_t, N> hashmap;
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(HASH_NOT_EXIST, hashmap.get(i, v));
    ASSERT_EQ(NULL, hashmap.get(i));
  }
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(HASH_INSERT_SUCC, hashmap.set(i, i * i));
  }
  ASSERT_EQ(OB_ERROR, hashmap.set(0, 0));
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(HASH_EXIST, hashmap.get(i, v));
    ASSERT_EQ(static_cast<int64_t>(i * i) , v);
    ASSERT_EQ(static_cast<int64_t>(i * i), *hashmap.get(i));
  }
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(HASH_OVERWRITE_SUCC, hashmap.set(i, i * i, 1));
    ASSERT_EQ(HASH_OVERWRITE_SUCC, hashmap.set(i, i * i, 1, 1));
  }
  ASSERT_EQ(OB_ERROR, hashmap.set(0, 0));
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(HASH_EXIST, hashmap.get(i, v));
    ASSERT_EQ(static_cast<int64_t>(i * i) , v);
    ASSERT_EQ(static_cast<int64_t>(i * i), *hashmap.get(i));
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv); 
  return RUN_ALL_TESTS();
}

