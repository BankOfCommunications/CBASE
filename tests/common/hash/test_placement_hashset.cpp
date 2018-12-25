#include "gtest/gtest.h"
#include "ob_placement_hashset.h"

using namespace oceanbase;
using namespace common;
using namespace hash;

TEST(TestObPlacementHashSet, single_bucket)
{
  ObPlacementHashSet<int64_t, 1> hashset;
  ASSERT_EQ(HASH_INSERT_SUCC, hashset.set(1));
  ASSERT_EQ(HASH_EXIST, hashset.set(1));
  ASSERT_EQ(HASH_EXIST, hashset.exist(1));
}

TEST(TestObPlacementHashSet, many_buckets)
{
  const uint64_t N = 10345;
  ObPlacementHashSet<int64_t, N> hashset;
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(HASH_NOT_EXIST, hashset.exist(i));
  }
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(HASH_INSERT_SUCC, hashset.set(i));
  }
  ASSERT_EQ(OB_ERROR, hashset.set(N));
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(HASH_EXIST, hashset.exist(i));
  }
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(HASH_EXIST, hashset.set(i));
  }
  ASSERT_EQ(OB_ERROR, hashset.set(N));
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(HASH_EXIST, hashset.exist(i));
  }

  hashset.clear();
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(HASH_NOT_EXIST, hashset.exist(i));
  }
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(HASH_INSERT_SUCC, hashset.set(i));
  }
  ASSERT_EQ(OB_ERROR, hashset.set(N));
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(HASH_EXIST, hashset.exist(i));
  }
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(HASH_EXIST, hashset.set(i));
  }
  ASSERT_EQ(OB_ERROR, hashset.set(N));
  for (uint64_t i = 0; i < N; i++)
  {
    ASSERT_EQ(HASH_EXIST, hashset.exist(i));
  }
}

TEST(TestObPlacementHashSet, many_buckets2)
{
  const uint64_t N = 10345;
  ObPlacementHashSet<int64_t, N> hashset;
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(HASH_NOT_EXIST, hashset.exist(i));
  }
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(HASH_INSERT_SUCC, hashset.set(i));
  }
  ASSERT_EQ(OB_ERROR, hashset.set(0));
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(HASH_EXIST, hashset.exist(i));
  }
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(HASH_EXIST, hashset.set(i));
  }
  ASSERT_EQ(OB_ERROR, hashset.set(0));
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(HASH_EXIST, hashset.exist(i));
  }

  hashset.clear();
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(HASH_NOT_EXIST, hashset.exist(i));
  }
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(HASH_INSERT_SUCC, hashset.set(i));
  }
  ASSERT_EQ(OB_ERROR, hashset.set(0));
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(HASH_EXIST, hashset.exist(i));
  }
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(HASH_EXIST, hashset.set(i));
  }
  ASSERT_EQ(OB_ERROR, hashset.set(0));
  for (uint64_t i = N; i > 0; i--)
  {
    ASSERT_EQ(HASH_EXIST, hashset.exist(i));
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv); 
  return RUN_ALL_TESTS();
}

