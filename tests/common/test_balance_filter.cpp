#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <tr1/unordered_map>
#include "common/ob_malloc.h"
#include "common/ob_balance_filter.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace common;

TEST(TestBalanceFilter, init)
{
  ObBalanceFilter bf;
  std::tr1::unordered_map<uint64_t, uint64_t> h_map;

  int ret = bf.init(22, true);
  EXPECT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < 100; i++)
  {
    uint64_t hash = rand();
    uint64_t filted = bf.filt(hash);
    EXPECT_LE(0, filted);
    h_map[hash] = filted;
    fprintf(stdout, "filt %lu==>%lu\n", hash, filted);
  }

  for (int64_t t = 0; t < 100000; t++)
  {
    std::tr1::unordered_map<uint64_t, uint64_t>::iterator iter;
    for (iter = h_map.begin(); iter != h_map.end(); iter++)
    {
      uint64_t filted = bf.filt(iter->first);
      EXPECT_EQ(filted, iter->second);
    }
  }

  bf.log_info();

  bf.sub_thread(1);
  std::tr1::unordered_map<uint64_t, uint64_t>::iterator iter;
  for (iter = h_map.begin(); iter != h_map.end(); iter++)
  {
    uint64_t filted = bf.filt(iter->first);
    if (filted != iter->second)
    {
      fprintf(stdout, "migrate %lu==>%lu\n", iter->second, filted);
      iter->second = filted;
    }
  }

  for (int64_t t = 0; t < 1000000; t++)
  {
    std::tr1::unordered_map<uint64_t, uint64_t>::iterator iter;
    for (iter = h_map.begin(); iter != h_map.end(); iter++)
    {
      uint64_t filted = bf.filt(iter->first);
      EXPECT_EQ(filted, iter->second);
    }
  }

  bf.log_info();
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

