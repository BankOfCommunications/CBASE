#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include "ob_hashset.h"
#include "ob_hashutils.h"
#include "ob_malloc.h"

#include "gtest/gtest.h"

using namespace oceanbase;
using namespace common;
using namespace hash;

int main(int argc, char **argv)
{
  UNUSED(argc);
  UNUSED(argv);
  ob_init_memory_pool();
  ObHashSet<int64_t> set;
  ObHashSet<int64_t>::iterator iter;
  set.create(10);

  for (int64_t i = 0; i < 10; i++)
  {
    assert(HASH_INSERT_SUCC == set.set(i));
  }

  for (int64_t i = 0; i < 10; i++)
  {
    assert(HASH_EXIST == set.exist(i));
  }

  int64_t i = 0;
  for (iter = set.begin(); iter != set.end(); iter++, i++)
  {
    assert(i == iter->first);
    fprintf(stderr, "%s\n", typeid(iter->second).name());
  }
}

