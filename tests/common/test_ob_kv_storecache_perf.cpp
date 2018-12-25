#include "ob_kv_storecache.h"
#include <gtest/gtest.h>
#include <tbsys.h>
#include <sys/time.h>
#include <stdlib.h>

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;


int64_t microseconds()
{
  struct timeval tv; 
  gettimeofday(&tv, NULL);
  return tv.tv_sec * 1000000L + tv.tv_usec;
}
class Random
{
  public:
    static uint64_t rand()
    {   
      return seed_ = (seed_ * 214013L + 2531011L) >> 16;
    }   
    static uint64_t srand(uint64_t seed)
    {   
      seed_ = seed;
      return seed_;
    }   
  private:
    static __thread uint64_t seed_;
};

__thread uint64_t Random::seed_;

class TestStress : public tbsys::CDefaultRunnable
{
  public:
    typedef int64_t                        K;
    typedef int64_t                        V;
    static const int64_t                   ITEM_SIZE = sizeof(K) + sizeof(V);
    typedef KeyValueCache<K, V, ITEM_SIZE> KVC;
    static const int64_t                   TOTAL_SIZE = 16L << 20;
    static const int64_t                   N = 200000;
  public:
    TestStress()
      : counter_(0)
    {
      TBSYS_LOG(WARN, "init ret = %d\n", kvc_.init(TOTAL_SIZE));
      for (int64_t i = 0; i < N; i++)
      {
        int err = kvc_.put(i, i);
        if (err != 0)
        {
          TBSYS_LOG(ERROR, "put err = %d\n", err);
        }
      }
    }
    void run(tbsys::CThread * thread, void * arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      Random::srand(microseconds());
      KVC & cache = kvc_;
      int err = 0;
      int64_t v = 0;
      int64_t counter = 0;
      while (!_stop)
      {
        err = cache.get(Random::rand() % N, v);
        if (err != 0)
        {
          TBSYS_LOG(ERROR, "get error, err = %d", err);
          break;
        }
        else
        {
          counter ++;
        }
      }
      TBSYS_LOG(INFO, "counter=%ld", counter);
    }
    int64_t get_counter()
    {
      return counter_;
    }
  protected:
    KVC            kvc_;
    int64_t        counter_;
};

TEST(KVStoreCache, perf)
{
  const int64_t THREAD_COUNT = 8;
  TestStress ts;
  ts.setThreadCount(THREAD_COUNT);
  ts.start();
  sleep(10);
  ts.stop();
  ts.wait();
}

int main(int argc, char **argv)
{
  TBSYS_LOGGER.setLogLevel("INFO");
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
