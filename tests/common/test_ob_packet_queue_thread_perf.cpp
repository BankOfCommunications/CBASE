#include <gtest/gtest.h>
#include <tbsys.h>
#include <sys/time.h>
#include <stdlib.h>

#include <ob_kv_storecache.h>
#include <ob_packet_queue_handler.h>
#include <ob_packet_queue_thread.h>

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;

int push_thread_count = 8;
int pop_thread_count = 100;
int64_t * push_counts = new int64_t[push_thread_count];
int running_time = 10;

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

class DummyHandler : public ObPacketQueueHandler
{
  public:
    virtual bool handlePacketQueue(ObPacket *packet, void* args)
    {
      UNUSED(packet);
      UNUSED(args);
      //for (int64_t i = 0; i < 1000000; i++) asm("pause\n");
      //TBSYS_LOG(INFO, "packet = %p", packet);
      return true;
    }
};

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
    TestStress(ObPacketQueueThread & qt)
      : qt_(qt), counter_(0)
    {
    }
    void run(tbsys::CThread * thread, void * arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      Random::srand(microseconds());
      int64_t counter = 0;
      while (!_stop)
      {
        ObPacket * packet = new (ob_tc_malloc(sizeof(ObPacket))) ObPacket();
        qt_.push(packet, N);
        packet->~ObPacket();
        ob_tc_free(packet);
        counter++;
      }
      TBSYS_LOG(INFO, "counter=%ld", counter);
      push_counts[reinterpret_cast<long>(arg)] = counter;
    }
    int64_t get_counter()
    {
      return counter_;
    }
  protected:
    ObPacketQueueThread & qt_;
    int64_t        counter_;
};

TEST(ObPacketQueueThread, perf)
{
  ObPacketQueueThread qt;
  DummyHandler queue_handler;
  qt.setThreadParameter(pop_thread_count, &queue_handler, NULL);
  qt.start();
  TestStress ts(qt);
  ts.setThreadCount(push_thread_count);
  ts.start();
  int64_t st = microseconds();
  sleep(running_time);
  ts.stop();
  ts.wait();
  qt.stop(true);
  qt.wait();
  int64_t et = microseconds();

  int64_t sum = 0;
  for (int64_t i = 0; i < push_thread_count; i++)
  {
    sum += push_counts[i];
  }
  double elapsed_time = double(et - st) / 1000000.0;
  TBSYS_LOG(INFO, "push_thread: %d  pop_thread: %d  running time: %fs  qps: %f",
      push_thread_count, pop_thread_count, elapsed_time, double(sum) / elapsed_time);
}

int main(int argc, char **argv)
{
  TBSYS_LOGGER.setLogLevel("INFO");
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  if (argc > 1) running_time = atoi(argv[1]);
  if (argc > 2) push_thread_count = atoi(argv[2]);
  if (argc > 3) pop_thread_count = atoi(argv[3]);
  return RUN_ALL_TESTS();
}
