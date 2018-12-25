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

int signal_thread_count = 8;
int wait_thread_count = 100;
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

class TestSignal : public tbsys::CDefaultRunnable
{
  public:
    TestSignal(pthread_cond_t & cond, pthread_mutex_t & mutex)
      : cond_(cond), mutex_(mutex), counter_(0)
    {
    }
    void run(tbsys::CThread * thread, void * arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      while (!_stop)
      {
        pthread_mutex_lock(&mutex_);
        pthread_cond_signal(&cond_);
        counter_++;
        pthread_mutex_unlock(&mutex_);
      }
      TBSYS_LOG(INFO, "counter_=%ld", counter_);
    }
    int64_t get_counter()
    {
      return counter_;
    }
  protected:
    pthread_cond_t &  cond_;
    pthread_mutex_t & mutex_;
    int64_t           counter_;
};

class TestWait : public tbsys::CDefaultRunnable
{
  public:
    TestWait(pthread_cond_t & cond, pthread_mutex_t & mutex)
      : cond_(cond), mutex_(mutex), counter_(0)
    {
    }
    void run(tbsys::CThread * thread, void * arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      struct timespec ts;
      while (!_stop)
      {
        pthread_mutex_lock(&mutex_);
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 10000000;
        if (0 == pthread_cond_timedwait(&cond_, &mutex_, &ts)) counter_++;
        pthread_mutex_unlock(&mutex_);
      }
      TBSYS_LOG(INFO, "counter_=%ld", counter_);
    }
    int64_t get_counter()
    {
      return counter_;
    }
  protected:
    pthread_cond_t &  cond_;
    pthread_mutex_t & mutex_;
    int64_t           counter_;
};

TEST(pthread_cond, perf)
{
  pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
  pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&cond, NULL);
  TestSignal ts(cond, mutex);
  TestWait tw(cond, mutex);
  ts.setThreadCount(signal_thread_count);
  tw.setThreadCount(wait_thread_count);
  tw.start();
  ts.start();
  int64_t st = microseconds();
  sleep(running_time);
  ts.stop();
  tw.stop();
  ts.wait();
  tw.wait();
  int64_t et = microseconds();

  double elapsed_time = double(et - st) / 1000000.0;
  TBSYS_LOG(INFO, "signal thread: %d  wait thread: %d  "
      "running time: %fs  signal qps: %f  wait qps: %f",
      signal_thread_count, wait_thread_count, elapsed_time,
      double(ts.get_counter()) / elapsed_time, double(tw.get_counter()) / elapsed_time);

  pthread_cond_destroy(&cond);
  pthread_mutex_destroy(&mutex);
}

int main(int argc, char **argv)
{
  TBSYS_LOGGER.setLogLevel("INFO");
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  if (argc > 1) running_time = atoi(argv[1]);
  if (argc > 2) signal_thread_count = atoi(argv[2]);
  if (argc > 3) wait_thread_count = atoi(argv[3]);
  return RUN_ALL_TESTS();
}
