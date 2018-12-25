#include <gtest/gtest.h>
#include <tbsys.h>
#include <sys/time.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <linux/futex.h>

#include <ob_kv_storecache.h>
#include <ob_packet_queue_handler.h>
#include <ob_packet_queue_thread.h>

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;

int signal_thread_count = 8;
int wait_thread_count = 100;
int running_time = 10;
int64_t * signal_counts = new int64_t[signal_thread_count];
int64_t * wait_counts = new int64_t[wait_thread_count];

#define futex(...) syscall(SYS_futex,__VA_ARGS__)
static int futex_wake(volatile int* p, int val)
{
  int err = 0;
  if (0 != futex((int*)p, FUTEX_WAKE_PRIVATE, val, NULL, NULL, 0))
  {
    err = errno;
  }
  return err;
}

static int futex_wait(volatile int* p, int val, const timespec* timeout)
{
  int err = 0;
  if (0 != futex((int*)p, FUTEX_WAIT_PRIVATE, val, timeout, NULL, 0))
  {
    err = errno;
  }
  return err;
}

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
    TestSignal(int fa) : fa_(fa), counter_(0)
    {
    }
    void run(tbsys::CThread * thread, void * arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      int64_t counter = 0;
      while (!_stop)
      {
        futex_wake(&fa_, 1);
        counter++;
      }
      TBSYS_LOG(INFO, "signal counter=%ld", counter);
      signal_counts[reinterpret_cast<long>(arg)] = counter;
    }
    int64_t get_counter()
    {
      return counter_;
    }
  protected:
    int             & fa_;
    int64_t           counter_;
};

class TestWait : public tbsys::CDefaultRunnable
{
  public:
    TestWait(int fa) : fa_(fa), counter_(0)
    {
    }
    void run(tbsys::CThread * thread, void * arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      struct timespec ts;
      int64_t counter = 0;
      while (!_stop)
      {
        ts.tv_sec = 0;
        ts.tv_nsec = 10000000;
        if (0 == futex_wait(&fa_, 0, &ts)) counter++;
      }
      TBSYS_LOG(INFO, "wait counter=%ld", counter);
      wait_counts[reinterpret_cast<long>(arg)] = counter;
    }
    int64_t get_counter()
    {
      return counter_;
    }
  protected:
    int             & fa_;
    int64_t           counter_;
};

TEST(pthread_cond, perf)
{
  int fa = 0;
  TestSignal ts(fa);
  TestWait tw(fa);
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

  int64_t signal_count = 0;
  int64_t wait_count = 0;
  for (int i = 0; i < signal_thread_count; i++) signal_count += signal_counts[i];
  for (int i = 0; i < wait_thread_count; i++) wait_count += wait_counts[i];

  double elapsed_time = double(et - st) / 1000000.0;
  TBSYS_LOG(INFO, "signal thread: %d  wait thread: %d  "
      "running time: %fs  signal qps: %f  wait qps: %f",
      signal_thread_count, wait_thread_count, elapsed_time,
      double(signal_count) / elapsed_time, double(wait_count) / elapsed_time);
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
