#define OCEAN_BASE_BTREE_DEBUG
#include <sys/types.h>
#include <pthread.h>
#include <string>
#include <sstream>
#include <vector>
#include "btree_base.h"
#include "thread.h"
#include <sys/time.h>
#include <stdlib.h>


using namespace oceanbase::common::cmbtree;

int64_t microseconds()
{
  struct timeval tv; 
  gettimeofday(&tv, NULL);
  return tv.tv_sec * 1000000L + tv.tv_usec;
}

class Number
{
  public:
    int64_t value;
    Number() : value(0) {}
    ~Number() {}
    Number(const int64_t & v) : value(v) {}
    int64_t operator - (const Number & r) const
    {
      return value - r.value;
    }
    std::string to_string() const
    {
      std::ostringstream oss;
      oss << value;
      return oss.str();
    }
    const char * to_cstring() const
    {
      static char buf[BUFSIZ];
      snprintf(buf, BUFSIZ, "%ld", value);
      return buf;
    }
};

class AddThread : public Thread
{
  public:
    AddThread(BtreeBase<Number, Number> & b)
      : b_(b), qps_(0)
    {
    }
    uint64_t rand()
    {
      return seed_ = ((seed_ * 214013L + 2531011L) >> 16);
    }
    void * run(void * arg)
    {
      int64_t counter = 0;
      int64_t start_time = microseconds();
      seed_ = microseconds();
      while (!stop_)
      {
        int64_t key = static_cast<int64_t>(rand());
        int64_t value = key * key;
        b_.put(Number(key), Number(value), true);
        ++counter;
      }
      int64_t end_time = microseconds();
      int64_t elapsed_time = end_time - start_time;
      qps_ = counter * 1000000 / elapsed_time;
      std::cout << "Thread " << " running " << elapsed_time
        << "ms did QPS: " << qps_ << std::endl;
      return NULL;
    }
    int64_t get_qps() const
    {
      return qps_;
    }
  protected:
    BtreeBase<Number, Number> & b_;
    int64_t qps_;
    uint64_t seed_;
};

class GetThread : public Thread
{
  public:
    GetThread(BtreeBase<Number, Number> & b)
      : b_(b), qps_(0)
    {
    }
    uint64_t rand()
    {
      return seed_ = ((seed_ * 214013L + 2531011L) >> 16);
    }
    void * run(void * arg)
    {
      int64_t counter = 0;
      int64_t start_time = microseconds();
      seed_ = microseconds();
      while (!stop_)
      {
        int64_t key = static_cast<int64_t>(rand());
        Number value;
        b_.get(Number(key), value);
        ++counter;
      }
      int64_t end_time = microseconds();
      int64_t elapsed_time = end_time - start_time;
      qps_ = counter * 1000000 / elapsed_time;
      std::cout << "Thread " << " running " << elapsed_time
        << "ms did QPS: " << qps_ << std::endl;
      return NULL;
    }
    int64_t get_qps() const
    {
      return qps_;
    }
  protected:
    BtreeBase<Number, Number> & b_;
    int64_t qps_;
    uint64_t seed_;
};

int main(int argc, char * argv[])
{
  if (argc < 3)
  {
    fprintf(stderr, "usage: %s <running time> <thread number>\n", argv[0]);
    return 1;
  }
  BtreeBase<Number, Number> b;
  b.init();
  int64_t running_time = atol(argv[1]);
  int64_t thread_num = atol(argv[2]);
  std::vector<AddThread> at(thread_num, AddThread(b));
  for (int64_t i = 0; i < thread_num; i++)
  {
    at[i].start();
  }
  sleep(running_time);
  for (int64_t i = 0; i < thread_num; i++)
  {
    at[i].stop();
  }
  for (int64_t i = 0; i < thread_num; i++)
  {
    at[i].wait();
  }
  int64_t sum_qps = 0;
  for (int64_t i = 0; i < thread_num; i++)
  {
    sum_qps += at[i].get_qps();
  }
  std::cout << "Total QPS: " << sum_qps << std::endl;

  std::vector<GetThread> gt(thread_num, GetThread(b));
  for (int64_t i = 0; i < thread_num; i++)
  {
    gt[i].start();
  }
  sleep(running_time);
  for (int64_t i = 0; i < thread_num; i++)
  {
    gt[i].stop();
  }
  for (int64_t i = 0; i < thread_num; i++)
  {
    gt[i].wait();
  }
  sum_qps = 0;
  for (int64_t i = 0; i < thread_num; i++)
  {
    sum_qps += gt[i].get_qps();
  }
  std::cout << "Total Get QPS: " << sum_qps << std::endl;
  //b.print();
  return 0;
}

