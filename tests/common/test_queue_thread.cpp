#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/syscall.h>
#include "common/ob_malloc.h"
#include "common/page_arena.h"
#include "common/ob_queue_thread.h"
#include "common/ob_fifo_allocator.h"
#include "gtest/gtest.h"

#define DISPATCH_THREAD_NUM 4
#define THREAD_NUM 12
#define TASK_NUM_PER_THREAD 1000000

using namespace oceanbase;
using namespace common;

pid_t gettid()
{
  return (pid_t)syscall(SYS_gettid);
}

FIFOAllocator g_allocator;

class S2MQueueThreadImpl : public S2MQueueThread
{
  public:
    S2MQueueThreadImpl() : num_(0) {};
    ~S2MQueueThreadImpl() {};
  public:
    void handle(void *task, void *ptr)
    {
      UNUSED(task);
      UNUSED(ptr);
      //fprintf(stderr, "[%d] handle task=%p\n", gettid(), task);
      g_allocator.free(task);
      *((int64_t*)ptr) += 1;
    };
    void *on_begin() {return &(get_tsi_num());}
    void on_end(void *ptr) {UNUSED(ptr); ATOMIC_ADD(&num_, get_tsi_num());};
    inline int64_t &get_tsi_num()
    {
      static __thread int64_t num = 0;
      return num;
    };
  public:
    volatile int64_t num_;
};

class M2SQueueThreadImpl : public M2SQueueThread
{
  public:
    M2SQueueThreadImpl() : num_(0) {};
    ~M2SQueueThreadImpl() {};
  public:
    void handle(void *task, void *ptr)
    {
      UNUSED(task);
      UNUSED(ptr);
      //fprintf(stderr, "[%d] handle task=%p\n", gettid(), task);
      g_allocator.free(task);
      num_ += 1;
    };
    void *on_begin() {return NULL;}
    void on_end(void *ptr) {UNUSED(ptr);};
    void on_idle() {idle_counter_++;};
  public:
    int64_t num_;
    int64_t idle_counter_;
};

TEST(TestS2MQueueThread, init)
{
  S2MQueueThreadImpl s2mqt;
  int64_t thread_num = THREAD_NUM;
  int64_t task_num = TASK_NUM_PER_THREAD * thread_num;

  bool queue_rebalance = true;
  bool dynamic_rebalance = false;
  int ret = s2mqt.init(1, TASK_NUM_PER_THREAD, queue_rebalance, dynamic_rebalance);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1, s2mqt.get_thread_num());

  ret = s2mqt.add_thread(thread_num + 10, TASK_NUM_PER_THREAD);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(thread_num + 11, s2mqt.get_thread_num());

  ret = s2mqt.sub_thread(11);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(thread_num, s2mqt.get_thread_num());

  //getc(stdin);

  int64_t timeu = tbsys::CTimeUtil::getTime();
  for (int64_t i = 1; i <= task_num; i++)
  {
    //void *task = (void*)i;
    void *task = g_allocator.alloc(10);
    ret = s2mqt.push(task);
    assert(OB_SUCCESS == ret);
    EXPECT_EQ(OB_SUCCESS, ret);
  }

  int64_t push_timeu = tbsys::CTimeUtil::getTime() - timeu;
  fprintf(stderr, "push_timeu=%ld speed=%0.2f\n", push_timeu, 1000000.0 * (double)task_num / (double)push_timeu);
  s2mqt.destroy();
  timeu = tbsys::CTimeUtil::getTime() - timeu;
  fprintf(stderr, "timeu=%ld speed=%0.2f\n", timeu, 1000000.0 * (double)task_num / (double)timeu);

  EXPECT_EQ(task_num, s2mqt.num_);
}

M2SQueueThreadImpl *m2sqt = NULL;
void *producer_func(void *data)
{
  UNUSED(data);
  for (int64_t i = 1; i <= TASK_NUM_PER_THREAD; i++)
  {
    //void *task = (void*)i;
    void *task = g_allocator.alloc(10);
    int ret = m2sqt->push(task);
    EXPECT_EQ(OB_SUCCESS, ret);
  }
  return NULL;
}

TEST(TestM2SQueueThread, init)
{
  m2sqt = new M2SQueueThreadImpl();
  const int64_t thread_num = THREAD_NUM;
  int64_t task_num = TASK_NUM_PER_THREAD * thread_num;
  pthread_t pd[thread_num];

  int ret = m2sqt->init(task_num, 5000);
  EXPECT_EQ(OB_SUCCESS, ret);

  //getc(stdin);

  int64_t timeu = tbsys::CTimeUtil::getTime();
  for (int64_t i = 0; i < thread_num; i++)
  {
    pthread_create(&pd[i], NULL, producer_func, NULL);
  }
  for (int64_t i = 0; i < thread_num; i++)
  {
    pthread_join(pd[i], NULL);
  }

  int64_t push_timeu = tbsys::CTimeUtil::getTime() - timeu;
  fprintf(stderr, "push_timeu=%ld speed=%0.2f\n", push_timeu, 1000000.0 * (double)task_num / (double)push_timeu);
  m2sqt->destroy();
  timeu = tbsys::CTimeUtil::getTime() - timeu;
  fprintf(stderr, "timeu=%ld speed=%0.2f\n", timeu, 1000000.0 * (double)task_num / (double)timeu);

  EXPECT_EQ(task_num, m2sqt->num_);
  fprintf(stderr, "idle_counter=%ld\n", m2sqt->idle_counter_);
}

S2MQueueThreadImpl *s2mqt = NULL;
void *producer_func2(void *data)
{
  UNUSED(data);
  for (int64_t i = 1; i <= TASK_NUM_PER_THREAD; i++)
  {
    //void *task = (void*)i;
    void *task = g_allocator.alloc(10);
    int ret = s2mqt->push(task);
    EXPECT_EQ(OB_SUCCESS, ret);
  }
  return NULL;
}

TEST(TestM2MQueueThread, init)
{
  s2mqt = new S2MQueueThreadImpl();
  const int64_t thread_num = THREAD_NUM;
  int64_t task_num = TASK_NUM_PER_THREAD * DISPATCH_THREAD_NUM;
  pthread_t pd[DISPATCH_THREAD_NUM];

  bool queue_rebalance = true;
  bool dynamic_rebalance = false;
  int ret = s2mqt->init(thread_num, TASK_NUM_PER_THREAD, queue_rebalance, dynamic_rebalance);
  EXPECT_EQ(OB_SUCCESS, ret);

  //getc(stdin);

  int64_t timeu = tbsys::CTimeUtil::getTime();
  for (int64_t i = 0; i < DISPATCH_THREAD_NUM; i++)
  {
    pthread_create(&pd[i], NULL, producer_func2, NULL);
  }
  for (int64_t i = 0; i < DISPATCH_THREAD_NUM; i++)
  {
    pthread_join(pd[i], NULL);
  }

  int64_t push_timeu = tbsys::CTimeUtil::getTime() - timeu;
  fprintf(stderr, "push_timeu=%ld speed=%0.2f\n", push_timeu, 1000000.0 * (double)task_num / (double)push_timeu);
  s2mqt->destroy();
  timeu = tbsys::CTimeUtil::getTime() - timeu;
  fprintf(stderr, "timeu=%ld speed=%0.2f\n", timeu, 1000000.0 * (double)task_num / (double)timeu);

  EXPECT_EQ(task_num, s2mqt->num_);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  g_allocator.init(16L * 1024L * 1024L * 1024L, 8L * 1024L * 1024L * 1024L, 1L * 1024L * 1024L);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
