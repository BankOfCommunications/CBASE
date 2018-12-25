#include <gtest/gtest.h>
#include <stdlib.h>
#include <time.h>
#include <vector>
#include <string>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include "common/ob_cell_array.h"
#include "common/ob_define.h"
#include "common/ob_cache.h"
#include "common/ob_string.h"
#include "common/ob_action_flag.h"
#include "common/ob_scan_param.h"
#include "common/ob_spop_spush_queue.h"

using namespace oceanbase::common;
using namespace testing;
using namespace std;

TEST(SPSP, basic)
{
  ObSPopSPushQueue sq;
  int64_t timeout_us = 0;
  int64_t queue_size  = 1024;
  EXPECT_EQ(sq.init(queue_size,0), OB_SUCCESS);
  void *res = NULL;
  EXPECT_NE(sq.pop(timeout_us, res), OB_SUCCESS);
  EXPECT_EQ(res, (void*)NULL);
  timeout_us = 3000000;
  EXPECT_NE(sq.pop(timeout_us, res), OB_SUCCESS);
  EXPECT_EQ(res, (void*)NULL);
  EXPECT_EQ(sq.size(),0ull);

  void *msg = (void*)5;
  EXPECT_EQ(sq.push(msg),OB_SUCCESS);
  EXPECT_EQ(sq.size(),1ull);
  timeout_us = 0;
  EXPECT_EQ(sq.pop(timeout_us, res), OB_SUCCESS);
  EXPECT_EQ(res, msg);
  EXPECT_EQ(sq.size(),0ull);

  int64_t msg_id = 0;
  for (msg_id = 0; msg_id < queue_size; msg_id++)
  {
    EXPECT_EQ(sq.push((void*)msg_id), OB_SUCCESS);
  }
  EXPECT_NE(sq.push((void*)msg_id), OB_SUCCESS);
  int64_t msg_id_i = 0;
  EXPECT_EQ(sq.pop(timeout_us, res),OB_SUCCESS);
  EXPECT_EQ(res,(void*)msg_id_i);
  msg_id_i ++;


  EXPECT_EQ(sq.push((void*)msg_id), OB_SUCCESS);
  for (;msg_id_i <= msg_id; msg_id_i ++)
  {
    EXPECT_EQ(sq.pop(timeout_us, res),OB_SUCCESS);
    EXPECT_EQ(res,(void*)msg_id_i);
  }
  EXPECT_EQ(sq.size(),0ull);
}

int64_t test_count = 1024*1024;

void *push_thread_routine(void *arg)
{
  ObSPopSPushQueue *sp = reinterpret_cast<ObSPopSPushQueue*>(arg);
  for (int64_t i = 1; i <= test_count; i ++)
  {
    int err = OB_SUCCESS;
    while (OB_SUCCESS != (err = sp->push((void*)i)))
    {
    }
    if (i == test_count/2)
    {
      sleep(3);
    }
  }
  return NULL;
}

void *pop_thread_routine(void *arg)
{
  ObSPopSPushQueue *sp = reinterpret_cast<ObSPopSPushQueue*>(arg);
  int64_t timeout_us = 4000000;
  void *msg = NULL;
  for (int64_t i = 1; i <= test_count; i ++)
  {
    while (OB_SUCCESS != sp->pop(timeout_us, msg))
    {
    }
    EXPECT_EQ(msg, (void*)i);
  }
  return NULL;
}

int main(int argc, char **argv)
{
  int err = 0;
  srandom(static_cast<uint32_t>(time(NULL)));
  ob_init_memory_pool(64*1024);
  InitGoogleTest(&argc, argv);
  err = RUN_ALL_TESTS();
  /// mult thread test
  ObSPopSPushQueue sq;
  int64_t queue_size  = 1024;
  EXPECT_EQ(sq.init(queue_size,0), OB_SUCCESS);
  pthread_t push_thread;
  pthread_t pop_thread;
  pthread_create(&pop_thread, NULL, pop_thread_routine, &sq);
  pthread_create(&push_thread, NULL, push_thread_routine, &sq);
  void *res  = NULL;
  pthread_join(push_thread, &res);
  pthread_join(pop_thread, &res);
  return err;
}
