/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 *
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *
 */

#include "gtest/gtest.h"
#include "common/ob_malloc.h"
#include "common/ob_queued_lock.h"
#include "common/thread_buffer.h"
#include "common/utility.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace test
  {
#define cfg(k, v) getenv(k)?:v
#define cfgi(k, v) atoll(cfg(k ,v))
    struct Config
    {
      int64_t n_thread;
      int64_t duration;
      Config()
      {
        n_thread = cfgi("n_thread", "2");
        duration = cfgi("duration", "10000000");
      }
    };

    class ObMutexSchedulingTest: public ::testing::Test, public tbsys::CDefaultRunnable, public Config {
      public:
        ObMutexSchedulingTest() {}
        ~ObMutexSchedulingTest(){}
      protected:
        int64_t n_err;        
        bool using_queued_lock;
        tbsys::CThreadMutex mutex;
        ObQueuedLock fair_lock;
      protected:
        virtual void SetUp(){
          srandom(static_cast<int32_t>(time(NULL)));
          using_queued_lock = false;
          n_err = 0;
        }
        virtual void TearDown(){
          //inspect(true);
        }

        void run(tbsys::CThread *thread, void* arg){
          int err = OB_SUCCESS;
          int64_t last_ts = tbsys::CTimeUtil::getTime();
          int64_t start_ts = last_ts;
          int64_t cur_ts = 0;
          int64_t idx = (long)(arg);
          UNUSED(thread);
          for(int i = 0; OB_SUCCESS == err; i++) {
            if (using_queued_lock)
            {
              ObQueuedLock::Guard guard(fair_lock);
              cur_ts = tbsys::CTimeUtil::getTime();
              if (cur_ts - start_ts > duration)
              {
                break;
              }
              if (cur_ts - last_ts > 100000)
              {
                TBSYS_LOG(WARN, "cur_ts-last_ts=%ldus\n", cur_ts - last_ts);
              }
              last_ts = cur_ts;
              usleep((__useconds_t)(idx * 1000));
            }
            else
            {
              tbsys::CThreadGuard guard(&mutex);
              cur_ts = tbsys::CTimeUtil::getTime();
              if (cur_ts - start_ts > duration)
              {
                break;
              }
              if (cur_ts - last_ts > 100000)
              {
                TBSYS_LOG(WARN, "cur_ts-last_ts=%ldus\n", cur_ts - last_ts);
              }
              last_ts = cur_ts;
              usleep((__useconds_t)(idx * 1000));
            }
          }
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(ERROR, "thread[%ld]=>%d", idx, err);
            __sync_fetch_and_add(&n_err, 1);
          }
        }
    };

    TEST_F(ObMutexSchedulingTest, PthreadMutexTest){ 
      setThreadCount((int32_t)n_thread);
      start();
      wait();
    }
    TEST_F(ObMutexSchedulingTest, QueuedMutexTest){
      setThreadCount((int32_t)n_thread);
      using_queued_lock = true;
      start();
      wait();
    }
  } // end namespace updateserver
} // end namespace oceanbase

using namespace oceanbase::test;
int main(int argc, char** argv)
{
  int err = OB_SUCCESS;
  TBSYS_LOGGER.setLogLevel("INFO");
  if (OB_SUCCESS != (err = ob_init_memory_pool()))
    return err;
  ::testing::InitGoogleTest(&argc, argv);
  if(argc >= 2)
  {
    TBSYS_LOGGER.setLogLevel(argv[1]);
  }
  return RUN_ALL_TESTS();
}
