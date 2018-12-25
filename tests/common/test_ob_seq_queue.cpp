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
#include "common/ob_seq_queue.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace test
  {
    struct Config
    {
      static int64_t MAX_N_DATA_ITEMS;
      int64_t max_num_items;
      int64_t timeout;
      int64_t n_thread;
      int64_t queue_len;
      int64_t report_every_n_item;
      Config()
      {
        max_num_items = MAX_N_DATA_ITEMS;
        timeout = 10000;
        n_thread = 4;
        queue_len = 4;
        report_every_n_item = 100;
      }
    };
    int64_t Config::MAX_N_DATA_ITEMS = 1<<12;

    class ObSeqQueueTest: public ::testing::Test, public tbsys::CDefaultRunnable, public Config {
      public:
        ObSeqQueueTest(){}
      protected:
        volatile int64_t n_err;
        volatile int64_t seq;
        ObSeqQueue queue;
      protected:
        virtual void SetUp(){
          n_err = 0;
          seq = 1;
          ASSERT_EQ(OB_SUCCESS, queue.init(queue_len));
          ASSERT_EQ(OB_SUCCESS, queue.start(seq));
        }
        virtual void TearDown(){
          inspect(true);
        }

        void inspect(bool verbose=false){
          fprintf(stderr, "ObSeqQueueTest{max_num_items=%ld}\n", max_num_items);
          if(verbose)
          {
          }
        }

        int run_as_get_worker(){
          int err = OB_SUCCESS;
          int64_t cur_seq = 0;
          int64_t data = 0;
          while((OB_SUCCESS == err || OB_EAGAIN == err) && !_stop && seq < max_num_items)
          {
            if (OB_SUCCESS == (err = queue.get(cur_seq, (void*&)data, timeout)))
            {
              if (cur_seq % report_every_n_item)
              {
                TBSYS_LOG(INFO, "cur_seq=%ld", cur_seq);
              }
              if ((cur_seq ^ data) != -1)
              {
                err = OB_INVALID_DATA;
                TBSYS_LOG(ERROR, "cur_seq[%ld] ^ data[%ld] != -1", cur_seq, data);
              }
            }
          }
          TBSYS_LOG(INFO, "get_worker, end_seq=%ld", cur_seq);
          return err;
        }

        int run_as_push_worker(){
          int err = OB_SUCCESS;
          int64_t cur_seq = 0;
          while((OB_SUCCESS == err || OB_EAGAIN == err) && !_stop && seq < max_num_items)
          {
            cur_seq = cur_seq?: __sync_fetch_and_add(&seq, 1);
            if(OB_SUCCESS == (err = queue.add(cur_seq, (void*)~cur_seq)))
            {
              cur_seq = 0;
            }
          }
          return err;
        }

        void run(tbsys::CThread *thread, void* arg){
          int err = OB_SUCCESS;
          int64_t idx = (long)(arg);
          UNUSED(thread);
          TBSYS_LOG(INFO, "worker[%ld] start", idx);
          if (idx > n_thread)
          {}
          else if(0 == idx)
          {
            err = run_as_get_worker();
          }
          else
          {
            err = run_as_push_worker();
          }
          TBSYS_LOG(INFO, "worker[%ld] stop, err=%d", idx, err);
          if (OB_EAGAIN == err)
          {
            err = OB_SUCCESS;
          }
          if (OB_SUCCESS != err)
          {
            __sync_fetch_and_add(&n_err, 1);
          }
          stop();
          ASSERT_EQ(OB_SUCCESS, err);
        }
    };

    TEST_F(ObSeqQueueTest, Inspect){
      inspect(true);
    }
    TEST_F(ObSeqQueueTest, Replay){
      setThreadCount((int)n_thread+1);
      start();
      wait();
      ASSERT_EQ(0, n_err);
    }
  } // end namespace updateserver
} // end namespace oceanbase

using namespace oceanbase::test;
int main(int argc, char** argv)
{
  int err = OB_SUCCESS;
  int n_data_items_shift = 0;
  TBSYS_LOGGER.setLogLevel("INFO");
  if (OB_SUCCESS != (err = ob_init_memory_pool()))
    return err;
  ::testing::InitGoogleTest(&argc, argv);
  if(argc >= 2)
  {
    TBSYS_LOGGER.setLogLevel(argv[1]);
  }
  if(argc >= 3)
  {
    n_data_items_shift = atoi(argv[2]);
    if(n_data_items_shift > 4)
      Config::MAX_N_DATA_ITEMS = 1 << n_data_items_shift;
  }
  return RUN_ALL_TESTS();
}
