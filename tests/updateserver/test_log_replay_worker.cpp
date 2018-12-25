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
 *   yuani.xhf <yuani.xhf@taobao.com>
 *
 */

#include "gtest/gtest.h"
#include "common/ob_malloc.h"
#include "common/ob_log_reader.h"
#include "updateserver/ob_ups_log_utils.h"
#include "updateserver/ob_log_replay_worker.h"
#include "common/thread_buffer.h"
#include "test_utils2.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

namespace oceanbase
{
  namespace test
  {
    struct Config
    {
      static int64_t MAX_N_DATA_ITEMS;
      int64_t log_buf_limit; // 1<<24
      int64_t log_buf_size;
      int64_t reserved_buf_len;
      int64_t max_num_items;
      int64_t retry_wait_time;
      int64_t n_replay_thread;
      int64_t max_trans_num;
      int64_t n_queue_len;
      Config()
      {
        log_buf_size = 1<<16;
        reserved_buf_len = 1<<14;
        retry_wait_time = 10 * 1000;
        max_num_items = MAX_N_DATA_ITEMS;
        n_replay_thread = 2;
        max_trans_num = 1<<10;
        n_queue_len = 16;
      }
    };
    int64_t Config::MAX_N_DATA_ITEMS = 1<<20;

    struct FakeMutator
    {
      int64_t trans_id_;
      int64_t checksum_before_;
      int64_t checksum_after_;
      int64_t checksum_;
      int64_t data_;
    };
    FakeMutator& set_fake_mutator(FakeMutator& mutator, const int64_t trans_id, const int64_t checksum)
    {
      
      mutator.trans_id_ = trans_id;
      mutator.data_ = ~trans_id;
      mutator.checksum_before_ = checksum;
      mutator.checksum_ = ob_crc64(mutator.data_, &mutator.trans_id_, sizeof(mutator.trans_id_));
      mutator.checksum_after_ = ob_crc64(checksum, &mutator.checksum_, sizeof(mutator.checksum_));
      return mutator;
    }
    struct LogGen
    {
      int64_t checksum_;
      ObLogCursor cursor_;
      char buf_[OB_MAX_LOG_BUFFER_SIZE];
      LogGen(): checksum_(0) {  set_cursor(cursor_, 1, 1, 0); }
      ~LogGen() {}
      int get_log(char*& buf, int64_t& len)
      {
        int err = OB_SUCCESS;
        int64_t pos = 0;
        if (OB_SUCCESS != (err = gen_log(buf_, sizeof(buf_), pos)))
        {
          TBSYS_LOG(ERROR, "gen_log()=>%d", err);
        }
        else
        {
          buf = buf_;
          len = pos;
        }
        return err;
      }
      int gen_log(char* buf, const int64_t limit, int64_t& pos)
      {
        int err = OB_SUCCESS;
        FakeMutator mutator;
        if (NULL == buf || 0 >= limit || 0 > pos || pos > limit)
        {
          err = OB_INVALID_ARGUMENT;
        }
        while(OB_SUCCESS == err && pos < limit)
        {
          set_fake_mutator(mutator, cursor_.log_id_, checksum_);
          if (OB_SUCCESS != (err = serialize_log_entry(buf, limit, pos, OB_LOG_NOP, cursor_.log_id_,
                                                       (const char*)&mutator, sizeof(mutator)))
              && OB_BUF_NOT_ENOUGH != err)
          {
            TBSYS_LOG(ERROR, "serialize_log_entry()=>%d", err);
          }
          else if (OB_BUF_NOT_ENOUGH == err)
          {
            err = OB_SUCCESS;
            break;
          }
          else if (OB_SUCCESS != (err = cursor_.advance(OB_LOG_NOP, cursor_.log_id_, sizeof(mutator))))
          {
            TBSYS_LOG(ERROR, "cursor.advance()=>%d", err);
          }
          else
          {
            checksum_ = mutator.checksum_after_;
          }
        }
        return err;
      }
    };
    class MockAsyncLogApplier: public IAsyncLogApplier
    {
      public:
        MockAsyncLogApplier(TransMgr& trans_mgr): trans_mgr_(trans_mgr), checksum_before_(0), checksum_after_(0)
      {}
        virtual ~MockAsyncLogApplier() {}
      public:
        virtual int on_submit(ObLogTask& log_task){ UNUSED(log_task); return 0; }        
        virtual int flush(ObLogTask& log_task){ UNUSED(log_task); return 0; }        
        virtual int on_destroy(ObLogTask& log_task){ UNUSED(log_task); return 0; }        
        virtual int start_transaction(ObLogTask& log_task){ UNUSED(log_task); return 0; }
        virtual int apply(ObLogTask& task) {
          int err = OB_SUCCESS;
          UNUSED(task);
          return err;
        }
        virtual int end_transaction(ObLogTask& log_task){ UNUSED(log_task); return 0; }
      private:
        TransMgr& trans_mgr_;
        volatile int64_t checksum_before_;
        volatile int64_t checksum_after_;
    };

    class ObLogReplayWorkerTest: public ::testing::Test, public Config {
      public:
        ObLogReplayWorkerTest(): applier(trans_mgr) {}
        ~ObLogReplayWorkerTest(){}
      protected:
        int err;
        TransMgr trans_mgr;
        MockAsyncLogApplier applier;
        ObLogReplayWorker worker;
        ThreadSpecificBuffer log_buffer;
        ObLogCursor start_cursor;
      protected:
        virtual void SetUp(){
          set_cursor(start_cursor, 1, 1, 0);
          ASSERT_EQ(OB_SUCCESS, trans_mgr.init(max_trans_num));
          ASSERT_EQ(OB_SUCCESS, worker.init(&applier, n_replay_thread, log_buf_limit, n_queue_len));
          ASSERT_EQ(OB_SUCCESS, worker.start_log(start_cursor));
          worker.start();
          srandom(static_cast<int32_t>(time(NULL)));
          err = OB_SUCCESS;
        }
        virtual void TearDown(){
          inspect(true);
        }
        void inspect(bool verbose=false){
          fprintf(stderr, "ObLogReplayWorkerTest{log_buf_size=%ld, max_num_items=%ld}\n",
                  log_buf_size, max_num_items);
          if(verbose)
          {
          }
        }
        int replay_log(int64_t end_id) {
          int err = OB_SUCCESS;
          ObLogCursor cursor;
          LogGen gen;
          char* buf = NULL;
          int64_t len = 0;
          gen.cursor_ = start_cursor;
          while(OB_SUCCESS == err && gen.cursor_.log_id_ < end_id)
          {
            if (OB_SUCCESS != (err = gen.get_log(buf, len)))
            {
              TBSYS_LOG(ERROR, "gen_batch_log()=>%d", err);
            }
            else if (OB_SUCCESS != (err = replay_batch_log(worker, buf, len)))
            {
              TBSYS_LOG(ERROR, "replay_batch_log()=>%d", err);
            }
            else
            {
              TBSYS_LOG(INFO, "replay(log_id=%ld)", gen.cursor_.log_id_);
            }
          }
          return err;
        }
    };

    TEST_F(ObLogReplayWorkerTest, Replay){
      ASSERT_EQ(OB_SUCCESS, replay_log(max_num_items));
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
