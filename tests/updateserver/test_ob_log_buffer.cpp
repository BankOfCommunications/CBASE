/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_ob_log_cache.cpp
 *
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *
 */

#include "gtest/gtest.h"
#include "common/ob_malloc.h"
#include "updateserver/ob_log_buffer.h"
#include "common/thread_buffer.h"
#include "log_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

namespace oceanbase
{
  namespace test
  {
    const int OB_ERROR_NOCHECK = 123456;
    struct Config
    {
      static int64_t MAX_N_DATA_ITEMS;
      int64_t log_generator_buf_size;
      int64_t block_size_shift;
      int64_t n_blocks;
      int64_t max_log_file_size;
      int64_t reserved_buf_len;
      int64_t max_num_items;
      int64_t n_reader;
      int64_t retry_wait_time;
      bool slow_write;
      char data_item[16];
      char* get_data(int64_t i)
      {
        snprintf(data_item, sizeof(data_item), "%ld", i);
        data_item[sizeof(data_item)-1] = 0;
        return data_item;
      }
      Config()
      {
        max_log_file_size = 1<<24;
        block_size_shift = 14;
        n_blocks = 1<<2;
        log_generator_buf_size = 1<<12;
        reserved_buf_len = 1<<4;
        retry_wait_time = 10 * 1000;
        max_num_items = MAX_N_DATA_ITEMS;
        n_reader = 4;
        slow_write = false;
      }
    };
    int64_t Config::MAX_N_DATA_ITEMS = 1<<20;

    class ObLogBufferAppender : public IObBatchLogHandler
    {
      private:
        ObLogBuffer& log_cache_;
      public:
        ObLogBufferAppender(ObLogBuffer& log_cache): log_cache_(log_cache) {}
        virtual ~ObLogBufferAppender() {}
        virtual int handle_log(const ObLogCursor& start_cursor, const ObLogCursor& end_cursor,
                           const char* log_data, const int64_t data_len) {
          return append_to_log_buffer(&log_cache_, start_cursor.log_id_, end_cursor.log_id_, log_data, data_len);
        }
    };

    int append_log_to_cache(ObLogBuffer& log_cache, ObLogGenerator& log_generator, const int64_t end_id)
    {
      ObLogBufferAppender log_cache_appender(log_cache);
      return gen_log_and_handle(&log_cache_appender, log_generator, end_id);
    }

    class ObLogCacheTest: public ::testing::Test, public tbsys::CDefaultRunnable, public Config {
      public:
        ObLogCacheTest() {}
        ~ObLogCacheTest() {}
      protected:
        int err;
        ObLogGenerator log_generator;
        ObLogBuffer log_cache;
        int64_t start_id;
        int64_t end_id;
        ThreadSpecificBuffer log_buffer;
      protected:
        virtual void SetUp(){
          ObLogCursor start_cursor;
          ASSERT_EQ(OB_SUCCESS, log_generator.init(log_generator_buf_size, max_log_file_size));
          ASSERT_EQ(OB_SUCCESS, log_generator.start_log(set_cursor(start_cursor, 1, 1, 0)));
          ASSERT_EQ(OB_SUCCESS, log_cache.init(n_blocks, block_size_shift));
          srandom(static_cast<int32_t>(time(NULL)));
          err = OB_SUCCESS;
          start_id = 1;
          end_id = 1;
        }
        virtual void TearDown(){
          inspect(true);
        }
        void inspect(bool verbose=false){
          fprintf(stderr, "ObLogCacheTest{buf=%ld*(1<<%ld), max_num_items=%ld}\n",
                  n_blocks, block_size_shift, max_num_items);
          if(verbose)
          {
            log_cache.dump_for_debug();
          }
        }

        int get(const int64_t& start, int64_t& end){
          int err = OB_SUCCESS;
          ThreadSpecificBuffer::Buffer *thread_buf = log_buffer.get_buffer();
          ObDataBuffer buf(thread_buf->current(), OB_MAX_LOG_BUFFER_SIZE);
          static __thread int64_t advised_pos = 0;
          static __thread int64_t real_pos = 0;
          if (OB_SUCCESS != (err = get_from_log_buffer(&log_cache, OB_DIRECT_IO_ALIGN_BITS, advised_pos, real_pos, start, end, buf.get_data(), buf.get_capacity(), buf.get_position())) && OB_DATA_NOT_SERVE != err)
          {
            TBSYS_LOG(ERROR, "log_cache.get_log(start_id=%ld)=>%d", start, err);
          }
          return err;
        }

        int push(const int64_t end_id) {
          return append_log_to_cache(log_cache, log_generator, end_id);
        }

        void run_as_writer(tbsys::CThread *thread, int64_t idx){
          UNUSED(thread);
          UNUSED(idx);
          int64_t i = 0;
          for(i = 0; !_stop && i < max_num_items; i += random() % 100)
          {
            if (i % 3 == 0){
              ASSERT_EQ(OB_SUCCESS, log_cache.reset());
            }
            ASSERT_EQ(OB_SUCCESS, push(i));
            if (slow_write)usleep(10);
          }
          ASSERT_EQ(OB_SUCCESS, push(i + 1));
        }

        void run_as_reader(tbsys::CThread *thread, int64_t idx){
          int err = OB_SUCCESS;
          int64_t start_id = 1;
          int64_t end_id = 0;
          static __thread int64_t miss = 0;
          static __thread int64_t hit = 0;
          
          UNUSED(thread);
          for (; !_stop && (OB_SUCCESS == err || OB_DATA_NOT_SERVE == err) && start_id < max_num_items; start_id = end_id)
          {
            if (OB_SUCCESS != (err = get(start_id, end_id))
                && OB_DATA_NOT_SERVE != err)
            {
              TBSYS_LOG(ERROR, "get(start_id=%ld)=>%d", start_id, err);
            }
            else if (OB_DATA_NOT_SERVE == err)
            {
              end_id = log_cache.get_start_id();
              miss++;
            }
            else
            {
              hit++;
            }
          }
          if (OB_DATA_NOT_SERVE == err)
          {
            err = OB_SUCCESS;
          }
          if (OB_SUCCESS != err)
          {
            stop();
          }
          ASSERT_EQ(OB_SUCCESS, err);
          TBSYS_LOG(INFO, "reader[%ld]: %ld/(%ld+%ld)=%ld/10000", idx, hit, hit, miss, 10000 * hit/(hit+miss));
        }

        void run(tbsys::CThread *thread, void* arg){
          int64_t idx = (long)(arg);
          if (idx > n_reader)
          {}
          else if(0 == idx)
          {
            run_as_writer(thread, idx);
          }
          else
          {
            run_as_reader(thread, idx);
          }
        }
    };
    TEST_F(ObLogCacheTest, Inspect){
      inspect(true);
    }

    TEST_F(ObLogCacheTest, Init){
      ObLogBuffer cache;
      ASSERT_EQ(OB_INVALID_ARGUMENT, cache.init(0, 0));
      ASSERT_EQ(OB_SUCCESS, cache.init(2, 1));
      ASSERT_EQ(OB_INIT_TWICE, cache.init(2, 1));
    }

    TEST_F(ObLogCacheTest, InvalidGet){
      ObLogBuffer cache;
      char cbuf[128];
      int64_t real_pos = 0;
      int64_t read_count = 0;
      const int64_t align_bits = OB_DIRECT_IO_ALIGN_BITS;
      ASSERT_EQ(OB_NOT_INIT, cache.get_log(0, align_bits, real_pos, start_id, end_id, cbuf, sizeof(cbuf), read_count));
      ASSERT_EQ(OB_INVALID_ARGUMENT, log_cache.get_log(0, align_bits, real_pos, start_id, end_id, NULL, sizeof(cbuf), read_count));
      start_id = 1;
      ASSERT_EQ(OB_DATA_NOT_SERVE, log_cache.get_log(2, align_bits, real_pos, start_id, end_id, cbuf, sizeof(cbuf), read_count));
    }

    TEST_F(ObLogCacheTest, InvalidPush){
      ObLogBuffer cache;
      char cbuf[128];
      ASSERT_EQ(OB_NOT_INIT, cache.append_log(start_id, end_id, cbuf, sizeof(cbuf)));
      ASSERT_EQ(OB_INVALID_ARGUMENT, log_cache.append_log(0, end_id, cbuf, sizeof(cbuf)));
      ASSERT_EQ(OB_INVALID_ARGUMENT, log_cache.append_log(1, end_id, NULL, sizeof(cbuf)));
    }

    TEST_F(ObLogCacheTest, PushAndGet){
      int64_t end_id2 = 5;
      ASSERT_EQ(OB_SUCCESS, push(end_id2));
      inspect(true);
      ASSERT_EQ(OB_SUCCESS, get(1, end_id));
      ASSERT_GE(end_id, end_id2);
    }

    TEST_F(ObLogCacheTest, PushStress){
      ASSERT_EQ(OB_SUCCESS, push(max_num_items/2));
    }

    TEST_F(ObLogCacheTest, Reset){
      ASSERT_EQ(OB_SUCCESS, push(max_num_items/2));
      ASSERT_EQ(OB_SUCCESS, log_cache.reset());
      ASSERT_EQ(OB_SUCCESS, push(max_num_items));
    }

#if 0
    TEST_F(ObLogCacheTest, PushAndGetSeq){
      int64_t end_id2 = 1;
      start_id = 1;
      // 必须要取两次，才能保证把这次循环产生的日志取完，因为有可能这次产生的日志恰好跨越了一个block的边界。
      for (end_id = 5; end_id < max_num_items; end_id += 5)
      {
        //TBSYS_LOG(DEBUG, "push(end_id=%ld)", end_id);
        ASSERT_EQ(OB_SUCCESS, push(end_id));
        //inspect(true);
        if (OB_SUCCESS != (err = get(start_id, end_id2))
            && OB_DATA_NOT_SERVE != err)
        {
          TBSYS_LOG(ERROR, "get(start_id=%ld)=>%d", start_id, err);
        }
        else if (OB_DATA_NOT_SERVE == err)
        {
          err = OB_SUCCESS;
          TBSYS_LOG(INFO, "get(start_id=%ld): OB_DATA_NOT_SERVE", start_id);
        }
        else
        {
          start_id = end_id2;
        }
        if (end_id2 >= end_id && false) // must try second time
        {}
        else if (OB_SUCCESS != (err = get(start_id, end_id2))
            && OB_DATA_NOT_SERVE != err)
        {
          TBSYS_LOG(ERROR, "get(start_id=%ld)=>%d", start_id, err);
        }
        else if (OB_DATA_NOT_SERVE == err)
        {
          err = OB_SUCCESS;
          TBSYS_LOG(INFO, "get(start_id=%ld): OB_DATA_NOT_SERVE", start_id);
        }
        else
        {
          start_id = end_id2;
        }
        ASSERT_EQ(OB_SUCCESS, err);
        ASSERT_GE(end_id2, end_id);
      }
    }
#endif
#if 0
    TEST_F(ObLogCacheTest, AsyncPushAndGet){
      setThreadCount((int)n_reader+1);
      start();
      wait();
      ASSERT_EQ(OB_SUCCESS, err);
    }
    TEST_F(ObLogCacheTest, AsyncPushAndGetSlowWrite){
      slow_write = true;
      setThreadCount((int)n_reader+1);
      start();
      wait();
      ASSERT_EQ(OB_SUCCESS, err);
    }
#endif
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
