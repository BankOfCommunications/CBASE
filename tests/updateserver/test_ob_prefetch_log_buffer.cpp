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
#include "updateserver/ob_prefetch_log_buffer.h"
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
      int64_t max_num_items;
      Config()
      {
        max_log_file_size = 1<<30; // 不要switch_log, 否则log_generator遇到swtich_log就frozen了，导致一次无法把生成的日志读完
        log_generator_buf_size = 1<<20;
        block_size_shift = 22;
        n_blocks = 1<<2;
        max_num_items = MAX_N_DATA_ITEMS;
      }
    };
    int64_t Config::MAX_N_DATA_ITEMS = 1<<20;

    class ObPrefetchLogBufferAppender : public IObBatchLogHandler
    {
      private:
        ObPrefetchLogBuffer& log_buffer_;
      public:
        ObPrefetchLogBufferAppender(ObPrefetchLogBuffer& log_buffer): log_buffer_(log_buffer) {}
        virtual ~ObPrefetchLogBufferAppender() {}
        virtual int handle_log(const ObLogCursor& start_cursor, const ObLogCursor& end_cursor,
                           const char* log_data, const int64_t data_len) {
          return log_buffer_.append_log(start_cursor.log_id_, end_cursor.log_id_, log_data, data_len);
        }
    };

    int append_log_to_prefetch_buf(ObPrefetchLogBuffer& log_buffer, ObLogGenerator& log_generator, const int64_t end_id)
    {
      ObPrefetchLogBufferAppender appender(log_buffer);
      return gen_log_and_handle(&appender, log_generator, end_id);
    }

    class ObPrefetchLogBufferTest: public ::testing::Test, public Config {
      public:
        ObPrefetchLogBufferTest() {}
        ~ObPrefetchLogBufferTest() {}
      protected:
        int err;
        ObLogGenerator log_generator;
        ObPrefetchLogBuffer log_buffer;
        int64_t start_id;
        int64_t end_id;
        ThreadSpecificBuffer thread_buffer;
      protected:
        virtual void SetUp(){
          ObLogCursor start_cursor;
          ASSERT_EQ(OB_SUCCESS, log_generator.init(log_generator_buf_size, max_log_file_size));
          ASSERT_EQ(OB_SUCCESS, log_generator.start_log(set_cursor(start_cursor, 1, 1, 0)));
          ASSERT_EQ(OB_SUCCESS, log_buffer.init(n_blocks, block_size_shift));
          srandom(static_cast<int32_t>(time(NULL)));
          err = OB_SUCCESS;
          start_id = 1;
          end_id = 1;
        }
        virtual void TearDown(){
          inspect(true);
        }
        void inspect(bool verbose=false){
          fprintf(stderr, "ObPrefetchLogBuffer{buf_blocks=%ld*1<<%ld, max_num_items=%ld, remain=%ld}\n", n_blocks, block_size_shift, max_num_items, log_buffer.get_remain());
          if(verbose)
          {
            log_buffer.dump_for_debug();
          }
        }

        int get(int64_t& start, int64_t& end){
          int err = OB_SUCCESS;
          const int64_t align_bits = OB_DIRECT_IO_ALIGN_BITS;
          ThreadSpecificBuffer::Buffer *thread_buf = thread_buffer.get_buffer();
          ObDataBuffer buf(thread_buf->current(), OB_MAX_LOG_BUFFER_SIZE);
          if (OB_SUCCESS != (err = log_buffer.get_log(align_bits, start, end, buf.get_data(), buf.get_capacity(), buf.get_position())))
          {
            TBSYS_LOG(ERROR, "log_buffer.get_log(start_id=%ld)=>%d", start, err);
          }
          return err;
        }

        int push(const int64_t end_id) {
          return append_log_to_prefetch_buf(log_buffer, log_generator, end_id);
        }
    };
    TEST_F(ObPrefetchLogBufferTest, Inspect){
      inspect(true);
    }

    TEST_F(ObPrefetchLogBufferTest, Init){
      ObPrefetchLogBuffer buf;
      ASSERT_EQ(OB_INVALID_ARGUMENT, buf.init(0));
      ASSERT_EQ(OB_INVALID_ARGUMENT, buf.init(1024, 22));
      ASSERT_EQ(OB_SUCCESS, buf.init(4, 22));
      ASSERT_EQ(OB_INIT_TWICE, buf.init(4, 22));
    }

    TEST_F(ObPrefetchLogBufferTest, InvalidGet){
      ObPrefetchLogBuffer buf;
      char cbuf[128];
      int64_t read_count = -1;
      const int64_t align_bits = OB_DIRECT_IO_ALIGN_BITS;
      ASSERT_EQ(OB_NOT_INIT, buf.get_log(align_bits, start_id, end_id, cbuf, sizeof(cbuf), read_count));
      ASSERT_EQ(OB_INVALID_ARGUMENT, log_buffer.get_log(align_bits, start_id, end_id, NULL, sizeof(cbuf), read_count));
      ASSERT_EQ(OB_SUCCESS, log_buffer.get_log(align_bits, start_id, end_id, cbuf, sizeof(cbuf), read_count));
      ASSERT_EQ(0, read_count);
    }

    TEST_F(ObPrefetchLogBufferTest, InvalidPush){
      ObPrefetchLogBuffer buf;
      char cbuf[128];
      ASSERT_EQ(OB_NOT_INIT, buf.append_log(start_id, end_id, cbuf, sizeof(cbuf)));
      ASSERT_EQ(OB_INVALID_ARGUMENT, log_buffer.append_log(0, end_id, cbuf, sizeof(cbuf)));
      ASSERT_EQ(OB_INVALID_ARGUMENT, log_buffer.append_log(1, end_id, NULL, sizeof(cbuf)));
      end_id = 2;
      ASSERT_EQ(OB_SUCCESS, log_buffer.append_log(1, end_id, cbuf, sizeof(cbuf)));
      ASSERT_EQ(OB_DISCONTINUOUS_LOG, log_buffer.append_log(1, end_id, cbuf, sizeof(cbuf)));
    }

    TEST_F(ObPrefetchLogBufferTest, PushAndGet){
      int64_t end_id2 = 5;
      ASSERT_EQ(OB_SUCCESS, push(end_id2));
      inspect(true);
      ASSERT_EQ(OB_SUCCESS, get(start_id, end_id));
      ASSERT_EQ(1, start_id);
      ASSERT_GE(end_id, end_id2);
    }

    TEST_F(ObPrefetchLogBufferTest, PushUntilBufFull){
      for (end_id = 5; OB_SUCCESS == err && end_id < max_num_items; end_id += 5){
        err = push(end_id);
      }
      inspect(true);
      ASSERT_EQ(OB_EAGAIN, err) << "maybe you don't push enough items: " << max_num_items << "\n";
      fprintf(stderr, "end_id=%ld", end_id);
      ASSERT_GE(end_id, 100);
    }

    TEST_F(ObPrefetchLogBufferTest, Reset){
      ASSERT_EQ(OB_EAGAIN, push(max_num_items/2)) << "maybe you don't push enough items: " << max_num_items << "\n";
      inspect(true);
      ASSERT_EQ(OB_SUCCESS, log_buffer.reset());
      ASSERT_EQ(OB_EAGAIN, push(max_num_items));
    }

    TEST_F(ObPrefetchLogBufferTest, PushAndGetSeq){
      int64_t end_id2 = 1;
      start_id = 1;
      for (end_id = 5; end_id < max_num_items; end_id += 40)
      {
        ASSERT_EQ(OB_SUCCESS, push(end_id));
        //inspect(true);
        if (OB_SUCCESS != (err = get(start_id, end_id2))
            && OB_DATA_NOT_SERVE != err)
        {
          TBSYS_LOG(ERROR, "get(start_id=%ld)=>%d", start_id, err);
        }
        else if (OB_DATA_NOT_SERVE == err)
        {
          TBSYS_LOG(INFO, "get(start_id=%ld): OB_DATA_NOT_SERVE", start_id);
        }
        else
        {
          start_id = end_id2;
        }
        //TBSYS_LOG(DEBUG, "push_and_get(end_id=%ld, end_id2=%ld)", end_id, end_id2);
        ASSERT_EQ(OB_SUCCESS, err);
        ASSERT_EQ(get_start_log_id(log_generator), end_id2);
      }
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
