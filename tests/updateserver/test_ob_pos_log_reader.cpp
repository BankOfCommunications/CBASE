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
#include "updateserver/ob_pos_log_reader.h"
#include "common/thread_buffer.h"
#include "test_utils2.h"
#include "log_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

namespace oceanbase
{
  namespace test
  {
    struct Config
    {
      static int64_t MAX_N_DATA_ITEMS;
      bool dio;
      const char* log_dir;
      int64_t log_file_max_size;
      int64_t log_buf_size;
      int64_t file_id_cache_capacity;
      int64_t log_id_cache_capacity;
      int64_t reserved_buf_len;
      int64_t max_num_items;
      int64_t retry_wait_time;
      Config()
      {
        dio = true;
        log_dir = "log";
        log_file_max_size = 1<<24;
        file_id_cache_capacity = 1<<10;
        log_id_cache_capacity = 1<<20;
        log_buf_size = 1<<16;
        reserved_buf_len = 1<<14;
        retry_wait_time = 10 * 1000;
        max_num_items = MAX_N_DATA_ITEMS;
      }
    };
    int64_t Config::MAX_N_DATA_ITEMS = 1<<20;

    class ObPosLogReaderTest: public ::testing::Test, public Config {
      public:
        ObPosLogReaderTest()
      {
      }
      protected:
        int err;
        MgTool mgt;
        ObLogGenerator log_generator;
        ObLogWriterV2 log_writer;
        ObPosLogReader pos_log_reader;
        ThreadSpecificBuffer log_buffer;
        ObLogCursor start_cursor;
        ObLogCursor end_cursor;
      protected:
        virtual void SetUp(){
          ObLogCursor start_cursor;
          ASSERT_EQ(OB_SUCCESS, mgt.sh("rm -rf %s", log_dir));
          srandom(static_cast<int32_t>(time(NULL)));
          ASSERT_EQ(OB_SUCCESS, log_generator.init(log_buf_size, log_file_max_size));
          ASSERT_EQ(OB_SUCCESS, log_generator.start_log(set_cursor(start_cursor, 1, 1, 0)));
          ASSERT_EQ(OB_SUCCESS, log_writer.init(log_dir, LOG_ALIGN-1, OB_LOG_SYNC));
          ASSERT_EQ(OB_SUCCESS, log_writer.start_log(set_cursor(start_cursor, 1, 1, 0)));
          ASSERT_EQ(OB_SUCCESS, pos_log_reader.init(log_dir, dio));
          err = OB_SUCCESS;
        }
        virtual void TearDown(){
          inspect(true);
        }
        void inspect(bool verbose=false){
          fprintf(stderr, "ObPosLogReaderTest{log_buf_size=%ld, max_num_items=%ld}\n",
                  log_buf_size, max_num_items);
          if(verbose)
          {
          }
        }

        int read_log(const int64_t start_id, int64_t& end_id) {
          int err = OB_SUCCESS;
          char* buf = NULL;
          int64_t len = 0;
          int64_t read_count = 0;
          ObLogLocation start_location;
          ObLogLocation end_location;
          if (OB_SUCCESS != (err = get_cbuf(log_buffer, buf, len)))
          {
            TBSYS_LOG(ERROR, "get_cbuf()=>%d", err);
          }
          else if (OB_SUCCESS != (err = pos_log_reader.get_log(start_id, start_location, end_location, buf, len, read_count)))
          {
            TBSYS_LOG(ERROR, "get_log(start_id=%ld)=>%d", start_id, err);
          }
          else
          {
            end_id = end_location.log_id_;
            TBSYS_LOG(INFO, "read_log([%ld,%ld])", start_id, end_id);
          }
          return err;
        }

        int read_log(ObLogLocation& start_location, ObLogLocation& end_location) {
          int err = OB_SUCCESS;
          char* buf = NULL;
          int64_t len = 0;
          int64_t read_count = 0;
          if (OB_SUCCESS != (err = get_cbuf(log_buffer, buf, len)))
          {
            TBSYS_LOG(ERROR, "get_cbuf()=>%d", err);
          }
          else if (OB_SUCCESS != (err = pos_log_reader.get_log(start_location.log_id_, start_location, end_location, buf, len, read_count)))
          {
            TBSYS_LOG(ERROR, "get_log(start_id=%ld)=>%d", start_location.log_id_, err);
          }
          else
          {
            TBSYS_LOG(INFO, "read_log([%ld,%ld])", start_location.log_id_, end_location.log_id_);
          }
          return err;
        }
    };

    TEST_F(ObPosLogReaderTest, Inspect){
      inspect(true);
    }

    TEST_F(ObPosLogReaderTest, Init){
      ObPosLogReader pos_log_reader2;
      ASSERT_EQ(OB_INVALID_ARGUMENT, pos_log_reader2.init(NULL, 0));
    }

    TEST_F(ObPosLogReaderTest, WriteAndReadByLocation){
      int64_t n_err = 0;
      ObLogLocation start_location;
      ObLogLocation end_location;
      start_location.log_id_ = 1;
      ASSERT_EQ(OB_SUCCESS, write_log_to_end(log_writer, log_generator, max_num_items));
      for (; start_location.log_id_ < max_num_items; )
      {
        if (OB_SUCCESS != (err = read_log(start_location, end_location)))
        {
          TBSYS_LOG(DEBUG, "read_log(%ld)=>%d", start_location.log_id_, err);
          n_err++;
        }
        else
        {
          start_location = end_location;
        }
      }
      ASSERT_EQ(0, n_err);
    }

    TEST_F(ObPosLogReaderTest, WriteAndReadById){
      int64_t end_id = 0;
      int64_t n_err = 0;
      ASSERT_EQ(OB_SUCCESS, write_log_to_end(log_writer, log_generator, max_num_items));
      for (int64_t i = 2; i < max_num_items; i = end_id)
      {
        if (OB_SUCCESS != (err = read_log(i, end_id)))
        {
          TBSYS_LOG(DEBUG, "read_log(%ld)=>%d", i, err);
          n_err++;
          end_id = i;
        }
      }
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
