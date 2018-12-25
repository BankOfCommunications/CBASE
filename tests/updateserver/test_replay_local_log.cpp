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
      int64_t reserved_buf_len;
      int64_t max_num_items;
      int64_t retry_wait_time;
      Config()
      {
        dio = true;
        log_dir = "log";
        log_file_max_size = 1<<24;
        log_buf_size = 1<<16;
        reserved_buf_len = 1<<14;
        retry_wait_time = 10 * 1000;
        max_num_items = MAX_N_DATA_ITEMS;
      }
    };
    int64_t Config::MAX_N_DATA_ITEMS = 1<<20;

    class ObReplayLogTest: public ::testing::Test, public Config {
      public:
        ObReplayLogTest()
      {
      }
      protected:
        int err;
        MgTool mgt;
        ObUpsLogMgr2 log_mgr;
        ThreadSpecificBuffer log_buffer;
      protected:
        virtual void SetUp(){
          ObLogCursor start_cursor;
          set_cursor(start_cursor, 1, 1, 0);
          ASSERT_EQ(OB_SUCCESS, mgt.sh("rm -rf %s", log_dir));
          srandom(static_cast<int32_t>(time(NULL)));
          ASSERT_EQ(OB_SUCCESS, log_mgr.init(log_dir, log_file_max_size, log_buf_size));
          ASSERT_EQ(OB_SUCCESS, log_mgr.start_log(start_cursor));
          err = OB_SUCCESS;
        }
        virtual void TearDown(){
          inspect(true);
        }
        void inspect(bool verbose=false){
          fprintf(stderr, "ObReplayLogTest{log_buf_size=%ld, max_num_items=%ld}\n",
                  log_buf_size, max_num_items);
          if(verbose)
          {
          }
        }

    };

    TEST_F(ObReplayLogTest, Inspect){
      inspect(true);
    }

    TEST_F(ObReplayLogTest, Replay){
      bool stop = false;
      int64_t log_file_id = 0;
      ObLogCursor log_cursor;
      ObLogCursor end_cursor;
      set_cursor(log_cursor, 1, 0, 0);
      ASSERT_EQ(OB_SUCCESS, write_log_to(log_mgr, max_num_items));
      ASSERT_EQ(OB_SUCCESS, log_mgr.switch_log(log_file_id));
      ASSERT_EQ(OB_SUCCESS, replay_local_log_func(stop, log_dir, log_cursor, end_cursor, NULL));
      ASSERT_EQ(log_file_id, end_cursor.file_id_);
      TBSYS_LOG(INFO, "after_replay: log_cursor=%s", end_cursor.to_str());
    }
    TEST_F(ObReplayLogTest, ReplayWithLastFileMissing){
      bool stop = false;
      int64_t log_file_id = 0;
      ObLogCursor log_cursor;
      ObLogCursor end_cursor;
      set_cursor(log_cursor, 1, 0, 0);
      ASSERT_EQ(OB_SUCCESS, write_log_to(log_mgr, max_num_items));
      ASSERT_EQ(OB_SUCCESS, log_mgr.switch_log(log_file_id));
      ASSERT_EQ(OB_SUCCESS, mgt.sh("rm -rf %s/%ld", log_dir, log_file_id));
      ASSERT_EQ(OB_SUCCESS, replay_local_log_func(stop, log_dir, log_cursor, end_cursor, NULL));
      ASSERT_EQ(log_file_id, end_cursor.file_id_);
      ASSERT_EQ(0, end_cursor.offset_);
      TBSYS_LOG(INFO, "after_replay: log_cursor=%s", end_cursor.to_str());
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
