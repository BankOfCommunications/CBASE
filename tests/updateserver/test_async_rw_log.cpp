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
#include "common/ob_log_reader.h"
#include "common/ob_repeated_log_reader.h"
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
      const char* file_for_append;
      const char* log_dir;
      int64_t log_file_max_size;
      int64_t log_buf_size;
      int64_t reserved_buf_len;
      int64_t max_num_items;
      int64_t retry_wait_time;
      int64_t n_reader;
      int64_t align_size;
      bool test_for_write_not_align;
      Config()
      {
        dio = true;
        file_for_append = "file.append";
        log_dir = "log";
        log_file_max_size = 1<<24;
        log_buf_size = 1<<16;
        reserved_buf_len = 1<<14;
        retry_wait_time = 10 * 1000;
        max_num_items = MAX_N_DATA_ITEMS;
        n_reader = 1;
        align_size = 1<<12;
        test_for_write_not_align = false;
      }
    };
    int64_t Config::MAX_N_DATA_ITEMS = 1<<16;

    int apply_log(const common::LogCommand cmd, const uint64_t seq,
                  const char* log_data, const int64_t data_len)
    {
      int err = OB_SUCCESS;
      UNUSED(cmd);
      UNUSED(seq);
      UNUSED(log_data);
      UNUSED(data_len);
      return err;
    }

    int replay_log_func(const volatile bool& stop, const char* log_dir,
                        const ObLogCursor& start_cursor, ObLogCursor& end_cursor)
    {
      int err = OB_SUCCESS;
      ObLogReader log_reader;
      ObRepeatedLogReader repeated_log_reader;
      ObSingleLogReader* single_log_reader = &repeated_log_reader;
      char* log_data = NULL;
      int64_t data_len = 0;
      LogCommand cmd = OB_LOG_UNKNOWN;
      uint64_t seq;
      end_cursor = start_cursor;

      if (NULL == log_dir || start_cursor.file_id_ <= 0 || start_cursor.log_id_ != 0 || start_cursor.offset_ != 0)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "invalid argument: log_dir=%s, log_cursor=%s", log_dir, start_cursor.to_str());
      }
      else if (OB_SUCCESS != (err = log_reader.init(single_log_reader, log_dir, start_cursor.file_id_, 0, false)))
      {
        TBSYS_LOG(ERROR, "ObLogReader init error[err=%d]", err);
      }
      while (!stop && OB_SUCCESS == err)
      {
        //TBSYS_LOG(INFO, "log_cursor=%s", end_cursor.to_str());
        if (OB_SUCCESS != (err = log_reader.read_log(cmd, seq, log_data, data_len)) &&
            OB_FILE_NOT_EXIST != err && OB_READ_NOTHING != err && OB_LAST_LOG_RUINNED != err)
        {
          TBSYS_LOG(ERROR, "ObLogReader read error[ret=%d]", err);
        }
        else if (OB_FILE_NOT_EXIST == err || OB_READ_NOTHING == err)
        {
          err = OB_SUCCESS;
          TBSYS_LOG(WARN, "read_log(cursor=[%ld:%ld])=>READ_NOTHING", end_cursor.file_id_, end_cursor.log_id_);
        }
        else if (OB_LAST_LOG_RUINNED == err)
        {
          TBSYS_LOG(WARN, "last_log[%s] broken!", end_cursor.to_str());
          err = OB_ITER_END;
        }
        else if (OB_SUCCESS != err)
        {
          err = OB_ITER_END; // replay all
        }
        else if (OB_SUCCESS != (err = log_reader.get_next_cursor(end_cursor)))
        {
          TBSYS_LOG(ERROR, "log_reader.get_cursor()=>%d",  err);
        }
        else if (OB_SUCCESS != (err = apply_log(cmd, seq, log_data, data_len)))
        {
          TBSYS_LOG(ERROR, "apply_log()=>%d", err);
        }
      }
      if (stop)
      {
        err = OB_CANCELED;
      }
      if (OB_ITER_END == err)
      {
        err = OB_SUCCESS;
      }
      TBSYS_LOG(INFO, "replay_log(log_dir=%s, end_cursor=%s)=>%d", log_dir, end_cursor.to_str(), err);
      return err;
    }

    int test_file_appender(int64_t worker_id, const char* path, int64_t align, int64_t n_iter)
    {
      int err = OB_SUCCESS;
      bool dio = true;
      bool is_trunc = true;
      ObFileAppender file;
      char buf[1<<9];
      char cmd[1024];
      int cmd_ret = 0;
      int64_t len = 0;
      if (NULL == path)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (0 > snprintf(cmd, sizeof(cmd), "rm -rf %s", path))
      {
        err = OB_BUF_NOT_ENOUGH;
      }
      for (int64_t i = 0; OB_SUCCESS == err && (i < n_iter); )
      {
        len = random() % ARRAYSIZEOF(buf);
        if (0 == (i++ % 1000))
        {
          TBSYS_LOG(INFO, "file[%ld].append(i=%ld)", worker_id, i);
          if (0 != (cmd_ret = system(cmd)))
          {
            err = OB_ERROR;
            TBSYS_LOG(ERROR, "system(cmd=%s)=>%d", cmd, cmd_ret);
          }
          else if (OB_SUCCESS != (err = file.open(ObString((int32_t)strlen(path), (int32_t)strlen(path), (char*)path), dio, true, is_trunc, align)))
          {
            TBSYS_LOG(ERROR, "file.open(path=%s)=>%d", path, err);
          }
        }
        if (OB_SUCCESS != err)
        {}
        else if (OB_SUCCESS != (err = file.append(buf, len, true)))
        {
          TBSYS_LOG(ERROR, "file.append(buf=%p[%ld])=>%d", buf, len, err);
        }
        else if (0 == (i % 1000))
        {
          file.close();
        }
      }
      return err;
    }

    class ObReplayLogTest: public ::testing::Test, public tbsys::CDefaultRunnable, public Config {
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
        void run_as_writer(tbsys::CThread *thread, int64_t idx){
          UNUSED(thread);
          int64_t i = 0;
          for(i = 0; !_stop && i < max_num_items; i += random() % 10)
          {
            err = write_log_to(log_mgr, i);
          }
          TBSYS_LOG(INFO, "writer[%ld] write to log_id[%ld]", idx, i);
          if (OB_CANCELED == err)
          {
            err = OB_SUCCESS;
          }
          stop();
          ASSERT_EQ(OB_SUCCESS, err);
        }

        void run_as_reader(tbsys::CThread *thread, int64_t idx){
          UNUSED(thread);
          UNUSED(idx);
          ObLogCursor start_cursor;
          ObLogCursor end_cursor;
          set_cursor(start_cursor, 1, 0, 0);
          err = replay_log_func(_stop, log_dir, start_cursor, end_cursor);
          TBSYS_LOG(INFO, "reader[%ld] read to log_id[%ld]", idx, end_cursor.log_id_);
          if (OB_CANCELED == err)
          {
            err = OB_SUCCESS;
          }
          if (OB_SUCCESS != err)
          {
            stop();
          }
          ASSERT_EQ(OB_SUCCESS, err);
        }

        void run(tbsys::CThread *thread, void* arg){
          int64_t idx = (long)(arg);
          const char* dev_list[] = {"/data/1", "/data/2", "/data/3", "/data/4", "/data/5", "/data/6", "/data/7", "/data/8", "/data/9", "/data/10"};
          if (test_for_write_not_align)
          {
            TBSYS_LOG(INFO, "test_file_appender(%ld)", idx);
            err = test_file_appender(idx, mgt.sf("%s/%s.%ld", dev_list[idx%ARRAYSIZEOF(dev_list)], file_for_append, idx), align_size, max_num_items);
            ASSERT_EQ(OB_SUCCESS, err);
          }
          else if (idx > n_reader)
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

    TEST_F(ObReplayLogTest, Inspect){
      inspect(true);
    }
#if 0
    TEST_F(ObReplayLogTest, WriteNotAlign){
      test_for_write_not_align = true;
      int64_t tmp_n_reader = atoll(getenv("n_writer")?: "0");
      n_reader = tmp_n_reader?: n_reader;
      setThreadCount((int)n_reader+1);
      start();
      wait();
      ASSERT_EQ(OB_SUCCESS, err);
    }
#endif
    TEST_F(ObReplayLogTest, Replay){
      setThreadCount((int)n_reader+1);
      start();
      wait();
      ASSERT_EQ(OB_SUCCESS, err);
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
