
#include <gtest/gtest.h>

#include "ob_log_replay_runnable.h"
#include "ob_log_writer.h"

#include "tbsys.h"
#include "Cond.h"
#include "Lock.h"
#include "Mutex.h"

#include "slave_mgr_4_test.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      class TestReplayThread : public ObLogReplayRunnable
      {
      public:
        const char* log_data_;
        int64_t data_len_;
        bool is_error_;

        tbutil::Cond cond_;
        tbutil::Mutex mutex_;
        tbutil::LockT<tbutil::Mutex> lock_;

        TestReplayThread() : lock_(mutex_)
        {
          log_data_ = NULL;
          data_len_ = 0;
          is_error_ = false;
        }
      protected:
        virtual int replay(LogCommand cmd, const uint64_t seq, const char* log_data, const int64_t data_len)
        {
          UNUSED(cmd);
          UNUSED(seq);

          log_data_ = log_data;
          data_len_ = data_len;
          cond_.wait(lock_);
          if (is_error_)
          {
            return OB_ERROR;
          }
          else
          {
            return OB_SUCCESS;
          }
        }
      };

      class TestObLogReplayRunnable: public ::testing::Test
      {
      public:
        virtual void SetUp()
        {
          strcpy(log_dir, "tmp1234");

          char cmd[100];
          snprintf(cmd, 100, "mkdir %s", log_dir);
          system(cmd);
        }

        virtual void TearDown()
        {
          char cmd[100];
          snprintf(cmd, 100, "rm -r %s", log_dir);
          system(cmd);
        }

        char log_dir[BUFSIZ];
      };

      TEST_F(TestObLogReplayRunnable, test_init)
      {
        int64_t log_file_max_size = 1L << 24;
        uint64_t log_file_max_id = 1100;
        uint64_t log_max_seq = 22000;

        char lf[BUFSIZ];
        memset(lf, 'A', BUFSIZ);
        lf[BUFSIZ - 1] = '\0';

        SlaveMgr4Test sm4t;

        uint64_t new_log_file_id;
        ObLogWriter log_writer;
        EXPECT_EQ(OB_SUCCESS, log_writer.init(log_dir, log_file_max_size, sm4t.get_slave_mgr(), 0));
        EXPECT_EQ(OB_SUCCESS, log_writer.start_log(log_file_max_id, log_max_seq));
        EXPECT_EQ(OB_SUCCESS, log_writer.switch_log_file(new_log_file_id));
        EXPECT_EQ(OB_SUCCESS, log_writer.switch_log_file(new_log_file_id));
        EXPECT_EQ(OB_SUCCESS, log_writer.write_log(OB_LOG_UPS_MUTATOR, lf, 100));
        EXPECT_EQ(OB_SUCCESS, log_writer.flush_log());
        EXPECT_EQ(OB_SUCCESS, log_writer.switch_log_file(new_log_file_id));
        EXPECT_EQ(OB_SUCCESS, log_writer.write_log(OB_LOG_UPS_MUTATOR, lf, 100));
        EXPECT_EQ(OB_SUCCESS, log_writer.flush_log());
        EXPECT_EQ(OB_SUCCESS, log_writer.switch_log_file(new_log_file_id));

        ObRoleMgr role_mgr;

        TestReplayThread replay_thread0;

        EXPECT_EQ(OB_SUCCESS, replay_thread0.init(log_dir, log_file_max_id, &role_mgr, 1000000));

        replay_thread0.start();

        usleep(1000);
        EXPECT_EQ(100, replay_thread0.data_len_);
        EXPECT_EQ(0, memcmp(replay_thread0.log_data_, lf, 100));
        replay_thread0.cond_.signal();

        usleep(1000);
        EXPECT_EQ(100, replay_thread0.data_len_);
        EXPECT_EQ(0, memcmp(replay_thread0.log_data_, lf, 100));
        replay_thread0.cond_.signal();

        usleep(1000);
        replay_thread0.stop();
        replay_thread0.wait();


        // test replay error
        TestReplayThread replay_thread1;

        EXPECT_EQ(OB_SUCCESS, replay_thread1.init(log_dir, log_file_max_id, &role_mgr, 1000000));

        replay_thread1.start();

        usleep(1000);
        EXPECT_EQ(100, replay_thread1.data_len_);
        EXPECT_EQ(0, memcmp(replay_thread1.log_data_, lf, 100));

        replay_thread1.is_error_ = true;
        replay_thread1.cond_.signal();

        usleep(1000);
        EXPECT_EQ(100, replay_thread1.data_len_);
        EXPECT_EQ(0, memcmp(replay_thread1.log_data_, lf, 100));
        replay_thread1.cond_.signal();

        replay_thread1.wait();



        // test open log file error
        ObLogWriter *log_writer2 = new ObLogWriter();
        EXPECT_EQ(OB_SUCCESS, log_writer2->init(log_dir, log_file_max_size, sm4t.get_slave_mgr(), 0));
        EXPECT_EQ(OB_SUCCESS, log_writer2->start_log(log_file_max_id - 10, log_max_seq));
        EXPECT_EQ(OB_SUCCESS, log_writer2->write_log(OB_LOG_UPS_MUTATOR, lf, 100));
        EXPECT_EQ(OB_SUCCESS, log_writer2->flush_log());
        EXPECT_EQ(OB_SUCCESS, log_writer2->switch_log_file(new_log_file_id));
        delete log_writer2;

        char cmd[100];
        snprintf(cmd, 100, "rm %s/%lu", log_dir, log_file_max_id - 8);
        system(cmd);

        TestReplayThread replay_thread2;
        EXPECT_EQ(OB_SUCCESS, replay_thread2.init(log_dir, log_file_max_id - 10, &role_mgr, 1000000));

        replay_thread2.start();

        usleep(1000);
        EXPECT_EQ(100, replay_thread2.data_len_);
        replay_thread2.cond_.signal();

        replay_thread2.wait();
      }
    }
  }
}

int main(int argc, char** argv)
{
  TBSYS_LOGGER.setLogLevel("DEBUG");
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
