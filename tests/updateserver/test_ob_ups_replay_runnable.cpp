/// 此单测需要将以下三个函数变成虚函数
///     ObUpdateServerMain的get_update_server
///     ObUpdateServer的get_table_mgr
///     UpsTableMgr的replay

#include <gtest/gtest.h>

#include "common/ob_log_writer.h"
#include "common/ob_packet_factory.h"
#include "updateserver/ob_ups_mutator.h"
#include "updateserver/ob_ups_table_mgr.h"
#include "updateserver/ob_ups_replay_runnable.h"
#include "updateserver/ob_update_server.h"
#include "updateserver/ob_update_server_param.h"
#include "updateserver/ob_update_server_main.h"

#include "tbsys.h"
#include "Cond.h"
#include "Lock.h"
#include "Mutex.h"

#include "../common/slave_mgr_4_test.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

namespace oceanbase
{
  namespace tests
  {
    namespace updateserver
    {
      class TestTableMgr : public ObUpsTableMgr
      {
      public:
        tbutil::Cond cond_;
        tbutil::Mutex mutex_;
        tbutil::LockT<tbutil::Mutex> lock_;

        bool is_error_;

        char log_data_[BUFSIZ];
        int64_t data_len_;

        int replay(ObUpsMutator& mutator)
        {
          data_len_ = 0;
          mutator.serialize(log_data_, BUFSIZ, data_len_);

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

        TestTableMgr() : lock_(mutex_)
        {
          is_error_ = false;
        }
      };
      class TestUpdateServer : public ObUpdateServer
      {
      public:
        TestUpdateServer() : ObUpdateServer(param) {}
        TestTableMgr test_table_;
        TestTableMgr& get_table_mgr()
        {
          return test_table_;
        }
        ObUpdateServerParam param;
      };
      class TestUpdateServerMain : public ObUpdateServerMain
      {
      public:
        TestUpdateServer test_server_;
        const TestUpdateServer& get_update_server() const
        {
          return test_server_;
        }

        TestUpdateServer& get_update_server()
        {
          return test_server_;
        }
      };

      class TestObUpsReplayRunnable: public ::testing::Test
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

      TEST_F(TestObUpsReplayRunnable, test_init)
      {
        int64_t log_file_max_size = 1L << 24;
        uint64_t log_file_max_id = 1100;
        uint64_t log_max_seq = 22000;

        char lf[BUFSIZ];
        int64_t lf_pos = 0;

        ObUpsMutator mutator;
        ASSERT_EQ(OB_SUCCESS, mutator.serialize(lf, BUFSIZ, lf_pos));

        SlaveMgr4Test sm4t;

        TestUpdateServerMain test_main;

        TestTableMgr &table_mgr = test_main.get_update_server().get_table_mgr();

        uint64_t new_log_file_id;
        ObLogWriter log_writer;
        ASSERT_EQ(OB_SUCCESS, log_writer.init(log_dir, log_file_max_size, sm4t.get_slave_mgr(), 0));
        ASSERT_EQ(OB_SUCCESS, log_writer.start_log(log_file_max_id, log_max_seq));
        ASSERT_EQ(OB_SUCCESS, log_writer.switch_log_file(new_log_file_id));
        ASSERT_EQ(log_file_max_id + 1, new_log_file_id);
        ASSERT_EQ(OB_SUCCESS, log_writer.switch_log_file(new_log_file_id));
        ASSERT_EQ(OB_SUCCESS, log_writer.write_log(OB_LOG_UPS_MUTATOR, lf, lf_pos));
        ASSERT_EQ(OB_SUCCESS, log_writer.flush_log());
        ASSERT_EQ(OB_SUCCESS, log_writer.switch_log_file(new_log_file_id));
        ASSERT_EQ(OB_SUCCESS, log_writer.write_log(OB_LOG_UPS_MUTATOR, lf, lf_pos));
        ASSERT_EQ(OB_SUCCESS, log_writer.flush_log());
        ASSERT_EQ(OB_SUCCESS, log_writer.switch_log_file(new_log_file_id));

        ObRoleMgr role_mgr;
        role_mgr.set_role(ObRoleMgr::SLAVE);

        ObUpsReplayRunnable replay_thread0;

        ASSERT_EQ(OB_SUCCESS, replay_thread0.init(log_dir, log_file_max_id, &role_mgr, true));

        replay_thread0.start();

        usleep(1000);
        ASSERT_EQ(lf_pos, table_mgr.data_len_);
        ASSERT_EQ(0, memcmp(table_mgr.log_data_, lf, lf_pos));
        table_mgr.cond_.signal();

        usleep(1000);
        ASSERT_EQ(lf_pos, table_mgr.data_len_);
        ASSERT_EQ(0, memcmp(table_mgr.log_data_, lf, lf_pos));
        table_mgr.cond_.signal();

        usleep(1000);
        replay_thread0.stop();
        replay_thread0.wait();


        // test replay error
        ObUpsReplayRunnable replay_thread1;
        //replay_thread1.ups_main_ = &test_main;

        ASSERT_EQ(OB_SUCCESS, replay_thread1.init(log_dir, log_file_max_id, &role_mgr, true));

        replay_thread1.start();

        usleep(1000);

        ASSERT_EQ(lf_pos, table_mgr.data_len_);
        ASSERT_EQ(0, memcmp(table_mgr.log_data_, lf, lf_pos));
        table_mgr.is_error_ = true;
        table_mgr.cond_.signal();

        usleep(1000);
        ASSERT_EQ(lf_pos, table_mgr.data_len_);
        ASSERT_EQ(0, memcmp(table_mgr.log_data_, lf, lf_pos));
        table_mgr.cond_.signal();

        replay_thread1.wait();



        table_mgr.is_error_ = false;

        // test open log file error
        ObLogWriter *log_writer2 = new ObLogWriter();
        ASSERT_EQ(OB_SUCCESS, log_writer2->init(log_dir, log_file_max_size, sm4t.get_slave_mgr(), 0));
        ASSERT_EQ(OB_SUCCESS, log_writer2->start_log(log_file_max_id - 10, log_max_seq));
        ASSERT_EQ(OB_SUCCESS, log_writer2->write_log(OB_LOG_UPS_MUTATOR, lf, lf_pos));
        ASSERT_EQ(OB_SUCCESS, log_writer2->flush_log());
        ASSERT_EQ(OB_SUCCESS, log_writer2->switch_log_file(new_log_file_id));
        delete log_writer2;

        char cmd[100];
        snprintf(cmd, 100, "rm %s/%lu", log_dir, log_file_max_id - 9);
        system(cmd);

        ObUpsReplayRunnable replay_thread2;
        //replay_thread2.ups_main_ = &test_main;

        ASSERT_EQ(OB_SUCCESS, replay_thread2.init(log_dir, log_file_max_id - 10, &role_mgr, true));

        replay_thread2.start();

        usleep(1000);
        ASSERT_EQ(lf_pos, table_mgr.data_len_);
        ASSERT_EQ(0, memcmp(table_mgr.log_data_, lf, lf_pos));
        table_mgr.cond_.signal();

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
