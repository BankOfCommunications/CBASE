#include <gtest/gtest.h>

#include "common/file_directory_utils.h"
#include "common/ob_log_writer.h"
#include "updateserver/ob_ups_replay_runnable.h"
#include "updateserver/ob_ups_log_mgr.h"

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
      class TestObUpsLogMgr: public ::testing::Test
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

        int erase_file(int id) 
        {
          char cmd[BUFSIZ];
          snprintf(cmd, BUFSIZ, "rm -f %s/%d", log_dir, id);
          return system(cmd);
        }

        bool exists(const char* n)
        {
          char fn[BUFSIZ];
          snprintf(fn, BUFSIZ, "%s/%s", log_dir, n);
          return FileDirectoryUtils::exists(fn);
        }

        char log_dir[BUFSIZ];
      };

      TEST_F(TestObUpsLogMgr, test_init)
      {
        int64_t log_file_max_size = 1L << 28;
        ObRoleMgr role_mgr;

        SlaveMgr4Test sm4t;

        ObUpsLogMgr log_mgr1;
        ASSERT_EQ(OB_SUCCESS, log_mgr1.init(log_dir, log_file_max_size, sm4t.get_slave_mgr(), &role_mgr, 0));
        ASSERT_EQ(1U, log_mgr1.get_replay_point());

        erase_file(1);

        uint64_t log_file_max_id = 1100;
        uint64_t log_max_seq = 22000;

        char lf[BUFSIZ];
        int64_t lf_pos = 0;

        ObUpsMutator mutator;
        ASSERT_EQ(OB_SUCCESS, mutator.serialize(lf, BUFSIZ, lf_pos));

        uint64_t new_log_file_id;
        ObLogWriter log_writer;
        ASSERT_EQ(OB_SUCCESS, log_writer.init(log_dir, log_file_max_size, sm4t.get_slave_mgr(), 0));
        ASSERT_EQ(OB_SUCCESS, log_writer.start_log(log_file_max_id, log_max_seq));
        ASSERT_EQ(OB_SUCCESS, log_writer.switch_log_file(new_log_file_id));
        ASSERT_EQ(OB_SUCCESS, log_writer.switch_log_file(new_log_file_id));
        ASSERT_EQ(OB_SUCCESS, log_writer.write_log(OB_LOG_UPS_MUTATOR, lf, lf_pos));
        ASSERT_EQ(OB_SUCCESS, log_writer.flush_log());
        ASSERT_EQ(OB_SUCCESS, log_writer.switch_log_file(new_log_file_id));
        ASSERT_EQ(OB_SUCCESS, log_writer.write_log(OB_LOG_UPS_MUTATOR, lf, lf_pos));
        ASSERT_EQ(OB_SUCCESS, log_writer.flush_log());
        ASSERT_EQ(OB_SUCCESS, log_writer.switch_log_file(new_log_file_id));

        ASSERT_EQ(false, exists(ObUpsLogMgr::UPS_LOG_REPLAY_POINT_FILE));

        ObUpsLogMgr log_mgr2;
        ASSERT_EQ(OB_SUCCESS, log_mgr2.init(log_dir, log_file_max_size, sm4t.get_slave_mgr(), &role_mgr, 0));

        ASSERT_EQ(log_file_max_id, log_mgr2.get_replay_point());

        log_mgr2.set_replay_point(100);

        ASSERT_EQ(100U, log_mgr2.get_replay_point());

        log_mgr2.set_replay_point(log_file_max_id + 1);
        ASSERT_EQ(log_file_max_id + 1, log_mgr2.get_replay_point());

        ObUpsTableMgr table_mgr;
        ASSERT_EQ(OB_SUCCESS, table_mgr.init());

        ASSERT_EQ(0U, log_mgr2.get_cur_log_seq());

        ASSERT_EQ(OB_SUCCESS, log_mgr2.replay_log(table_mgr));

        ASSERT_NE(0U, log_mgr2.get_cur_log_seq());

        ASSERT_EQ(OB_SUCCESS, log_mgr2.write_log(OB_LOG_UPS_MUTATOR, lf, lf_pos));
        ASSERT_EQ(OB_SUCCESS, log_mgr2.flush_log());
        ASSERT_EQ(OB_SUCCESS, log_mgr2.switch_log_file(new_log_file_id));
        log_mgr2.set_replay_point(new_log_file_id);
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
