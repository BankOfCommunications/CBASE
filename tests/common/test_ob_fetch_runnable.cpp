// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1
// Unit Test Steps
// build trust relationship with 10.232.35.41
// at 10.232.35.41
// rm -r ~/code/oceanbase/tests/common/tmp1234
// mkdir ~/code/oceanbase/tests/common/tmp1234
// touch ~/code/oceanbase/tests/common/tmp1234/1001
// touch ~/code/oceanbase/tests/common/tmp1234/1002
// touch ~/code/oceanbase/tests/common/tmp1234/1003
// touch ~/code/oceanbase/tests/common/tmp1234/1004
// touch ~/code/oceanbase/tests/common/tmp1234/1005
// touch ~/code/oceanbase/tests/common/tmp1234/1006
// touch ~/code/oceanbase/tests/common/tmp1234/1007
// touch ~/code/oceanbase/tests/common/tmp1234/1008
// touch ~/code/oceanbase/tests/common/tmp1234/1009
// touch ~/code/oceanbase/tests/common/tmp1234/1010
// touch ~/code/oceanbase/tests/common/tmp1234/1001.checkpoint
// touch ~/code/oceanbase/tests/common/tmp1234/99998888.checkpoint
// touch ~/code/oceanbase/tests/common/tmp1234/99998888.schema
// touch ~/code/oceanbase/tests/common/tmp1234/99998888.memtable
// touch ~/code/oceanbase/tests/common/tmp1234/9999.checkpoint
// touch ~/code/oceanbase/tests/common/tmp1234/9999.schema
// touch ~/code/oceanbase/tests/common/tmp1234/9999.memtable
// touch ~/code/oceanbase/tests/common/tmp1234/0

#include <gtest/gtest.h>

#include "ob_fetch_runnable.h"
#include "ob_malloc.h"
#include "file_directory_utils.h"
#include "ob_log_replay_runnable.h"

#include "tbsys.h"
#include <string>

using namespace oceanbase::common;
using namespace std;

namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      class TestObFetchRunnable: public ::testing::Test
      {
      public:
        virtual void SetUp()
        {
          log_dir = "tmp1234";

          char cmd[BUFSIZ];
          snprintf(cmd, BUFSIZ, "mkdir %s", log_dir.c_str());
          system(cmd);

          host_ip = "10.232.35.41";
          host_port = 12222;
          master1.set_ipv4_addr(host_ip.c_str(), host_port);
        }

        virtual void TearDown()
        {

          char cmd[BUFSIZ];
          snprintf(cmd, BUFSIZ, "rm -r %s", log_dir.c_str());
          system(cmd);
        }

        int create_file(uint64_t id)
        {
          char cmd[BUFSIZ];
          snprintf(cmd, BUFSIZ, "touch %s/%lu", log_dir.c_str(), id);
          return system(cmd);
        }
        int create_file(char* file)
        {
          char cmd[BUFSIZ];
          snprintf(cmd, BUFSIZ, "touch %s/%s", log_dir.c_str(), file);
          return system(cmd);
        }

        int erase_file(uint64_t id)
        {
          char cmd[BUFSIZ];
          snprintf(cmd, BUFSIZ, "rm -f %s/%lu", log_dir.c_str(), id);
          return system(cmd);
        }

        bool exists(uint64_t id)
        {
          char fn[BUFSIZ];
          snprintf(fn, BUFSIZ, "%s/%lu", log_dir.c_str(), id);
          return FileDirectoryUtils::exists(fn);
        }

        bool exists(uint64_t id, const char* ext)
        {
          char fn[BUFSIZ];
          snprintf(fn, BUFSIZ, "%s/%lu.%s", log_dir.c_str(), id, ext);
          return FileDirectoryUtils::exists(fn);
        }

        string log_dir;
        string host_ip;
        int host_port;
        ObServer master1;

        class TestReplayRunnable : public ObLogReplayRunnable
        {
          virtual int replay(LogCommand cmd, uint64_t seq, const char* log_data, const int64_t data_len) {return 0;}
        };
      };

      TEST_F(TestObFetchRunnable, test_fill_fetch_cmd_)
      {
        TestReplayRunnable replay_thread;
        ObRoleMgr role_mgr;
        ObFetchRunnable fetch_log;
        ObFetchParam fetch_param;
        fetch_param.min_log_id_ = 0;
        fetch_param.max_log_id_ = 0;
        fetch_param.ckpt_id_ = 0;
        fetch_param.fetch_log_ = false;
        fetch_param.fetch_ckpt_ = false;

        ASSERT_EQ(OB_SUCCESS, fetch_log.init(master1, log_dir.c_str(), fetch_param, &role_mgr, &replay_thread));
        uint64_t id1 = 1001;
        char cwd[BUFSIZ];
        ASSERT_EQ(cwd, getcwd(cwd, BUFSIZ));
        char gencmd[BUFSIZ];
        char cmd[BUFSIZ];
        snprintf(cmd, BUFSIZ, "rsync -e \"ssh -oStrictHostKeyChecking=no\" -avz --inplace --bwlimit=16384 %s:%s/%s/%lu %s/", host_ip.c_str(), cwd, log_dir.c_str(), id1, log_dir.c_str());
        ASSERT_EQ(OB_SUCCESS, fetch_log.gen_fetch_cmd_(1001, NULL, gencmd, BUFSIZ));
        ASSERT_STREQ(cmd, gencmd);

        create_file(id1);
        ASSERT_EQ(0, system(gencmd));

        snprintf(cmd, BUFSIZ, "rsync -e \"ssh -oStrictHostKeyChecking=no\" -avz --inplace --bwlimit=16384 %s:%s/%s/%lu.checkpoint %s/", host_ip.c_str(), cwd, log_dir.c_str(), id1, log_dir.c_str());
        ASSERT_EQ(OB_SUCCESS, fetch_log.gen_fetch_cmd_(1001, "checkpoint", gencmd, BUFSIZ));
        ASSERT_STREQ(cmd, gencmd);
        ASSERT_EQ(0, system(gencmd));
      }

      TEST_F(TestObFetchRunnable, test_init)
      {
        TestReplayRunnable replay_thread;
        ObRoleMgr role_mgr;
        ObFetchRunnable fetch_log1;

        ObFetchParam fetch_param;
        fetch_param.min_log_id_ = 0;
        fetch_param.max_log_id_ = 0;
        fetch_param.ckpt_id_ = 0;
        fetch_param.fetch_log_ = false;
        fetch_param.fetch_ckpt_ = false;

        ASSERT_EQ(OB_SUCCESS, fetch_log1.init(master1, log_dir.c_str(), fetch_param, &role_mgr, &replay_thread));
        fetch_log1.start();
        fetch_log1.wait();
      }

      TEST_F(TestObFetchRunnable, test_init2)
      {
        TestReplayRunnable replay_thread;
        ObRoleMgr role_mgr;
        ObFetchRunnable fetch_log1;

        ObFetchParam fetch_param;
        fetch_param.min_log_id_ = 1001;
        fetch_param.max_log_id_ = 1010;
        fetch_param.ckpt_id_ = 0;
        fetch_param.fetch_log_ = true;
        fetch_param.fetch_ckpt_ = false;

        ASSERT_EQ(OB_SUCCESS, fetch_log1.init(master1, log_dir.c_str(), fetch_param, &role_mgr, &replay_thread));

        for (uint64_t i = fetch_param.min_log_id_; i <= fetch_param.max_log_id_; i++)
        {
          ASSERT_EQ(false, exists(i));
          ASSERT_EQ(false, exists(i, "checkpoint"));
        }

        fetch_log1.start();
        fetch_log1.wait();

        for (uint64_t i = fetch_param.min_log_id_; i <= fetch_param.max_log_id_; i++)
        {
          ASSERT_EQ(true, exists(i));
          ASSERT_EQ(false, exists(i, "checkpoint"));
        }
      }

      TEST_F(TestObFetchRunnable, test_init3)
      {
        TestReplayRunnable replay_thread;
        ObRoleMgr role_mgr;
        ObFetchRunnable fetch_log1;

        ObFetchParam fetch_param;
        fetch_param.min_log_id_ = 0;
        fetch_param.max_log_id_ = 0;
        fetch_param.ckpt_id_ = 1001;
        fetch_param.fetch_log_ = false;
        fetch_param.fetch_ckpt_ = true;

        ASSERT_EQ(OB_SUCCESS, fetch_log1.init(master1, log_dir.c_str(), fetch_param, &role_mgr, &replay_thread));

        ASSERT_EQ(false, exists(fetch_param.ckpt_id_, "checkpoint"));
        ASSERT_EQ(false, exists(fetch_param.ckpt_id_, "schema"));

        fetch_log1.start();
        fetch_log1.wait();

        ASSERT_EQ(true, exists(fetch_param.ckpt_id_, "checkpoint"));
        ASSERT_EQ(false, exists(fetch_param.ckpt_id_, "schema"));
      }

      TEST_F(TestObFetchRunnable, test_init4)
      {
        TestReplayRunnable replay_thread;
        ObRoleMgr role_mgr;
        ObFetchRunnable fetch_log1;

        ObFetchParam fetch_param;
        fetch_param.min_log_id_ = 0;
        fetch_param.max_log_id_ = 0;
        fetch_param.ckpt_id_ = 99998888;
        fetch_param.fetch_log_ = false;
        fetch_param.fetch_ckpt_ = true;

        ASSERT_EQ(OB_SUCCESS, fetch_log1.init(master1, log_dir.c_str(), fetch_param, &role_mgr, &replay_thread));
        ASSERT_EQ(OB_SUCCESS, fetch_log1.add_ckpt_ext("schema"));
        ASSERT_EQ(OB_ENTRY_EXIST, fetch_log1.add_ckpt_ext("schema"));
        ASSERT_EQ(OB_SUCCESS, fetch_log1.add_ckpt_ext("memtable"));

        ASSERT_EQ(false, exists(fetch_param.ckpt_id_, "checkpoint"));
        ASSERT_EQ(false, exists(fetch_param.ckpt_id_, "schema"));
        ASSERT_EQ(false, exists(fetch_param.ckpt_id_, "memtable"));

        ASSERT_EQ(ObRoleMgr::INIT, role_mgr.get_state());

        fetch_log1.start();
        fetch_log1.wait();

        ASSERT_EQ(ObRoleMgr::INIT, role_mgr.get_state());

        ASSERT_EQ(true, exists(fetch_param.ckpt_id_, "checkpoint"));
        ASSERT_EQ(true, exists(fetch_param.ckpt_id_, "schema"));
        ASSERT_EQ(true, exists(fetch_param.ckpt_id_, "memtable"));
      }

      TEST_F(TestObFetchRunnable, test_init5)
      {
        class TestFetch : public ObFetchRunnable
        {
          public:
            int got_ckpt(uint64_t ckpt_id)
            {
              ckpt_id_ = ckpt_id;
              return OB_SUCCESS;
            }

            int got_log(uint64_t log_id)
            {
              log_id_ = log_id;
              return OB_SUCCESS;
            }

            uint64_t ckpt_id_;
            uint64_t log_id_;
        };

        TestReplayRunnable replay_thread;
        ObRoleMgr role_mgr;
        TestFetch fetch_log1;

        ObFetchParam fetch_param;
        fetch_param.min_log_id_ = 0;
        fetch_param.max_log_id_ = 1;
        fetch_param.ckpt_id_ = 9999;
        fetch_param.fetch_log_ = true;
        fetch_param.fetch_ckpt_ = true;

        ASSERT_EQ(OB_SUCCESS, fetch_log1.init(master1, log_dir.c_str(), fetch_param, &role_mgr, &replay_thread));
        ASSERT_EQ(OB_SUCCESS, fetch_log1.add_ckpt_ext("schema"));
        ASSERT_EQ(OB_SUCCESS, fetch_log1.add_ckpt_ext("memtable"));

        ASSERT_EQ(false, exists(0));
        ASSERT_EQ(false, exists(1));
        ASSERT_EQ(false, exists(fetch_param.ckpt_id_, "checkpoint"));
        ASSERT_EQ(false, exists(fetch_param.ckpt_id_, "schema"));
        ASSERT_EQ(false, exists(fetch_param.ckpt_id_, "memtable"));

        ASSERT_EQ(ObRoleMgr::INIT, role_mgr.get_state());

        fetch_log1.start();
        fetch_log1.wait();

        ASSERT_EQ(ObRoleMgr::ERROR, role_mgr.get_state());

        ASSERT_EQ(fetch_log1.ckpt_id_, 9999U);
        ASSERT_EQ(fetch_log1.log_id_, 0U);

        ASSERT_EQ(true, exists(fetch_param.ckpt_id_, "checkpoint"));
        ASSERT_EQ(true, exists(fetch_param.ckpt_id_, "schema"));
        ASSERT_EQ(true, exists(fetch_param.ckpt_id_, "memtable"));
        ASSERT_EQ(true, exists(0));
        ASSERT_EQ(false, exists(1));
      }

      TEST_F(TestObFetchRunnable, test_init6)
      {
        class TestFetch : public ObFetchRunnable
        {
          public:
            int got_ckpt(uint64_t ckpt_id)
            {
              ckpt_id_ = ckpt_id;
              return OB_SUCCESS;
            }

            int got_log(uint64_t log_id)
            {
              log_id_ = log_id;
              return OB_ERROR;
            }

            uint64_t ckpt_id_;
            uint64_t log_id_;
        };

        TestReplayRunnable replay_thread;
        ObRoleMgr role_mgr;
        TestFetch fetch_log1;

        ObFetchParam fetch_param;
        fetch_param.min_log_id_ = 0;
        fetch_param.max_log_id_ = 0;
        fetch_param.ckpt_id_ = 9999;
        fetch_param.fetch_log_ = true;
        fetch_param.fetch_ckpt_ = true;

        ASSERT_EQ(OB_SUCCESS, fetch_log1.init(master1, log_dir.c_str(), fetch_param, &role_mgr, &replay_thread));
        ASSERT_EQ(OB_SUCCESS, fetch_log1.add_ckpt_ext("schema"));
        ASSERT_EQ(OB_SUCCESS, fetch_log1.add_ckpt_ext("memtable"));

        ASSERT_EQ(false, exists(0));
        ASSERT_EQ(false, exists(fetch_param.ckpt_id_, "checkpoint"));
        ASSERT_EQ(false, exists(fetch_param.ckpt_id_, "schema"));
        ASSERT_EQ(false, exists(fetch_param.ckpt_id_, "memtable"));

        ASSERT_EQ(ObRoleMgr::INIT, role_mgr.get_state());

        fetch_log1.start();
        fetch_log1.wait();

        ASSERT_EQ(ObRoleMgr::ERROR, role_mgr.get_state());

        ASSERT_EQ(fetch_log1.ckpt_id_, 9999U);
        ASSERT_EQ(fetch_log1.log_id_, 0U);

        ASSERT_EQ(true, exists(fetch_param.ckpt_id_, "checkpoint"));
        ASSERT_EQ(true, exists(fetch_param.ckpt_id_, "schema"));
        ASSERT_EQ(true, exists(fetch_param.ckpt_id_, "memtable"));
        ASSERT_EQ(true, exists(0));
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
