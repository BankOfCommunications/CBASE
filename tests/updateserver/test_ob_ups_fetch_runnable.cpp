// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1
// Unit Test Steps
// Build environment at a host with more than 6 disks
// assuming path to disks is /data/X
// run command followd
// mkdir -p /data/1/test_ups_fetch/
// mkdir -p /data/2/test_ups_fetch/
// mkdir -p /data/3/test_ups_fetch/
// mkdir -p /data/4/test_ups_fetch/
// mkdir -p /data/5/test_ups_fetch/
// mkdir -p /data/6/test_ups_fetch/
// mkdir -p raid1
// mkdir -p raid2
// ln -s /data/1/test_ups_fetch raid1/store1
// ln -s /data/2/test_ups_fetch raid1/store2
// ln -s /data/3/test_ups_fetch raid1/store3
// ln -s /data/4/test_ups_fetch raid2/store1
// ln -s /data/5/test_ups_fetch raid2/store2
// ln -s /data/6/test_ups_fetch raid2/store3
//
// build trust relationship with another host with same user
// rm -r ~/tmp1234
// mkdir ~/tmp1234
// mkdir -p ~/tmp1234/raid1/store1
// mkdir -p ~/tmp1234/raid1/store2
// mkdir -p ~/tmp1234/raid2/store1
// touch ~/tmp1234/raid1/store1/ss1
// touch ~/tmp1234/raid1/store2/ss2
// touch ~/tmp1234/raid2/store1/ss3

#include <gtest/gtest.h>

#include "updateserver/ob_ups_fetch_runnable.h"
#include "updateserver/ob_sstable_mgr.h"
#include "common/ob_malloc.h"
#include "common/file_directory_utils.h"
#include "common/ob_log_replay_runnable.h"

#include "tbsys.h"
#include <string>

using namespace oceanbase::common;
using namespace oceanbase::updateserver;
using namespace std;

namespace oceanbase
{
  namespace tests
  {
    namespace updateserver
    {
      class TestObUpsFetchRunnable: public ::testing::Test
      {
      public:
        virtual void SetUp()
        {
          host_ip = "10.232.36.31";
          host_port = 12222;
          master1.set_ipv4_addr(host_ip.c_str(), host_port);

          log_dir = ".";
        }

        virtual void TearDown()
        {
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

      TEST_F(TestObUpsFetchRunnable, test_gen_fetch_sstable_cmd_)
      {
        SSTableMgr sstable_mgr;
        ASSERT_EQ(sstable_mgr.init(".", "^raid[0-9]$", "^store[0-9]"), OB_SUCCESS);
        sstable_mgr.load_new();

        TestReplayRunnable replay_thread;
        ObRoleMgr role_mgr;
        ObUpsFetchRunnable fetch_log;
        ObUpsFetchParam fetch_param;
        fetch_param.min_log_id_ = 0;
        fetch_param.max_log_id_ = 0;
        fetch_param.ckpt_id_ = 0;
        fetch_param.fetch_log_ = false;
        fetch_param.fetch_ckpt_ = false;
        fetch_param.fetch_sstable_ = true;

        ASSERT_EQ(OB_SUCCESS, fetch_log.init(master1, log_dir.c_str(), fetch_param, &role_mgr, &replay_thread, &sstable_mgr));
        char cwd[BUFSIZ];
        ASSERT_EQ(cwd, getcwd(cwd, BUFSIZ));
        char gencmd[BUFSIZ];
        char cmd[BUFSIZ];
        snprintf(cmd, BUFSIZ, "rsync -e \"ssh -oStrictHostKeyChecking=no\" -avz --inplace --bwlimit=16384 %s:%s/%s %s/", host_ip.c_str(), "src_path", "10_1-3_1.sst", "dst_path");
        ASSERT_EQ(OB_SUCCESS, fetch_log.gen_fetch_sstable_cmd_("10_1-3_1.sst", "src_path", "dst_path", gencmd, BUFSIZ));
        ASSERT_STREQ(cmd, gencmd);
      }

      TEST_F(TestObUpsFetchRunnable, test_init)
      {
        SSTableMgr sstable_mgr;
        ASSERT_EQ(sstable_mgr.init(".", "^raid[0-9]$", "^store[0-9]"), OB_SUCCESS);
        sstable_mgr.load_new();

        string base_path = "/home/yanran.hfs/tmp1234";
        ObUpsFetchParam fetch_param;
        SSTFileInfo sst_fi;
        string sst_path;
        string sst_name;

        sst_path = base_path + "/raid1/store1";
        sst_name = "ss1";
        sst_fi.path = ObString(sst_path.length() + 1, sst_path.length() + 1, (char*)sst_path.c_str());
        sst_fi.name = ObString(sst_name.length() + 1, sst_name.length() + 1, (char*)sst_name.c_str());
        fetch_param.add_sst_file_info(sst_fi);

        sst_path = base_path + "/raid1/store2";
        sst_name = "ss2";
        sst_fi.path = ObString(sst_path.length() + 1, sst_path.length() + 1, (char*)sst_path.c_str());
        sst_fi.name = ObString(sst_name.length() + 1, sst_name.length() + 1, (char*)sst_name.c_str());
        fetch_param.add_sst_file_info(sst_fi);

        sst_path = base_path + "/raid2/store1";
        sst_name = "ss3";
        sst_fi.path = ObString(sst_path.length() + 1, sst_path.length() + 1, (char*)sst_path.c_str());
        sst_fi.name = ObString(sst_name.length() + 1, sst_name.length() + 1, (char*)sst_name.c_str());
        fetch_param.add_sst_file_info(sst_fi);

        fetch_param.fetch_sstable_ = true;

        SSTList::iterator i = fetch_param.sst_list_.begin();
        ASSERT_STREQ(i->path.ptr(), "/home/yanran.hfs/tmp1234/raid1/store1");
        ASSERT_STREQ(i->name.ptr(), "ss1");
        ++i;
        ASSERT_STREQ(i->path.ptr(), "/home/yanran.hfs/tmp1234/raid1/store2");
        ASSERT_STREQ(i->name.ptr(), "ss2");
        ++i;
        ASSERT_STREQ(i->path.ptr(), "/home/yanran.hfs/tmp1234/raid2/store1");
        ASSERT_STREQ(i->name.ptr(), "ss3");
        ++i;
        ASSERT_EQ(i, fetch_param.sst_list_.end());

        ObUpsFetchParam test_param;
        ASSERT_EQ(test_param.clone(fetch_param), OB_SUCCESS);
        ASSERT_EQ(true, test_param.fetch_sstable_);
        i = test_param.sst_list_.begin();
        ASSERT_STREQ(i->path.ptr(), "/home/yanran.hfs/tmp1234/raid1/store1");
        ASSERT_STREQ(i->name.ptr(), "ss1");
        ++i;
        ASSERT_STREQ(i->path.ptr(), "/home/yanran.hfs/tmp1234/raid1/store2");
        ASSERT_STREQ(i->name.ptr(), "ss2");
        ++i;
        ASSERT_STREQ(i->path.ptr(), "/home/yanran.hfs/tmp1234/raid2/store1");
        ASSERT_STREQ(i->name.ptr(), "ss3");
        ++i;
        ASSERT_EQ(i, test_param.sst_list_.end());

        char buf[BUFSIZ];
        int64_t pos = 0;
        ASSERT_EQ(test_param.serialize(buf, BUFSIZ, pos), OB_SUCCESS);

        ObUpsFetchParam test_param2;
        int64_t pos2 = 0;
        ASSERT_EQ(test_param2.deserialize(buf, pos, pos2), OB_SUCCESS);
        ASSERT_EQ(false, test_param2.fetch_log_);
        ASSERT_EQ(false, test_param2.fetch_ckpt_);
        ASSERT_EQ(true, test_param2.fetch_sstable_);
        i = test_param2.sst_list_.begin();
        ASSERT_STREQ(i->path.ptr(), "/home/yanran.hfs/tmp1234/raid1/store1");
        ASSERT_STREQ(i->name.ptr(), "ss1");
        ++i;
        ASSERT_STREQ(i->path.ptr(), "/home/yanran.hfs/tmp1234/raid1/store2");
        ASSERT_STREQ(i->name.ptr(), "ss2");
        ++i;
        ASSERT_STREQ(i->path.ptr(), "/home/yanran.hfs/tmp1234/raid2/store1");
        ASSERT_STREQ(i->name.ptr(), "ss3");
        ++i;
        ASSERT_EQ(i, test_param2.sst_list_.end());

        TestReplayRunnable replay_thread;
        ObRoleMgr role_mgr;
        ObUpsFetchRunnable fetch_sst;

        ASSERT_EQ(OB_SUCCESS, fetch_sst.init(master1, log_dir.c_str(), fetch_param, &role_mgr, &replay_thread, &sstable_mgr));

        fetch_sst.start();
        fetch_sst.wait();

        ASSERT_EQ(role_mgr.get_state(), ObRoleMgr::INIT);
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
