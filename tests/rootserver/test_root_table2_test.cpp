/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 * First Create_time: 2011-8-5
 *
 * Authors:
 *   rongxuan <rongxuan.lc@taobao.com>
 *     - some work details if you want
 */

#include <gtest/gtest.h>
#include <unistd.h>
#include "common/ob_malloc.h"
#include "common/ob_vector.h"
#include "rootserver/ob_tablet_info_manager.h"
#include "rootserver/ob_root_meta2.h"
#include "rootserver/ob_root_table2.h"
#include "../common/test_rowkey_helper.h"
using namespace std;
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

namespace oceanbase
{
  namespace test
  {
    namespace rootserver
    {
      static CharArena allocator_;
      void build_range(ObNewRange & r, int64_t tid, int8_t flag, const char* sk, const char* ek)
      {
        r.table_id_ = tid;
        r.border_flag_.set_data(flag);
        r.start_key_ = make_rowkey(sk, &allocator_);
        r.end_key_ = make_rowkey(ek, &allocator_);

      }
      class TestRootTable2 : public ::testing::Test
      {
        virtual void SetUp()
        {
        }
        virtual void TearDown()
        {
        }
      };

      TEST_F(TestRootTable2, test_check_lost_data)
      {
        ObNewRange r1, r2, r3, r4, r5, r6;
        const char* key1 = "foo1";
        const char* key2 = "key2";
        const char* key3 = "too3";
        const char* key4 = "woo4";
        uint64_t table1 = 20;
        build_range(r1, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MIN_VALUE, key1, key2);
        build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
        build_range(r3, table1, ObBorderFlag::INCLUSIVE_END, key3, key4);
        build_range(r4, table1, ObBorderFlag::MAX_VALUE, key4, key4);
        ObTabletInfo t1(r1, 0, 0);
        ObTabletInfo t2(r2, 0, 0);
        ObTabletInfo t3(r3, 0, 0);
        ObTabletInfo t4(r4, 0, 0);

        ObTabletInfoManager* info_manager = new ObTabletInfoManager();
        ObRootTable2* root_table = new ObRootTable2(info_manager);
        root_table->add(t1, 0, 2);
        root_table->add(t2, 1, 2);
        root_table->add(t2, 2, 2);
        root_table->add(t3, 0, 2);
        root_table->add(t3, 1, 2);
        root_table->add(t3, 2, 2);
        root_table->add(t4, 2, 2);
        root_table->sort();
        ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
        ObRootTable2* shrink_table = new ObRootTable2(info_manager2);
        ObTabletReportInfoList delete_list;
        root_table->shrink_to(shrink_table, delete_list);
        shrink_table->sort();
        int64_t server_index = OB_INVALID_INDEX;
        shrink_table->dump();
        ASSERT_EQ(true, shrink_table->check_lost_data(server_index));
        if (NULL != info_manager)
        {
          delete info_manager;
          info_manager = NULL;
        }
        if (NULL != info_manager)
        {
          delete info_manager;
          info_manager = NULL;
        }
        if (NULL != root_table)
        {
          delete root_table;
          root_table = NULL;
        }
        if (NULL != shrink_table)
        {
          delete shrink_table;
          shrink_table = NULL;
        }
      }
      TEST_F(TestRootTable2, test_table_id_exist)
      {
        ObNewRange r1, r2, r3, r4, r5, r6;
        const char* key1 = "foo1";
        const char* key2 = "key2";
        const char* key3 = "too3";
        const char* key4 = "woo4";

        uint64_t table1 = 20;


        build_range(r1, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MIN_VALUE, key1, key2);
        build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
        build_range(r3, table1, ObBorderFlag::INCLUSIVE_END, key3, key4);
        build_range(r4, table1, ObBorderFlag::MAX_VALUE, key4, key4);
        ObTabletInfo t1(r1, 0, 0);
        ObTabletInfo t2(r2, 0, 0);
        ObTabletInfo t3(r3, 0, 0);
        ObTabletInfo t4(r4, 0, 0);

        ObTabletInfoManager* info_manager = new ObTabletInfoManager();
        ObRootTable2* root_table = new ObRootTable2(info_manager);
        root_table->add(t1, 0, 2);
        root_table->add(t2, 1, 2);
        root_table->add(t3, 2, 2);
        root_table->sort();
        ObRootTable2* shrink_table = new ObRootTable2(info_manager);
        ObTabletReportInfoList delete_list;
        root_table->shrink_to(shrink_table, delete_list);
        shrink_table->sort();
        EXPECT_TRUE(shrink_table->table_is_exist(table1));
        uint64_t table2 = 1002;
        EXPECT_FALSE(shrink_table->table_is_exist(table2));
        //====
        ObRootTable2* root_table2 = new ObRootTable2(info_manager);
        root_table2->add(t1, 0, 2);
        root_table2->add(t2, 1, 2);
        root_table2->add(t4, 2, 2);
        root_table2->sort();
        ObRootTable2* shrink_table2 = new ObRootTable2(info_manager);
        root_table2->shrink_to(shrink_table2, delete_list);
        shrink_table2->sort();
        EXPECT_TRUE(shrink_table2->table_is_exist(table1));
        //====
        ObRootTable2* root_table3 = new ObRootTable2(info_manager);
        root_table3->add(t1, 0, 2);
        root_table3->add(t2, 1, 2);
        root_table3->add(t3, 1, 2);
        root_table3->add(t4, 2, 2);
        root_table3->sort();
        ObRootTable2* shrink_table3 = new ObRootTable2(info_manager);
        root_table3->shrink_to(shrink_table3, delete_list);
        shrink_table3->sort();
        EXPECT_EQ(true, shrink_table3->table_is_exist(table1));

        uint64_t table3 = 1;
        EXPECT_FALSE(shrink_table3->table_is_exist(table3));
        uint64_t table4 = 1000;
        EXPECT_FALSE(shrink_table3->table_is_exist(table4));
        uint64_t table5 = 100000;
        EXPECT_FALSE(shrink_table3->table_is_exist(table5));

        if (root_table != NULL)
        {
          delete root_table;
        }
        if (root_table2 != NULL)
        {
          delete root_table2;
        }
        if (root_table3 != NULL)
        {
          delete root_table3;
        }
        if (info_manager != NULL)
        {
          delete info_manager;
        }
        if (shrink_table != NULL)
        {
          delete shrink_table;
        }
        if (shrink_table2 != NULL)
        {
          delete shrink_table2;
        }
        if (shrink_table3 != NULL)
        {
          delete shrink_table3;
        }

      }
        TEST_F(TestRootTable2, test_check_tablet_version)
        {
          ObNewRange r1, r2, r3, r4, r5, r6;
          const char* key1 = "foo1";
          const char* key2 = "key2";
          const char* key3 = "too3";
          const char* key4 = "woo4";

          uint64_t table1 = 20;


          build_range(r1, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MIN_VALUE, key1, key2);
          build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
          build_range(r3, table1, ObBorderFlag::INCLUSIVE_END, key3, key4);

          ObTabletInfo t1(r1, 0, 0);
          ObTabletInfo t2(r2, 0, 0);
          ObTabletInfo t3(r3, 0, 0);


          ObTabletInfoManager* info_manager = new ObTabletInfoManager();
          ObRootTable2* root_table = new ObRootTable2(info_manager);
          root_table->add(t1, 0, 2);
          root_table->add(t1, 1, 2);
          root_table->add(t2, 1, 2);
          root_table->add(t2, 2, 2);
          root_table->add(t3, 1, 2);
          root_table->add(t3, 3, 2);
          root_table->add(t3, 2, 2);
          root_table->sort();
          //int32_t num = 2;
          bool is_merged = false;
          //root_table->set_replica_num(num);

          {
            ObRootTable2* shrink_table = new ObRootTable2(info_manager);
            ObTabletReportInfoList delete_list;
            root_table->shrink_to(shrink_table, delete_list);
            shrink_table->sort();
            ASSERT_EQ(OB_SUCCESS, shrink_table->check_tablet_version_merged(1, 1, is_merged));
            ASSERT_EQ(is_merged, true);

            ASSERT_EQ(OB_SUCCESS, shrink_table->check_tablet_version_merged(2, 1, is_merged));
            ASSERT_EQ(is_merged, true);

            ASSERT_EQ(OB_SUCCESS, shrink_table->check_tablet_version_merged(3, 1, is_merged));
            ASSERT_EQ(is_merged, false);

            //SOME  cs offline, but have leagl tablet version
            shrink_table->server_off_line(1, 1);
            ASSERT_EQ(OB_SUCCESS, shrink_table->check_tablet_version_merged(2, 1, is_merged));
            ASSERT_EQ(is_merged, true);

            //add some copy
            root_table->add(t1, 1, 2);
            root_table->add(t2, 1, 2);
            root_table->add(t3, 1, 1);
            root_table->sort();
          }
          {
            ObTabletReportInfoList delete_list;
            ObRootTable2* shrink_table = new ObRootTable2(info_manager);
            root_table->shrink_to(shrink_table, delete_list);
            //online cs have no_legal tablet_version
            ASSERT_EQ(OB_SUCCESS, root_table->check_tablet_version_merged(2, 1, is_merged));
            ASSERT_EQ(is_merged, false);

            //some cs offline ,but have replica_num's tablet legal
            shrink_table->server_off_line(1, 1);
            ASSERT_EQ(OB_SUCCESS, shrink_table->check_tablet_version_merged(2, 1, is_merged));
            ASSERT_EQ(is_merged, true);
          }
        }
      }  //end_updateserver
  }
}//end_oceanbase

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

