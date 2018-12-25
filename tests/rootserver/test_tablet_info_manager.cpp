/*
 *   (C) 2007-2010 Taobao Inc.
 *   
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License version 2 as
 *   published by the Free Software Foundation.
 *       
 *         
 *         
 *   Version: $Id: ipvsadm.c,v 1.27 2005/12/10 16:00:07 wensong Exp $
 *           
 *   Authors:
 *      daoan <daoan@taobao.com>
 *               
 */

#include <gtest/gtest.h>
#include <unistd.h>
#include "common/ob_malloc.h"
#include "common/ob_vector.h"
#include "rootserver/ob_tablet_info_manager.h"
#include "rootserver/ob_root_meta2.h"
#include "../common/test_rowkey_helper.h"
using namespace oceanbase::common;
using namespace oceanbase::rootserver;
static CharArena allocator_;
namespace 
{
  void build_range(ObNewRange & r, int64_t tid, int8_t flag, const char* sk, const char* ek)
  {
    r.table_id_ = tid;
    r.border_flag_.set_data(flag);
    r.start_key_ = make_rowkey(sk, &allocator_);
    r.end_key_ = make_rowkey(ek, &allocator_);

  }
}

TEST(TabletInfoManagerTest, test)
{
  ObNewRange r1, r2, r3, r4;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key3 = "too3";
  const char* key4 = "woo4";

  uint64_t table1 = 20;
  uint64_t table2 = 40;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_START, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_START, key2, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::MAX_VALUE, key3, key4);
  build_range(r4, table2, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, key1, key4);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);
  ObTabletInfo t4(r4, 0, 0);

  ObTabletInfoManager* tester = new ObTabletInfoManager();
  int32_t index = -1;
  ASSERT_EQ(OB_SUCCESS, tester->add_tablet_info(t1, index));
  ASSERT_EQ(0, index);

  ASSERT_EQ(OB_SUCCESS, tester->add_tablet_info(t2, index));
  ASSERT_EQ(1, index);

  ObTabletInfo* ret = tester->get_tablet_info(1);
  ASSERT_TRUE(ret->equal(t2));
  ASSERT_TRUE(1 == tester->get_index(ret));
  int s = 0;
  for (ret = tester->begin(); ret != tester->end(); ret ++)
  {
    s++;
  }
  ASSERT_EQ(2, s);
  ASSERT_EQ(2, tester->get_array_size());

  delete tester;
}
TEST(TabletInfoManagerTest, compare)
{
  ObNewRange r1, r2, r3, r4;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key3 = "too3";
  const char* key4 = "woo4";

  uint64_t table1 = 20;
  uint64_t table2 = 40;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_START, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_START, key2, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::MAX_VALUE, key3, key4);
  build_range(r4, table2, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, key1, key4);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);
  ObTabletInfo t4(r4, 0, 0);

  ObTabletInfoManager* tester = new ObTabletInfoManager();
  int32_t index = -1;
  ASSERT_EQ(OB_SUCCESS, tester->add_tablet_info(t1, index));

  ASSERT_EQ(OB_SUCCESS, tester->add_tablet_info(t2, index));
  ASSERT_EQ(OB_SUCCESS, tester->add_tablet_info(t1, index));
  ASSERT_EQ(OB_SUCCESS, tester->add_tablet_info(t3, index));

  ObRootMeta2CompareHelper helper(tester);
  ASSERT_TRUE(helper.compare(0,1) < 0);
  ASSERT_TRUE(helper.compare(1,0) > 0);
  ASSERT_TRUE(helper.compare(2,0) == 0);
  ASSERT_TRUE(helper.compare(0,2) == 0);

  delete tester;
}

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


