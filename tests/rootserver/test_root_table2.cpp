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
#include "common/utility.h"
#include "rootserver/ob_tablet_info_manager.h"
#include "rootserver/ob_root_meta2.h"
#include "rootserver/ob_root_table2.h"
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

TEST(RootTable2Test, test_sort)
{
  ObNewRange r1, r2, r3, r4;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key3 = "too3";
  const char* key4 = "woo4";

  uint64_t table1 = 20;
  uint64_t table2 = 40;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, key3, key4);
  build_range(r4, table2, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, key1, key4);

  ASSERT_TRUE(r1.compare_with_endkey(r2) < 0 );
  ASSERT_TRUE(r2.compare_with_endkey(r3) < 0 );

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);
  ObTabletInfo t4(r4, 0, 0);

  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  ObRootTable2* root_table = new ObRootTable2(info_manager);
  root_table->add(t2, 1, 0);
  root_table->add(t3, 2, 0);
  root_table->add(t1, 3, 0);
  root_table->add(t1, 4, 0);

  root_table->sort();
  ObRootMeta2* it = root_table->begin();

  const ObTabletInfo* tablet_info = ((const ObRootTable2*)root_table)->get_tablet_info(it);

  EXPECT_TRUE(tablet_info != NULL);
  EXPECT_TRUE(tablet_info->range_.equal(r1));

  it++; it++; it++;
  tablet_info = ((const ObRootTable2*)root_table)->get_tablet_info(it);

  EXPECT_TRUE(tablet_info != NULL);
  EXPECT_TRUE(tablet_info->range_.equal(r3));
  delete info_manager;
  info_manager = NULL;
  delete root_table;
  root_table = NULL;
}
TEST(RootTable2Test, test_shrink_to)
{
  ObNewRange r1, r2, r3, r4;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key3 = "too3";
  const char* key4 = "woo4";

  uint64_t table1 = 20;
  uint64_t table2 = 40;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, key3, key4);
  build_range(r4, table2, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, key1, key4);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);
  ObTabletInfo t4(r4, 0, 0);


  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  ObRootTable2* root_table = new ObRootTable2(info_manager);
  root_table->add(t2, 0, 0);
  root_table->add(t3, 0, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->sort();
  ObRootMeta2* it = root_table->begin();

  ObTabletReportInfoList delete_list;
  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  ObRootTable2* shrink_table = new ObRootTable2(info_manager2);
  root_table->shrink_to(shrink_table, delete_list);
  delete root_table;
  delete info_manager;

  it = shrink_table->begin();
  const ObTabletInfo* tablet_info = ((const ObRootTable2*)shrink_table)->get_tablet_info(it);

  EXPECT_TRUE(tablet_info != NULL);
  EXPECT_TRUE(tablet_info->range_.equal(r1));

  it++; it++;
  tablet_info = ((const ObRootTable2*)shrink_table)->get_tablet_info(it);

  EXPECT_TRUE(tablet_info != NULL);
  EXPECT_TRUE(tablet_info->range_.equal(r3));
  //shrink_table->dump();

  delete shrink_table;
  delete info_manager2;
}
TEST(RootTable2Test, test_find_key)
{
  ObNewRange r1, r2, r3, r4, r5, r6;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key3 = "too3";
  const char* key4 = "woo4";
  const char* key5 = "xoo5";
  const char* key6 = "yoo6";

  uint64_t table1 = 20;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END, key3, key4);
  build_range(r4, table1, ObBorderFlag::INCLUSIVE_END, key4, key5);
  build_range(r5, table1, ObBorderFlag::INCLUSIVE_END, key5, key6);
  build_range(r6, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, key6, key6);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);
  ObTabletInfo t4(r4, 0, 0);
  ObTabletInfo t5(r5, 0, 0);
  ObTabletInfo t6(r6, 0, 0);


  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  ObRootTable2* root_table = new ObRootTable2(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->add(t5, 1, 0);
  root_table->add(t6, 1, 0);
  root_table->add(t4, 1, 0);
  root_table->sort();

  ObTabletReportInfoList delete_list;
  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  ObRootTable2* shrink_table = new ObRootTable2(info_manager2);
  root_table->shrink_to(shrink_table, delete_list);

  shrink_table->sort();

  delete root_table;
  delete info_manager;

  char fk[] = "key3";
  ObRootTable2::const_iterator start;
  ObRootTable2::const_iterator end;
  ObRootTable2::const_iterator ptr;
  ObRowkey obfk = make_rowkey(fk, &allocator_);
  ASSERT_EQ(OB_SUCCESS, shrink_table->find_key(table1, obfk, 10, start, end, ptr));
  const ObTabletInfo* tablet_info = ((const ObRootTable2*)shrink_table)->get_tablet_info(ptr);
  printf(" count = %ld %ld\n", end - start, ptr - start);

  EXPECT_TRUE( tablet_info->range_.equal(r2));
  delete shrink_table;
  delete info_manager2;
}
TEST(RootTable2Test, test_find_range)
{
  ObNewRange r1, r2, r3, r4, r5;
  const char* key1 = "0001";
  const char* key2 = "0010";
  const char* key3 = "0100";
  const char* key4 = "1000";

  uint64_t table1 = 20;
  uint64_t table2 = 40;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r5, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key3, key4);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, key4, key4);
  build_range(r4, table2, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE|ObBorderFlag::MIN_VALUE, key1, key4);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);
  ObTabletInfo t4(r4, 0, 0);


  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  ObRootTable2* root_table = new ObRootTable2(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->add(t4, 1, 0);
  root_table->sort();

  ObTabletReportInfoList delete_list;
  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  ObRootTable2* shrink_table = new ObRootTable2(info_manager2);
  root_table->shrink_to(shrink_table, delete_list);

  shrink_table->sort();

  delete root_table;
  delete info_manager;

  ObRootTable2::const_iterator first;
  ObRootTable2::const_iterator last;
  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r5, first, last));
  EXPECT_TRUE(first == last);
  const ObTabletInfo* tablet_info = ((const ObRootTable2*)shrink_table)->get_tablet_info(first);
  tablet_info->range_.hex_dump();
  EXPECT_TRUE( tablet_info->range_.equal(r2));

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  EXPECT_TRUE(first == last);
  tablet_info = ((const ObRootTable2*)shrink_table)->get_tablet_info(first);
  tablet_info->range_.hex_dump();
  EXPECT_TRUE( tablet_info->range_.equal(r4));
  delete shrink_table;
  delete info_manager2;
}
TEST(RootTable2Test, test_find_range2)
{
  ObNewRange r1, r2, r3, r4, r5;
  const char* key1 = "0001";
  const char* key2 = "0010";
  const char* key3 = "0100";
  const char* key4 = "1000";
  const char* key5 = "1100";

  uint64_t table1 = 20;

  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END, key3, key4);
  build_range(r4, table1, ObBorderFlag::INCLUSIVE_END, key4, key5);
  build_range(r5, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, key5, key5);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);


  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  ObRootTable2* root_table = new ObRootTable2(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->sort();

  ObTabletReportInfoList delete_list;
  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  ObRootTable2* shrink_table = new ObRootTable2(info_manager2);
  root_table->shrink_to(shrink_table, delete_list);

  shrink_table->sort();

  delete root_table;
  delete info_manager;

  ObRootTable2::const_iterator first;
  ObRootTable2::const_iterator last;
  ASSERT_EQ(OB_FIND_OUT_OF_RANGE, shrink_table->find_range(r4, first, last));
  EXPECT_TRUE(first == shrink_table->end());
  ASSERT_EQ(OB_FIND_OUT_OF_RANGE, shrink_table->find_range(r5, first, last));
  EXPECT_TRUE(first == shrink_table->end());

  delete shrink_table;
  delete info_manager2;
}
TEST(RootTable2Test, test_range_pos_type)
{
  ObNewRange r1, r2, r3, r4, r5, r6, r7, r8;
  const char* key1 = "foo1";
  const char* key1_2 = "foo2";
  const char* key1_3 = "foo3";
  const char* key2 = "key2";
  const char* key2_3 = "key3";
  const char* key3 = "too3";
  const char* key4 = "woo4";

  uint64_t table1 = 20;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MIN_VALUE, key1, key1_2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, key3, key4);
  build_range(r4, table1, ObBorderFlag::INCLUSIVE_END, key2, key2_3);
  build_range(r5, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, key2, key4);

  build_range(r6, table1, ObBorderFlag::INCLUSIVE_END, key1_2, key1_3);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);

  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  ObRootTable2* root_table = new ObRootTable2(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->sort();

  ObTabletReportInfoList delete_list;
  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  ObRootTable2* shrink_table = new ObRootTable2(info_manager2);
  root_table->shrink_to(shrink_table, delete_list);

  shrink_table->sort();

  delete root_table;
  delete info_manager;

  ObRootTable2::const_iterator first;
  ObRootTable2::const_iterator last;
  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r2, first, last));
  EXPECT_TRUE(first == last);
  int range_pos_type =shrink_table->get_range_pos_type(r2, first, last);
  ASSERT_EQ(ObRootTable2::POS_TYPE_SAME_RANGE, range_pos_type);

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  EXPECT_TRUE(first == last);
  range_pos_type =shrink_table->get_range_pos_type(r4, first, last);
  ASSERT_EQ(ObRootTable2::POS_TYPE_SPLIT_RANGE, range_pos_type);

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r5, first, last));
  range_pos_type =shrink_table->get_range_pos_type(r5, first, last);
  ASSERT_EQ(ObRootTable2::POS_TYPE_MERGE_RANGE, range_pos_type);

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r6, first, last));
  range_pos_type =shrink_table->get_range_pos_type(r6, first, last);
  ASSERT_EQ(ObRootTable2::POS_TYPE_ADD_RANGE, range_pos_type);

  delete shrink_table;
  delete info_manager2;
}
TEST(RootTable2Test, test_split_range_top)
{
  ObNewRange r1, r2, r3, r4, r5;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key2_3 = "key3";
  const char* key3 = "too3";
  const char* key4 = "woo4";

  uint64_t table1 = 20;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, key3, key4);
  build_range(r4, table1, ObBorderFlag::INCLUSIVE_END, key2, key2_3);
  build_range(r5, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, key2, key4);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);

  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  ObRootTable2* root_table = new ObRootTable2(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->sort();

  ObTabletReportInfoList delete_list;
  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  ObRootTable2* shrink_table = new ObRootTable2(info_manager2);
  root_table->shrink_to(shrink_table, delete_list);

  shrink_table->sort();

  delete root_table;
  delete info_manager;

  ObRootTable2::const_iterator first;
  ObRootTable2::const_iterator last;

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  EXPECT_TRUE(first == last);
  int range_pos_type =shrink_table->get_range_pos_type(r4, first, last);
  ASSERT_EQ(ObRootTable2::POS_TYPE_SPLIT_RANGE, range_pos_type);

  ObTabletInfo t4(r4,0,0);

  shrink_table->split_range(t4,first,1,5 );
  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  shrink_table->dump();
  EXPECT_TRUE(first == last);
  const ObTabletInfo* tablet_info = ((const ObRootTable2*)shrink_table)->get_tablet_info(first);
  EXPECT_TRUE( tablet_info->range_.equal(r4));

  delete shrink_table;
  delete info_manager2;
}
TEST(RootTable2Test, test_split_range_middle)
{
  ObNewRange r1, r2, r3, r4, r5;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key2_3 = "key3";
  const char* key2_4 = "key4";
  const char* key3 = "too3";
  const char* key4 = "woo4";

  uint64_t table1 = 20;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, key3, key4);
  build_range(r4, table1, ObBorderFlag::INCLUSIVE_END, key2_3, key2_4);
  build_range(r5, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, key2, key4);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);

  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  ObRootTable2* root_table = new ObRootTable2(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->sort();

  ObTabletReportInfoList delete_list;
  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  ObRootTable2* shrink_table = new ObRootTable2(info_manager2);
  root_table->shrink_to(shrink_table, delete_list);

  shrink_table->sort();

  delete root_table;
  delete info_manager;

  ObRootTable2::const_iterator first;
  ObRootTable2::const_iterator last;

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  EXPECT_TRUE(first == last);
  int range_pos_type =shrink_table->get_range_pos_type(r4, first, last);
  ASSERT_EQ(ObRootTable2::POS_TYPE_SPLIT_RANGE, range_pos_type);

  ObTabletInfo t4(r4,0,0);

  shrink_table->split_range(t4,first,1,5 );
  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  shrink_table->dump();
  EXPECT_TRUE(first == last);
  const ObTabletInfo* tablet_info = ((const ObRootTable2*)shrink_table)->get_tablet_info(first);
  EXPECT_TRUE( tablet_info->range_.equal(r4));

  delete shrink_table;
  delete info_manager2;
}
TEST(RootTable2Test, test_add_range)
{
  ObNewRange r1, r2, r3, r4, r5;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key2_3 = "key3";
  const char* key2_4 = "key4";
  const char* key3 = "too3";
  const char* key4 = "woo4";

  uint64_t table1 = 20;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2_4, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, key3, key4);

  build_range(r4, table1, ObBorderFlag::INCLUSIVE_END, key2, key2_3);
  build_range(r5, table1, ObBorderFlag::INCLUSIVE_END, key2_3, key2_4);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);

  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  ObRootTable2* root_table = new ObRootTable2(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->sort();

  ObTabletReportInfoList delete_list;
  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  ObRootTable2* shrink_table = new ObRootTable2(info_manager2);
  root_table->shrink_to(shrink_table, delete_list);

  shrink_table->sort();

  delete root_table;
  delete info_manager;

  ObRootTable2::const_iterator first;
  ObRootTable2::const_iterator last;

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r5, first, last));
  EXPECT_TRUE(first == last);
  int range_pos_type =shrink_table->get_range_pos_type(r5, first, last);
  ASSERT_EQ(ObRootTable2::POS_TYPE_ADD_RANGE, range_pos_type);
  ObTabletInfo t5(r5,0,0);
  shrink_table->add_range(t5,first,1,5 );
  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r5, first, last));
  shrink_table->dump();
  EXPECT_TRUE(first == last);
  const ObTabletInfo* tablet_info = ((const ObRootTable2*)shrink_table)->get_tablet_info(first);
  EXPECT_TRUE( tablet_info->range_.equal(r5));

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  EXPECT_TRUE(first == last);
  range_pos_type =shrink_table->get_range_pos_type(r4, first, last);
  ASSERT_EQ(ObRootTable2::POS_TYPE_ADD_RANGE, range_pos_type);
  ObTabletInfo t4(r4,0,0);
  shrink_table->add_range(t4,first,1,5 );
  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  shrink_table->dump();
  EXPECT_TRUE(first == last);
  tablet_info = ((const ObRootTable2*)shrink_table)->get_tablet_info(first);
  EXPECT_TRUE( tablet_info->range_.equal(r4));

  delete shrink_table;
  delete info_manager2;
}
TEST(RootTable2Test,test_add_lost_range)
{
  ObNewRange r1, r2, r3, r4, r5;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key2_4 = "key4";
  const char* key3 = "too3";
  const char* key4 = "woo4";

  uint64_t table1 = 20;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2_4, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, key3, key4);

  build_range(r4, table1, ObBorderFlag::INCLUSIVE_END, key2, key2_4);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);

  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  ObRootTable2* root_table = new ObRootTable2(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->sort();

  ObTabletReportInfoList delete_list;
  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  ObRootTable2* shrink_table = new ObRootTable2(info_manager2);
  root_table->shrink_to(shrink_table, delete_list);

  shrink_table->sort();
  shrink_table->add_lost_range();

  delete root_table;
  delete info_manager;

  ObRootTable2::const_iterator first;
  ObRootTable2::const_iterator last;

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  EXPECT_TRUE(first == last);
  const ObTabletInfo* tablet_info = ((const ObRootTable2*)shrink_table)->get_tablet_info(first);
  EXPECT_TRUE( tablet_info->range_.equal(r4));

  shrink_table->dump();

  delete shrink_table;
  delete info_manager2;
}
TEST(RootTable2Test, test_create_table)
{
  ObNewRange r1, r2, r3, r4, r5, r6;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key3 = "too3";
  const char* key4 = "woo4";

  uint64_t table1 = 20;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, key3, key4);
  build_range(r4, 30, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE|ObBorderFlag::MIN_VALUE, key1, key4);
  build_range(r5, 25, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE|ObBorderFlag::MIN_VALUE, key1, key4);
  build_range(r6, 35, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE|ObBorderFlag::MIN_VALUE, key1, key4);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);
  ObTabletInfo t4(r4, 0, 0);
  ObTabletInfo t5(r5, 0, 0);
  ObTabletInfo t6(r6, 0, 0);

  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  ObRootTable2* root_table = new ObRootTable2(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->add(t4, 1, 0);
  root_table->sort();

  ObTabletReportInfoList delete_list;
  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  ObRootTable2* shrink_table = new ObRootTable2(info_manager2);
  root_table->shrink_to(shrink_table, delete_list);

  shrink_table->sort();

  delete root_table;
  delete info_manager;

  ObArray<int> server_indexes;
  server_indexes.push_back(0);
  server_indexes.push_back(0);
  server_indexes.push_back(0);
  ASSERT_EQ(OB_SUCCESS, shrink_table->create_table(t5, server_indexes, 0));
  shrink_table->dump();
  ObRootTable2::const_iterator it = shrink_table->end();
  it--;
  const ObTabletInfo* tablet_info = ((const ObRootTable2*)shrink_table)->get_tablet_info(it);
  ASSERT_TRUE(tablet_info->range_.equal(r4));
  it--;
  tablet_info = ((const ObRootTable2*)shrink_table)->get_tablet_info(it);
  ASSERT_TRUE(tablet_info->range_.equal(r5));

  ASSERT_EQ(OB_SUCCESS, shrink_table->create_table(t6, server_indexes, 0));
  shrink_table->dump();
  it = shrink_table->end();
  it--;
  tablet_info = ((const ObRootTable2*)shrink_table)->get_tablet_info(it);
  ASSERT_TRUE(tablet_info->range_.equal(r6));

  delete shrink_table;
  delete info_manager2;
}

TEST(RootTable2Test, test_shrink)
{
  ObNewRange r1, r2, r3, r4, r5, r6, r7;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key3 = "too3";
  const char* key4 = "woo4";
  const char* key12 = "foy2";

  uint64_t table1 = 20;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r3, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE, key3, key4);
  build_range(r4, 30, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE|ObBorderFlag::MIN_VALUE, key1, key4);
  build_range(r5, 25, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE|ObBorderFlag::MIN_VALUE, key1, key4);
  build_range(r6, 35, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE|ObBorderFlag::MIN_VALUE, key1, key4);
  build_range(r7, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MIN_VALUE, key1, key12);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);
  ObTabletInfo t4(r4, 0, 0);
  ObTabletInfo t5(r5, 0, 0);
  ObTabletInfo t6(r6, 0, 0);
  ObTabletInfo t7(r7, 0, 0);

  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  ObRootTable2* root_table = new ObRootTable2(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->add(t4, 1, 0);
  root_table->add(t7, 1, 0);
  root_table->sort();
  root_table->dump();
  ObTabletReportInfoList delete_list;
  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  ObRootTable2* shrink_table = new ObRootTable2(info_manager2);
  root_table->shrink_to(shrink_table, delete_list);

  shrink_table->sort();

  delete root_table;
  delete info_manager;
  shrink_table->dump();

  delete shrink_table;
  delete info_manager2;
}

TEST(RootTable2Test, test_split_range_top_max)
{
  ObNewRange r1, r2, r3, r4, r5;
  const char* key1 = "foo1";
  const char* key2 = "key2";
  const char* key3 = "too3";
  const char* key3_4 = "too4";
  const char* key4 = "woo4";

  uint64_t table1 = 20;


  build_range(r1, table1, ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MIN_VALUE, key1, key2);
  build_range(r2, table1, ObBorderFlag::INCLUSIVE_END, key2, key3);
  build_range(r3, table1, ObBorderFlag::MAX_VALUE, key3, key4);

  ObTabletInfo t1(r1, 0, 0);
  ObTabletInfo t2(r2, 0, 0);
  ObTabletInfo t3(r3, 0, 0);

  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  ObRootTable2* root_table = new ObRootTable2(info_manager);
  root_table->add(t2, 2, 0);
  root_table->add(t3, 3, 0);
  root_table->add(t1, 0, 0);
  root_table->add(t1, 1, 0);
  root_table->sort();

  ObTabletReportInfoList delete_list;
  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  ObRootTable2* shrink_table = new ObRootTable2(info_manager2);
  root_table->shrink_to(shrink_table, delete_list);

  shrink_table->sort();
  shrink_table->dump();

  delete root_table;
  delete info_manager;

  ObRootTable2::const_iterator first;
  ObRootTable2::const_iterator last;

  build_range(r4, table1, ObBorderFlag::INCLUSIVE_END, key3, key3_4);

  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  EXPECT_TRUE(first == last);
  int range_pos_type =shrink_table->get_range_pos_type(r4, first, last);
  ASSERT_EQ(ObRootTable2::POS_TYPE_SPLIT_RANGE, range_pos_type);

  ObTabletInfo t4(r4,0,0);

  shrink_table->split_range(t4,first,1,5 );
  ASSERT_EQ(OB_SUCCESS, shrink_table->find_range(r4, first, last));
  TBSYS_LOG(WARN, "================================================================");
  shrink_table->dump();
  EXPECT_TRUE(first == last);
  const ObTabletInfo* tablet_info = ((const ObRootTable2*)shrink_table)->get_tablet_info(first);
  fprintf(stderr, "%s,%s\n", to_cstring(tablet_info->range_), to_cstring(r4));
  EXPECT_TRUE( tablet_info->range_.equal(r4));
  EXPECT_TRUE(tablet_info->range_.border_flag_.get_data() == r4.border_flag_.get_data());

  delete shrink_table;
  delete info_manager2;
}

TEST(RootTable2Test, test_delete_tables)
{
  static const int32_t TABLES_NUM = 4;
  static const int32_t TABLETS_PER_TABLE = 4;
  static const int32_t CS_NUM = 3;

  const char* keys[TABLETS_PER_TABLE+1] = {"1", "2", "3", "4", "5"};
  uint64_t tables_id[TABLES_NUM] = {1001,1002,1003,1004};
  ObTabletInfo tablets[TABLES_NUM*TABLETS_PER_TABLE];
  for (int i = 0; i < TABLES_NUM; ++i)
  {
    for (int j = 0; j < TABLETS_PER_TABLE; ++j)
    {
      int8_t flag = 0;
      if (0 == j)
      {
        // min
        flag = ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MIN_VALUE;
      }
      else if (j == TABLETS_PER_TABLE - 1)
      {
        // max
        flag = ObBorderFlag::INCLUSIVE_END|ObBorderFlag::MAX_VALUE;
      }
      else
      {
        flag = ObBorderFlag::INCLUSIVE_END;
      }
      build_range(tablets[i*TABLETS_PER_TABLE+j].range_, tables_id[i], flag, keys[j], keys[j+1]);
      tablets[i*TABLETS_PER_TABLE+j].occupy_size_ = 1023;
      tablets[i*TABLETS_PER_TABLE+j].row_count_ = 1234;
      tablets[i*TABLETS_PER_TABLE+j].crc_sum_ = 0;
    }
  }

  ObTabletInfoManager* info_manager = new ObTabletInfoManager();
  ObRootTable2* root_table = new ObRootTable2(info_manager);
  OB_ASSERT(info_manager);
  OB_ASSERT(root_table);
  int64_t tablet_version = 111;
  for (int i = 0; i < CS_NUM; ++i)
  {
    for (int j = 0; j < TABLES_NUM*TABLETS_PER_TABLE; ++j)
    {
      OB_ASSERT(OB_SUCCESS == root_table->add(tablets[j], i, tablet_version));
    }
  }
  root_table->sort();

  ObTabletReportInfoList delete_list;
  ObTabletInfoManager* info_manager2 = new ObTabletInfoManager();
  ObRootTable2* shrink_table = new ObRootTable2(info_manager2);
  ASSERT_EQ(OB_SUCCESS, root_table->shrink_to(shrink_table, delete_list));
  shrink_table->sort();
  shrink_table->dump();
  for (int i = 0; i < TABLES_NUM; ++i)
  {
    ASSERT_TRUE(shrink_table->table_is_exist(tables_id[i]));
  }

  ObArray<uint64_t> deleted_tables;
  deleted_tables.push_back(tables_id[1]);
  deleted_tables.push_back(tables_id[3]);
  ASSERT_EQ(OB_SUCCESS, shrink_table->delete_tables(deleted_tables));
  shrink_table->dump();
  for (int i = 0; i < TABLES_NUM; ++i)
  {
    if (1 == i || 3 == i)
    {
      ASSERT_TRUE(!shrink_table->table_is_exist(tables_id[i]));
    }
  }

  delete root_table;
  delete info_manager;

  delete shrink_table;
  delete info_manager2;
}

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


