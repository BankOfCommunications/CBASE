/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*   
*   
*   Version: 0.1 2010-09-08
*   
*   Authors:
*          daoan(daoan@taobao.com)
*   
*
================================================================*/
#include <gtest/gtest.h>
#include "rootserver/ob_server_balance_info.h"
#include <tbsys.h>
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::rootserver;
TEST(ObServerBalanceInfo, update_server_info)
{
  ObServerBalanceInfo sbi;
  sbi.update_server_info(1,2,3);
  ASSERT_EQ(1, sbi.get_disk_capacity());
  ASSERT_EQ(2, sbi.get_disk_used());
  ASSERT_EQ(3, sbi.get_pressure_capacity());
  sbi.update_server_info(5, 7,6);
  ASSERT_EQ(5, sbi.get_disk_capacity());
  ASSERT_EQ(7, sbi.get_disk_used());
  ASSERT_EQ(6, sbi.get_pressure_capacity());
}
TEST(ObServerBalanceInfo, update_tablet_pressure)
{
  ObServerBalanceInfo sbi;
  ObTabletPresure info1;
  char buf1[30];
  char buf2[30];
  info1.range_.table_id_ = 10001;
  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.unset_max_value();

  info1.range_.start_key_.assign_buffer(buf1, 30);
  info1.range_.end_key_.assign_buffer(buf2, 30);
  info1.range_.start_key_.write("aa1", 3);
  info1.range_.end_key_.write("ba1", 3);

  info1.pressure_ = 30;
  sbi.update_tablet_pressure(&info1);
  ASSERT_EQ(30, sbi.get_total_pressure());
  ASSERT_EQ(30, sbi.get_pressure(info1.range_));

  info1.range_.start_key_.assign_buffer(buf1, 30);
  info1.range_.end_key_.assign_buffer(buf2, 30);
  info1.range_.start_key_.write("ba1", 3);
  info1.range_.end_key_.write("ca1", 3);
  info1.pressure_ = 20;

  sbi.update_tablet_pressure(&info1);
  ASSERT_EQ(50, sbi.get_total_pressure());
  ASSERT_EQ(20, sbi.get_pressure(info1.range_));

  info1.range_.start_key_.assign_buffer(buf1, 30);
  info1.range_.end_key_.assign_buffer(buf2, 30);
  info1.range_.start_key_.write("aa1", 3);
  info1.range_.end_key_.write("ba1", 3);

  ASSERT_EQ(30, sbi.get_pressure(info1.range_));

}
TEST(ObServerBalanceInfo, clone)
{
  ObServerBalanceInfo sbi;
  ObTabletPresure info1;
  char buf1[30];
  char buf2[30];
  info1.range_.table_id_ = 10001;
  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.unset_max_value();

  info1.range_.start_key_.assign_buffer(buf1, 30);
  info1.range_.end_key_.assign_buffer(buf2, 30);
  info1.range_.start_key_.write("aa1", 3);
  info1.range_.end_key_.write("ba1", 3);

  info1.pressure_ = 30;
  sbi.update_tablet_pressure(&info1);
  ASSERT_EQ(30, sbi.get_total_pressure());
  ASSERT_EQ(30, sbi.get_pressure(info1.range_));

  info1.range_.start_key_.assign_buffer(buf1, 30);
  info1.range_.end_key_.assign_buffer(buf2, 30);
  info1.range_.start_key_.write("ba1", 3);
  info1.range_.end_key_.write("ca1", 3);
  info1.pressure_ = 20;

  sbi.update_tablet_pressure(&info1);
  ASSERT_EQ(50, sbi.get_total_pressure());
  ASSERT_EQ(20, sbi.get_pressure(info1.range_));

  info1.range_.start_key_.assign_buffer(buf1, 30);
  info1.range_.end_key_.assign_buffer(buf2, 30);
  info1.range_.start_key_.write("aa1", 3);
  info1.range_.end_key_.write("ba1", 3);

  ASSERT_EQ(30, sbi.get_pressure(info1.range_));

  ObServerBalanceInfo sbj;
  sbj.clone(sbi);

  info1.range_.start_key_.assign_buffer(buf1, 30);
  info1.range_.end_key_.assign_buffer(buf2, 30);
  info1.range_.start_key_.write("ba1", 3);
  info1.range_.end_key_.write("ca1", 3);

  ASSERT_EQ(50, sbj.get_total_pressure());
  ASSERT_EQ(20, sbj.get_pressure(info1.range_));

  info1.range_.start_key_.assign_buffer(buf1, 30);
  info1.range_.end_key_.assign_buffer(buf2, 30);
  info1.range_.start_key_.write("aa1", 3);
  info1.range_.end_key_.write("ba1", 3);

  ASSERT_EQ(30, sbj.get_pressure(info1.range_));

}
int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
