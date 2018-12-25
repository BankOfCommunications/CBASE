/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-03-13 20:42:26 fufeng.syd>
 * Version: $Id$
 * Filename: ob_config_test.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#include <gtest/gtest.h>
#include "common/ob_config.h"

using namespace oceanbase::common;
/* using namespace oceanbase::common::hash; */

class TestServerConfig
  : public ::testing::Test
{
  public:
    TestServerConfig()
      : container_(p_container_)
    {
    }
    ObConfigContainer container_;
    static ObConfigContainer *p_container_;

  public:
    bool check() const
    {
      bool ret = true;
      ObConfigContainer::const_iterator it = container_.begin();
      for (; it != container_.end(); it++)
      {
        ret &= it->second->check();
      }
      return ret;
    }

    void check_each_true() const
    {
      ObConfigContainer::const_iterator it = container_.begin();
      for (; it != container_.end(); it++)
      {
        EXPECT_TRUE(it->second->check()) << it->first.str() << " not pass checker!";
      }
    }

    DEF_INT(num0, "-1", "[,0]", "info");
    DEF_INT(num1, "11", "(10,11]", "info");
    DEF_INT(num2, "10", "[10,11]", "info");
    DEF_INT(num3, "-1", "[-1,-1]", "info");
    DEF_INT(num4, "1", "(0,2)", "info");
    DEF_INT(num5, "1", "[1,2)", "info");
    DEF_INT(num6, "1", "(0,1]", "info");
    DEF_INT(num7, "1", "[1,1]", "info");
    DEF_INT(num8, "1", "info");

    DEF_INT(num9, "1", "[1,1]", OB_CONFIG_STATIC, "info");
    DEF_INT(num10, "1", "[1,1]", OB_CONFIG_STATIC, "info");

    DEF_TIME(time1, "1h", "[60m,3600]", "info");
    DEF_TIME(time2, "1s", "[1,2)", "info");
    DEF_TIME(time3, "1m", "(59s,60s]", "info");
    DEF_TIME(time4, "1h", "[60m,3600]", "info");

    DEF_TIME(time5, "1h", "[,3600]", OB_CONFIG_STATIC, "info");
    DEF_TIME(time6, "1h", "[60m,]", OB_CONFIG_STATIC, "info");

    DEF_CAP(cap1, "10M", "[10m,10m]", "info");
    DEF_CAP(cap2, "0", "[0,0]", "info");
    DEF_CAP(cap3, "-1", "[0,0]", "info");
    DEF_CAP(cap4, "1g", "[1024M,1g]", "info");
    DEF_CAP(cap5, "1M", "[1024k,1024k]", "info");
    DEF_CAP(cap6, "1M", "(1023k,1025k)", "info");

    DEF_CAP(cap7, "1M", "[1024k,]", OB_CONFIG_STATIC, "info");
    DEF_CAP(cap8, "1M", "[,1024k]", OB_CONFIG_STATIC, "info");

    DEF_INT(num_out, "15", "[0,20]", "info");

    DEF_BOOL(bool1, "True", "info");
    DEF_BOOL(bool2, "fALse", "info");

    DEF_STR(str1, "some string", "info");
    DEF_STR(str2, "another string", OB_CONFIG_STATIC, "info");
	DEF_MOMENT(major_freeze_duty_time, "Disable", OB_CONFIG_DYNAMIC, "major freeze duty time");

    DEF_IP(ip1, "1.2.3.4", "some ip description");
    DEF_IP(ip2, "1.2.3.4",OB_CONFIG_STATIC, "some ip description");
    DEF_IP(ip3, "111.2.3.4", "some ip description");
};

int main(int argc, char* argv[])
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

ObConfigContainer *TestServerConfig::p_container_;

TEST_F(TestServerConfig, ALL)
{
  check_each_true();

  EXPECT_EQ(OB_CONFIG_STATIC, num9.type());
  EXPECT_EQ(OB_CONFIG_STATIC, num10.type());
  EXPECT_EQ(OB_CONFIG_STATIC, time5.type());
  EXPECT_EQ(OB_CONFIG_STATIC, time6.type());
  EXPECT_EQ(OB_CONFIG_STATIC, cap7.type());
  EXPECT_EQ(OB_CONFIG_STATIC, cap8.type());

  num2.set_value("11");
  EXPECT_TRUE(num2.check());
  num2.set_value("12");
  EXPECT_FALSE(num2.check());

  EXPECT_FALSE(check());

  EXPECT_TRUE(num_out.check());
  num_out.set_value("-1");
  EXPECT_FALSE(num_out.check());
  /* EXPECT_EQ(8, xxServerConfig::container.size()); */
  EXPECT_EQ(11, num1);

  EXPECT_TRUE(bool1);
  EXPECT_FALSE(bool2);

  bool2.set_value("t");
  EXPECT_TRUE(bool2);

  EXPECT_STREQ("some string", str1);
  EXPECT_EQ(OB_CONFIG_DYNAMIC, str1.type());
  EXPECT_STREQ("another string", str2);
  EXPECT_EQ(OB_CONFIG_STATIC, str2.type());

  ip3.set_value("1234.12.3.4");
  EXPECT_FALSE(ip3.check());

  const int64_t &i = num0;
  EXPECT_EQ(-1, i);
  num0 = 2;
  EXPECT_EQ(2, i);
}

 TEST_F(TestServerConfig, Dutytime)
{
  oceanbase::common::DutyTimeNode dtn;


  check_each_true();
  EXPECT_TRUE(major_freeze_duty_time.disable());

  EXPECT_TRUE(major_freeze_duty_time.set("18:40"));

  major_freeze_duty_time.set("disable");
  EXPECT_TRUE(major_freeze_duty_time.disable());
  dtn.hour_ = 18;
  dtn.minute_ = 40;
  EXPECT_FALSE(major_freeze_duty_time.is_duty_time(dtn));

  EXPECT_FALSE(major_freeze_duty_time.set("18"));
  EXPECT_FALSE(major_freeze_duty_time.set("18:40|18:41|18:42|18:43|18:44|18:45|18:46|18:47|18:48|19:01"));

  EXPECT_TRUE(major_freeze_duty_time.set("18:40|18:41|18:42|18:43|18:44|18:45|18:46|18:47|18:48"));
  major_freeze_duty_time.set("18");
  EXPECT_TRUE(major_freeze_duty_time.disable());
  dtn.hour_ = 18;

  dtn.minute_ = 40;
  EXPECT_FALSE(major_freeze_duty_time.is_duty_time(dtn));

  dtn.minute_ = 41;
  EXPECT_FALSE(major_freeze_duty_time.is_duty_time(dtn));
  
  dtn.minute_ = 42;
  EXPECT_FALSE(major_freeze_duty_time.is_duty_time(dtn));

  dtn.minute_ = 43;
  EXPECT_FALSE(major_freeze_duty_time.is_duty_time(dtn));

  dtn.minute_ = 44;
  EXPECT_FALSE(major_freeze_duty_time.is_duty_time(dtn));

  dtn.minute_ = 45;
  EXPECT_FALSE(major_freeze_duty_time.is_duty_time(dtn));

  dtn.minute_ = 46;
  EXPECT_FALSE(major_freeze_duty_time.is_duty_time(dtn));

  dtn.minute_ = 47;
  EXPECT_FALSE(major_freeze_duty_time.is_duty_time(dtn));

  dtn.minute_ = 48;
  EXPECT_FALSE(major_freeze_duty_time.is_duty_time(dtn));

  dtn.hour_ = -1;
  dtn.minute_ = -1;
  EXPECT_TRUE(major_freeze_duty_time.is_duty_time(dtn));

  major_freeze_duty_time.set("18:40");
  EXPECT_FALSE(major_freeze_duty_time.disable());
  dtn.hour_ = 18;
  dtn.minute_ = 40;
  EXPECT_TRUE(major_freeze_duty_time.is_duty_time(dtn));

  major_freeze_duty_time.set("19:40");
  EXPECT_FALSE(major_freeze_duty_time.disable());
  EXPECT_FALSE(major_freeze_duty_time.is_duty_time(dtn));

  major_freeze_duty_time.set("19:40|19:41|19:42");
  major_freeze_duty_time.set("19:40|19:41");
  EXPECT_FALSE(major_freeze_duty_time.disable());
  dtn.hour_ = 19;

  dtn.minute_ = 40;
  EXPECT_TRUE(major_freeze_duty_time.is_duty_time(dtn));

  dtn.minute_ = 41;
  EXPECT_TRUE(major_freeze_duty_time.is_duty_time(dtn));

  dtn.minute_ = 42;
  EXPECT_FALSE(major_freeze_duty_time.is_duty_time(dtn));
}
