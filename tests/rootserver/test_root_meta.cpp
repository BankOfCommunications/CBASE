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
 *      qushan <qushan@taobao.com>
 *        - some work details if you want
 *      Author Name ...
 *               
 */

#include <gtest/gtest.h>
#include "ob_root_meta.h"
using namespace oceanbase::common;
using namespace oceanbase::rootserver;

namespace oceanbase
{
  namespace tests
  {
    namespace rootserver
    {

      class TestRootMeta : public ::testing::Test {

        public:
          virtual void SetUp()
          {
          }
          virtual void TearDown()
          {
          }

          void build_range(ObRange & r, int64_t tid, int8_t flag, const char* sk, const char* ek)
          {

            ObString start_key(strlen(sk), strlen(sk), (char*)sk);
            ObString end_key(strlen(ek), strlen(ek), (char*)ek);

            r.table_id_ = tid;
            r.border_flag_.set_data(flag);
            r.start_key_ = start_key;
            r.end_key_ = end_key;

          }
        protected:
          ObRootMetaSortList root_meta_;
      };

     // TEST_F(TestRootMeta, testNew)
     // {
     //   int ret = root_meta_.reserve(0);
     //   EXPECT_EQ(0, ret);
     //   ret = root_meta_.reserve(-1);
     //   EXPECT_EQ(0, ret);

     //   ret = root_meta_.reserve(5);
     //   EXPECT_EQ(5, ret);
     //   EXPECT_EQ(5, root_meta_.capacity());
     //   EXPECT_EQ(0, root_meta_.size());
     // }


     // TEST_F(TestRootMeta, testAdd)
     // {
     //   ObRange r1, r2, r3, r4;
     //   const char* key1 = "foo1";
     //   const char* key2 = "key2";
     //   const char* key3 = "too3";
     //   const char* key4 = "woo4";

     //   int64_t table1 = 20;
     //   int64_t table2 = 40;

     //   ObServer server1;
     //   ObServer server2;
     //   server1.set_ipv4_addr("192.168.208.27", 3200);
     //   server2.set_ipv4_addr("192.168.208.28", 3200);
     //   ObTabletLocation location1(1334, server1);
     //   ObTabletLocation location2(1335, server2);

     //   build_range(r1, table1, ObBorderFlag::INCLUSIVE_START, key1, key2);
     //   build_range(r2, table1, ObBorderFlag::INCLUSIVE_START, key2, key3);
     //   build_range(r3, table1, ObBorderFlag::INCLUSIVE_START, key3, key4);
     //   build_range(r4, table2, ObBorderFlag::INCLUSIVE_START|ObBorderFlag::INCLUSIVE_END, key1, key4);

     //   ObTabletInfo t1(r1, 0, 0);
     //   ObTabletInfo t2(r2, 0, 0);
     //   ObTabletInfo t3(r3, 0, 0);
     //   ObTabletInfo t4(r4, 0, 0);

     //   root_meta_.add(t3, location1);
     //   root_meta_.add(t2, location1);
     //   root_meta_.add(t1, location1);
     //   root_meta_.add(t2, location2);
     //   root_meta_.add(t3, location2);
     //   root_meta_.add(t4, location2);
     //   EXPECT_EQ(4, root_meta_.size());

     //   // check intersect range.
     //   ObRange rinst;
     //   build_range(rinst, table1, ObBorderFlag::INCLUSIVE_START, key1, key3);
     //   ObTabletInfo te(rinst, 0, 0);
     //   EXPECT_NE(0, root_meta_.add(te, location1));
     //   EXPECT_EQ(4, root_meta_.size());

     //   // check all rootmeta elements is sorted?
     //   for (int i = 0 ; i < root_meta_.size() - 1; ++i)
     //   {
     //     int cmp = root_meta_[i]->compare(*root_meta_[i+1]);
     //     EXPECT_GT(0, cmp);
     //   }

     // }

    }
  }
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


