/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * test_ob_string_search.cpp is for what ...
 *
 * Version: ***: test_ob_string_search.cpp  Thu Mar 24 13:35:57 2011 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji.hcm <fangji.hcm@taobao.com>
 *     -some work detail if you want
 *
 */
#include <tblog.h>
#include <gtest/gtest.h>
#include "ob_string_search.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      class TestObStringSearch: public ::testing::Test
      {
      public:
        virtual void SetUp()
        {
        }
        virtual void TearDown()
        {
        }
      };
      TEST_F(TestObStringSearch, test_kr_fast_print)
      {
        const char *pattern[8]={"Free softw","d be free "," socially ","bjects—su"," that it c","y. These p","abcdefghigklmnopqrstuvwxyzABCDEFGIJKLMNOPQRSTUVWXYZ", "hellohello"};

        const char *text    = "Free software is a matter of freedom: people should be free to use software in all the ways that are socially useful. Software differs from material objects—such as chairs, sandwiches, and gasoline—in that it can be copied and changed much more easily. These possibilities make software as useful as abcdefghigklmnopqrstuvwxyzABCDEFGIJKLMNOPQRSTUVWXYZabcdefghigklmnopqrstuvwxyz it is; we believe software use";
        ObString txt;
        txt.assign(const_cast<char*>(text), static_cast<int32_t>(strlen(text)));
        for (int i = 0; i < 8; ++i)
        {
          ObString pat(static_cast<int32_t>(strlen(pattern[i])), static_cast<int32_t>(strlen(pattern[i])), const_cast<char*>(pattern[i]));
          uint64_t pat_print = ObStringSearch::cal_print(pat);
          if ( 7 == i)
          {
            EXPECT_EQ(-1, ObStringSearch::kr_search(pat, pat_print, txt));
            EXPECT_EQ(-1, ObStringSearch::kr_search(pat, txt));
            EXPECT_EQ(-1, ObStringSearch::fast_search(pat, txt));
          }
          else
          {
            EXPECT_EQ(0, memcmp(pattern[i], text+ObStringSearch::kr_search(pat, pat_print, txt), strlen(pattern[i])));
            EXPECT_TRUE(0 ==  memcmp(pattern[i], text+ObStringSearch::kr_search(pat, txt), strlen(pattern[i])));
            EXPECT_TRUE(0 ==  memcmp(pattern[i], text+ObStringSearch::fast_search(pat, txt), strlen(pattern[i])));
          }
          EXPECT_TRUE(ObStringSearch::kr_search(pat, pat_print, txt) == ObStringSearch::kr_search(pat, txt));
          EXPECT_TRUE(ObStringSearch::kr_search(pat, txt) == ObStringSearch::fast_search(pat, txt));
        }
      }
    }//namespace common
  }//namespace tests
}//namespace oceanbase
int main(int argc, char** argv)
{
  TBSYS_LOGGER.setLogLevel("ERROR");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
