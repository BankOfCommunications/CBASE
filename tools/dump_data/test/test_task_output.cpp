#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <stdlib.h>
#include <gtest/gtest.h>
#include "task_output.h"

using namespace std;
using namespace oceanbase::tools;

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestTaskOutput: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};

TEST_F(TestTaskOutput, test_add)
{
  TaskOutput list;
  int64_t count = 50;
  for (int i = 0; i < count; ++i)
  {
    string output("/home/xielun/");
    output += i + 64; 
    EXPECT_TRUE(0 == list.add(i, i + 1224, output));
  }
  EXPECT_TRUE(list.size() == count);
  list.print(stdout);
}


