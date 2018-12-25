#include <gtest/gtest.h>
#include "test_main.h"
using namespace oceanbase::common;
TEST(BaseMainTest, basic)
{
  BaseMain* pmain = BaseMainTest::get_instance();
  int argc = 3;
  const char* argv[] = {"ddd", "-p", "2001"};

  pmain->start(argc, (char**)argv);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

