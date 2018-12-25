#include <gtest/gtest.h>
#include "limit_array.h"
using namespace oceanbase;
using namespace oceanbase::common;
TEST(LimitTest, basicTest)
{
  LimitArray<int, 10> limit_array;
  int a[4];
  int b[4];
  for (int i = 0; i < 4; i++)
    a[i]=b[i]=-1;
  {
    ObArrayHelper<int> hel(4,a);
    limit_array.expand(hel);
  }
  limit_array.push_back(1);
  limit_array.push_back(2);
  limit_array.push_back(3);
  limit_array.push_back(4);

  ASSERT_EQ(1,*limit_array.at(0));
  ASSERT_EQ(1,a[0]);

  ASSERT_EQ(4,*limit_array.at(3));
  ASSERT_EQ(4,a[3]);

  ASSERT_EQ(4,*limit_array.pop());

  ASSERT_EQ(4,limit_array.get_size());
  ASSERT_EQ(3,limit_array.get_index());

  limit_array.push_back(5);
  ASSERT_EQ(5,*limit_array.at(3));
  ASSERT_EQ(5,a[3]);

  ASSERT_EQ(4,limit_array.get_size());
  ASSERT_EQ(4,limit_array.get_index());
  {
    ObArrayHelper<int> hel(4,b);
    limit_array.expand(hel);
  }
  ASSERT_EQ(8,limit_array.get_size());
  ASSERT_EQ(4,limit_array.get_index());

  limit_array.push_back(10);
  limit_array.push_back(11);

  ASSERT_EQ(5,*limit_array.at(3));
  ASSERT_EQ(5,a[3]);

  ASSERT_EQ(10,*limit_array.at(4));
  ASSERT_EQ(10,b[0]);

  ASSERT_EQ(11,*limit_array.pop());
  ASSERT_EQ(10,*limit_array.pop());
  ASSERT_EQ(5,*limit_array.pop());
  limit_array.push_back(30);
  limit_array.push_back(31);
  limit_array.push_back(32);
  ASSERT_EQ(32,*limit_array.at(5));
  ASSERT_EQ(30,a[3]);
  ASSERT_EQ(31,b[0]);
  ASSERT_EQ(32,b[1]);
  ASSERT_EQ(8,limit_array.get_size());
  ASSERT_EQ(6,limit_array.get_index());
}
int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
