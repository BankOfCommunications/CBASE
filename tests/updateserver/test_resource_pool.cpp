#include <gtest/gtest.h>
#include "common/ob_resource_pool.h"

using namespace oceanbase::common;

class A
{
  public:
    A()
    {
      fprintf(stdout, "construct A this=%p\n", this);
    };
    ~A()
    {
      fprintf(stdout, "destruct A this=%p\n", this);
    };
    void reset()
    {
      fprintf(stdout, "reset A this=%p\n", this);
    };
};

TEST(TestResourcePool, get)
{
  A *nil = NULL;
  ObResourcePool<A> rp;

  {
    ObResourcePool<A>::Guard guard(rp);
    A *v1 = rp.get(guard);
    EXPECT_NE(nil, v1);
    A *v2 = rp.get(guard);
    EXPECT_NE(nil, v2);
    A *v3 = rp.get(guard);
    EXPECT_NE(nil, v3);
    A *v4 = rp.get(guard);
    EXPECT_NE(nil, v4);
    A *v5 = rp.get(guard);
    EXPECT_NE(nil, v5);
    EXPECT_NE(v1, v2);
    EXPECT_NE(v2, v3);
    EXPECT_NE(v3, v4);
    EXPECT_NE(v4, v5);
    EXPECT_NE(v5, v1);
  }

  {
    ObResourcePool<A>::Guard guard(rp);
    A *v1 = rp.get(guard);
    EXPECT_NE(nil, v1);
    A *v2 = rp.get(guard);
    EXPECT_NE(nil, v2);
    A *v3 = rp.get(guard);
    EXPECT_NE(nil, v3);
    A *v4 = rp.get(guard);
    EXPECT_NE(nil, v4);
    A *v5 = rp.get(guard);
    EXPECT_NE(nil, v5);
    A *v6 = rp.get(guard);
    EXPECT_NE(nil, v6);
    EXPECT_NE(v1, v2);
    EXPECT_NE(v2, v3);
    EXPECT_NE(v3, v4);
    EXPECT_NE(v4, v5);
    EXPECT_NE(v5, v6);
    EXPECT_NE(v6, v1);
  }

  {
    ObResourcePool<A>::Guard guard(rp);
    fprintf(stdout, "have not alloc\n");
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

