#include "ob_define.h"
#include "gtest/gtest.h"
#include "ob_probability_random.h"

using namespace oceanbase::common;

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestPercentRandom: public ::testing::Test
{
public:
  
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
};

const static int32_t MAX_COUNT = 10000;

TEST_F(TestPercentRandom, init)
{
  ObProbabilityRandom random;
  int32_t percent[10];
  memset(percent, 0, sizeof(percent));
  percent[0] = 11;
  percent[1] = 0;
  percent[2] = -1;
  // init failed
  EXPECT_TRUE(OB_SUCCESS != random.init(percent, 10));
  EXPECT_TRUE(-1 == random.random());
  percent[2] = 25;
  percent[3] = 44;
  percent[4] = 20;
  int32_t counter[10];
  memset(counter, 0, sizeof(counter));

  EXPECT_TRUE(OB_SUCCESS != random.init(percent, 0));
  EXPECT_TRUE(OB_SUCCESS == random.init(percent, 5));
  
  for (int32_t i = 0; i < MAX_COUNT; ++i)
  {
    ++counter[random.random()];
  }
  printf("result output:\n"); 
  for (int32_t i = 0; i < 5; ++i)
  {
    printf("counter:percent[%d], count[%d]\n", percent[i], counter[i]);
  }
  percent[6] = 10;
  percent[9] = 34;
  percent[8] = 10;
  EXPECT_TRUE(OB_SUCCESS == random.init(percent, 10));
  memset(counter, 0, sizeof(counter));
  for (int32_t i = 0; i < MAX_COUNT; ++i)
  {
    ++counter[random.random()];
  }
  printf("result output:\n"); 
  for (int32_t i = 0; i < 10; ++i)
  {
    printf("counter:percent[%d], count[%d]\n", percent[i], counter[i]);
  }

  // fail but use old percent
  percent[2] = -1;
  EXPECT_TRUE(OB_SUCCESS != random.init(percent, 10));
  memset(counter, 0, sizeof(counter));
  for (int32_t i = 0; i < MAX_COUNT; ++i)
  {
    ++counter[random.random()];
  }
  percent[2] = 25;
  printf("result output:\n"); 
  for (int32_t i = 0; i < 10; ++i)
  {
    printf("counter:percent[%d], count[%d]\n", percent[i], counter[i]);
  }

  percent[0] = 0;
  percent[1] = 0;
  memset(counter, 0, sizeof(counter));
  EXPECT_TRUE(OB_SUCCESS != random.init(percent, 2));
}

TEST_F(TestPercentRandom, random1)
{
  int32_t percent[10];
  percent[0] = 11;
  percent[1] = 0;
  percent[2] = 25;
  percent[3] = 44;
  percent[4] = 20;
  ObProbabilityRandom random;
  int32_t count = 5;
  EXPECT_TRUE(OB_SUCCESS == random.init(percent, count));

  int32_t counter[10];
  memset(counter, 0, sizeof(counter));
  for (int32_t i = 0; i < MAX_COUNT; ++i)
  {
    ++counter[random.random()];
  }
  printf("result output:\n"); 
  for (int32_t i = 0; i < count; ++i)
  {
    printf("counter:percent[%d], count[%d]\n", percent[i], counter[i]);
  }

  percent[0] = 10;
  percent[1] = 20;
  percent[2] = 30;
  percent[3] = 40;
  count = 4;
  EXPECT_TRUE(OB_SUCCESS == random.init(percent, count));
  memset(counter, 0, sizeof(counter));
  for (int32_t i = 0; i < MAX_COUNT; ++i)
  {
    ++counter[random.random()];
  }
  printf("result output:\n"); 
  for (int32_t i = 0; i < count; ++i)
  {
    printf("counter:percent[%d], count[%d]\n", percent[i], counter[i]);
  }

  percent[0] = 0;
  percent[1] = 1;
  percent[2] = 0;
  percent[3] = 1;
  percent[4] = 2;
  percent[5] = 1;
  percent[6] = 2;
  count = 7;
  EXPECT_TRUE(OB_SUCCESS == random.init(percent, count));
  memset(counter, 0, sizeof(counter));
  for (int32_t i = 0; i < MAX_COUNT; ++i)
  {
    ++counter[random.random()];
  }
  printf("result output:\n"); 
  for (int32_t i = 0; i < count; ++i)
  {
    printf("counter:percent[%d], count[%d]\n", percent[i], counter[i]);
  }

  count = 6;
  EXPECT_TRUE(OB_SUCCESS == random.init(percent, count));
  memset(counter, 0, sizeof(counter));
  for (int32_t i = 0; i < MAX_COUNT; ++i)
  {
    ++counter[random.random()];
  }
  printf("result output:\n"); 
  for (int32_t i = 0; i < count; ++i)
  {
    printf("counter:percent[%d], count[%d]\n", percent[i], counter[i]);
  }
}

TEST_F(TestPercentRandom, random2)
{
  int32_t percent[5];
  percent[0] = 11;
  percent[1] = 0;
  percent[2] = 25;
  percent[3] = 44;
  percent[4] = 20;

  int32_t counter[5];
  memset(counter, 0, sizeof(counter));
  for (int32_t i = 0; i < MAX_COUNT; ++i)
  {
    ++counter[ObStalessProbabilityRandom::random(percent, 5, 100)];
  }

  printf("result output:\n"); 
  for (int32_t i = 0; i < 5; ++i)
  {
    printf("counter:percent[%d], count[%d]\n", percent[i], counter[i]);
  }

  percent[0] = 10;
  percent[1] = 20;
  percent[2] = 30;
  percent[3] = 40;
  memset(counter, 0, sizeof(counter));
  for (int32_t i = 0; i < MAX_COUNT; ++i)
  {
    ++counter[ObStalessProbabilityRandom::random(percent, 4, 100)];
  }
  printf("result output:\n"); 
  for (int32_t i = 0; i < 4; ++i)
  {
    printf("counter:percent[%d], count[%d]\n", percent[i], counter[i]);
  }
}

bool is_change = false;
int32_t count = 7;

void * random_routine(void * argv)
{
  ObProbabilityRandom * random = (ObProbabilityRandom *) (argv);
  int32_t counter[10];
  memset(counter, 0, sizeof(counter));
  for (int32_t i = 0; i < MAX_COUNT; ++i)
  {
    ++counter[random->random()];
  }

  printf("first test result output:\n"); 
  for (int32_t i = 0; i < count; ++i)
  {
    printf("counter:count[%d]\n", counter[i]);
  }
  // wait change
  while (!is_change)
  {
    usleep(10);
  }

  for (int32_t i = 0; i < MAX_COUNT; ++i)
  {
    ++counter[random->random()];
  }
  printf("second test result output:\n"); 
  for (int32_t i = 0; i < count; ++i)
  {
    printf("counter:count[%d]\n", counter[i]);
  }
  return NULL;
}

#if false
// multi-thread
TEST_F(TestPercentRandom, random3)
{
  int32_t percent[10];
  percent[0] = 11;
  percent[1] = 0;
  percent[2] = 25;
  percent[3] = 44;
  percent[4] = 20;
  percent[5] = 10;
  percent[6] = 21;
  ObProbabilityRandom random;
  EXPECT_TRUE(OB_SUCCESS == random.init(percent, count));

  const static int32_t MAX_THREAD = 35; 
  pthread_t thread[MAX_THREAD];
  for (int32_t i = 0; i < MAX_THREAD; ++i)
  {
    EXPECT_TRUE(OB_SUCCESS == pthread_create(&thread[i], NULL, random_routine, &random));
  }

  usleep(1000);
  is_change = true;
  count = 5;
  usleep(1000);
  // change the percent
  percent[0] = 0;
  percent[1] = 0;
  percent[2] = 10;
  percent[3] = 10;
  percent[4] = 20;
  EXPECT_TRUE(OB_SUCCESS == random.init(percent, count));
  for (int32_t i = 0; i < MAX_THREAD; ++i)
  {
    EXPECT_TRUE(OB_SUCCESS == pthread_join(thread[i], NULL));
  }
}

#endif
