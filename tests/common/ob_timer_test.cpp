#include <gtest/gtest.h>
#include <stdint.h>
#include <tbsys.h>
#include "common/ob_define.h"
#include "common/ob_timer.h"

using namespace oceanbase;
using namespace oceanbase::common;

class voidTask : public ObTimerTask
{
  public:
    ~voidTask() {};
    void runTimerTask() 
    {
      printf("Hello,\n");
    }
};

class voidTask2 : public ObTimerTask
{
  public:
    ~voidTask2() {};
    void runTimerTask() 
    {
      printf("world\n");
    }
};

class voidTask3 : public ObTimerTask
{
  public:
    ~voidTask3() {};
    void runTimerTask() 
    {
      printf("Timer\n");
    }
};

TEST(TIMER,test)
{
  ObTimer timer;
  timer.init();
  voidTask task;
  voidTask2 task2;
  voidTask3 task3;
  timer.schedule(task,5000000,true);
  timer.schedule(task2,4000000,true);
  timer.schedule(task3,3000000,true);
  ASSERT_EQ(timer.get_tasks_num(),3);
  sleep(6);
  timer.dump();
  ASSERT_EQ(timer.get_tasks_num(),3);
  timer.cancel(task);
  ASSERT_EQ(timer.get_tasks_num(),2);
  timer.dump();
  sleep(5);
  timer.dump();
  timer.destroy();
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

