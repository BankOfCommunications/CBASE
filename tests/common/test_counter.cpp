#define __OB_UNIT_TEST
#include <gtest/gtest.h>
#include <stdlib.h>
#include <time.h>
#include <vector>
#include <string>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include "common/ob_cell_array.h"
#include "common/ob_define.h"
#include "common/ob_cache.h"
#include "common/ob_string.h"
#include "common/ob_action_flag.h"
#include "common/ob_scan_param.h"
#include "common/ob_counter.h"

using namespace oceanbase::common;
using namespace testing;
using namespace std;

class ObTestCounterInfo : public ObCounterInfos
{
public:
  virtual const char * get_counter_name(const int64_t counter_id) const
  {
    UNUSED(counter_id);
    return "test_counter";
  }
  virtual int64_t get_counter_static_interval(const int64_t counter_id) const
  {
    UNUSED(counter_id);
    return 10000;
  }
  virtual int64_t get_max_counter_size() const
  {
    return 1;
  }
};

TEST(ObCounter, basic)
{
  ObCounterSet & c_set = *(new ObCounterSet);
  ObTestCounterInfo test_info;
  EXPECT_EQ(c_set.init(test_info),OB_SUCCESS);
  int64_t counter_id = 0;
  int count = 1000;
  int timeused = 0;
  for(int32_t i = 0; i < count; i++)
  {
    timeused+=i;
    c_set.inc(counter_id,i);
  }
  for(int j =1; j < 5; j ++)
  {
    EXPECT_EQ(c_set.get_avg_count(counter_id, j), count);
    EXPECT_EQ(c_set.get_avg_time_used(counter_id, j), timeused/count);
  }
  usleep(50000);
  EXPECT_EQ(c_set.get_avg_count(counter_id, 1), 0);
  EXPECT_GE(c_set.get_avg_count(counter_id, 6), count/6);

  usleep(10000*5000);
  EXPECT_EQ(c_set.get_avg_count(counter_id, 1), 0);
  EXPECT_GE(c_set.get_avg_count(counter_id, 5000), 0);
}

TEST(ObCounter, double_round)
{
  ObCounterSet & c_set = *(new ObCounterSet);
  ObTestCounterInfo test_info;
  EXPECT_EQ(c_set.init(test_info),OB_SUCCESS);
  int64_t counter_id = 0;

  for(int32_t i = 0; i < 5000; i ++)
  {
    c_set.inc(counter_id,10);
    c_set.inc(counter_id,10);
    c_set.inc(counter_id,10);
    usleep(9500);
  }
  EXPECT_GE(c_set.get_avg_count(counter_id, 4096), 2);
  EXPECT_EQ(c_set.get_avg_time_used(counter_id, 4096), 10);

  for(int32_t i = 0; i < 5000; i ++)
  {
    c_set.inc(counter_id,100);
    c_set.inc(counter_id,100);
    c_set.inc(counter_id,100);
    c_set.inc(counter_id,100);
    usleep(9500);
  }
  EXPECT_GE(c_set.get_avg_count(counter_id, 4096), 3);
  EXPECT_EQ(c_set.get_avg_time_used(counter_id, 4096), 100);
  c_set.print_static_info(TBSYS_LOG_LEVEL_WARN);
}




int main(int argc, char **argv)
{
  srandom(static_cast<uint32_t>(time(NULL)));
  ob_init_memory_pool(64*1024);
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
