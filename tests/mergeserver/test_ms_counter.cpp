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
#include "mergeserver/ob_ms_counter_infos.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace testing;
using namespace std;


TEST(ObCounter, basic)
{
  ObCounterSet & c_set = ms_get_counter_set();
  EXPECT_EQ(c_set.init(ms_get_counter_infos()), OB_SUCCESS);
  int64_t counter_id = ObMergerCounterIds::C_MS_TEST;
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
  c_set.print_static_info(TBSYS_LOG_LEVEL_WARN);
  usleep(50000);
  EXPECT_EQ(c_set.get_avg_count(counter_id, 1), 0);
  EXPECT_GE(c_set.get_avg_count(counter_id, 6), count/6);

  usleep(10000*5000);
  EXPECT_EQ(c_set.get_avg_count(counter_id, 1), 0);
  EXPECT_GE(c_set.get_avg_count(counter_id, 5000), 0);
}

TEST(ObCounter, double_round)
{
  ObCounterSet & c_set = ms_get_counter_set();
  EXPECT_EQ(c_set.init(ms_get_counter_infos()), OB_SUCCESS);
  int64_t counter_id = ObMergerCounterIds::C_MS_TEST;

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
