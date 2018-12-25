#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "ob_merge_server_params.h"

using namespace std;
using namespace oceanbase::mergeserver;
using namespace oceanbase;
using namespace oceanbase::common;


int main(int argc, char **argv)
{
  common::ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestMsParams: public ::testing::Test
{
public:
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
};

TEST_F(TestMsParams, test_reload_dump)
{
  ObMergeServerParams param;
  EXPECT_EQ(OB_SUCCESS, param.reload_from_config("mergeserver.conf.template"));
  param.dump_config();
  EXPECT_EQ(OB_SUCCESS, param.dump_config_to_file("my.conf"));
}

TEST_F(TestMsParams, test_update_item)
{
  ObMergeServerParams param;
  EXPECT_EQ(OB_SUCCESS, param.reload_from_config("mergeserver.conf.template"));
  param.dump_config();

  ObObj name;
  ObString name_str;
  char *name_c = (char*)"log_interval_count";
  ObObj value;
  
  name_str.assign(name_c, (const int32_t)strlen(name_c));
  name.set_varchar(name_str);
  value.set_int(20);
  EXPECT_EQ(OB_SUCCESS, param.update_item(name, value));
  EXPECT_TRUE(20 == param.get_log_interval_count());

  name_c = (char*)"wrong_string_log_interval_count";
  name_str.assign(name_c, (const int32_t)strlen(name_c));
  name.set_varchar(name_str);
  value.set_int(30);
  EXPECT_NE(OB_SUCCESS, param.update_item(name, value));
  EXPECT_TRUE(20 == param.get_log_interval_count());
  param.dump_config();
}


