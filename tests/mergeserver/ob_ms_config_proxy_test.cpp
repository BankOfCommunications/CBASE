#include <tbsys.h>
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "ob_merge_server_params.h"
#include "ob_ms_config_proxy.h"
#include "ob_ms_config_manager.h"
#include "common/ob_nb_accessor.h"
#include "mock_ob_nb_accessor.h"

using namespace std;
using namespace oceanbase::mergeserver;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace testing;

int main(int argc, char **argv)
{
  common::ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestMsConfigProxy: public ::testing::Test
{
public:
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
};

TEST_F(TestMsConfigProxy, test_init_proxy)
{
  MockObNbAccessor accessor;
  ObMergerConfigManager manager;
  ObMergeServerParams params;
  EXPECT_EQ(OB_SUCCESS, manager.init(&params));
  ObMergerConfigProxy proxy(&accessor, &manager);
  EXPECT_EQ(true, proxy.check_inner_stat());
}


MockQueryRes my_query_res;
MockTableRow my_table_row;
ObCellInfo my_cell_info[3];
char *config_item_1 = (char*)"retry_times";
char *config_item_2 = (char*)"network_timeout_us";

int callback_func_scan(QueryRes *&res, Unused, Unused, Unused, Unused)
{
  res = &my_query_res;
  return OB_SUCCESS;
}

int callbakc_func_get_row(TableRow **row)
{
  *row = &my_table_row;
  return OB_SUCCESS;
}

int callbakc_func_next_row()
{
  int static i = 0;
  i++;
  return (i < 3) ? OB_SUCCESS : OB_ITER_END;
}

ObCellInfo *callback_func_get_cell_info(const char* column_name)
{
  TBSYS_LOG(INFO, "%s", column_name);
  if (0 == strcmp(column_name, "name"))
  {
    ObString str;
    str.assign(config_item_1, (int32_t)strlen(config_item_1));
    my_cell_info[0].value_.set_varchar(str);
    return &my_cell_info[0];
  }
  else if (0 == strcmp(column_name, "value"))
  {
    my_cell_info[1].value_.set_int(102);
    return &my_cell_info[1];
  }
  else
  {
    return NULL;
  }
}


TEST_F(TestMsConfigProxy, test_proxy_scan)
{
  MockObNbAccessor accessor;
  ObMergerConfigManager manager;
  ObMergeServerParams params;
  tbsys::CConfig config;
  ASSERT_EQ(EXIT_SUCCESS, config.load("mergeserver.conf.template"));
  ASSERT_EQ(OB_SUCCESS, params.load_from_config(config));
  params.reload_from_config("mergeserver.conf.template");
  params.dump_config();

  EXPECT_CALL(accessor, scan(_,_,_,_,_))
    .Times(AtLeast(1))
    .WillRepeatedly(Invoke(callback_func_scan));

  EXPECT_CALL(my_query_res, get_row(_))
    .WillRepeatedly(Invoke(callbakc_func_get_row));

  EXPECT_CALL(my_query_res, next_row())
    .WillRepeatedly(Invoke(callbakc_func_next_row));

  EXPECT_CALL(my_table_row, get_cell_info(_))
    .WillRepeatedly(Invoke(callback_func_get_cell_info));

  EXPECT_EQ(OB_SUCCESS, manager.init(&params));
  ObMergerConfigProxy proxy(&accessor, &manager);
  EXPECT_EQ(true, proxy.check_inner_stat());
  EXPECT_EQ(OB_SUCCESS, proxy.fetch_config(0));

}







