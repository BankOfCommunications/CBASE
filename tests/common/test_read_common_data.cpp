#include "common/ob_read_common_data.h"
#include "gtest/gtest.h"
#include "common/ob_malloc.h"
#include "common/ob_string.h"

using namespace oceanbase;
using namespace oceanbase::common;
TEST(TestObReadParam, basicTest)
{
  char buf[1024];
  int64_t pos = 0;
  int64_t data_len = 0;
  int64_t timestamp = 1000;
  bool is_read_frozen_only = true;
  bool is_result_cached = false;
  ObReadParam param;
  ObReadParam dparam;
  EXPECT_FALSE(param.get_is_read_frozen_only());
  EXPECT_EQ(param.get_timestamp(),0);
  EXPECT_TRUE(param.get_is_result_cached());
  param.set_is_read_frozen_only(is_read_frozen_only);
  param.set_timestamp(timestamp);
  param.set_is_result_cached(is_result_cached);
  EXPECT_TRUE(param.get_is_read_frozen_only());
  EXPECT_EQ(param.get_timestamp(),timestamp);
  EXPECT_FALSE(param.get_is_result_cached());

  EXPECT_EQ(param.serialize(buf,sizeof(buf),pos),0);
  EXPECT_EQ(pos, param.get_serialize_size());
  data_len = pos;
  pos = 0;
  EXPECT_EQ(dparam.deserialize(buf,data_len,pos),0);
  EXPECT_TRUE(dparam.get_is_read_frozen_only());
  EXPECT_EQ(dparam.get_timestamp(),timestamp);
  EXPECT_FALSE(dparam.get_is_result_cached());
}

TEST(TestObScanParam, TestColumnName)
{
  int64_t pos = 0;
  int64_t data_len = 0;
  int64_t orderby_column_size = 0;
  int64_t limit_offset = 0;
  int64_t limit_count = 0;
  const uint64_t *column_ids = NULL;
  const ObString *column_names = NULL;
  const uint8_t *orders = NULL;
  char serialize_buf[2048];
  char column1_name_ptr[512] = "col1";
  char column2_name_ptr[512]= "col2";
  ObString  column1_name_str;
  ObString  column2_name_str;
  column1_name_str.assign(column1_name_ptr,strlen(column1_name_ptr) + 1);
  column2_name_str.assign(column2_name_ptr,strlen(column2_name_ptr) + 1);
  ObScanParam param;
  ObScanParam dparam;
  /// add scan column
  EXPECT_EQ(param.add_column(column1_name_str), 0);
  EXPECT_LT(param.add_column(1ull),0);
  EXPECT_EQ(param.add_column(column2_name_str), 0);
  /// add orderby column 
  EXPECT_EQ(param.add_orderby_column(column1_name_str, ObScanParam::ASC),0);
  EXPECT_LT(param.add_orderby_column(1ull, ObScanParam::ASC),0);
  EXPECT_EQ(param.add_orderby_column(column2_name_str, ObScanParam::DESC),0);
  EXPECT_EQ(param.get_orderby_column_size(),2);
  /// add limit info
  limit_offset = 1000;
  limit_count = 1000;
  EXPECT_EQ(param.set_limit_info(limit_offset, limit_count),0);

  EXPECT_EQ(param.serialize(serialize_buf,sizeof(serialize_buf),pos),0);
  EXPECT_EQ(pos, param.get_serialize_size());
  data_len = pos;
  pos = 0;
  EXPECT_EQ(dparam.deserialize(serialize_buf,data_len,pos),0);

  EXPECT_EQ(dparam.get_column_name_size(),2);
  EXPECT_EQ(dparam.get_column_id_size(),0);

  EXPECT_EQ(dparam.get_orderby_column_size(),2);
  dparam.get_orderby_column(column_names,orders, orderby_column_size);
  EXPECT_EQ(orderby_column_size, 2);
  EXPECT_EQ(orders[0], ObScanParam::ASC);
  EXPECT_EQ(orders[1], ObScanParam::DESC);
  EXPECT_TRUE(column_names[0] == column1_name_str);
  EXPECT_TRUE(column_names[1] == column2_name_str);
  dparam.get_orderby_column(column_ids,orders,orderby_column_size);
  EXPECT_EQ(column_ids, reinterpret_cast<uint64_t*>(0));
  EXPECT_EQ(orderby_column_size, 2);

  dparam.get_limit_info(limit_offset,limit_count);
  EXPECT_EQ(limit_offset,1000);
  EXPECT_EQ(limit_count,1000);
  column_names =  dparam.get_column_name();
  EXPECT_TRUE(column_names[0] == column1_name_str);
  EXPECT_TRUE(column_names[1] == column2_name_str);
}


TEST(TestObScanParam, TestColumnId)
{
  int64_t pos = 0;
  int64_t data_len = 0;
  int64_t orderby_column_size = 0;
  int64_t limit_offset = 0;
  int64_t limit_count = 0;
  const uint64_t *column_ids = NULL;
  const ObString *column_names = NULL;
  const uint8_t *orders = NULL;
  char serialize_buf[2048];
  char column1_name_ptr[512] = "col1";
  char column2_name_ptr[512]= "col2";
  ObString  column1_name_str;
  ObString  column2_name_str;
  column1_name_str.assign(column1_name_ptr,strlen(column1_name_ptr) + 1);
  column2_name_str.assign(column2_name_ptr,strlen(column2_name_ptr) + 1);
  uint64_t column_id1 = 1;
  uint64_t column_id2 = 2;
  ObScanParam param;
  ObScanParam dparam;
  /// add scan column
  EXPECT_EQ(param.add_column(column_id1), 0);
  EXPECT_LT(param.add_column(column1_name_str),0);
  EXPECT_EQ(param.add_column(column_id2), 0);
  /// add orderby column 
  EXPECT_EQ(param.add_orderby_column(column_id1, ObScanParam::ASC),0);
  EXPECT_LT(param.add_orderby_column(column1_name_str, ObScanParam::ASC),0);
  EXPECT_EQ(param.add_orderby_column(column_id2, ObScanParam::DESC),0);
  EXPECT_EQ(param.get_orderby_column_size(),2);
  /// add limit info
  limit_offset = 1000;
  limit_count = 1000;
  EXPECT_EQ(param.set_limit_info(limit_offset, limit_count),0);

  EXPECT_EQ(param.serialize(serialize_buf,sizeof(serialize_buf),pos),0);
  EXPECT_EQ(pos, param.get_serialize_size());
  data_len = pos;
  pos = 0;
  EXPECT_EQ(dparam.deserialize(serialize_buf,data_len,pos),0);

  EXPECT_EQ(dparam.get_column_id_size(),2);
  EXPECT_EQ(dparam.get_column_name_size(),0);

  EXPECT_EQ(dparam.get_orderby_column_size(),2);
  dparam.get_orderby_column(column_ids,orders, orderby_column_size);
  EXPECT_EQ(orderby_column_size, 2);
  EXPECT_EQ(orders[0], ObScanParam::ASC);
  EXPECT_EQ(orders[1], ObScanParam::DESC);
  EXPECT_TRUE(column_ids[0] == 1);
  EXPECT_TRUE(column_ids[1] == 2);
  dparam.get_orderby_column(column_names,orders,orderby_column_size);
  EXPECT_EQ(column_names, reinterpret_cast<ObString*>(0));
  EXPECT_EQ(orderby_column_size, 2);

  dparam.get_limit_info(limit_offset,limit_count);
  EXPECT_EQ(limit_offset,1000);
  EXPECT_EQ(limit_count,1000);
  column_ids =  dparam.get_column_id();
  EXPECT_TRUE(column_ids[0] == 1);
  EXPECT_TRUE(column_ids[1] == 2);
}

int main(int argc, char **argv)
{
  oceanbase::common::ob_init_memory_pool();

  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
