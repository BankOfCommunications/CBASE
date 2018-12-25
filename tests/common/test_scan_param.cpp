#include <math.h>
#include "gtest/gtest.h"
#include "ob_malloc.h"
#include "ob_range.h"
#include "ob_read_common_data.h"
#include "test_rowkey_helper.h"

static CharArena allocator_;

using namespace oceanbase;
using namespace common;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestScanParam: public ::testing::Test
{
public:

  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
};


bool is_equal(const ObScanParam & one, const ObScanParam & two)
{
  bool bret = false;
  if (&one == &two)
  {
    bret = true;
  }
  else
  {
    bret = (one.get_table_id() == two.get_table_id())
    && (one.get_table_name() == two.get_table_name())
      && (one.get_is_result_cached() == two.get_is_result_cached())
      && (one.get_is_read_consistency() == two.get_is_read_consistency())
      && (one.get_is_result_cached() == two.get_is_result_cached())
      && (one.get_is_read_consistency() == two.get_is_read_consistency())
      // && (*one.get_range() == *two.get_range())
    && (one.get_scan_size() == two.get_scan_size())
    && (one.get_scan_direction() == two.get_scan_direction())
    && (one.get_column_name_size() == two.get_column_name_size())
    && (one.get_column_id_size() == two.get_column_id_size())
    && (const_cast<ObScanParam &>(one).get_filter_info() == const_cast<ObScanParam &>(two).get_filter_info())
    && (one.get_orderby_column_size() == two.get_orderby_column_size());

    int64_t limit, limit1, count, count1, sharding_minimum, topk1;
    double precision, precision1;
    one.get_limit_info(limit, count);
    one.get_topk_precision(sharding_minimum, precision);
    two.get_limit_info(limit1, count1);
    two.get_topk_precision(topk1, precision1);
    bret = bret && (limit == limit1) && (count == count1) && (precision == precision1);
    // not check list items
  }
  return bret;
}

TEST(TestScanParam, set_condition)
{
  ObScanParam param;
  param.set_is_result_cached(true);

  // version
  ObVersionRange range;
  range.border_flag_.set_min_value();
  range.border_flag_.set_max_value();
  range.start_version_ = 11;
  range.end_version_ = 2001;
  param.set_version_range(range);

  // table name
  uint64_t table_id = 123;
  ObString table_name;
  ObNewRange scan_range;

  ObString key;
  scan_range.table_id_ = 23455;
  char * start_key = (char*)"start_row_key_start";
  char * end_key = (char*)"end_row_key_end";
  scan_range.end_key_.set_max_row();
  key.assign(start_key, static_cast<int32_t>(strlen(start_key)));
  scan_range.start_key_ = TestRowkeyHelper(key, &allocator_);
  key.assign(end_key, static_cast<int32_t>(strlen(end_key)));
  scan_range.end_key_ = TestRowkeyHelper(key, &allocator_);
  param.set(table_id, table_name, scan_range);

  ObString column;
  char * temp_buff[32];
  for (uint64_t i = 11; i < 21; ++i)
  {
    temp_buff[i] = new char[32];
    sprintf(temp_buff[i], "%lu_scan_column_%lu", i, i);
    column.assign(temp_buff[i], static_cast<int32_t>(strlen(temp_buff[i])));
    EXPECT_TRUE(OB_SUCCESS == param.add_column(column));
  }

  ObLogicOperator operate = LIKE;
  char * ptr = (char*)"test_test";
  ObString sub_string;
  sub_string.assign(ptr, static_cast<int32_t>(strlen(ptr)));
  ObObj operand;
  operand.set_varchar(sub_string);

  for (uint64_t i = 11; i < 21; ++i)
  {
    sprintf(temp_buff[i], "%lu_scan_column_%lu", i, i);
    column.assign(temp_buff[i], static_cast<int32_t>(strlen(temp_buff[i])));
    EXPECT_TRUE(OB_SUCCESS == param.add_where_cond(column, operate, operand));
  }

  char temp_name[1024] = "";
  for (int64_t i = 22; i < 32; ++i)
  {
    sprintf(temp_name, "%lu_scan_column_%lu", i, i);
    column.assign(temp_name, static_cast<int32_t>(strlen(temp_name)));
    EXPECT_TRUE(OB_SUCCESS == param.add_where_cond(column, operate, operand));
  }
}

TEST(TestScanParam, safe_copy)
{
  ObScanParam param;
  // cache
  param.set_is_result_cached(true);
  param.set_scan_direction(ScanFlag::BACKWARD);
  // version
  ObVersionRange range;
  range.border_flag_.set_min_value();
  range.border_flag_.set_max_value();
  range.start_version_ = 11;
  range.end_version_ = 2001;
  param.set_version_range(range);

  // table name
  uint64_t table_id = 123;
  ObString table_name;
  ObNewRange scan_range;

  char * start_key = (char*)"start_row_key_start";
  char * end_key = (char*)"end_row_key_end";
  ObString key;
  scan_range.table_id_ = 23455;
  scan_range.end_key_.set_max_row();
  key.assign(start_key, static_cast<int32_t>(strlen(start_key)));
  scan_range.start_key_ = TestRowkeyHelper(key, &allocator_);
  key.assign(end_key, static_cast<int32_t>(strlen(end_key)));
  scan_range.end_key_ = TestRowkeyHelper(key, &allocator_);
  param.set(table_id, table_name, scan_range);
  uint64_t offset = 234;
  uint64_t count = 12345555;
  param.set_limit_info(offset, count);

  for (uint64_t i = 11; i < 21; ++i)
  {
    EXPECT_TRUE(OB_SUCCESS == param.add_column(i));
  }

  {
    ObScanParam temp_param;
    temp_param.safe_copy(param);
    EXPECT_TRUE(is_equal(temp_param, param));
    temp_param.clear_column();
    EXPECT_FALSE(is_equal(temp_param, param));
  }

  // ObScanParam temp_param = param;
  // EXPECT_TRUE(is_equal(temp_param, param));
}


TEST(TestScanParam, serialize_int)
{
  ObScanParam param;
  // cache
  param.set_is_result_cached(true);
  param.set_is_read_consistency(false);
  param.set_scan_direction(ScanFlag::BACKWARD);
  // version
  ObVersionRange range;
  range.border_flag_.set_min_value();
  range.border_flag_.set_max_value();
  range.start_version_ = 11;
  range.end_version_ = 2001;
  param.set_version_range(range);

  // table name
  uint64_t table_id = 123;
  ObString table_name;
  ObNewRange scan_range;

  char * start_key = (char*)"start_row_key_start";
  char * end_key = (char*)"end_row_key_end";
  ObString key;
  scan_range.table_id_ = 23455;
  scan_range.end_key_.set_max_row();
  key.assign(start_key, static_cast<int32_t>(strlen(start_key)));
  scan_range.start_key_ = TestRowkeyHelper(key, &allocator_);
  key.assign(end_key, static_cast<int32_t>(strlen(end_key)));
  scan_range.end_key_ = TestRowkeyHelper(key, &allocator_);
  param.set(table_id, table_name, scan_range);

  for (uint64_t i = 11; i < 21; ++i)
  {
    EXPECT_TRUE(OB_SUCCESS == param.add_column(i));
  }

  uint64_t size = 1234;
  param.set_scan_size(size);
  param.set_scan_direction(ScanFlag::BACKWARD);
  for (uint64_t i = 21; i < 33; ++i)
  {
    if (i % 2)
    {
      EXPECT_TRUE(OB_SUCCESS == param.add_orderby_column(i, ObScanParam::ASC));
    }
    else
    {
      EXPECT_TRUE(OB_SUCCESS == param.add_orderby_column(i, ObScanParam::DESC));
    }
  }

  uint64_t offset = 234;
  uint64_t count = 12345555;
  param.set_limit_info(offset, count);

  // no filter
  size = param.get_serialize_size();
  EXPECT_TRUE(size > 0);

  char * temp = new char[size];
  EXPECT_TRUE(NULL != temp);
  int64_t pos = 0;
  // size small
  EXPECT_TRUE(OB_SUCCESS != param.serialize(temp, size - 1, pos));
  pos = 0;
  EXPECT_TRUE(OB_SUCCESS == param.serialize(temp, size, pos));
  EXPECT_TRUE((int64_t)size == pos);

  ObScanParam temp_param;
  int64_t new_pos = 0;
  EXPECT_TRUE(OB_SUCCESS != temp_param.deserialize(temp, size - 1, new_pos));
  new_pos = 0;
  EXPECT_TRUE(OB_SUCCESS == temp_param.deserialize(temp, size, new_pos));
  EXPECT_TRUE(pos == new_pos);

  EXPECT_TRUE(param.get_is_result_cached() == true);
  EXPECT_TRUE(temp_param.get_is_result_cached() == true);
  EXPECT_TRUE(temp_param.get_is_read_consistency() == false);
  EXPECT_TRUE(param.get_is_read_consistency() == false);

  // same as each other
  EXPECT_TRUE(is_equal(temp_param, param));
  EXPECT_TRUE(is_equal(param, temp_param));
  delete temp;
  temp = NULL;
}

TEST(TestScanParam, serialize_string)
{
  ObScanParam param;
  // cache
  param.set_is_result_cached(false);
  param.set_scan_direction(ScanFlag::BACKWARD);

  // version
  ObVersionRange range;
  range.border_flag_.set_min_value();
  range.start_version_ = 201;
  range.end_version_ = 1;
  param.set_version_range(range);

  // table name
  char * name = (char*)"table_test";
  ObString table_name;
  table_name.assign(name, static_cast<int32_t>(strlen(name)));

  ObNewRange scan_range;
  char * start_key = (char*)"start_row_key_start";
  char * end_key = (char*)"end_row_key_end";
  ObString key;

  scan_range.table_id_ = 234;
  scan_range.end_key_.set_max_row();
  key.assign(start_key, static_cast<int32_t>(strlen(start_key)));
  scan_range.start_key_ = TestRowkeyHelper(key, &allocator_);
  key.assign(end_key, static_cast<int32_t>(strlen(end_key)));
  scan_range.end_key_ = TestRowkeyHelper(key, &allocator_);
  param.set(OB_INVALID_ID, table_name, scan_range);

  ObString column;
  char * temp_buff[32];
  for (uint64_t i = 11; i < 21; ++i)
  {
    temp_buff[i] = new char[32];
    sprintf(temp_buff[i], "%lu_scan_column_%lu", i, i);
    column.assign(temp_buff[i], static_cast<int32_t>(strlen(temp_buff[i])));
    EXPECT_TRUE(OB_SUCCESS == param.add_column(column));
  }

  uint64_t size = 23;
  param.set_scan_size(size);
  param.set_scan_direction(ScanFlag::FORWARD);
  for (uint64_t i = 21; i < 33; ++i)
  {
    temp_buff[i] = new char[32];
    sprintf(temp_buff[i], "%lu_order_column_%lu", i, i);
    column.assign(temp_buff[i], static_cast<int32_t>(strlen(temp_buff[i])));
    if (i % 2)
    {
      EXPECT_TRUE(OB_SUCCESS == param.add_orderby_column(column, ObScanParam::DESC));
    }
    else
    {
      EXPECT_TRUE(OB_SUCCESS == param.add_orderby_column(column, ObScanParam::ASC));
    }
  }

  uint64_t offset = 2340;
  uint64_t count = 1234;
  param.set_limit_info(offset, count);

  // filter
  ObLogicOperator operate = LIKE;
  char * ptr = (char*)"test_test";
  ObString sub_string;
  sub_string.assign(ptr, static_cast<int32_t>(strlen(ptr)));
  ObObj operand;
  operand.set_varchar(sub_string);
  ObString column_name;
  char temp_name[1024] = "";
  for (uint64_t i = 11; i < 21; ++i)
  {
    snprintf(temp_name, sizeof(temp_name), "%lu_scan_column_%lu", i, i);
    column_name.assign(temp_name, static_cast<int32_t>(strlen(temp_name)));
    EXPECT_TRUE(param.add_where_cond(column_name, operate, operand) == OB_SUCCESS);
  }

  size = param.get_serialize_size();
  EXPECT_TRUE(size > 0);

  char * temp = new char[size];
  EXPECT_TRUE(NULL != temp);
  int64_t pos = 0;
  // size small
  EXPECT_TRUE(OB_SUCCESS != param.serialize(temp, size - 1, pos));
  pos = 0;
  EXPECT_TRUE(OB_SUCCESS == param.serialize(temp, size, pos));
  EXPECT_TRUE((int64_t)size == pos);

  ObScanParam temp_param;
  int64_t new_pos = 0;
  EXPECT_TRUE(OB_SUCCESS != temp_param.deserialize(temp, size - 1, new_pos));
  new_pos = 0;
  EXPECT_TRUE(OB_SUCCESS == temp_param.deserialize(temp, size, new_pos));
  EXPECT_TRUE(pos == new_pos);

  EXPECT_TRUE(param.get_is_result_cached() == false);
  EXPECT_TRUE(temp_param.get_is_result_cached() == false);
  EXPECT_TRUE(temp_param.get_is_read_consistency() == true);
  EXPECT_TRUE(param.get_is_read_consistency() == true);

  // same as each other
  EXPECT_TRUE(is_equal(temp_param, param));
  EXPECT_TRUE(is_equal(param, temp_param));
  delete temp;
  temp = NULL;
}

TEST(TestScanParam, serialize_empty)
{
  ObScanParam param;
  param.set_scan_direction(ScanFlag::FORWARD);
  // version
  char * name = (char*)"table_test";
  ObString table_name;
  table_name.assign(name, static_cast<int32_t>(strlen(name)));

  ObNewRange scan_range;
  scan_range.border_flag_.unset_inclusive_start();
  scan_range.border_flag_.set_inclusive_end();
  scan_range.set_whole_range();
  param.set(OB_INVALID_ID, table_name, scan_range);

  // no filter
  int size = static_cast<int32_t>(param.get_serialize_size());
  EXPECT_TRUE(size > 0);

  char * temp = new char[size];
  EXPECT_TRUE(NULL != temp);
  int64_t pos = 0;
  // size small
  EXPECT_TRUE(OB_SUCCESS != param.serialize(temp, size - 1, pos));
  pos = 0;
  EXPECT_TRUE(OB_SUCCESS == param.serialize(temp, size, pos));
  EXPECT_TRUE((int64_t)size == pos);

  ObScanParam temp_param;
  int64_t new_pos = 0;
  EXPECT_TRUE(OB_SUCCESS != temp_param.deserialize(temp, size - 1, new_pos));
  new_pos = 0;
  EXPECT_TRUE(OB_SUCCESS == temp_param.deserialize(temp, size, new_pos));
  EXPECT_TRUE(pos == new_pos);
  // same as each other
  EXPECT_TRUE(is_equal(temp_param, param));
  EXPECT_TRUE(is_equal(param, temp_param));
  delete temp;
  temp = NULL;
}

TEST(TestScanParam, set_both_table_name_and_id)
{
  ObScanParam sp;
  char* tn = (char*)"table_name";
  int tn_len = static_cast<int32_t>(strlen(tn));
  char* rk = (char*)"123";
  int rk_len = static_cast<int32_t>(strlen(rk));

  ObString table_name(tn_len, tn_len, tn);
  ObString rowkey(rk_len, rk_len, rk);

  ObNewRange range;
  range.start_key_ = TestRowkeyHelper(rowkey, &allocator_);
  range.end_key_ = TestRowkeyHelper(rowkey, &allocator_);
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();

  ObVersionRange vrange;
  vrange.border_flag_.set_min_value();
  vrange.start_version_ = 1;
  vrange.end_version_ = 10;

  sp.set(1, table_name, range);
  sp.set_version_range(vrange);

  char buf[BUFSIZ];
  int64_t pos = 0;
  ASSERT_EQ(sp.serialize(buf, BUFSIZ, pos), OB_ERROR);
}

TEST(ObScanParam, str_expr_composite_column)
{
  char serialize_buf[2048];
  int64_t buf_len = sizeof(serialize_buf);
  UNUSED(buf_len);
  int64_t pos = 0;
  UNUSED(pos);
  int64_t data_len = 0;
  UNUSED(data_len);
  ObStringBuf buffer;

  ObScanParam param;
  ObScanParam decoded_param;

  ObString stored_expr;
  ObString stored_cname;
  ObString stored_table_name;
  ObString str;
  const char *c_str = NULL;
  ObNewRange range;
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  range.set_whole_range();
  c_str = "table";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_table_name), OB_SUCCESS);
  EXPECT_EQ(param.set(OB_INVALID_ID, stored_table_name, range, true), OB_SUCCESS);

  c_str = "cname";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_cname), OB_SUCCESS);
  EXPECT_EQ(param.add_column(stored_cname,false), OB_SUCCESS);

  c_str = "`a` + `b`";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_expr), OB_SUCCESS);

  c_str = "a_add_b";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_cname), OB_SUCCESS);
  EXPECT_EQ(param.add_column(stored_expr, stored_cname), OB_SUCCESS);

  EXPECT_EQ(param.get_composite_columns().get_array_index(), 1);
  EXPECT_EQ(param.get_column_name_size(), 1);
  EXPECT_EQ(param.get_return_info_size(), 2);
  EXPECT_TRUE(param.get_table_name() == stored_table_name);
  EXPECT_NE(param.get_table_name().ptr() , stored_table_name.ptr());
  EXPECT_TRUE(param.get_composite_columns().at(0)->get_as_column_name() == stored_cname);
  EXPECT_NE(param.get_composite_columns().at(0)->get_as_column_name().ptr() , stored_cname.ptr());
  EXPECT_TRUE(param.get_composite_columns().at(0)->get_infix_expr() == stored_expr);
  EXPECT_NE(param.get_composite_columns().at(0)->get_infix_expr().ptr() , stored_expr.ptr());
  EXPECT_EQ(*param.get_return_infos().at(0), false);
  EXPECT_EQ(*param.get_return_infos().at(1), true);

  EXPECT_EQ(param.serialize(serialize_buf, buf_len, pos), OB_SUCCESS);
  data_len = pos;
  pos  = 0;
  EXPECT_EQ(decoded_param.deserialize(serialize_buf,data_len, pos), OB_SUCCESS);

  EXPECT_EQ(decoded_param.get_composite_columns().get_array_index(), 1);
  EXPECT_EQ(decoded_param.get_column_name_size(), 1);
  EXPECT_EQ(decoded_param.get_return_info_size(), 2);
  EXPECT_TRUE(decoded_param.get_table_name() == stored_table_name);
  EXPECT_NE(decoded_param.get_table_name().ptr() , stored_table_name.ptr());
  EXPECT_TRUE(decoded_param.get_composite_columns().at(0)->get_as_column_name() == stored_cname);
  EXPECT_NE(decoded_param.get_composite_columns().at(0)->get_as_column_name().ptr() , stored_cname.ptr());
  EXPECT_TRUE(decoded_param.get_composite_columns().at(0)->get_infix_expr() == stored_expr);
  EXPECT_NE(decoded_param.get_composite_columns().at(0)->get_infix_expr().ptr() , stored_expr.ptr());
  EXPECT_EQ(*decoded_param.get_return_infos().at(0), false);
  EXPECT_EQ(*decoded_param.get_return_infos().at(1), true);

  ObScanParam safe_copied_param;
  EXPECT_EQ(safe_copied_param.safe_copy(decoded_param), OB_SUCCESS);

  EXPECT_EQ(safe_copied_param.get_composite_columns().get_array_index(), 1);
  EXPECT_EQ(safe_copied_param.get_column_name_size(), 1);
  EXPECT_EQ(safe_copied_param.get_return_info_size(), 2);
  EXPECT_TRUE(safe_copied_param.get_table_name() == stored_table_name);
  EXPECT_NE(safe_copied_param.get_table_name().ptr() , stored_table_name.ptr());
  EXPECT_TRUE(safe_copied_param.get_composite_columns().at(0)->get_as_column_name() == stored_cname);
  EXPECT_NE(safe_copied_param.get_composite_columns().at(0)->get_as_column_name().ptr() , stored_cname.ptr());
  EXPECT_TRUE(safe_copied_param.get_composite_columns().at(0)->get_infix_expr() == stored_expr);
  EXPECT_NE(safe_copied_param.get_composite_columns().at(0)->get_infix_expr().ptr() , stored_expr.ptr());
  EXPECT_EQ(*safe_copied_param.get_return_infos().at(0), false);
  EXPECT_EQ(*safe_copied_param.get_return_infos().at(1), true);
}


TEST(ObScanParam, return_info)
{
  char serialize_buf[2048];
  int64_t buf_len = sizeof(serialize_buf);
  UNUSED(buf_len);
  int64_t pos = 0;
  UNUSED(pos);
  int64_t data_len = 0;
  UNUSED(data_len);
  ObStringBuf buffer;

  ObScanParam param;
  ObScanParam decoded_param;

  ObString stored_expr;
  ObString stored_cname;
  ObString stored_table_name;
  ObString str;
  const char *c_str = NULL;
  ObNewRange range;
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  range.set_whole_range();
  c_str = "table";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_table_name), OB_SUCCESS);
  EXPECT_EQ(param.set(OB_INVALID_ID, stored_table_name, range, true), OB_SUCCESS);

  c_str = "cname";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_cname), OB_SUCCESS);
  EXPECT_EQ(param.add_column(stored_cname,false), OB_SUCCESS);

  c_str = "`a` + `b`";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_expr), OB_SUCCESS);

  c_str = "a_add_b";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_cname), OB_SUCCESS);
  EXPECT_EQ(param.add_column(stored_expr, stored_cname), OB_SUCCESS);

  /// param.get_return_infos()
  (const_cast<ObArrayHelpers<bool>*>(&param.get_return_infos()))->clear();
  EXPECT_EQ(param.serialize(serialize_buf, buf_len, pos), OB_SUCCESS);
  data_len = pos;
  pos  = 0;
  EXPECT_EQ(decoded_param.deserialize(serialize_buf,data_len, pos), OB_SUCCESS);


  EXPECT_EQ(decoded_param.get_return_info_size(), 2);
  EXPECT_EQ(*decoded_param.get_return_infos().at(0), true);
  EXPECT_EQ(*decoded_param.get_return_infos().at(1), true);

  ObScanParam safe_copied_param;
  EXPECT_EQ(safe_copied_param.safe_copy(decoded_param), OB_SUCCESS);

  EXPECT_EQ(safe_copied_param.get_return_info_size(), 2);
  EXPECT_EQ(*safe_copied_param.get_return_infos().at(0), true);
  EXPECT_EQ(*safe_copied_param.get_return_infos().at(1), true);
}

TEST(ObScanParam, where)
{
  char serialize_buf[2048];
  int64_t buf_len = sizeof(serialize_buf);
  UNUSED(buf_len);
  int64_t pos = 0;
  UNUSED(pos);
  int64_t data_len = 0;
  UNUSED(data_len);
  ObStringBuf buffer;

  ObScanParam param;
  ObScanParam decoded_param;

  ObString stored_expr;
  ObString stored_cname;
  ObString stored_table_name;
  ObString str;
  const char *c_str = NULL;
  ObNewRange range;
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  range.set_whole_range();
  c_str = "table";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_table_name), OB_SUCCESS);
  EXPECT_EQ(param.set(OB_INVALID_ID, stored_table_name, range, true), OB_SUCCESS);

  c_str = "cname";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_cname), OB_SUCCESS);
  EXPECT_EQ(param.add_column(stored_cname,true), OB_SUCCESS);

  c_str = "`a` + `b` > 5";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_expr), OB_SUCCESS);
  EXPECT_EQ(param.add_where_cond(stored_expr), OB_SUCCESS);

  EXPECT_EQ(param.serialize(serialize_buf, buf_len, pos), OB_SUCCESS);
  data_len = pos;
  pos  = 0;
  EXPECT_EQ(decoded_param.deserialize(serialize_buf,data_len, pos), OB_SUCCESS);
  EXPECT_EQ(decoded_param.get_return_info_size(), 2);
  EXPECT_EQ(*decoded_param.get_return_infos().at(0), true);
  EXPECT_EQ(*decoded_param.get_return_infos().at(1), false);
  EXPECT_TRUE(decoded_param.get_composite_columns().at(0)->get_infix_expr() == stored_expr);
  EXPECT_NE(decoded_param.get_composite_columns().at(0)->get_infix_expr().ptr() , stored_expr.ptr());

  char where_as_name[128];
  ObString    where_as_name_str;
  where_as_name_str.assign(where_as_name,
    snprintf(where_as_name, sizeof(where_as_name), "%s%d",SELECT_CLAUSE_WHERE_COND_AS_CNAME_PREFIX, 1));
  EXPECT_EQ(decoded_param.get_filter_info().get_count(), 1);
  EXPECT_TRUE(decoded_param.get_filter_info()[0]->get_column_name() == where_as_name_str);
  EXPECT_EQ(decoded_param.get_filter_info()[0]->get_logic_operator() , NE);
  ObObj cond_val;
  cond_val.set_bool(false);
  EXPECT_TRUE(decoded_param.get_filter_info()[0]->get_right_operand() == cond_val);

  ObScanParam safe_copied_param;
  EXPECT_EQ(safe_copied_param.safe_copy(decoded_param), OB_SUCCESS);

  EXPECT_EQ(safe_copied_param.get_filter_info().get_count(), 1);
  EXPECT_TRUE(safe_copied_param.get_filter_info()[0]->get_column_name() == where_as_name_str);
  EXPECT_EQ(safe_copied_param.get_filter_info()[0]->get_logic_operator() , NE);
  EXPECT_TRUE(safe_copied_param.get_filter_info()[0]->get_right_operand() == cond_val);
}

TEST(ObScanParam, limit_info)
{
  ObScanParam org_param;
  ObScanParam deserialized_param;
  char buf[1024];
  int64_t pos = 0;
  int64_t buf_len = sizeof(buf);
  ObNewRange range;
  const char * c_table_name = "table_name";
  ObString table_name;
  table_name.assign((char*)c_table_name, static_cast<int32_t>(strlen(c_table_name)));
  org_param.set(OB_INVALID_ID,table_name,range);
  EXPECT_EQ(org_param.serialize(buf,buf_len, pos), OB_SUCCESS);
  buf_len = pos;
  pos = 0;
  EXPECT_EQ(deserialized_param.deserialize(buf, buf_len,pos), OB_SUCCESS);
  EXPECT_TRUE(is_equal(org_param, deserialized_param));

  int64_t limit_offset = 0;
  int64_t limit_count = 0;
  int64_t sharding_minimum = 0;
  double precision = 0;
  deserialized_param.get_limit_info(limit_offset, limit_count);
  deserialized_param.get_topk_precision(sharding_minimum, precision);
  EXPECT_EQ(limit_offset, 0);
  EXPECT_EQ(limit_count, 0);
  EXPECT_EQ(precision, 0);

  int res = 1;
  double org_precision = 0.5;
  org_param.set_limit_info(res,res);
  org_param.set_topk_precision(res,org_precision);
  buf_len = sizeof(buf);
  pos = 0;
  EXPECT_EQ(org_param.serialize(buf,buf_len, pos), OB_SUCCESS);
  buf_len = pos;
  pos = 0;
  EXPECT_EQ(deserialized_param.deserialize(buf, buf_len,pos), OB_SUCCESS);
  EXPECT_TRUE(is_equal(org_param, deserialized_param));

  deserialized_param.get_limit_info(limit_offset, limit_count);
  deserialized_param.get_topk_precision(sharding_minimum, precision);
  EXPECT_EQ(limit_offset, res);
  EXPECT_EQ(limit_count, res);
  EXPECT_EQ(sharding_minimum, res);
  EXPECT_LT(fabs(precision - org_precision), 0.00001);
}
