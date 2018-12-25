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
#include "common/ob_groupby.h"
#include "common/ob_read_common_data.h"
#include "common/ob_action_flag.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace testing;
using namespace std;

TEST(ObAggregateColumn, agg_func)
{
  /// sum
  ObObj agg_obj;
  ObObj new_obj;
  int64_t result_val;
  ObAggregateColumn sum_column(5,0,SUM);
  agg_obj.set_int(5);
  new_obj.set_int(3);
  EXPECT_EQ(sum_column.calc_aggregate_val(agg_obj,new_obj), OB_SUCCESS);
  EXPECT_EQ(agg_obj.get_int(result_val), OB_SUCCESS);
  EXPECT_EQ(result_val, 8);

  agg_obj.set_null();
  new_obj.set_null();
  EXPECT_EQ(sum_column.calc_aggregate_val(agg_obj,new_obj), OB_SUCCESS);
  EXPECT_EQ(agg_obj.get_type(), ObNullType);

  agg_obj.set_null();
  new_obj.set_int(3);
  EXPECT_EQ(sum_column.calc_aggregate_val(agg_obj,new_obj), OB_SUCCESS);
  EXPECT_EQ(agg_obj.get_int(result_val), OB_SUCCESS);
  EXPECT_EQ(result_val, 3);

  /// count
  ObAggregateColumn count_column(5,0,COUNT);
  agg_obj.set_int(5);
  new_obj.set_int(8);
  EXPECT_EQ(count_column.calc_aggregate_val(agg_obj,new_obj), OB_SUCCESS);
  EXPECT_EQ(agg_obj.get_int(result_val), OB_SUCCESS);
  EXPECT_EQ(result_val, 6);

  /// max
  ObAggregateColumn max_column(5,0,MAX);
  agg_obj.set_int(5);
  new_obj.set_int(8);
  EXPECT_EQ(max_column.calc_aggregate_val(agg_obj,new_obj), OB_SUCCESS);
  EXPECT_EQ(agg_obj.get_int(result_val), OB_SUCCESS);
  EXPECT_EQ(result_val, 8);

  agg_obj.set_null();
  new_obj.set_null();
  EXPECT_EQ(max_column.calc_aggregate_val(agg_obj,new_obj), OB_SUCCESS);
  EXPECT_EQ(agg_obj.get_type(), ObNullType);

  agg_obj.set_null();
  new_obj.set_int(3);
  EXPECT_EQ(max_column.calc_aggregate_val(agg_obj,new_obj), OB_SUCCESS);
  EXPECT_EQ(agg_obj.get_int(result_val), OB_SUCCESS);
  EXPECT_EQ(result_val, 3);

  /// min
  ObAggregateColumn min_column(5,0,MIN);
  agg_obj.set_int(5);
  new_obj.set_int(8);
  EXPECT_EQ(min_column.calc_aggregate_val(agg_obj,new_obj), OB_SUCCESS);
  EXPECT_EQ(agg_obj.get_int(result_val), OB_SUCCESS);
  EXPECT_EQ(result_val, 5);

  agg_obj.set_null();
  new_obj.set_null();
  EXPECT_EQ(min_column.calc_aggregate_val(agg_obj,new_obj), OB_SUCCESS);
  EXPECT_EQ(agg_obj.get_type(), ObNullType);

  agg_obj.set_null();
  new_obj.set_int(3);
  EXPECT_EQ(min_column.calc_aggregate_val(agg_obj,new_obj), OB_SUCCESS);
  EXPECT_EQ(agg_obj.get_int(result_val), OB_SUCCESS);
  EXPECT_EQ(result_val, 3);
}


TEST(ObGroupByParam, interface)
{
  char serialize_buf[2048];
  int64_t buf_len = sizeof(serialize_buf);
  int64_t pos = 0;
  int64_t data_len = 0;
  ObGroupByParam decoded_param;
  ObStringBuf buffer;
  ObGroupByParam param;
  ObString column_name;
  ObString return_column_name;
  ObString group_by_column_name;
  ObString aggregate_column_name;
  ObString as_column_name;
  column_name.assign(const_cast<char*>("return"),static_cast<int32_t>(strlen("return")));
  EXPECT_EQ(buffer.write_string(column_name,&return_column_name),OB_SUCCESS);
  column_name.assign(const_cast<char*>("groupby"),static_cast<int32_t>(strlen("groupby")));
  EXPECT_EQ(buffer.write_string(column_name,&group_by_column_name),OB_SUCCESS);
  column_name.assign(const_cast<char*>("aggregate"),static_cast<int32_t>(strlen("aggregate")));
  EXPECT_EQ(buffer.write_string(column_name,&aggregate_column_name),OB_SUCCESS);
  column_name.assign(const_cast<char*>("as"),static_cast<int32_t>(strlen("as")));
  EXPECT_EQ(buffer.write_string(column_name,&as_column_name),OB_SUCCESS);

  /// test add column order
  param.reset();
  EXPECT_EQ(param.add_groupby_column(group_by_column_name),OB_SUCCESS);
  EXPECT_EQ(param.add_return_column(return_column_name),OB_SUCCESS);
  EXPECT_EQ(param.add_aggregate_column(aggregate_column_name,as_column_name,SUM),OB_SUCCESS);
  pos = 0;
  EXPECT_EQ(param.serialize(serialize_buf,buf_len,pos), OB_SUCCESS);
  data_len = pos;
  pos = 0;
  EXPECT_EQ(decoded_param.deserialize(serialize_buf,data_len,pos),OB_SUCCESS);
  EXPECT_EQ(pos,data_len);
  EXPECT_TRUE(decoded_param == param);

  int64_t group_by_column_idx = 1;
  int64_t return_column_idx = 2;
  int64_t aggregate_column_idx = 3;

  /// test add column order
  param.reset();
  EXPECT_EQ(param.add_groupby_column(group_by_column_idx),OB_SUCCESS);
  EXPECT_EQ(param.add_return_column(return_column_idx),OB_SUCCESS);
  EXPECT_EQ(param.add_aggregate_column(aggregate_column_idx,SUM),OB_SUCCESS);
  pos = 0;
  EXPECT_EQ(param.serialize(serialize_buf,buf_len,pos), OB_SUCCESS);
  data_len = pos;
  pos = 0;
  EXPECT_EQ(decoded_param.deserialize(serialize_buf,data_len,pos),OB_SUCCESS);
  EXPECT_EQ(pos,data_len);
  EXPECT_TRUE(decoded_param == param);

  /// id and name mix
  param.reset();
  EXPECT_EQ(param.add_groupby_column(group_by_column_idx),OB_SUCCESS);
  EXPECT_EQ(param.add_aggregate_column(aggregate_column_name,as_column_name,SUM),OB_SUCCESS);
  pos = 0;
  EXPECT_NE(param.serialize(serialize_buf,buf_len,pos), OB_SUCCESS);

  /// name and id mix
  param.reset();
  EXPECT_EQ(param.add_groupby_column(group_by_column_name),OB_SUCCESS);
  EXPECT_EQ(param.add_aggregate_column(aggregate_column_idx,SUM),OB_SUCCESS);
  pos = 0;
  EXPECT_NE(param.serialize(serialize_buf,buf_len,pos), OB_SUCCESS);

  /// no aggregate column
  param.reset();
  EXPECT_EQ(param.add_groupby_column(group_by_column_name),OB_SUCCESS);
  EXPECT_EQ(param.add_return_column(return_column_name),OB_SUCCESS);
  pos = 0;
  EXPECT_EQ(param.serialize(serialize_buf,buf_len,pos), OB_SUCCESS);

  /// no aggregate column
  param.reset();
  EXPECT_EQ(param.add_groupby_column(group_by_column_idx),OB_SUCCESS);
  EXPECT_EQ(param.add_return_column(return_column_idx),OB_SUCCESS);
  pos = 0;
  EXPECT_EQ(param.serialize(serialize_buf,buf_len,pos), OB_SUCCESS);

  /// empty param
  ObGroupByParam empty_param;
  pos = 0;
  EXPECT_EQ(empty_param.get_serialize_size(),0);
  EXPECT_EQ(empty_param.serialize(serialize_buf,buf_len,pos),OB_SUCCESS);
  EXPECT_EQ(pos,0);
  ObObj ext_obj;
  ext_obj.set_ext(ObActionFlag::SELECT_CLAUSE_WHERE_FIELD);
  pos = 0;
  EXPECT_EQ(ext_obj.serialize(serialize_buf, buf_len,pos), OB_SUCCESS);
  data_len = pos;
  pos = 0;
  EXPECT_EQ(empty_param.deserialize(serialize_buf,data_len,pos),OB_SUCCESS);
  EXPECT_EQ(pos,0);
  EXPECT_EQ(empty_param.get_aggregate_row_width(),0);
}


TEST(ObGroupByParam, function)
{
  char serialize_buf[2048];
  int64_t buf_len = sizeof(serialize_buf);
  int64_t pos = 0;
  int64_t data_len = 0;
  /// row width 7;
  /// 0, not used int
  /// 1, groupby int
  /// 2, return int
  /// 3, group str
  /// 4, group int
  /// 5, aggregate column int
  /// 6. not used string
  ObCellArray org_cell_array;
  /// row width 6
  /// 0, group int
  /// 1, group str
  /// 2, return int
  /// 3, return str
  /// 4, aggregate sum
  /// 5, aggregate count
  ObCellArray agg_cell_array;
  ObCellArray all_in_one_group_agg_cell_array;
  ObStringBuf buffer;
  ObGroupByParam param;
  ObGroupByParam all_in_one_group_param;
  ObGroupByParam decoded_param;
  int64_t basic_column_num = 4;
  for (int64_t i = 0; i <= basic_column_num; i++)
  {
    /// 1, 3
    if (i%2)
    {
      EXPECT_EQ(param.add_groupby_column(i),OB_SUCCESS);
    }
  }
  for (int64_t i = 0; i <= basic_column_num; i++)
  {
    /// 2, 4
    if (i%2 == 0 && i > 0)
    {
      EXPECT_EQ(param.add_return_column(i),OB_SUCCESS);
    }
  }
  /// 5
  EXPECT_EQ(param.add_aggregate_column(basic_column_num+1,SUM),OB_SUCCESS);
  EXPECT_EQ(param.add_aggregate_column(basic_column_num+1,COUNT),OB_SUCCESS);
  EXPECT_EQ(all_in_one_group_param.add_aggregate_column(basic_column_num + 1, SUM), OB_SUCCESS);
  EXPECT_EQ(all_in_one_group_param.add_aggregate_column(basic_column_num + 1, COUNT), OB_SUCCESS);

  pos = 0;
  EXPECT_EQ(param.serialize(serialize_buf,buf_len,pos), OB_SUCCESS);
  data_len = pos;
  pos = 0;
  EXPECT_EQ(decoded_param.deserialize(serialize_buf,data_len,pos),OB_SUCCESS);
  EXPECT_EQ(pos,data_len);
  EXPECT_TRUE(decoded_param == param);

  int64_t agg_sum = 0;
  int64_t agg_count = 0;
  ObCellInfo cell;
  ObInnerCellInfo *cell_out = NULL;
  ObString str;
  ObObj return_str_obj;
  ObObj groupby_str_obj;
  const int64_t no_int_value = -1;
  const int64_t return_int_value = 0;
  ObObj return_int_obj;
  return_int_obj.set_int(return_int_value);
  const int64_t groupby_int_value = 1;
  ObObj groupby_int_obj;
  groupby_int_obj.set_int(groupby_int_value);

  ObObj agg_sum_obj;
  ObObj agg_count_obj;
  /// prepare first row in org cell array
  /// cell not used, 0
  cell.value_.set_int(no_int_value);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  /// groupby cell, 1
  cell.value_.set_int(groupby_int_value);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  /// return cell, 2
  cell.value_.set_int(return_int_value);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  /// groupby cell, 3
  str.assign(const_cast<char*>("groupby"),static_cast<int32_t>(strlen("groupby")));
  cell.value_.set_varchar(str);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  groupby_str_obj = cell_out->value_;
  /// return cell, 4
  str.assign(const_cast<char*>("return"),static_cast<int32_t>(strlen("return")));
  cell.value_.set_varchar(str);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  return_str_obj = cell_out->value_;
  /// aggregate value, 5
  cell.value_.set_int(5);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  agg_count += 1;
  agg_sum += 5;
  /// cell not used, 6
  str.assign(const_cast<char*>("nouse"),static_cast<int32_t>(strlen("nouse")));
  cell.value_.set_varchar(str);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);

  /// aggregate first cell
  EXPECT_EQ(param.aggregate(org_cell_array,0,6,agg_cell_array,0,5), OB_SUCCESS);
  agg_sum_obj.set_int(agg_sum);
  agg_count_obj.set_int(agg_count);
  EXPECT_TRUE(agg_cell_array[0].value_== groupby_int_obj);
  EXPECT_TRUE(agg_cell_array[1].value_== groupby_str_obj);
  EXPECT_TRUE(agg_cell_array[2].value_== return_int_obj);
  EXPECT_TRUE(agg_cell_array[3].value_== return_str_obj);
  EXPECT_TRUE(agg_cell_array[4].value_== agg_sum_obj);
  EXPECT_TRUE(agg_cell_array[5].value_== agg_count_obj);

  EXPECT_EQ(all_in_one_group_param.aggregate(org_cell_array,0,6,all_in_one_group_agg_cell_array,0,1), OB_SUCCESS);
  EXPECT_TRUE(all_in_one_group_agg_cell_array[0].value_== agg_sum_obj);
  EXPECT_TRUE(all_in_one_group_agg_cell_array[1].value_== agg_count_obj);

  /// prepare second row in org cell, only change return column value
  /// cell not used, 0
  cell.value_.set_int(no_int_value + 5);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  /// groupby cell, 1
  cell.value_.set_int(groupby_int_value);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  /// return cell, 2
  cell.value_.set_int(return_int_value + 6);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  /// groupby cell, 3
  str.assign(const_cast<char*>("groupby"),static_cast<int32_t>(strlen("groupby")));
  cell.value_.set_varchar(str);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  groupby_str_obj = cell_out->value_;
  /// return cell, 4
  str.assign(const_cast<char*>("return1"),static_cast<int32_t>(strlen("return1")));
  cell.value_.set_varchar(str);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  /// aggregate value, 5
  cell.value_.set_int(-8);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  agg_count += 1;
  agg_sum += -8;
  /// cell not used, 6
  str.assign(const_cast<char*>("nouse1"),static_cast<int32_t>(strlen("nouse1")));
  cell.value_.set_varchar(str);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);

  /// aggregate second cell
  EXPECT_EQ(param.aggregate(org_cell_array,7,13,agg_cell_array,0,5), OB_SUCCESS);
  agg_sum_obj.set_int(agg_sum);
  agg_count_obj.set_int(agg_count);
  EXPECT_TRUE(agg_cell_array[0].value_== groupby_int_obj);
  EXPECT_TRUE(agg_cell_array[1].value_== groupby_str_obj);
  EXPECT_TRUE(agg_cell_array[2].value_== return_int_obj);
  EXPECT_TRUE(agg_cell_array[3].value_== return_str_obj);
  EXPECT_TRUE(agg_cell_array[4].value_== agg_sum_obj);
  EXPECT_TRUE(agg_cell_array[5].value_== agg_count_obj);

  EXPECT_EQ(all_in_one_group_param.aggregate(org_cell_array,7,13,all_in_one_group_agg_cell_array,0,1), OB_SUCCESS);
  EXPECT_TRUE(all_in_one_group_agg_cell_array[0].value_== agg_sum_obj);
  EXPECT_TRUE(all_in_one_group_agg_cell_array[1].value_== agg_count_obj);

  /// test case for keys
  ObGroupKey first_org_key;
  EXPECT_EQ(first_org_key.init(org_cell_array,param,0,6,ObGroupKey::ORG_KEY),OB_SUCCESS);
  ObGroupKey second_org_key;
  EXPECT_EQ(second_org_key.init(org_cell_array,param,7,13,ObGroupKey::ORG_KEY),OB_SUCCESS);
  ObGroupKey first_agg_key;
  EXPECT_EQ(first_agg_key.init(agg_cell_array,param,0,5,ObGroupKey::AGG_KEY),OB_SUCCESS);
  EXPECT_TRUE(first_org_key == second_org_key);
  EXPECT_TRUE(first_org_key == first_agg_key);
  EXPECT_TRUE(second_org_key == first_agg_key);

  /// prepare third org row, change groupby column value
  /// cell not used, 0
  cell.value_.set_int(no_int_value + 5);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  /// groupby cell, 1
  cell.value_.set_int(groupby_int_value);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  /// return cell, 2
  cell.value_.set_int(return_int_value + 6);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  /// groupby cell, 3
  str.assign(const_cast<char*>("groupby1"),static_cast<int32_t>(strlen("groupby1")));
  cell.value_.set_varchar(str);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  groupby_str_obj = cell_out->value_;
  /// return cell, 4
  str.assign(const_cast<char*>("return1"),static_cast<int32_t>(strlen("return1")));
  cell.value_.set_varchar(str);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  /// aggregate value, 5
  cell.value_.set_int(-8);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);
  agg_count += 1;
  agg_sum += -8;
  /// cell not used, 6
  str.assign(const_cast<char*>("nouse1"),static_cast<int32_t>(strlen("nouse1")));
  cell.value_.set_varchar(str);
  EXPECT_EQ(org_cell_array.append(cell, cell_out),OB_SUCCESS);

  /// aggregate third row
  agg_sum_obj.set_int(agg_sum);
  agg_count_obj.set_int(agg_count);
  EXPECT_EQ(all_in_one_group_param.aggregate(org_cell_array,14,20,all_in_one_group_agg_cell_array,0,1), OB_SUCCESS);
  EXPECT_TRUE(all_in_one_group_agg_cell_array[0].value_== agg_sum_obj);
  EXPECT_TRUE(all_in_one_group_agg_cell_array[1].value_== agg_count_obj);

  ObGroupKey third_org_key;
  EXPECT_EQ(third_org_key.init(org_cell_array,param,14,20,ObGroupKey::ORG_KEY),OB_SUCCESS);
  EXPECT_FALSE(first_org_key == third_org_key);
  EXPECT_FALSE(second_org_key == third_org_key);
  EXPECT_FALSE(first_agg_key == third_org_key);

  /// all in group keys
  ObGroupKey all_1st_org_key;
  ObGroupKey all_2nd_org_key;
  ObGroupKey all_3rd_org_key;
  ObGroupKey all_agg_key;
  EXPECT_EQ(all_1st_org_key.init(org_cell_array,all_in_one_group_param,0,6,ObGroupKey::ORG_KEY), OB_SUCCESS);
  EXPECT_EQ(all_2nd_org_key.init(org_cell_array,all_in_one_group_param,0,6,ObGroupKey::ORG_KEY), OB_SUCCESS);
  EXPECT_EQ(all_3rd_org_key.init(org_cell_array,all_in_one_group_param,0,6,ObGroupKey::ORG_KEY), OB_SUCCESS);
  EXPECT_EQ(all_agg_key.init(all_in_one_group_agg_cell_array,all_in_one_group_param,0,1,ObGroupKey::AGG_KEY),OB_SUCCESS);
  EXPECT_TRUE(all_agg_key == all_1st_org_key);
  EXPECT_TRUE(all_agg_key == all_2nd_org_key);
  EXPECT_TRUE(all_agg_key == all_3rd_org_key);
  EXPECT_TRUE(all_2nd_org_key == all_1st_org_key);
  EXPECT_TRUE(all_3rd_org_key == all_1st_org_key);
  EXPECT_TRUE(all_3rd_org_key == all_2nd_org_key);
}


TEST(ObGroupByParam, str_expr_composite_column)
{
  char serialize_buf[2048];
  int64_t buf_len = sizeof(serialize_buf);
  UNUSED(buf_len);
  int64_t pos = 0;
  UNUSED(pos);
  int64_t data_len = 0;
  UNUSED(data_len);
  ObStringBuf buffer;
  ObGroupByParam param;
  ObGroupByParam decoded_param;
  ObString stored_expr;
  ObString stored_cname;
  ObString str;
  const char *c_str =NULL;
  c_str = "groupby_c";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_cname), OB_SUCCESS);
  EXPECT_EQ(param.add_groupby_column(stored_cname), OB_SUCCESS);

  c_str = "agg_c";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_cname), OB_SUCCESS);
  EXPECT_EQ(param.add_aggregate_column(stored_cname, stored_cname, SUM, false), OB_SUCCESS);

  c_str ="`a` + `b`";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_expr), OB_SUCCESS);

  c_str = "a_add_b";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_cname), OB_SUCCESS);
  EXPECT_EQ(param.add_column(stored_expr, stored_cname ,false), OB_SUCCESS);

  EXPECT_EQ(param.get_composite_columns().get_array_index(), 1);
  EXPECT_EQ(param.get_groupby_columns().get_array_index(), 1);
  EXPECT_EQ(param.get_aggregate_columns().get_array_index(), 1);
  EXPECT_EQ(param.get_return_columns().get_array_index(),0);
  EXPECT_EQ(param.get_return_infos().get_array_index(), 3);
  EXPECT_EQ(*param.get_return_infos().at(0), true);
  EXPECT_EQ(*param.get_return_infos().at(1), false);
  EXPECT_EQ(*param.get_return_infos().at(2), false);
  EXPECT_TRUE(param.get_composite_columns().at(0)->get_as_column_name() == stored_cname);
  EXPECT_NE(param.get_composite_columns().at(0)->get_as_column_name().ptr() , stored_cname.ptr());
  EXPECT_TRUE(param.get_composite_columns().at(0)->get_infix_expr() == stored_expr);
  EXPECT_NE(param.get_composite_columns().at(0)->get_infix_expr().ptr() , stored_expr.ptr());

  EXPECT_EQ(param.serialize(serialize_buf,buf_len,pos), OB_SUCCESS);
  data_len = pos;
  pos = 0;
  EXPECT_EQ(decoded_param.deserialize(serialize_buf,data_len,pos), OB_SUCCESS);

  EXPECT_EQ(decoded_param.get_composite_columns().get_array_index(), 1);
  EXPECT_EQ(decoded_param.get_groupby_columns().get_array_index(), 1);
  EXPECT_EQ(decoded_param.get_aggregate_columns().get_array_index(), 1);
  EXPECT_EQ(decoded_param.get_return_columns().get_array_index(),0);
  EXPECT_EQ(decoded_param.get_return_infos().get_array_index(), 3);
  EXPECT_EQ(*decoded_param.get_return_infos().at(0), true);
  EXPECT_EQ(*decoded_param.get_return_infos().at(1), false);
  EXPECT_EQ(*decoded_param.get_return_infos().at(2), false);
  EXPECT_TRUE(decoded_param.get_composite_columns().at(0)->get_as_column_name() == stored_cname);
  EXPECT_NE(decoded_param.get_composite_columns().at(0)->get_as_column_name().ptr() , stored_cname.ptr());
  EXPECT_TRUE(decoded_param.get_composite_columns().at(0)->get_infix_expr() == stored_expr);
  EXPECT_NE(decoded_param.get_composite_columns().at(0)->get_infix_expr().ptr() , stored_expr.ptr());

  ObGroupByParam safe_copied_param;
  EXPECT_EQ(safe_copied_param.safe_copy(decoded_param), OB_SUCCESS);
  EXPECT_EQ(safe_copied_param.get_composite_columns().get_array_index(), 1);
  EXPECT_EQ(safe_copied_param.get_groupby_columns().get_array_index(), 1);
  EXPECT_EQ(safe_copied_param.get_aggregate_columns().get_array_index(), 1);
  EXPECT_EQ(safe_copied_param.get_return_columns().get_array_index(),0);
  EXPECT_EQ(safe_copied_param.get_return_infos().get_array_index(), 3);
  EXPECT_EQ(*safe_copied_param.get_return_infos().at(0), true);
  EXPECT_EQ(*safe_copied_param.get_return_infos().at(1), false);
  EXPECT_EQ(*safe_copied_param.get_return_infos().at(2), false);
  EXPECT_TRUE(safe_copied_param.get_composite_columns().at(0)->get_as_column_name() == stored_cname);
  EXPECT_NE(safe_copied_param.get_composite_columns().at(0)->get_as_column_name().ptr() , stored_cname.ptr());
  EXPECT_TRUE(safe_copied_param.get_composite_columns().at(0)->get_infix_expr() == stored_expr);
  EXPECT_NE(safe_copied_param.get_composite_columns().at(0)->get_infix_expr().ptr() , stored_expr.ptr());
}

TEST(ObGroupByParam, return_info)
{
  char serialize_buf[2048];
  int64_t buf_len = sizeof(serialize_buf);
  UNUSED(buf_len);
  int64_t pos = 0;
  UNUSED(pos);
  int64_t data_len = 0;
  UNUSED(data_len);
  ObStringBuf buffer;
  ObGroupByParam param;
  ObGroupByParam decoded_param;
  ObString stored_expr;
  ObString stored_cname;
  ObString str;
  const char *c_str =NULL;
  c_str = "groupby_c";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_cname), OB_SUCCESS);
  EXPECT_EQ(param.add_groupby_column(stored_cname), OB_SUCCESS);

  c_str = "agg_c";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_cname), OB_SUCCESS);
  EXPECT_EQ(param.add_aggregate_column(stored_cname, stored_cname, SUM, false), OB_SUCCESS);

  c_str ="`a` + `b`";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_expr), OB_SUCCESS);

  c_str = "a_add_b";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_cname), OB_SUCCESS);
  EXPECT_EQ(param.add_column(stored_expr, stored_cname ,false), OB_SUCCESS);

  (const_cast<oceanbase::common::ObArrayHelpers<bool>*>(&param.get_return_infos()))->clear();
  EXPECT_EQ(param.serialize(serialize_buf,buf_len,pos), OB_SUCCESS);
  data_len = pos;
  pos = 0;
  EXPECT_EQ(decoded_param.deserialize(serialize_buf,data_len,pos), OB_SUCCESS);

  EXPECT_EQ(decoded_param.get_return_infos().get_array_index(), 3);
  EXPECT_EQ(*decoded_param.get_return_infos().at(0), true);
  EXPECT_EQ(*decoded_param.get_return_infos().at(1), true);
  EXPECT_EQ(*decoded_param.get_return_infos().at(2), true);

  ObGroupByParam safe_copied_param;
  EXPECT_EQ(safe_copied_param.safe_copy(decoded_param), OB_SUCCESS);
  EXPECT_EQ(safe_copied_param.get_return_infos().get_array_index(), 3);
  EXPECT_EQ(*safe_copied_param.get_return_infos().at(0), true);
  EXPECT_EQ(*safe_copied_param.get_return_infos().at(1), true);
  EXPECT_EQ(*safe_copied_param.get_return_infos().at(2), true);
}

TEST(ObGroupByParam, having)
{
  char serialize_buf[2048];
  int64_t buf_len = sizeof(serialize_buf);
  UNUSED(buf_len);
  int64_t pos = 0;
  UNUSED(pos);
  int64_t data_len = 0;
  UNUSED(data_len);
  ObStringBuf buffer;
  ObGroupByParam param;
  ObGroupByParam decoded_param;
  ObString stored_expr;
  ObString stored_cname;
  ObString str;
  const char *c_str =NULL;
  c_str = "groupby_c";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_cname), OB_SUCCESS);
  EXPECT_EQ(param.add_groupby_column(stored_cname), OB_SUCCESS);

  c_str = "agg_c";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_cname), OB_SUCCESS);
  EXPECT_EQ(param.add_aggregate_column(stored_cname, stored_cname, SUM, false), OB_SUCCESS);

  c_str ="`a` + `b` > 5";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_expr), OB_SUCCESS);

  c_str = "a_add_b";
  str.assign((char*)c_str, (int)strlen(c_str));
  ASSERT_EQ(buffer.write_string(str,&stored_cname), OB_SUCCESS);
  EXPECT_EQ(param.add_having_cond(stored_expr), OB_SUCCESS);

  EXPECT_EQ(param.serialize(serialize_buf,buf_len,pos), OB_SUCCESS);
  data_len = pos;
  pos = 0;
  EXPECT_EQ(decoded_param.deserialize(serialize_buf,data_len,pos), OB_SUCCESS);

  EXPECT_EQ(decoded_param.get_return_infos().get_array_index(), 3);
  EXPECT_EQ(*decoded_param.get_return_infos().at(0), true);
  EXPECT_EQ(*decoded_param.get_return_infos().at(1), false);
  EXPECT_EQ(*decoded_param.get_return_infos().at(2), false);
  EXPECT_EQ(decoded_param.get_aggregate_columns().get_array_index(), 1);
  EXPECT_EQ(decoded_param.get_groupby_columns().get_array_index(), 1);
  EXPECT_EQ(decoded_param.get_composite_columns().get_array_index(), 1);

  EXPECT_TRUE(decoded_param.get_composite_columns().at(0)->get_infix_expr() == stored_expr);
  EXPECT_NE(decoded_param.get_composite_columns().at(0)->get_infix_expr().ptr() , stored_expr.ptr());

  char having_as_cname[128];
  ObString having_as_cname_str;
  having_as_cname_str.assign(having_as_cname,
    snprintf(having_as_cname,sizeof(having_as_cname), "%s%d", GROUPBY_CLAUSE_HAVING_COND_AS_CNAME_PREFIX,2));
  EXPECT_TRUE(decoded_param.get_composite_columns().at(0)->get_as_column_name() == having_as_cname_str);

  EXPECT_EQ(decoded_param.get_having_condition().get_count(), 1);
  EXPECT_TRUE(decoded_param.get_having_condition()[0]->get_column_name() == having_as_cname_str );
  EXPECT_TRUE(decoded_param.get_having_condition()[0]->get_logic_operator() == NE );
  ObObj cond_val;
  cond_val.set_bool(false);
  EXPECT_TRUE(decoded_param.get_having_condition()[0]->get_right_operand() == cond_val);

  ObGroupByParam safe_copied_param;
  EXPECT_EQ(safe_copied_param.safe_copy(decoded_param), OB_SUCCESS);
  EXPECT_TRUE(safe_copied_param.get_composite_columns().at(0)->get_as_column_name() == having_as_cname_str);
  EXPECT_EQ(safe_copied_param.get_having_condition().get_count(), 1);
  EXPECT_TRUE(safe_copied_param.get_having_condition()[0]->get_column_name() == having_as_cname_str );
  EXPECT_TRUE(safe_copied_param.get_having_condition()[0]->get_logic_operator() == NE );
  EXPECT_TRUE(safe_copied_param.get_having_condition()[0]->get_right_operand() == cond_val);
}


int main(int argc, char **argv)
{
  srandom(static_cast<uint32_t>(time(NULL)));
  ob_init_memory_pool(64*1024);
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
