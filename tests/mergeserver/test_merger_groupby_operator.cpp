/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * test_read_param_decoder.cpp is for what ...
 *
 * Version: $id: test_read_param_decoder.cpp,v 0.1 3/29/2011 4:54p wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */

#include <gtest/gtest.h>
#include <stdlib.h>
#include <time.h>
#include <vector>
#include <string>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include <math.h>
#include "common/ob_cell_array.h"
#include "common/ob_define.h"
#include "common/ob_cache.h"
#include "common/ob_string.h"
#include "common/ob_action_flag.h"
#include "common/ob_groupby.h"
#include "common/ob_scanner.h"
#include "mergeserver/ob_groupby_operator.h"
#include "mergeserver/ob_read_param_decoder.h"
#include "mergeserver/ob_ms_scan_param.h"
#include "mergeserver/ob_merger_sorted_operator.h"
#include "mergeserver/ob_merger_groupby_operator.h"
#include "../common/test_rowkey_helper.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace testing;
using namespace std;
static CharArena allocator_;
static const int64_t max_memory_size = 10 * 1024 * 1024;
TEST(ObMergerGroupByOperator, sort_result)
{
  const int32_t sharding_count = 5;
  const int32_t cell_count_each_sharding = 2;
  ObScanner *sharding_res_arr = new ObScanner[sharding_count];
  ObNewRange   *q_range_arr = new ObNewRange[sharding_count];
  ObStringBuf buf;
  char row_key_buf[32];
  int32_t row_key_len = 0;
  ObRowkey str;
  ObRowkey row_key;
  ObCellInfo cell;
  cell.table_id_ = 1;
  cell.column_id_ = 1;
  ObScanParam scan_param;
  EXPECT_EQ(scan_param.add_column(1),OB_SUCCESS);
  row_key_len = snprintf(row_key_buf, sizeof(row_key_buf),"%d",0);
  str = make_rowkey(row_key_buf,row_key_len, &allocator_);
  EXPECT_EQ(buf.write_string(str,&row_key),OB_SUCCESS);
  ObNewRange q_range;
  q_range.start_key_ = row_key;
  q_range.border_flag_.set_inclusive_start();
  q_range.end_key_.set_max_row();
  ObScanParam param;
  ObString table_name;
  EXPECT_EQ(param.set(1,table_name, q_range), OB_SUCCESS);
  EXPECT_EQ(param.add_column(1ull), OB_SUCCESS);
  EXPECT_EQ(param.add_orderby_column(0), OB_SUCCESS);

  ObMergerGroupByOperator g_operator;
  EXPECT_EQ(g_operator.set_param(max_memory_size, param), OB_SUCCESS);
  EXPECT_EQ(g_operator.get_result_row_width(), 1);
  for (int32_t i = sharding_count * cell_count_each_sharding - 1; i >= 0; i -= 2)
  {
    int sharding_idx = (i-1)/2;
    /// prepare range
    if (i > 1)
    {
      row_key_len = snprintf(row_key_buf, sizeof(row_key_buf),"%d",i - 2);
    }
    else
    {
      row_key_len = snprintf(row_key_buf, sizeof(row_key_buf),"%d",0);
    }
    str = make_rowkey(row_key_buf,row_key_len, &allocator_);
    EXPECT_EQ(buf.write_string(str,&row_key),OB_SUCCESS);
    q_range_arr[sharding_idx].start_key_ = row_key;
    if (i > 1)
    {
      q_range_arr[sharding_idx].border_flag_.unset_inclusive_start();
    }
    else
    {
      q_range_arr[sharding_idx].border_flag_.set_inclusive_start();
    }
    q_range_arr[sharding_idx].end_key_.set_max_row();

    /// first cell
    row_key_len = snprintf(row_key_buf, sizeof(row_key_buf),"%d",i - 1);
    str = make_rowkey(row_key_buf,row_key_len, &allocator_);
    EXPECT_EQ(buf.write_string(str,&row_key),OB_SUCCESS);
    cell.row_key_ = row_key;
    cell.value_.set_int(i - 1);
    EXPECT_EQ(sharding_res_arr[sharding_idx].add_cell(cell), OB_SUCCESS);

    /// second cell
    row_key_len = snprintf(row_key_buf, sizeof(row_key_buf),"%d",i);
    str = make_rowkey(row_key_buf,row_key_len, &allocator_);
    EXPECT_EQ(buf.write_string(str,&row_key),OB_SUCCESS);
    cell.row_key_ = row_key;
    cell.value_.set_int(i - 1);
    EXPECT_EQ(sharding_res_arr[sharding_idx].add_cell(cell), OB_SUCCESS);

    bool is_finish = true;
    EXPECT_EQ(g_operator.add_sharding_result(sharding_res_arr[sharding_idx], q_range_arr[sharding_idx], 0,is_finish),false);
    EXPECT_FALSE(is_finish);
  }
  EXPECT_EQ(g_operator.seal(),OB_SUCCESS);
  int32_t beg = 0;
  ObInnerCellInfo *cur_cell = NULL;
  int err = OB_SUCCESS;
  while ((err = g_operator.next_cell()) == OB_SUCCESS)
  {
    EXPECT_EQ(g_operator.get_cell(&cur_cell),OB_SUCCESS);
    row_key_len = snprintf(row_key_buf, sizeof(row_key_buf),"%d",beg);
    str = make_rowkey(row_key_buf,row_key_len, &allocator_);
    EXPECT_TRUE(cur_cell->row_key_ == str);
    beg ++;
  }
  EXPECT_EQ(OB_ITER_END, err);
  EXPECT_EQ(beg, sharding_count * cell_count_each_sharding);
  delete [] sharding_res_arr;
  delete [] q_range_arr;
}

TEST(ObMergerGroupByOperator, group_result)
{
  ObStringBuf buf;
  char row_key_buf[32];
  int32_t row_key_len = 0;
  ObString str;
  ObRowkey key;
  ObRowkey row_key;
  ObCellInfo cell;
  cell.table_id_ = 1;
  cell.column_id_ = 1;
  row_key_len = snprintf(row_key_buf, sizeof(row_key_buf),"%d",0);
  key = make_rowkey(row_key_buf,row_key_len, &allocator_);
  EXPECT_EQ(buf.write_string(key, &row_key), OB_SUCCESS);
  ObNewRange q_range;
  q_range.start_key_ = row_key;
  q_range.border_flag_.set_inclusive_start();
  q_range.end_key_.set_max_row();
  ObScanParam org_param;
  ObScanParam decoded_param;
  ObString table_name;
  ObString column_name;
  const char *c_ptr = NULL;
  c_ptr = "collect_info";
  str.assign_ptr(const_cast<char*>(c_ptr), static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(buf.write_string(str,&table_name), OB_SUCCESS);
  EXPECT_EQ(org_param.set(OB_INVALID_ARGUMENT,table_name, q_range), OB_SUCCESS);


  c_ptr = "item_category";
  str.assign_ptr(const_cast<char*>(c_ptr), static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(buf.write_string(str,&column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.get_group_by_param().add_groupby_column(column_name), OB_SUCCESS);

  c_ptr = "item_price";
  str.assign_ptr(const_cast<char*>(c_ptr), static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(buf.write_string(str,&column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.get_group_by_param().add_aggregate_column(column_name,column_name,SUM), OB_SUCCESS);

  c_ptr = "`item_price`/10000.0";
  ObString expr;
  str.assign_ptr(const_cast<char*>(c_ptr), static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(buf.write_string(str,&expr), OB_SUCCESS);
  c_ptr = "double_price";
  str.assign_ptr(const_cast<char*>(c_ptr), static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(buf.write_string(str,&column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.get_group_by_param().add_column(expr,column_name), OB_SUCCESS);

  c_ptr = "`item_category` = 2";
  str.assign_ptr(const_cast<char*>(c_ptr), static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(buf.write_string(str,&expr), OB_SUCCESS);
  EXPECT_EQ(org_param.get_group_by_param().add_having_cond(expr), OB_SUCCESS);

  ObSchemaManagerV2 *mgr = new ObSchemaManagerV2();
  tbsys::CConfig config;
  EXPECT_TRUE(mgr->parse_from_file("schema.ini",config));

  EXPECT_EQ(ob_decode_scan_param(org_param,*mgr, decoded_param), OB_SUCCESS);
  ObMergerScanParam ms_scan_param;
  EXPECT_EQ(ms_scan_param.set_param(decoded_param),OB_SUCCESS);


  ObMergerGroupByOperator g_operator;
  uint64_t table_id = 1001;
  cell.table_id_ = table_id;
  EXPECT_EQ(g_operator.set_param(max_memory_size, *ms_scan_param.get_ms_param()), OB_SUCCESS);
  EXPECT_EQ(g_operator.get_result_row_width(), 4);
  /// add item_category 1
  ObScanner result_of_category_1;
  ObNewRange   range_of_category_1;
  range_of_category_1.set_whole_range();
  range_of_category_1.border_flag_.set_inclusive_end();
  range_of_category_1.border_flag_.set_inclusive_start();
  int row_key_count = 0;
  row_key_len = snprintf(row_key_buf,sizeof(row_key_buf),"%d", row_key_count);
  key = make_rowkey(row_key_buf,row_key_len, &allocator_);
  EXPECT_EQ(buf.write_string(key,&row_key),OB_SUCCESS);
  cell.row_key_ = row_key;
  row_key_count ++;
  /// category
  int64_t category = 1;
  cell.value_.set_int(category);
  EXPECT_EQ(result_of_category_1.add_cell(cell), OB_SUCCESS);
  /// price
  int64_t price = 10000;
  cell.value_.set_int(price);
  EXPECT_EQ(result_of_category_1.add_cell(cell), OB_SUCCESS);
  /// double price
  EXPECT_EQ(result_of_category_1.add_cell(cell), OB_SUCCESS);
  /// having condition
  EXPECT_EQ(result_of_category_1.add_cell(cell), OB_SUCCESS);

  int64_t fullfilled_item_num = 1;
  EXPECT_EQ(result_of_category_1.set_is_req_fullfilled(false,fullfilled_item_num),OB_SUCCESS);

  bool is_finish = false;
  EXPECT_EQ(g_operator.add_sharding_result(result_of_category_1,range_of_category_1,0,is_finish),OB_SUCCESS);
  /// add item_category 2
  ObScanner result_of_category_2;
  ObNewRange   range_of_category_2;
  range_of_category_2.end_key_.set_max_row();
  range_of_category_2.border_flag_.unset_inclusive_start();
  range_of_category_2.start_key_ = row_key;
  category = 2;
  int64_t total_price = 0;
  fullfilled_item_num = 0;
  for (int32_t i = 1; i < 9; i ++,row_key_count ++, fullfilled_item_num ++)
  {
    row_key_len = snprintf(row_key_buf,sizeof(row_key_buf),"%d", row_key_count);
    key = make_rowkey(row_key_buf,row_key_len, &allocator_);
    EXPECT_EQ(buf.write_string(key,&row_key),OB_SUCCESS);
    cell.row_key_ = row_key;

    cell.value_.set_int(category);
    EXPECT_EQ(result_of_category_2.add_cell(cell), OB_SUCCESS);

    int64_t price = i*10000;
    total_price += price;
    cell.value_.set_int(price);
    EXPECT_EQ(result_of_category_2.add_cell(cell), OB_SUCCESS);

    /// double price
    EXPECT_EQ(result_of_category_2.add_cell(cell), OB_SUCCESS);
    /// having condition
    EXPECT_EQ(result_of_category_2.add_cell(cell), OB_SUCCESS);
  }

  EXPECT_EQ(result_of_category_2.set_is_req_fullfilled(false,fullfilled_item_num),OB_SUCCESS);
  EXPECT_EQ(g_operator.add_sharding_result(result_of_category_2,range_of_category_2,0, is_finish),OB_SUCCESS);

  EXPECT_EQ(g_operator.seal(),OB_SUCCESS);

  bool row_changed = false;
  ObInnerCellInfo *cur_cell = NULL;
  int64_t int_val = 0;
  /// category
  EXPECT_EQ(g_operator.next_cell(), OB_SUCCESS);
  EXPECT_EQ(g_operator.get_cell(&cur_cell,&row_changed),OB_SUCCESS);
  EXPECT_EQ(cur_cell->value_.get_int(int_val),OB_SUCCESS);
  EXPECT_EQ(int_val,2);

  /// price
  EXPECT_EQ(g_operator.next_cell(), OB_SUCCESS);
  EXPECT_EQ(g_operator.get_cell(&cur_cell,&row_changed),OB_SUCCESS);
  EXPECT_EQ(cur_cell->value_.get_int(int_val),OB_SUCCESS);
  EXPECT_EQ(int_val,total_price);

  double double_val = 0;
  /// double price
  EXPECT_EQ(g_operator.next_cell(), OB_SUCCESS);
  EXPECT_EQ(g_operator.get_cell(&cur_cell,&row_changed),OB_SUCCESS);
  EXPECT_EQ(cur_cell->value_.get_double(double_val),OB_SUCCESS);
  EXPECT_EQ(double_val,(double)total_price/10000.0);


  bool bool_val = false;
  /// condition
  EXPECT_EQ(g_operator.next_cell(), OB_SUCCESS);
  EXPECT_EQ(g_operator.get_cell(&cur_cell,&row_changed),OB_SUCCESS);
  EXPECT_EQ(cur_cell->value_.get_bool(bool_val),OB_SUCCESS);
  EXPECT_TRUE(bool_val);

  EXPECT_EQ(g_operator.next_cell(), OB_ITER_END);
  delete mgr;
}


int main(int argc, char **argv)
{
  srandom(static_cast<uint32_t>(time(NULL)));
  ob_init_memory_pool(64*1024);
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

