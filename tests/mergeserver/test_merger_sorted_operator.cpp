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
#include "../common/test_rowkey_helper.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace testing;
using namespace std;
static CharArena allocator_;

TEST(ObSortedOperator, forward)
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

  ObMergerSortedOperator s_operator;
  EXPECT_EQ(s_operator.set_param(param), OB_SUCCESS);
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
    q_range_arr[sharding_idx].border_flag_.set_max_value();

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
    EXPECT_EQ(s_operator.add_sharding_result(sharding_res_arr[sharding_idx], q_range_arr[sharding_idx],is_finish),false);
    if (sharding_idx == 0)
    {
      EXPECT_TRUE(is_finish);
    }
    else
    {
      EXPECT_FALSE(is_finish);
    }
  }
  EXPECT_EQ(s_operator.seal(),OB_SUCCESS);
  int32_t beg = 0;
  ObInnerCellInfo *cur_cell = NULL;
  int err = OB_SUCCESS;
  while ((err = s_operator.next_cell()) == OB_SUCCESS)
  {
    EXPECT_EQ(s_operator.get_cell(&cur_cell),OB_SUCCESS);
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

TEST(ObSortedOperator, backword)
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
  row_key_len = snprintf(row_key_buf, sizeof(row_key_buf),"%d",9);
  str = make_rowkey(row_key_buf,row_key_len, &allocator_);
  EXPECT_EQ(buf.write_string(str,&row_key),OB_SUCCESS);
  ObNewRange q_range;
  q_range.end_key_ = row_key;
  q_range.border_flag_.set_inclusive_end();
  q_range.start_key_.set_min_row();
  ObScanParam param;
  ObString table_name;
  EXPECT_EQ(param.set(1,table_name, q_range), OB_SUCCESS);
  param.set_scan_direction(ObScanParam::BACKWARD);

  ObMergerSortedOperator s_operator;
  EXPECT_EQ(s_operator.set_param(param), OB_SUCCESS);
  /// for (int32_t i = sharding_count * cell_count_each_sharding - 1; i >= 0; i -= 2)
  for (int32_t i = 0; i < sharding_count * cell_count_each_sharding; i += 2)
  {
    int sharding_idx = i/2;
    /// prepare range
    if (i < sharding_count * cell_count_each_sharding - 2)
    {
      row_key_len = snprintf(row_key_buf, sizeof(row_key_buf),"%d",i + 2);
    }
    else
    {
      row_key_len = snprintf(row_key_buf, sizeof(row_key_buf),"%d",sharding_count * cell_count_each_sharding - 1);
    }
    str = make_rowkey(row_key_buf,row_key_len, &allocator_);
    EXPECT_EQ(buf.write_string(str,&row_key),OB_SUCCESS);
    q_range_arr[sharding_idx].end_key_ = row_key;
    if (i < sharding_count * cell_count_each_sharding - 2)
    {
      q_range_arr[sharding_idx].border_flag_.unset_inclusive_end();
    }
    else
    {
      q_range_arr[sharding_idx].border_flag_.set_inclusive_end();
    }
    q_range_arr[sharding_idx].border_flag_.set_min_value();
    q_range_arr[sharding_idx].border_flag_.set_inclusive_start();

    /// first cell
    row_key_len = snprintf(row_key_buf, sizeof(row_key_buf),"%d",i + 1);
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
    EXPECT_EQ(s_operator.add_sharding_result(sharding_res_arr[sharding_idx], q_range_arr[sharding_idx],is_finish),false);
    EXPECT_FALSE(is_finish);
  }

  EXPECT_EQ(s_operator.seal(),OB_SUCCESS);
  int32_t beg = sharding_count*cell_count_each_sharding - 1;
  ObInnerCellInfo *cur_cell = NULL;
  int err = OB_SUCCESS;
  while ((err = s_operator.next_cell()) == OB_SUCCESS)
  {
    EXPECT_EQ(s_operator.get_cell(&cur_cell),OB_SUCCESS);
    row_key_len = snprintf(row_key_buf, sizeof(row_key_buf),"%d",beg);
    str = make_rowkey(row_key_buf,row_key_len, &allocator_);
    EXPECT_TRUE(cur_cell->row_key_ == str);
    beg --;
  }
  EXPECT_EQ(OB_ITER_END, err);
  EXPECT_EQ(beg, -1);
  delete [] sharding_res_arr;
  delete [] q_range_arr;
}

TEST(ObSortedOperator, forward_limit)
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
  param.set_limit_info(2,2);

  ObMergerSortedOperator s_operator;
  EXPECT_EQ(s_operator.set_param(param), OB_SUCCESS);
  for (int32_t i = 0; i < sharding_count * cell_count_each_sharding;  i += 2)
  {
    int sharding_idx = i/2;
    /// prepare range
    if (i > 0)
    {
      row_key_len = snprintf(row_key_buf, sizeof(row_key_buf),"%d",i - 1);
    }
    else
    {
      row_key_len = snprintf(row_key_buf, sizeof(row_key_buf),"%d",0);
    }
    str = make_rowkey(row_key_buf,row_key_len, &allocator_);
    EXPECT_EQ(buf.write_string(str,&row_key),OB_SUCCESS);
    q_range_arr[sharding_idx].start_key_ = row_key;
    if (i > 0)
    {
      q_range_arr[sharding_idx].border_flag_.unset_inclusive_start();
    }
    else
    {
      q_range_arr[sharding_idx].border_flag_.set_inclusive_start();
    }
    q_range_arr[sharding_idx].border_flag_.set_max_value();

    /// first cell
    row_key_len = snprintf(row_key_buf, sizeof(row_key_buf),"%d",i);
    str = make_rowkey(row_key_buf,row_key_len, &allocator_);
    EXPECT_EQ(buf.write_string(str,&row_key),OB_SUCCESS);
    cell.row_key_ = row_key;
    cell.value_.set_int(i - 1);
    EXPECT_EQ(sharding_res_arr[sharding_idx].add_cell(cell), OB_SUCCESS);

    /// second cell
    row_key_len = snprintf(row_key_buf, sizeof(row_key_buf),"%d",i + 1);
    str = make_rowkey(row_key_buf,row_key_len, &allocator_);
    EXPECT_EQ(buf.write_string(str,&row_key),OB_SUCCESS);
    cell.row_key_ = row_key;
    cell.value_.set_int(i - 1);
    EXPECT_EQ(sharding_res_arr[sharding_idx].add_cell(cell), OB_SUCCESS);

    EXPECT_EQ(sharding_res_arr[sharding_idx].set_is_req_fullfilled(false,2), OB_SUCCESS);
    bool is_finish = true;
    EXPECT_EQ(s_operator.add_sharding_result(sharding_res_arr[sharding_idx], q_range_arr[sharding_idx],is_finish),false);
    if(i < 2)
    {
      EXPECT_FALSE(is_finish);
    }
    else
    {
      EXPECT_TRUE(is_finish);
    }
  }
  EXPECT_EQ(s_operator.seal(),OB_SUCCESS);
  int32_t beg = 0;
  ObInnerCellInfo *cur_cell = NULL;
  int err = OB_SUCCESS;
  while ((err = s_operator.next_cell()) == OB_SUCCESS)
  {
    EXPECT_EQ(s_operator.get_cell(&cur_cell),OB_SUCCESS);
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

int main(int argc, char **argv)
{
  srandom(static_cast<uint32_t>(time(NULL)));
  ob_init_memory_pool(64*1024);
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

