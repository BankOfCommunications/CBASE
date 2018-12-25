/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * param_modifier_test.cpp is for what ...
 *
 * Version: $id: param_modifier_test.cpp,v 0.1 10/28/2010 9:24a wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#include <gtest/gtest.h>
#include <stdlib.h>
#include <string>
#include "mergeserver/ob_read_param_modifier.h"
#include "common/ob_define.h"
#include "../common/test_rowkey_helper.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace testing;
using namespace std;

static CharArena allocator_;
bool operator==(const ObReadParam &pa, const ObReadParam &pb)
{
  bool result = false;
  result = ((pa.get_is_result_cached() == pb.get_is_result_cached())
    &&(pa.get_version_range().start_version_ == pb.get_version_range().start_version_)
    &&(pa.get_version_range().end_version_ == pb.get_version_range().end_version_)
    &&(pa.get_version_range().border_flag_.get_data()
    == pb.get_version_range().border_flag_.get_data())
    );
  return result;
}

/// bool operator ==(const ObNewRange & ra, const ObNewRange &rb)
/// {
///   return(ra.start_key_ == ra.start_key_
///     && ra.end_key_ == rb.end_key_
///     && ra.border_flag_.get_data() == rb.border_flag_.get_data()
///     );
/// }


TEST(ParamModifier, get_param)
{
  ObGetParam org_param;
  ObReadParam &org_read_param = org_param;
  ObScanner   cur_result;
  ObGetParam cur_param;
  ObReadParam &cur_read_param = cur_param;
  int64_t got_num = 0;
  int64_t cell_num = 1024;
  char rowkey_val = 'a';
  ObRowkey rowkey;
  bool is_cached = false;
  rowkey = make_rowkey(&rowkey_val, &allocator_);

  ObVersionRange version_range;
  version_range.border_flag_.set_max_value();
  version_range.border_flag_.set_min_value();
  org_param.set_version_range(version_range);
  org_param.set_is_result_cached(is_cached);

  for (int i = 0; i < cell_num; i ++)
  {
    ObCellInfo cur_cell;
    cur_cell.table_id_ = i+5;
    cur_cell.column_id_ = i + 6;
    cur_cell.row_key_ = rowkey;
    EXPECT_EQ(org_param.add_cell(cur_cell), OB_SUCCESS);
  }
  ObMSGetCellArray org_get_cells(org_param);
  EXPECT_EQ(org_param.get_cell_size(), cell_num);
  /// first time
  while (got_num < cell_num)
  {
    TBSYS_LOG(WARN,"got_num:%ld", got_num);
    cur_param.reset();
    EXPECT_EQ(get_next_param(org_read_param,org_get_cells,got_num, &cur_param),OB_SUCCESS);
    EXPECT_TRUE(org_read_param==cur_read_param);
    EXPECT_EQ(cur_param.get_cell_size() + got_num, org_param.get_cell_size());
    for (int64_t cell_idx = got_num; cell_idx < cell_num; cell_idx ++)
    {
      ASSERT_EQ(cur_param[cell_idx - got_num]->column_id_, org_param[cell_idx]->column_id_);
      ASSERT_EQ(cur_param[cell_idx - got_num]->table_id_, org_param[cell_idx]->table_id_);
      ASSERT_TRUE(cur_param[cell_idx - got_num]->row_key_ == org_param[cell_idx]->row_key_);
    }
    int64_t fullfilled_item_num = random()%(cell_num - got_num) + 1;
    int64_t memtable_version = random()%1024;
    EXPECT_EQ(cur_result.set_is_req_fullfilled(true, fullfilled_item_num), OB_SUCCESS);
    cur_result.set_data_version(memtable_version);

    int64_t rollback_num = cur_param.get_cell_size() - fullfilled_item_num;
    EXPECT_EQ(get_ups_param(cur_param,cur_result),OB_SUCCESS);
    EXPECT_EQ(cur_param.get_cell_size() + got_num + rollback_num, org_param.get_cell_size());
    EXPECT_EQ(cur_param.get_version_range().start_version_ , memtable_version + 1);
    EXPECT_TRUE(cur_param.get_version_range().border_flag_.is_max_value());
    EXPECT_TRUE(cur_param.get_version_range().border_flag_.inclusive_start());
    for (int64_t cell_idx = got_num; cell_idx < cell_num - rollback_num; cell_idx ++)
    {
      ASSERT_EQ(cur_param[cell_idx - got_num]->column_id_, org_param[cell_idx]->column_id_);
      ASSERT_EQ(cur_param[cell_idx - got_num]->table_id_, org_param[cell_idx]->table_id_);
      ASSERT_TRUE(cur_param[cell_idx - got_num]->row_key_ == org_param[cell_idx]->row_key_);
    }

    got_num += fullfilled_item_num;
  }
  EXPECT_EQ(get_next_param(org_read_param,org_get_cells,got_num, &cur_param), OB_ITER_END);
}

TEST(ParamModifier, scan_param)
{
  ObMemBuffer range_buffer;
  ObScanParam org_param;
  ObReadParam &org_read_param = org_param;
  ObScanParam cur_param;
  ObReadParam &cur_read_param = cur_param;
  ObScanner cur_result;

  int64_t pos = 0;
  char tablet_range_buf[1024];
  ObString tablet_range_str;
  ObNewRange tablet_range;

  bool is_cached = false;
  ObVersionRange version_range;
  version_range.border_flag_.set_max_value();
  version_range.border_flag_.set_min_value();
  org_param.set_version_range(version_range);
  org_param.set_is_result_cached(is_cached);

  char cur_last_key_val = 'c';
  string start_key_val("a");
  string end_key_val("z");
  string tablet_range_end_key_val("z");
  string cur_last_key(&cur_last_key_val, &cur_last_key_val + 1);
  ObCellInfo last_cell;
  ObRowkey rowkey;
  ObString start_key;
  ObString end_key;
  ObRowkey tablet_range_end_key;
  start_key.assign((char*)start_key_val.c_str(),static_cast<int32_t>(start_key_val.size()));
  end_key.assign((char*)end_key_val.c_str(),static_cast<int32_t>(end_key_val.size()));
  tablet_range_end_key = make_rowkey(tablet_range_end_key_val.c_str(), &allocator_);
  tablet_range.start_key_ = TestRowkeyHelper(start_key, &allocator_);
  tablet_range.end_key_  = TestRowkeyHelper(end_key, &allocator_);
  tablet_range.border_flag_.set_inclusive_start();
  tablet_range.border_flag_.set_inclusive_end();
  org_param.set(1,start_key,tablet_range);
  UNUSED(pos);
  UNUSED(tablet_range_buf);
  UNUSED(tablet_range_str);
  bool is_fullfilled = false;
  int64_t memtable_version = 24;
  /// first time
  EXPECT_EQ(get_next_param(org_param,cur_result,&cur_param, range_buffer),OB_SUCCESS);
  EXPECT_TRUE(org_read_param== cur_read_param);
  EXPECT_TRUE(*org_param.get_range() == *cur_param.get_range());

  /// current tablet not finish
  ObScanParam cur_param_1;
  ObScanParam &cur_read_param_1 = cur_param_1;
  cur_last_key[0]='c';
  cur_result.clear();
  rowkey = make_rowkey(cur_last_key.c_str(), &allocator_);
  last_cell.row_key_ = rowkey;
  EXPECT_EQ(cur_result.add_cell(last_cell),OB_SUCCESS);
  EXPECT_EQ(cur_result.set_is_req_fullfilled(is_fullfilled,1), OB_SUCCESS);
  cur_result.set_data_version(memtable_version);
  ///ups
  EXPECT_EQ(get_ups_param(cur_param,cur_result, range_buffer),OB_SUCCESS);
  EXPECT_TRUE(cur_param.get_range()->start_key_ == org_param.get_range()->start_key_);
  EXPECT_TRUE(cur_param.get_range()->border_flag_.inclusive_start()
    == org_param.get_range()->border_flag_.inclusive_start());
  EXPECT_TRUE(cur_param.get_range()->end_key_ == rowkey);
  EXPECT_TRUE(cur_param.get_range()->border_flag_.inclusive_end());
  ///cs
  EXPECT_EQ(get_next_param(org_param,cur_result,&cur_param_1,range_buffer),OB_SUCCESS);
  EXPECT_TRUE(org_read_param== cur_read_param_1);
  EXPECT_TRUE(cur_param_1.get_range()->start_key_ == last_cell.row_key_);
  EXPECT_TRUE(!cur_param_1.get_range()->border_flag_.inclusive_start());
  EXPECT_TRUE(cur_param_1.get_range()->end_key_ == org_param.get_range()->end_key_);
  EXPECT_TRUE(cur_param_1.get_range()->border_flag_.inclusive_end()
    == org_param.get_range()->border_flag_.inclusive_end());


  /// current tablet finish, all not finish
  is_fullfilled = true;
  ObScanParam cur_param_2;
  ObScanParam &cur_read_param_2 = cur_param_2;
  cur_last_key[0]='f';
  cur_result.clear();
  rowkey = make_rowkey(cur_last_key.c_str(), &allocator_);
  last_cell.row_key_ = rowkey;
  EXPECT_EQ(cur_result.add_cell(last_cell),OB_SUCCESS);
  EXPECT_EQ(cur_result.set_is_req_fullfilled(is_fullfilled,1), OB_SUCCESS);
  cur_result.set_data_version(memtable_version);
  tablet_range_end_key_val[0] = 'g';
  tablet_range_end_key = make_rowkey(tablet_range_end_key_val.c_str(), &allocator_);
  tablet_range.end_key_ = tablet_range_end_key;
  pos = 0;
  EXPECT_EQ(tablet_range.serialize(tablet_range_buf,sizeof(tablet_range_buf),pos),OB_SUCCESS);
  tablet_range_str.assign(tablet_range_buf,static_cast<int32_t>(pos));
  EXPECT_EQ(cur_result.set_range(tablet_range),OB_SUCCESS);
  ///ups
  EXPECT_EQ(get_ups_param(cur_param_1,cur_result,range_buffer),OB_SUCCESS);
  EXPECT_TRUE(cur_param_1.get_range()->end_key_ == tablet_range_end_key);
  EXPECT_TRUE(cur_param_1.get_range()->border_flag_.inclusive_end());
  ///cs
  EXPECT_EQ(get_next_param(org_param,cur_result,&cur_param_2,range_buffer),OB_SUCCESS);
  EXPECT_TRUE(org_read_param== cur_read_param_2);
  EXPECT_TRUE(cur_param_2.get_range()->start_key_ == tablet_range_end_key);
  EXPECT_TRUE(!cur_param_2.get_range()->border_flag_.inclusive_start());
  EXPECT_TRUE(cur_param_2.get_range()->end_key_ == org_param.get_range()->end_key_);
  EXPECT_TRUE(cur_param_2.get_range()->border_flag_.inclusive_end()
    == org_param.get_range()->border_flag_.inclusive_end());

  /// current tablet finish and all finish
  is_fullfilled = true;
  ObScanParam cur_param_3;
  cur_last_key[0]='f';
  cur_result.clear();
  rowkey = make_rowkey(cur_last_key.c_str(), &allocator_);
  last_cell.row_key_ = rowkey;
  EXPECT_EQ(cur_result.add_cell(last_cell),OB_SUCCESS);
  EXPECT_EQ(cur_result.set_is_req_fullfilled(is_fullfilled,1), OB_SUCCESS);
  cur_result.set_data_version(memtable_version);
  tablet_range_end_key_val[0] = 'z';
  tablet_range_end_key = make_rowkey(tablet_range_end_key_val.c_str(), &allocator_);
  tablet_range.end_key_ = tablet_range_end_key;
  pos = 0;
  EXPECT_EQ(tablet_range.serialize(tablet_range_buf,sizeof(tablet_range_buf),pos),OB_SUCCESS);
  tablet_range_str.assign(tablet_range_buf,static_cast<int32_t>(pos));
  EXPECT_EQ(cur_result.set_range(tablet_range),OB_SUCCESS);
  EXPECT_EQ(get_next_param(org_param,cur_result,&cur_param_3,range_buffer),OB_ITER_END);

  /// current tablet finish and all finish
  is_fullfilled = true;
  ObScanParam cur_param_4;
  cur_last_key[0]='f';
  cur_result.clear();
  rowkey = make_rowkey(cur_last_key.c_str(), &allocator_);
  last_cell.row_key_ = rowkey;
  EXPECT_EQ(cur_result.add_cell(last_cell),OB_SUCCESS);
  EXPECT_EQ(cur_result.set_is_req_fullfilled(is_fullfilled,1), OB_SUCCESS);
  cur_result.set_data_version(memtable_version);
  tablet_range_end_key_val[0] = 'a';
  tablet_range_end_key = make_rowkey(tablet_range_end_key_val.c_str(), &allocator_);
  tablet_range.end_key_ = tablet_range_end_key;
  tablet_range.end_key_.set_max_row();
  pos = 0;
  EXPECT_EQ(tablet_range.serialize(tablet_range_buf,sizeof(tablet_range_buf),pos),OB_SUCCESS);
  tablet_range_str.assign(tablet_range_buf,static_cast<int32_t>(pos));
  EXPECT_EQ(cur_result.set_range(tablet_range),OB_SUCCESS);
  EXPECT_EQ(get_next_param(org_param,cur_result,&cur_param_4,range_buffer),OB_ITER_END);
}


TEST(get_next_param, trivial_scan_forward)
{
  ObStringBuf buf;
  ObScanParam scan_param;
  ObNewRange q_range;
  ObScanner prev_result;
  int64_t prev_limit_offset = 0;
  ObNewRange prev_tablet_range;

  ObNewRange cur_range;
  int64_t cur_limit_offset = 0;

  uint64_t table_id = 1001;

  /// last tablet and full fill
  q_range.set_whole_range();
  EXPECT_EQ(scan_param.set(table_id, ObString(),q_range), OB_SUCCESS);
  prev_tablet_range.set_whole_range();

  EXPECT_EQ(prev_result.set_range(prev_tablet_range), OB_SUCCESS);
  EXPECT_EQ(prev_result.set_is_req_fullfilled(true,0), OB_SUCCESS);

  EXPECT_EQ(get_next_range(scan_param, prev_result,prev_limit_offset,cur_range,cur_limit_offset,buf), OB_ITER_END);

  /// last tablet but not fullfill
  ObCellInfo cell;
  ObRowkey str;
  const char *c_ptr = "9";
  str = make_rowkey(c_ptr, &allocator_);
  EXPECT_EQ(buf.write_string(str, &cell.row_key_),OB_SUCCESS);
  EXPECT_EQ(prev_result.add_cell(cell),OB_SUCCESS);
  EXPECT_EQ(prev_result.set_is_req_fullfilled(false,1), OB_SUCCESS);
  EXPECT_EQ(get_next_range(scan_param, prev_result,prev_limit_offset,cur_range,cur_limit_offset,buf), OB_SUCCESS);
  EXPECT_EQ(cur_limit_offset, 0);
  EXPECT_TRUE(cur_range.end_key_.is_max_row());
  EXPECT_FALSE(cur_range.start_key_.is_min_row());
  EXPECT_TRUE(cur_range.start_key_ == cell.row_key_);
  EXPECT_FALSE(cur_range.start_key_.is_min_row());
  EXPECT_FALSE(cur_range.border_flag_.inclusive_start());

  /// middle table and full fill
  prev_tablet_range.reset();
  cur_range.reset();
  prev_tablet_range.end_key_ = cell.row_key_;
  EXPECT_EQ(get_next_range(scan_param, prev_result,prev_limit_offset,cur_range,cur_limit_offset,buf), OB_SUCCESS);
  EXPECT_EQ(prev_result.set_is_req_fullfilled(true,1), OB_SUCCESS);
  EXPECT_EQ(cur_limit_offset, 0);
  EXPECT_TRUE(cur_range.is_whole_range());
  EXPECT_TRUE(cur_range.start_key_ == cell.row_key_);
  EXPECT_FALSE(cur_range.start_key_.is_min_row());
  EXPECT_FALSE(cur_range.border_flag_.inclusive_start());

  /// middle table and not full fill
  prev_tablet_range.reset();
  cur_range.reset();
  prev_tablet_range.end_key_ = cell.row_key_;
  prev_result.reset();
  c_ptr = "5";
  str = make_rowkey(c_ptr, &allocator_);
  EXPECT_EQ(buf.write_string(str, &cell.row_key_),OB_SUCCESS);
  EXPECT_EQ(prev_result.set_range(prev_tablet_range), OB_SUCCESS);
  EXPECT_EQ(prev_result.add_cell(cell),OB_SUCCESS);
  EXPECT_EQ(prev_result.set_is_req_fullfilled(false,1), OB_SUCCESS);
  EXPECT_EQ(get_next_range(scan_param, prev_result,prev_limit_offset,cur_range,cur_limit_offset,buf), OB_SUCCESS);
  EXPECT_EQ(cur_limit_offset, 0);
  EXPECT_TRUE(cur_range.is_whole_range());
  EXPECT_TRUE(cur_range.start_key_ == cell.row_key_);
  EXPECT_FALSE(cur_range.start_key_.is_min_row());
  EXPECT_FALSE(cur_range.border_flag_.inclusive_start());
}

TEST(get_next_param, trivial_scan_backward)
{
  ObStringBuf buf;
  ObScanParam scan_param;
  ObNewRange q_range;
  ObScanner prev_result;
  int64_t prev_limit_offset = 0;
  ObNewRange prev_tablet_range;

  ObNewRange cur_range;
  int64_t cur_limit_offset = 0;

  uint64_t table_id = 1001;

  /// last tablet and full fill
  q_range.set_whole_range();
  EXPECT_EQ(scan_param.set(table_id, ObString(),q_range), OB_SUCCESS);
  scan_param.set_scan_direction(ObScanParam::BACKWARD);
  prev_tablet_range.set_whole_range();

  EXPECT_EQ(prev_result.set_range(prev_tablet_range), OB_SUCCESS);
  EXPECT_EQ(prev_result.set_is_req_fullfilled(true,0), OB_SUCCESS);

  EXPECT_EQ(get_next_range(scan_param, prev_result,prev_limit_offset,cur_range,cur_limit_offset,buf), OB_ITER_END);

  /// last tablet but not fullfill
  ObCellInfo cell;
  ObRowkey str;
  const char *c_ptr = "8";
  str = make_rowkey(c_ptr, &allocator_);
  EXPECT_EQ(buf.write_string(str, &cell.row_key_),OB_SUCCESS);
  EXPECT_EQ(prev_result.add_cell(cell),OB_SUCCESS);
  EXPECT_EQ(prev_result.set_is_req_fullfilled(false,1), OB_SUCCESS);
  EXPECT_EQ(get_next_range(scan_param, prev_result,prev_limit_offset,cur_range,cur_limit_offset,buf), OB_SUCCESS);
  EXPECT_EQ(cur_limit_offset, 0);
  EXPECT_TRUE(cur_range.is_whole_range());
  EXPECT_TRUE(cur_range.end_key_ == cell.row_key_);
  EXPECT_TRUE(cur_range.start_key_.is_min_row());
  EXPECT_FALSE(cur_range.border_flag_.inclusive_end());

  /// middle table and full fill
  prev_tablet_range.reset();
  cur_range.reset();
  prev_result.clear();
  prev_tablet_range.start_key_ = cell.row_key_;
  c_ptr = "9";
  str = make_rowkey(c_ptr, &allocator_);
  EXPECT_EQ(buf.write_string(str, &cell.row_key_),OB_SUCCESS);
  EXPECT_EQ(prev_result.add_cell(cell),OB_SUCCESS);
  EXPECT_EQ(prev_result.set_is_req_fullfilled(true,1), OB_SUCCESS);
  EXPECT_EQ(prev_result.set_range(prev_tablet_range), OB_SUCCESS);
  EXPECT_EQ(get_next_range(scan_param, prev_result,prev_limit_offset,cur_range,cur_limit_offset,buf), OB_SUCCESS);
  EXPECT_EQ(cur_limit_offset, 0);
  EXPECT_TRUE(cur_range.is_whole_range());
  EXPECT_TRUE(cur_range.end_key_ == prev_tablet_range.start_key_);
  EXPECT_TRUE(cur_range.border_flag_.inclusive_end());

  /// middle table and not full fill
  cur_range.reset();
  prev_result.clear();
  c_ptr = "9";
  str = make_rowkey(c_ptr, &allocator_);
  EXPECT_EQ(buf.write_string(str, &cell.row_key_),OB_SUCCESS);
  EXPECT_EQ(prev_result.add_cell(cell),OB_SUCCESS);
  EXPECT_EQ(prev_result.set_is_req_fullfilled(false,1), OB_SUCCESS);
  EXPECT_EQ(prev_result.set_range(prev_tablet_range), OB_SUCCESS);
  EXPECT_EQ(get_next_range(scan_param, prev_result,prev_limit_offset,cur_range,cur_limit_offset,buf), OB_SUCCESS);
  EXPECT_EQ(cur_limit_offset, 0);
  EXPECT_TRUE(cur_range.is_whole_range());
  EXPECT_TRUE(cur_range.end_key_ == cell.row_key_);
  EXPECT_FALSE(cur_range.border_flag_.inclusive_end());
}

TEST(get_next_param, orderby)
{
  ObStringBuf buf;
  ObScanParam scan_param;
  ObNewRange q_range;
  ObScanner prev_result;
  int64_t prev_limit_offset = 0;
  ObNewRange prev_tablet_range;

  ObNewRange cur_range;
  int64_t cur_limit_offset = 0;

  uint64_t table_id = 1001;

  /// last tablet and full fill
  q_range.is_whole_range();
  EXPECT_EQ(scan_param.set(table_id, ObString(),q_range), OB_SUCCESS);
  //scan_param.set_scan_direction(ObScanParam::BACKWARD);
  prev_tablet_range.set_whole_range();
  EXPECT_EQ(scan_param.add_orderby_column(0),OB_SUCCESS);

  const char *c_ptr = "9";
  ObRowkey str;
  ObCellInfo cell;
  str = make_rowkey(c_ptr, &allocator_);
  EXPECT_EQ(buf.write_string(str, &cell.row_key_),OB_SUCCESS);
  EXPECT_EQ(prev_result.add_cell(cell),OB_SUCCESS);
  EXPECT_EQ(prev_result.set_is_req_fullfilled(false,1), OB_SUCCESS);
  EXPECT_EQ(prev_result.set_range(prev_tablet_range), OB_SUCCESS);

  EXPECT_EQ(get_next_range(scan_param, prev_result,prev_limit_offset,cur_range,cur_limit_offset,buf), OB_SUCCESS);
  EXPECT_EQ(cur_limit_offset, 1);
  EXPECT_TRUE(cur_range.is_whole_range());
}

int main(int argc, char **argv)
{
  srandom(static_cast<uint32_t>(time(NULL)));
  ob_init_memory_pool(64*1024);
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
