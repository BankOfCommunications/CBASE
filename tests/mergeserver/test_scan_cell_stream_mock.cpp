/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * test_scan_cell_stream_mock.cpp is for what ...
 *
 * Version: $id: test_scan_cell_stream_mock.cpp,v 0.1 10/12/2010 10:59a wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#include <gtest/gtest.h>
#include "mergeserver/ob_ms_scan_cell_stream.h"
#include "mergeserver/ob_merge_join_agent.h"
#include "mergeserver/ob_cell_array.h"
#include "mergeserver/ob_cell_operator.h"
#include "mergeserver/ob_cell_stream.h"
#include "mergeserver/ob_ms_btreemap.h"
#include "mergeserver/ob_ms_tablet_location_item.h"
#include "mergeserver/ob_ms_cache_table.h"
#include "common/ob_define.h"
#include "common/ob_cache.h"
#include "common/ob_string.h"
#include "common/ob_action_flag.h"
#include "common/ob_scanner.h"
#include "common/ob_schema.h"
#include "common/ob_cache.h"
#include <vector>
#include <iostream>
using namespace testing;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace std;
namespace 
{
  ObServer root_server;
  ObServer update_server;
  ObServer merge_server;
  class MockRPCProxy:public ObMergerRpcProxy
  {
  public:
    MockRPCProxy()
    :ObMergerRpcProxy()
    {
      reset();
    }
    void add_scanner(ObScanner & scanner)
    {
      cell_scanner_vector_.push_back(&scanner);
    }
    void reset()
    {
      current_scanner_idx_ = 0;
      scan_param_ = NULL;
      prev_cell_ = NULL;
    }

    const ObScanParam *obtain_scan_param()
    {
      return scan_param_;
    }
  public:  
    // get
    virtual int cs_get(const common::ObGetParam & get_param, ObMergerTabletLocation & list, 
                       common::ObScanner & scanner)
    {
      UNUSED(list);
      UNUSED(get_param);
      return get_scanner_(scanner);
    }

    // scan
    virtual int cs_scan(const common::ObScanParam & scan_param, 
                        ObMergerTabletLocation & list, 
                        common::ObScanner & scanner)
    {
      UNUSED(list);
      scan_param_ = &scan_param;
      return get_scanner_(scanner);
    }

    virtual int ups_get(const common::ObGetParam & get_param, common::ObScanner & scanner)
    {
      UNUSED(get_param);
      return get_scanner_(scanner);
    }

    virtual int ups_scan(const common::ObScanParam & scan_param, common::ObScanner & scanner)
    {
      scan_param_ = &scan_param;
      return get_scanner_(scanner);
    }

  private:
    int get_scanner_(ObScanner &scanner)
    {
      int err  = OB_SUCCESS;
      ObRange self_range;
      ObCBtreeTable<int,int>::MapKey mapkey;
      char self_range_serialize_buf[1024];
      int64_t pos = 0;

      ObCellInfo *cur_cell = NULL;
      if (current_scanner_idx_ >= cell_scanner_vector_.size())
      {
        scanner.clear();
        scanner.set_is_req_fullfilled(true,0);
      }
      else
      {
        bool is_fullfilled = false;
        int64_t item_num = 0;
        bool is_row_changed = false;
        if (current_scanner_idx_ > 0)
        {
          assert(mapkey.compare_range_with_key(scan_param_->get_range()->table_id_,
                                               prev_cell_->row_key_, ObBorderFlag(),
                                               0, *scan_param_->get_range() ) == 0);
        }
        err = cell_scanner_vector_[current_scanner_idx_]->next_cell();
        if (OB_SUCCESS == err)
        {
          err = cell_scanner_vector_[current_scanner_idx_]->get_cell(&cur_cell, &is_row_changed);
          if (OB_SUCCESS == err)
          {
            self_range.start_key_ = cur_cell->row_key_;
          }
        }
        while (OB_SUCCESS == err)
        { 
          err = cell_scanner_vector_[current_scanner_idx_]->get_cell(&cur_cell, &is_row_changed);
          if (OB_SUCCESS == err
              &&NULL != scan_param_
              && mapkey.compare_range_with_key(scan_param_->get_range()->table_id_,
                                               cur_cell->row_key_, ObBorderFlag(), 
                                               0, *scan_param_->get_range() )== 0
             )
          {
            err = scanner.add_cell(*cur_cell);
            if (OB_SUCCESS == err)
            {
              prev_cell_ = cur_cell;
            }
            if (OB_SUCCESS == err)
            {
              item_num ++;
              err = cell_scanner_vector_[current_scanner_idx_]->next_cell();
            }
          }
        }
        if (OB_SUCCESS == err || OB_ITER_END == err)
        {
          self_range.end_key_ = cur_cell->row_key_;
          if (current_scanner_idx_ == cell_scanner_vector_.size() - 1)
          {
            is_fullfilled  = true;
          }
          err = OB_SUCCESS;
          current_scanner_idx_ ++;
          if(current_scanner_idx_ >= cell_scanner_vector_.size())
          {
            self_range.border_flag_.set_max_value();
          }
          err = self_range.serialize(self_range_serialize_buf,sizeof(self_range_serialize_buf),pos);
          if(OB_SUCCESS == err)
          {
            ObString extend_field_str;
            extend_field_str.assign(self_range_serialize_buf,pos);
            err = scanner.set_range(self_range);
          }
        }
        assert(item_num%scan_param_->get_column_id_size() == 0);
        prev_fullfill_ = (current_scanner_idx_%2 == 0);
        scanner.set_is_req_fullfilled(true, item_num/scan_param_->get_column_id_size());
        TBSYS_LOG(DEBUG, "fullfilled_item_num:%ld", item_num/scan_param_->get_column_id_size());
      }
      return err;
    }
    uint32_t current_scanner_idx_;
    vector<ObScanner*> cell_scanner_vector_;
    const ObScanParam *scan_param_;
    ObCellInfo        *prev_cell_;
    bool prev_fullfill_;
  };
}

TEST(str, str)
{
  char char_0 = 0;
  char char_m128 = -128;
  ObString str_0;
  ObString str_m128;
  str_0.assign(&char_0,1);
  str_m128.assign(&char_m128,1);
  EXPECT_TRUE(str_0 < str_m128);
}

TEST(ObMergerGetCellStream, GetRowEdge)
{
  int err = OB_SUCCESS;
  UNUSED(err);
  MockRPCProxy rpc_proxy;
  ObMSScanCellStream cell_stream(&rpc_proxy);
  ObVarCache<> get_cache;
  EXPECT_EQ(get_cache.init(-1,-1,1024),0);
  cell_stream.set_cache(get_cache);
  ObCellArray get_array;
  ObReadParam read_param;
  ObScanParam scan_param;
  ObMergerTabletLocation cs_addr;
  ObRange scan_range;
  int32_t row_width = 7;
  int32_t row_size = 32;
  int32_t row_size_each_get = (OB_MAX_COLUMN_NUMBER/row_width);
  int32_t scanner_size = (row_size +  row_size_each_get - 1)/row_size_each_get;
  ObScanner scanners[scanner_size];
  char rowkey_buf[128];
  int32_t rowkey_len = 1;
  char rowkey_val = random()%26 + 'a';
  memset(rowkey_buf,rowkey_val,sizeof(rowkey_buf));
  ObString rowkey;
  int64_t cell_value = 0;
  uint64_t table_id = 1;
  uint64_t column_id = 2;
  ObCellInfo cur_cell;
  ObCellInfo *cell_out;

  for (int32_t row_idx = 0; row_idx < row_size; row_idx ++)
  {
    rowkey.assign(rowkey_buf, rowkey_len);
    for (int32_t cell_idx = 0; cell_idx < row_width; cell_idx ++)
    {
      cur_cell.column_id_ = column_id  + cell_idx + 1;
      if(0 == row_idx)
      {
        EXPECT_EQ(scan_param.add_column(cur_cell.column_id_),0);
      }
      cur_cell.table_id_ = table_id;
      cur_cell.row_key_ = rowkey;
      cur_cell.value_.set_int(cell_value ++);
      EXPECT_EQ(scanners[row_idx/row_size_each_get].add_cell(cur_cell),0);
      EXPECT_EQ(get_array.append(cur_cell,cell_out),0);
    }
    rowkey_len ++;
  }
  for (int32_t i = 0; i < scanner_size; i++)
  {
    rpc_proxy.add_scanner(scanners[i]);
  }
  scan_range.table_id_ = table_id;
  scan_range.border_flag_.set_min_value();
  scan_range.border_flag_.set_max_value();
  scan_range.border_flag_.set_inclusive_start();
  scan_range.border_flag_.set_inclusive_end();
  scan_param.set(table_id,ObString(),scan_range);
  EXPECT_EQ(cell_stream.scan(scan_param,cs_addr),0);

  ObCellInfo *org_cell = NULL;
  for (int32_t row_idx = 0; row_idx < row_size; row_idx ++)
  {
    bool is_row_changed = false;
    for (int32_t cell_idx = 0; cell_idx < row_width; cell_idx ++)
    {
      EXPECT_EQ(cell_stream.next_cell(),0);
      EXPECT_EQ(get_array.get_cell(row_idx*row_width*1ll + cell_idx,org_cell),0);
      EXPECT_EQ(cell_stream.get_cell(&cell_out, &is_row_changed),0);
      if (0 == cell_idx)
      {
        EXPECT_EQ(is_row_changed,true);
      }
      else
      {
        EXPECT_EQ(is_row_changed,false);
      }
      EXPECT_TRUE(cell_out->row_key_ == org_cell->row_key_);
      EXPECT_EQ(cell_out->table_id_ , org_cell->table_id_);
      EXPECT_EQ(cell_out->column_id_, org_cell->column_id_);
    }
  }
  EXPECT_EQ(cell_stream.next_cell(),OB_ITER_END);

}



int main(int argc, char **argv)
{
  srandom(time(NULL));
  ob_init_memory_pool(64*1024);
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
