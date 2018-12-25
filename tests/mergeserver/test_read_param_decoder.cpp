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
#include "common/ob_tsi_factory.h"
#include "mergeserver/ob_groupby_operator.h"
#include "mergeserver/ob_read_param_decoder.h"
#include "mergeserver/ob_ms_tsi.h"
#include "../common/test_rowkey_helper.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace testing;
using namespace std;

static CharArena allocator_;
TEST(ob_read_param_decoder, get_param)
{
  ObGetParam org_param;
  ObGetParam decoded_param;
  ObSchemaManagerV2 *mgr = new ObSchemaManagerV2();
  tbsys::CConfig config;
  EXPECT_TRUE(mgr->parse_from_file("schema.ini",config));
  ObString str;
  ObString table_name;
  ObString column_name;
  ObStringBuf buffer;
  ObRowkey row_key;
  ObRowkey key;
  ObCellInfo cell;
  ObCellInfo id_cell;
  vector<ObCellInfo> id_vec;
  key = make_rowkey("rowkey", &allocator_);
  EXPECT_EQ(buffer.write_string(key, &row_key),OB_SUCCESS);
  cell.row_key_ = row_key;

  str.assign(const_cast<char*>("collect_info"),static_cast<int32_t>(strlen("collect_info")));
  EXPECT_EQ(buffer.write_string(str,&table_name),OB_SUCCESS);
  str.assign(const_cast<char*>("info_user_nick"),static_cast<int32_t>(strlen("info_user_nick")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  cell.table_name_ = table_name;
  cell.column_name_ = column_name;
  EXPECT_EQ(org_param.add_cell(cell),OB_SUCCESS);
  id_cell.table_id_ = 1001;
  id_cell.column_id_ = 2;
  id_vec.push_back(id_cell);

  str.assign(const_cast<char*>("collect_info"),static_cast<int32_t>(strlen("collect_info")));
  EXPECT_EQ(buffer.write_string(str,&table_name),OB_SUCCESS);
  str.assign(const_cast<char*>("info_is_shared"),static_cast<int32_t>(strlen("info_is_shared")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  cell.table_name_ = table_name;
  cell.column_name_ = column_name;
  EXPECT_EQ(org_param.add_cell(cell),OB_SUCCESS);
  id_cell.table_id_ = 1001;
  id_cell.column_id_ = 3;
  id_vec.push_back(id_cell);

  str.assign(const_cast<char*>("collect_item"),static_cast<int32_t>(strlen("collect_item")));
  EXPECT_EQ(buffer.write_string(str,&table_name),OB_SUCCESS);
  str.assign(const_cast<char*>("item_picurl"),static_cast<int32_t>(strlen("item_picurl")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  cell.table_name_ = table_name;
  cell.column_name_ = column_name;
  EXPECT_EQ(org_param.add_cell(cell),OB_SUCCESS);
  id_cell.table_id_ = 1002;
  id_cell.column_id_ = 11;
  id_vec.push_back(id_cell);


  str.assign(const_cast<char*>("collect_info"),static_cast<int32_t>(strlen("collect_info")));
  EXPECT_EQ(buffer.write_string(str,&table_name),OB_SUCCESS);
  str.assign(const_cast<char*>("info_tag"),static_cast<int32_t>(strlen("info_tag")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  cell.table_name_ = table_name;
  cell.column_name_ = column_name;
  EXPECT_EQ(org_param.add_cell(cell),OB_SUCCESS);
  id_cell.table_id_ = 1001;
  id_cell.column_id_ = 8;
  id_vec.push_back(id_cell);

  /// get all
  cell.column_name_ = ObGetParam::OB_GET_ALL_COLUMN_NAME;
  EXPECT_EQ(org_param.add_cell(cell),OB_SUCCESS);
  const ObTableSchema *table_schema = mgr->get_table_schema(table_name);
  EXPECT_NE(table_schema, (const ObTableSchema *)0);
  const ObColumnSchemaV2 *column_beg = NULL;
  int32_t column_size = 0;
  column_beg =  mgr->get_table_schema(table_schema->get_table_id(),column_size);

  ObGetParam get_param_with_name;
  EXPECT_EQ(ob_decode_get_param(org_param,*mgr,decoded_param,get_param_with_name),OB_SUCCESS);
  EXPECT_EQ(decoded_param.get_cell_size(), static_cast<int64_t>(id_vec.size()) + column_size);
  for (int64_t i = 0; i < static_cast<int64_t>(id_vec.size()); i++)
  {
    EXPECT_EQ(decoded_param[i]->table_id_, id_vec[i].table_id_);
    EXPECT_EQ(decoded_param[i]->column_id_, id_vec[i].column_id_);
  }

  /// check get all

  EXPECT_NE(column_beg, (const ObColumnSchemaV2 *)0);
  for (int32_t i = 0; i < column_size; i ++)
  {
    EXPECT_EQ(decoded_param[id_vec.size() + i]->table_id_, column_beg[i].get_table_id());
    EXPECT_EQ(decoded_param[id_vec.size() + i]->column_id_, column_beg[i].get_id());
  }


  ObResultCode rc;
  char err_msg[256];
  rc.message_.assign(err_msg,sizeof(err_msg));
  str.assign(const_cast<char*>("xxxx"),static_cast<int32_t>(strlen("xxxx")));
  EXPECT_EQ(buffer.write_string(str,&table_name),OB_SUCCESS);
  str.assign(const_cast<char*>("xxxx"),static_cast<int32_t>(strlen("xxxx")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  cell.table_name_ = table_name;
  cell.column_name_ = column_name;
  EXPECT_EQ(org_param.add_cell(cell),OB_SUCCESS);
  EXPECT_NE(ob_decode_get_param(org_param,*mgr,decoded_param,get_param_with_name,&rc),OB_SUCCESS);
  TBSYS_LOG(WARN,"err_msg:%s", err_msg);
}


TEST(ob_read_param_decoder, scan_param_without_group)
{
  ObScanParam org_param;
  ObScanParam decoded_param;
  ObSchemaManagerV2 *mgr = new ObSchemaManagerV2();
  tbsys::CConfig config;
  EXPECT_TRUE(mgr->parse_from_file("schema.ini",config));
  ObString str;
  ObString table_name;
  ObString column_name;
  ObStringBuf buffer;
  ObString row_key;
  ObCellInfo cell;
  ObNewRange range;
  vector<uint64_t> id_vec;

  str.assign(const_cast<char*>("collect_info"),static_cast<int32_t>(strlen("collect_info")));
  EXPECT_EQ(buffer.write_string(str,&table_name),OB_SUCCESS);
  range.border_flag_.unset_inclusive_start();
  range.set_whole_range();
  range.border_flag_.unset_inclusive_start();
  org_param.set(OB_INVALID_ID,table_name,range);

  /// add columns
  str.assign(const_cast<char*>("info_user_nick"),static_cast<int32_t>(strlen("info_user_nick")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name),OB_SUCCESS);
  id_vec.push_back(2);


  str.assign(const_cast<char*>("item_title"),static_cast<int32_t>(strlen("item_title")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name),OB_SUCCESS);
  id_vec.push_back(10);

  str.assign(const_cast<char*>("info_is_shared"),static_cast<int32_t>(strlen("info_is_shared")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name),OB_SUCCESS);
  id_vec.push_back(3);

  str.assign(const_cast<char*>("item_picurl"),static_cast<int32_t>(strlen("item_picurl")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name),OB_SUCCESS);
  id_vec.push_back(11);

  str.assign(const_cast<char*>("item_price"),static_cast<int32_t>(strlen("item_price")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name),OB_SUCCESS);
  id_vec.push_back(14);

  /// add filter
  ObObj obj;
  std::vector<int64_t> cond_ids;

  str.assign(const_cast<char*>("info_is_shared"),static_cast<int32_t>(strlen("info_is_shared")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  obj.set_int(5);
  EXPECT_EQ(org_param.add_where_cond(column_name,LT,obj), OB_SUCCESS);
  cond_ids.push_back(2);

  str.assign(const_cast<char*>("item_price"),static_cast<int32_t>(strlen("item_price")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  obj.set_int(5);
  EXPECT_EQ(org_param.add_where_cond(column_name,GT,obj), OB_SUCCESS);
  cond_ids.push_back(4);

  /// add order by
  vector<int64_t> order_idx;
  str.assign(const_cast<char*>("info_is_shared"),static_cast<int32_t>(strlen("info_is_shared")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.add_orderby_column(column_name, ObScanParam::ASC),OB_SUCCESS);
  order_idx.push_back(2);

  str.assign(const_cast<char*>("item_price"),static_cast<int32_t>(strlen("item_price")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.add_orderby_column(column_name, ObScanParam::ASC),OB_SUCCESS);
  order_idx.push_back(4);

  /// add limit
  const int64_t org_limit_offset = 10;
  const int64_t org_limit_count = 15;
  EXPECT_EQ(org_param.set_limit_info(org_limit_offset,org_limit_count),OB_SUCCESS);

  /// decode
  GET_TSI_MULT(ObMSSchemaDecoderAssis, TSI_MS_SCHEMA_DECODER_ASSIS_1)->clear();
  EXPECT_EQ(ob_decode_scan_param(org_param,*mgr,decoded_param),OB_SUCCESS);
  EXPECT_EQ(decoded_param.get_table_id(), 1001ull);

  EXPECT_EQ(decoded_param.get_column_id_size(), static_cast<int64_t>(id_vec.size()));
  for (uint32_t i = 0; i < id_vec.size(); i++)
  {
    EXPECT_EQ(decoded_param.get_column_id()[i], id_vec[i]);
  }

  EXPECT_EQ(decoded_param.get_filter_info().get_count(), static_cast<int64_t>(cond_ids.size()));
  for (uint32_t i = 0; i < cond_ids.size();i++)
  {
    EXPECT_EQ(cond_ids[i], decoded_param.get_filter_info()[i]->get_column_index());
  }

  int64_t orderby_size = -1;
  int64_t const *order_columns = NULL;
  uint8_t const *orders = NULL;
  decoded_param.get_orderby_column(order_columns,orders,orderby_size);
  EXPECT_EQ(orderby_size, static_cast<int64_t>(order_idx.size()));
  for (uint32_t i = 0; i < order_idx.size();i ++)
  {
    EXPECT_EQ(order_idx[i], order_columns[i]);
  }

  int64_t limit_offset = -1;
  int64_t limit_count = -1;
  decoded_param.get_limit_info(limit_offset,limit_count);
  EXPECT_EQ(org_limit_count,limit_count);
  EXPECT_EQ(org_limit_offset,limit_offset);

  ObResultCode rc;
  char err_msg[256];
  rc.message_.assign(err_msg,sizeof(err_msg));
  str.assign((char*)"xxxxx",static_cast<int32_t>(strlen("xxxxx")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.add_orderby_column(column_name),OB_SUCCESS);
  GET_TSI_MULT(ObMSSchemaDecoderAssis, TSI_MS_SCHEMA_DECODER_ASSIS_1)->clear();
  EXPECT_NE(ob_decode_scan_param(org_param,*mgr,decoded_param, &rc),OB_SUCCESS);
  TBSYS_LOG(WARN,"err_msg:%s", err_msg);
}

TEST(ob_read_param_decoder, scan_param_with_group)
{
  ObScanParam org_param;
  ObScanParam decoded_param;
  ObSchemaManagerV2 *mgr = new ObSchemaManagerV2();
  tbsys::CConfig config;
  EXPECT_TRUE(mgr->parse_from_file("schema.ini",config));
  ObString str;
  ObString table_name;
  ObString column_name;
  ObStringBuf buffer;
  ObString row_key;
  ObCellInfo cell;
  ObNewRange range;
  vector<uint64_t> id_vec;

  str.assign(const_cast<char*>("collect_info"),static_cast<int32_t>(strlen("collect_info")));
  EXPECT_EQ(buffer.write_string(str,&table_name),OB_SUCCESS);
  range.border_flag_.unset_inclusive_start();
  range.set_whole_range();
  org_param.set(OB_INVALID_ID,table_name,range);

  /// add columns
  str.assign(const_cast<char*>("info_user_nick"),static_cast<int32_t>(strlen("info_user_nick")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name),OB_SUCCESS);
  id_vec.push_back(2);

  str.assign(const_cast<char*>("item_title"),static_cast<int32_t>(strlen("item_title")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name),OB_SUCCESS);
  id_vec.push_back(10);

  str.assign(const_cast<char*>("info_is_shared"),static_cast<int32_t>(strlen("info_is_shared")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name),OB_SUCCESS);
  id_vec.push_back(3);

  str.assign(const_cast<char*>("item_picurl"),static_cast<int32_t>(strlen("item_picurl")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name),OB_SUCCESS);
  id_vec.push_back(11);

  str.assign(const_cast<char*>("item_price"),static_cast<int32_t>(strlen("item_price")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name),OB_SUCCESS);
  id_vec.push_back(14);

  /// add filter
  ObObj obj;
  std::vector<int64_t> cond_ids;

  str.assign(const_cast<char*>("info_is_shared"),static_cast<int32_t>(strlen("info_is_shared")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  obj.set_int(5);
  EXPECT_EQ(org_param.add_where_cond(column_name,LT,obj), OB_SUCCESS);
  cond_ids.push_back(2);

  str.assign(const_cast<char*>("item_price"),static_cast<int32_t>(strlen("item_price")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  obj.set_int(5);
  EXPECT_EQ(org_param.add_where_cond(column_name,GT,obj), OB_SUCCESS);
  cond_ids.push_back(4);

  /// add group by
  vector<int64_t> groupby_idx;
  str.assign(const_cast<char*>("info_user_nick"),static_cast<int32_t>(strlen("info_user_nick")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.get_group_by_param().add_groupby_column(column_name),OB_SUCCESS);
  groupby_idx.push_back(0);

  vector<int64_t> return_idx;
  str.assign(const_cast<char*>("info_is_shared"),static_cast<int32_t>(strlen("info_is_shared")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.get_group_by_param().add_return_column(column_name),OB_SUCCESS);
  return_idx.push_back(2);

  vector<int64_t> agg_idx;
  ObString as_column_name;
  str.assign(const_cast<char*>("item_price"),static_cast<int32_t>(strlen("item_price")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  str.assign(const_cast<char*>("sum_item_price"),static_cast<int32_t>(strlen("sum_item_price")));
  EXPECT_EQ(buffer.write_string(str,&as_column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.get_group_by_param().add_aggregate_column(column_name,as_column_name,SUM),OB_SUCCESS);
  agg_idx.push_back(4);

  /// add order by
  vector<int64_t> order_idx;
  str.assign(const_cast<char*>("info_is_shared"),static_cast<int32_t>(strlen("info_is_shared")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.add_orderby_column(column_name, ObScanParam::ASC),OB_SUCCESS);
  order_idx.push_back(1);

  str.assign(const_cast<char*>("sum_item_price"),static_cast<int32_t>(strlen("sum_item_price")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.add_orderby_column(column_name, ObScanParam::ASC),OB_SUCCESS);
  order_idx.push_back(2);

  /// add limit
  const int64_t org_limit_offset = 10;
  const int64_t org_limit_count = 15;
  EXPECT_EQ(org_param.set_limit_info(org_limit_offset,org_limit_count),OB_SUCCESS);

  /// decode
  GET_TSI_MULT(ObMSSchemaDecoderAssis, TSI_MS_SCHEMA_DECODER_ASSIS_1)->clear();
  EXPECT_EQ(ob_decode_scan_param(org_param,*mgr,decoded_param),OB_SUCCESS);
  EXPECT_EQ(decoded_param.get_table_id(), 1001ull);



  /// basic columns
  EXPECT_EQ(decoded_param.get_column_id_size(), static_cast<int64_t>(id_vec.size()));
  for (uint32_t i = 0; i < id_vec.size(); i++)
  {
    EXPECT_EQ(decoded_param.get_column_id()[i], id_vec[i]);
  }

  /// filter
  EXPECT_EQ(decoded_param.get_filter_info().get_count(), static_cast<int64_t>(cond_ids.size()));
  for (uint32_t i = 0; i < cond_ids.size();i++)
  {
    EXPECT_EQ(cond_ids[i], decoded_param.get_filter_info()[i]->get_column_index());
  }

  /// group by
  EXPECT_EQ(decoded_param.get_group_by_param().get_aggregate_row_width(),
    static_cast<int64_t>(groupby_idx.size() + return_idx.size() + agg_idx.size()));
  EXPECT_EQ(decoded_param.get_group_by_param().get_groupby_columns().get_array_index(),
    static_cast<int64_t>(groupby_idx.size()));
  for (uint32_t i = 0; i < groupby_idx.size(); i++)
  {
    EXPECT_EQ(groupby_idx[i], decoded_param.get_group_by_param().get_groupby_columns().at(i)->org_column_idx_);
  }

  EXPECT_EQ(decoded_param.get_group_by_param().get_return_columns().get_array_index(),
    static_cast<int64_t>(return_idx.size()));
  for (uint32_t i = 0; i < return_idx.size(); i++)
  {
    EXPECT_EQ(return_idx[i], decoded_param.get_group_by_param().get_return_columns().at(i)->org_column_idx_);
  }

  EXPECT_EQ(decoded_param.get_group_by_param().get_aggregate_columns().get_array_index(),
    static_cast<int64_t>(agg_idx.size()));
  for (uint32_t i = 0; i < agg_idx.size(); i++)
  {
    EXPECT_EQ(agg_idx[i], decoded_param.get_group_by_param().get_aggregate_columns().at(i)->get_org_column_idx());
  }


  /// orders
  int64_t orderby_size = -1;
  int64_t const *order_columns = NULL;
  uint8_t const *orders = NULL;
  decoded_param.get_orderby_column(order_columns,orders,orderby_size);
  EXPECT_EQ(orderby_size, static_cast<int64_t>(order_idx.size()));
  for (uint32_t i = 0; i < order_idx.size();i ++)
  {
    EXPECT_EQ(order_idx[i], order_columns[i]);
  }

  /// limit
  int64_t limit_offset = -1;
  int64_t limit_count = -1;
  decoded_param.get_limit_info(limit_offset,limit_count);
  EXPECT_EQ(org_limit_count,limit_count);
  EXPECT_EQ(org_limit_offset,limit_offset);


  /// orderby not exist column
  /// add columns
  str.assign(const_cast<char*>("wrong_column"),static_cast<int32_t>(strlen("wrong_column")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.add_orderby_column(column_name), OB_SUCCESS);

  str.assign(const_cast<char*>("info_user_nick"),static_cast<int32_t>(strlen("info_user_nick")));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(org_param.add_orderby_column(column_name), OB_SUCCESS);
  GET_TSI_MULT(ObMSSchemaDecoderAssis, TSI_MS_SCHEMA_DECODER_ASSIS_1)->clear();
  EXPECT_NE(ob_decode_scan_param(org_param,*mgr,decoded_param),OB_SUCCESS);


}

TEST(ob_read_param_decoder, scan_param_select_all)
{
  ObScanParam org_param;
  ObScanParam decoded_param;
  ObSchemaManagerV2 *mgr = new ObSchemaManagerV2();
  tbsys::CConfig config;
  EXPECT_TRUE(mgr->parse_from_file("schema.ini",config));
  ObString str;
  ObString table_name;
  ObString column_name;
  ObStringBuf buffer;
  ObString row_key;
  ObCellInfo cell;
  ObNewRange range;
  vector<uint64_t> id_vec;

  str.assign(const_cast<char*>("collect_info"),static_cast<int32_t>(strlen("collect_info")));
  EXPECT_EQ(buffer.write_string(str,&table_name),OB_SUCCESS);
  range.set_whole_range();
  range.border_flag_.unset_inclusive_start();
  org_param.set(OB_INVALID_ID,table_name,range);

  GET_TSI_MULT(ObMSSchemaDecoderAssis, TSI_MS_SCHEMA_DECODER_ASSIS_1)->clear();
  EXPECT_EQ(ob_decode_scan_param(org_param,*mgr,decoded_param),OB_SUCCESS);
  const int64_t sql_buf_size = 4096;
  char *sql_buf = new char[sql_buf_size];
  int64_t sql_pos = 0;
  EXPECT_EQ(org_param.to_str(sql_buf,sql_buf_size,sql_pos), OB_SUCCESS);
  TBSYS_LOG(WARN,"org_sql:%s", sql_buf);

  sql_pos = 0;
  EXPECT_EQ(decoded_param.to_str(sql_buf,sql_buf_size,sql_pos), OB_SUCCESS);
  TBSYS_LOG(WARN,"decoded_sql:%s", sql_buf);

  delete [] sql_buf;

  uint64_t table_id = OB_INVALID_ID;
  const ObTableSchema *table_schema = NULL;
  table_schema = mgr->get_table_schema(table_name);
  EXPECT_NE(table_schema,reinterpret_cast<void*>( 0));
  EXPECT_EQ(table_schema->get_table_id(),1001ull);
  table_id = table_schema->get_table_id();
  table_schema = mgr->get_table_schema(table_id);
  EXPECT_NE(table_schema,reinterpret_cast<void*>( 0));
  const ObColumnSchemaV2 * column_info = NULL;
  int32_t column_size = 0;
  column_info = mgr->get_table_schema(table_schema->get_table_id(),column_size);
  EXPECT_NE(column_info,reinterpret_cast<void*>(0));
  EXPECT_GE(column_size, 0);
  for (int32_t i = 0;i < column_size; i++)
  {
    EXPECT_EQ(column_info[i].get_id(), decoded_param.get_column_id()[i]);
  }
}


TEST(ob_read_param_decoder, return_info_and_composite_column)
{
  ObStringBuf buf;
  ObScanParam org_param;
  ObScanParam decoded_param;
  const char * c_student_name = "name";
  const char * c_course_name = "course";
  const char * c_score_name = "score";
  const char * c_gravity_name = "gravity";
  const char * table_name_ptr  = "score";
  const char * c_ptr = NULL;
  UNUSED(c_ptr);
  ObSchemaManagerV2 *mgr = new ObSchemaManagerV2();
  tbsys::CConfig config;
  EXPECT_TRUE(mgr->parse_from_file("score.ini",config));
  ObString table_name;
  ObString str;
  ObString column_name;
  str.assign((char*)table_name_ptr,static_cast<int32_t>(strlen(table_name_ptr)));
  EXPECT_EQ(buf.write_string(str,&table_name), OB_SUCCESS);
  const char *start_key_cptr = "a";
  const char *end_key_cptr = "z";
  ObNewRange scan_range;
  scan_range.border_flag_.set_inclusive_start();
  scan_range.start_key_ = make_rowkey(start_key_cptr, &allocator_);
  scan_range.end_key_ = make_rowkey(end_key_cptr, &allocator_);

  EXPECT_EQ(org_param.set(OB_INVALID_ID,table_name,scan_range), OB_SUCCESS);

  str.assign((char*)c_course_name,static_cast<int32_t>(strlen(c_course_name)));
  EXPECT_EQ(buf.write_string(str,&column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name,false),OB_SUCCESS);

  str.assign((char*)c_student_name,static_cast<int32_t>(strlen(c_student_name)));
  EXPECT_EQ(buf.write_string(str,&column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name,true),OB_SUCCESS);

  str.assign((char*)c_score_name,static_cast<int32_t>(strlen(c_score_name)));
  EXPECT_EQ(buf.write_string(str,&column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name,true),OB_SUCCESS);

  str.assign((char*)c_gravity_name,static_cast<int32_t>(strlen(c_gravity_name)));
  EXPECT_EQ(buf.write_string(str,&column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(column_name,false),OB_SUCCESS);
  /// basic columns
  GET_TSI_MULT(ObMSSchemaDecoderAssis, TSI_MS_SCHEMA_DECODER_ASSIS_1)->clear();
  EXPECT_EQ(ob_decode_scan_param(org_param,*mgr,decoded_param), OB_SUCCESS);

  EXPECT_EQ(decoded_param.get_column_id_size(), 4);
  EXPECT_EQ(decoded_param.get_composite_columns().get_array_index(), 0);
  EXPECT_EQ(decoded_param.get_return_info_size(), 4);
  for (int64_t i = 0; i < decoded_param.get_return_info_size(); i ++)
  {
    EXPECT_EQ(*org_param.get_return_infos().at(i), *decoded_param.get_return_infos().at(i));
  }
  EXPECT_EQ(decoded_param.get_column_id()[0],3ull);
  EXPECT_EQ(decoded_param.get_column_id()[1],2ull);
  EXPECT_EQ(decoded_param.get_column_id()[2],4ull);
  EXPECT_EQ(decoded_param.get_column_id()[3],5ull);

  /// composite columns
  ObString expr;
  ObString as_name;
  c_ptr = "`score`*`gravity`/100";
  str.assign((char*)c_ptr,static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(buf.write_string(str, &expr), OB_SUCCESS);
  const char * c_weighted_score_name = "weighted_score";
  c_ptr = c_weighted_score_name;
  str.assign((char*)c_ptr,static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(buf.write_string(str, &as_name), OB_SUCCESS);
  EXPECT_EQ(org_param.add_column(expr,as_name, true),OB_SUCCESS);
  GET_TSI_MULT(ObMSSchemaDecoderAssis, TSI_MS_SCHEMA_DECODER_ASSIS_1)->clear();
  EXPECT_EQ(ob_decode_scan_param(org_param,*mgr,decoded_param), OB_SUCCESS);

  EXPECT_EQ(decoded_param.get_composite_columns().get_array_index(), 1);
  EXPECT_EQ(decoded_param.get_return_info_size(), 5);
  for (int64_t i = 0; i < decoded_param.get_return_info_size(); i ++)
  {
    EXPECT_EQ(*org_param.get_return_infos().at(i), *decoded_param.get_return_infos().at(i));
  }
  ObCellArray org_array;
  ObString val_str;
  ObCellInfo cell;
  ObInnerCellInfo *cell_out = NULL;;
  ///  math | wushi  | 90 | 40
  c_ptr = "math";
  str.assign((char*)c_ptr,static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(buf.write_string(str, &val_str), OB_SUCCESS);
  cell.value_.set_varchar(val_str);
  EXPECT_EQ(org_array.append(cell, cell_out), OB_SUCCESS);

  c_ptr = "wushi";
  str.assign((char*)c_ptr,static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(buf.write_string(str, &val_str), OB_SUCCESS);
  cell.value_.set_varchar(val_str);
  EXPECT_EQ(org_array.append(cell, cell_out), OB_SUCCESS);

  int64_t val_int = 90;
  double val_double = 90.0;
  cell.value_.set_int(val_int);
  EXPECT_EQ(org_array.append(cell, cell_out), OB_SUCCESS);

  val_int = 40;
  cell.value_.set_int(val_int);
  EXPECT_EQ(org_array.append(cell, cell_out), OB_SUCCESS);

  ObObj comp_val;
  EXPECT_EQ(decoded_param.get_composite_columns().at(0)->calc_composite_val(comp_val, org_array,0,3), OB_SUCCESS);
  EXPECT_EQ(comp_val.get_double(val_double), OB_SUCCESS);
  EXPECT_EQ(val_double, (double)90.0*40/100);
  cell.value_ = comp_val;
  /// append comp column
  EXPECT_EQ(org_array.append(cell, cell_out), OB_SUCCESS);

  /// where true condition
  c_ptr = "`course` = 'math' and `name` = 'wushi'";
  str.assign((char*)c_ptr,static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(buf.write_string(str, &expr), OB_SUCCESS);
  EXPECT_EQ(org_param.add_where_cond(expr),OB_SUCCESS);
  GET_TSI_MULT(ObMSSchemaDecoderAssis, TSI_MS_SCHEMA_DECODER_ASSIS_1)->clear();
  EXPECT_EQ(ob_decode_scan_param(org_param,*mgr,decoded_param), OB_SUCCESS);

  EXPECT_EQ(decoded_param.get_composite_columns().get_array_index(), 2);
  EXPECT_EQ(decoded_param.get_return_info_size(), 6);
  for (int64_t i = 0; i < decoded_param.get_return_info_size(); i ++)
  {
    EXPECT_EQ(*org_param.get_return_infos().at(i), *decoded_param.get_return_infos().at(i));
  }
  EXPECT_FALSE(*decoded_param.get_return_infos().at(decoded_param.get_return_info_size()-1));
  EXPECT_EQ(decoded_param.get_composite_columns().at(1)->calc_composite_val(comp_val, org_array,0,4), OB_SUCCESS);
  bool val_bool = false;
  EXPECT_EQ(comp_val.get_bool(val_bool), OB_SUCCESS);
  EXPECT_TRUE(val_bool);
  cell.value_ = comp_val;
  /// append where true condition
  EXPECT_EQ(org_array.append(cell, cell_out), OB_SUCCESS);

  bool check_result = false;
  EXPECT_EQ(decoded_param.get_filter_info().check(org_array,0,5,check_result), OB_SUCCESS);
  EXPECT_TRUE(check_result);

  /// where false condtion
  c_ptr = "`weighted_score` >= 50";
  str.assign((char*)c_ptr,static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(buf.write_string(str, &expr), OB_SUCCESS);
  EXPECT_EQ(org_param.add_where_cond(expr),OB_SUCCESS);
  GET_TSI_MULT(ObMSSchemaDecoderAssis, TSI_MS_SCHEMA_DECODER_ASSIS_1)->clear();
  EXPECT_EQ(ob_decode_scan_param(org_param,*mgr,decoded_param), OB_SUCCESS);

  EXPECT_EQ(decoded_param.get_composite_columns().get_array_index(), 3);
  EXPECT_EQ(decoded_param.get_return_info_size(), 7);
  for (int64_t i = 0; i < decoded_param.get_return_info_size(); i ++)
  {
    EXPECT_EQ(*org_param.get_return_infos().at(i), *decoded_param.get_return_infos().at(i));
  }
  EXPECT_FALSE(*decoded_param.get_return_infos().at(decoded_param.get_return_info_size()-1));
  EXPECT_EQ(decoded_param.get_composite_columns().at(2)->calc_composite_val(comp_val, org_array,0,4), OB_SUCCESS);
  EXPECT_EQ(comp_val.get_bool(val_bool), OB_SUCCESS);
  EXPECT_FALSE(val_bool);
  cell.value_ = comp_val;
  /// append where false condition
  EXPECT_EQ(org_array.append(cell, cell_out), OB_SUCCESS);

  /// prepare second row
  ///  english | wushi | 85 | 20
  c_ptr = "english";
  str.assign((char*)c_ptr,static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(buf.write_string(str, &val_str), OB_SUCCESS);
  cell.value_.set_varchar(val_str);
  EXPECT_EQ(org_array.append(cell, cell_out), OB_SUCCESS);


  c_ptr = "wushi";
  str.assign((char*)c_ptr,static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(buf.write_string(str, &val_str), OB_SUCCESS);
  cell.value_.set_varchar(val_str);
  EXPECT_EQ(org_array.append(cell, cell_out), OB_SUCCESS);

  val_int = 85;
  cell.value_.set_int(val_int);
  EXPECT_EQ(org_array.append(cell, cell_out), OB_SUCCESS);

  val_int = 20;
  cell.value_.set_int(val_int);
  EXPECT_EQ(org_array.append(cell, cell_out), OB_SUCCESS);


  EXPECT_EQ(decoded_param.get_composite_columns().at(0)->calc_composite_val(comp_val, org_array,7,10), OB_SUCCESS);
  cell.value_ = comp_val;
  EXPECT_EQ(comp_val.get_double(val_double), OB_SUCCESS);
  EXPECT_EQ(val_double, (double)85.0*20/100);
  EXPECT_EQ(org_array.append(cell, cell_out), OB_SUCCESS);
  EXPECT_EQ(decoded_param.get_composite_columns().at(1)->calc_composite_val(comp_val, org_array,0,4), OB_SUCCESS);
  cell.value_ = comp_val;
  EXPECT_EQ(comp_val.get_bool(val_bool), OB_SUCCESS);
  EXPECT_TRUE(val_bool);
  EXPECT_EQ(org_array.append(cell, cell_out), OB_SUCCESS);
  EXPECT_EQ(decoded_param.get_composite_columns().at(2)->calc_composite_val(comp_val, org_array,0,4), OB_SUCCESS);
  cell.value_ = comp_val;
  EXPECT_EQ(comp_val.get_bool(val_bool), OB_SUCCESS);
  EXPECT_FALSE(val_bool);
  EXPECT_EQ(org_array.append(cell, cell_out), OB_SUCCESS);
  /// groupby
  str.assign((char*)c_student_name,static_cast<int32_t>(strlen(c_student_name)));
  EXPECT_EQ(buf.write_string(str,&column_name), OB_SUCCESS);
  EXPECT_EQ(org_param.get_group_by_param().add_groupby_column(column_name,true), OB_SUCCESS);
  str.assign((char*)c_score_name,static_cast<int32_t>(strlen(c_score_name)));
  EXPECT_EQ(buf.write_string(str,&column_name), OB_SUCCESS);
  c_ptr = "total";
  str.assign((char*)c_ptr,static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(buf.write_string(str,&as_name), OB_SUCCESS);
  EXPECT_EQ(org_param.get_group_by_param().add_aggregate_column(column_name,as_name, SUM), OB_SUCCESS);
  c_ptr = c_weighted_score_name;
  str.assign((char*)c_ptr,static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(buf.write_string(str, &as_name), OB_SUCCESS);
  EXPECT_EQ(org_param.get_group_by_param().add_aggregate_column(as_name,as_name, SUM), OB_SUCCESS);

  ObGroupByOperator groupby_cells;
  UNUSED(groupby_cells);
  GET_TSI_MULT(ObMSSchemaDecoderAssis, TSI_MS_SCHEMA_DECODER_ASSIS_1)->clear();
  EXPECT_EQ(ob_decode_scan_param(org_param,*mgr,decoded_param), OB_SUCCESS);
  EXPECT_EQ(decoded_param.get_group_by_param().get_groupby_columns().get_array_index(), 1);
  EXPECT_EQ(decoded_param.get_group_by_param().get_return_columns().get_array_index(), 0);
  EXPECT_EQ(decoded_param.get_group_by_param().get_aggregate_columns().get_array_index(), 2);
  EXPECT_EQ(decoded_param.get_group_by_param().get_composite_columns().get_array_index(), 0);
  EXPECT_EQ(decoded_param.get_group_by_param().get_return_infos().get_array_index(), 3);
  for (int64_t i = 0;
    i < decoded_param.get_group_by_param().get_return_infos().get_array_index() - 1;
    i++)
  {
    EXPECT_EQ(*org_param.get_group_by_param().get_return_infos().at(i),*decoded_param.get_group_by_param().get_return_infos().at(i));
  }
  EXPECT_EQ(groupby_cells.init(decoded_param.get_group_by_param(),1024*1024*10),OB_SUCCESS);
  EXPECT_EQ(groupby_cells.add_row(org_array,0,6),OB_SUCCESS);
  //EXPECT_EQ(groupby_cells.add_row(org_array,7,13),OB_SUCCESS);
  EXPECT_EQ(groupby_cells.get_cell_size(),3);
  EXPECT_EQ(groupby_cells[0].value_.get_varchar(column_name),OB_SUCCESS);
  c_ptr = "wushi";
  str.assign((char*)c_ptr,static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_TRUE(str==column_name);
  //EXPECT_EQ(groupby_cells[1].value_.get_int(val_int),OB_SUCCESS);
  //EXPECT_EQ(val_int,90+85);
  //EXPECT_EQ(groupby_cells[2].value_.get_double(val_double),OB_SUCCESS);
  //EXPECT_EQ(val_double,90.0*40/100+85.0*20/100);

  /// composite column
  c_ptr = "`weighted_score`*1.0/`total`";
  str.assign((char*)c_ptr,static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(buf.write_string(str,&expr),OB_SUCCESS);
  c_ptr = "weight";
  str.assign((char*)c_ptr,static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(buf.write_string(str,&as_name),OB_SUCCESS);
  EXPECT_EQ(org_param.get_group_by_param().add_column(expr,as_name,false),OB_SUCCESS);
  GET_TSI_MULT(ObMSSchemaDecoderAssis, TSI_MS_SCHEMA_DECODER_ASSIS_1)->clear();
  EXPECT_EQ(ob_decode_scan_param(org_param,*mgr,decoded_param), OB_SUCCESS);
  EXPECT_EQ(decoded_param.get_group_by_param().get_groupby_columns().get_array_index(), 1);
  EXPECT_EQ(decoded_param.get_group_by_param().get_return_columns().get_array_index(), 0);
  EXPECT_EQ(decoded_param.get_group_by_param().get_aggregate_columns().get_array_index(), 2);
  EXPECT_EQ(decoded_param.get_group_by_param().get_composite_columns().get_array_index(), 1);
  EXPECT_EQ(decoded_param.get_group_by_param().get_return_infos().get_array_index(), 4);
  for (int64_t i = 0;
    i < decoded_param.get_group_by_param().get_return_infos().get_array_index() - 1;
    i++)
  {
    EXPECT_EQ(*org_param.get_group_by_param().get_return_infos().at(i),*decoded_param.get_group_by_param().get_return_infos().at(i));
  }
  EXPECT_EQ(decoded_param.get_group_by_param().get_composite_columns().at(0)->calc_composite_val(comp_val,groupby_cells,0,2),
    OB_SUCCESS);
  val_double = 0.0;
  EXPECT_EQ(comp_val.get_double(val_double),OB_SUCCESS);
  //const double ex_double = 0.000000001;
  //EXPECT_LE(fabs(val_double - (90*40/100+85*20/100)*1.0/(90+85)), ex_double);
  cell.value_ = comp_val;
  EXPECT_EQ(groupby_cells.append(cell, cell_out), OB_SUCCESS);

  /// having condition
  /// composite column
  c_ptr = "`weight` < 1.0";
  str.assign((char*)c_ptr,static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(buf.write_string(str,&expr),OB_SUCCESS);

  EXPECT_EQ(org_param.get_group_by_param().add_having_cond(expr),OB_SUCCESS);
  GET_TSI_MULT(ObMSSchemaDecoderAssis, TSI_MS_SCHEMA_DECODER_ASSIS_1)->clear();
  EXPECT_EQ(ob_decode_scan_param(org_param,*mgr,decoded_param), OB_SUCCESS);

  const int64_t sql_buf_size = 4096;
  char *sql_buf = new char[sql_buf_size];
  int64_t sql_pos = 0;
  EXPECT_EQ(org_param.to_str(sql_buf,sql_buf_size,sql_pos), OB_SUCCESS);
  TBSYS_LOG(WARN,"org_sql:%s", sql_buf);

  sql_pos = 0;
  EXPECT_EQ(decoded_param.to_str(sql_buf,sql_buf_size,sql_pos), OB_SUCCESS);
  TBSYS_LOG(WARN,"decoed_sql:%s", sql_buf);

  delete [] sql_buf;

  EXPECT_EQ(decoded_param.get_group_by_param().get_groupby_columns().get_array_index(), 1);
  EXPECT_EQ(decoded_param.get_group_by_param().get_return_columns().get_array_index(), 0);
  EXPECT_EQ(decoded_param.get_group_by_param().get_aggregate_columns().get_array_index(), 2);
  EXPECT_EQ(decoded_param.get_group_by_param().get_composite_columns().get_array_index(), 2);
  EXPECT_EQ(decoded_param.get_group_by_param().get_return_infos().get_array_index(), 5);
  for (int64_t i = 0;
    i < decoded_param.get_group_by_param().get_return_infos().get_array_index() - 1;
    i++)
  {
    EXPECT_EQ(*org_param.get_group_by_param().get_return_infos().at(i),*decoded_param.get_group_by_param().get_return_infos().at(i));
  }

  EXPECT_EQ(decoded_param.get_group_by_param().get_composite_columns().at(1)->calc_composite_val(comp_val,groupby_cells,0,3),
    OB_SUCCESS);
  EXPECT_EQ(comp_val.get_bool(val_bool),OB_SUCCESS);
  EXPECT_TRUE(val_bool);
}

int main(int argc, char **argv)
{
  TBSYS_LOGGER.setLogLevel("WARN");
  srandom(static_cast<uint32_t>(time(NULL)));
  ob_init_memory_pool(64*1024);
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

