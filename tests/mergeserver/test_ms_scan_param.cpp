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
#include "mergeserver/ob_ms_scan_param.h"
#include "mergeserver/ob_ms_tsi.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace testing;
using namespace std;
void clear_assis()
{
  GET_TSI_MULT(ObMSSchemaDecoderAssis, TSI_MS_SCHEMA_DECODER_ASSIS_1)->clear();
}

void init_decode_param(ObStringBuf &buf,  ObScanParam &org_scan_param)
{
  UNUSED(buf);
  const char *c_ptr;
  ObString str;
  ObNewRange q_range;
  q_range.set_whole_range();
  q_range.border_flag_.set_inclusive_start();
  q_range.border_flag_.set_inclusive_end();
  c_ptr = "collect_info";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  org_scan_param.reset();
  EXPECT_EQ(org_scan_param.set(OB_INVALID_ID, str,q_range), OB_SUCCESS);

  /// basic columns
  bool is_return = false;
  c_ptr = "item_collect_count";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.add_column(str,is_return), OB_SUCCESS );

  is_return = false;
  c_ptr = "item_category";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.add_column(str,is_return), OB_SUCCESS );

  is_return = true;
  c_ptr = "item_price";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.add_column(str,is_return), OB_SUCCESS );

  /// composite columns
  c_ptr = "`item_collect_count`*`item_price`";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.add_column(str,str, is_return), OB_SUCCESS );

  /// where condition
  c_ptr = "`item_price` > 10";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.add_where_cond(str), OB_SUCCESS );

  /// groupby::group by columns
  is_return = false;
  c_ptr = "item_category";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.get_group_by_param().add_groupby_column(str,is_return), OB_SUCCESS);

  /// aggregate columns
  is_return = true;
  c_ptr = "item_price";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.get_group_by_param().add_aggregate_column(str,str,  SUM, is_return), OB_SUCCESS);

  /// aggregate columns
  is_return = false;
  c_ptr = "item_collect_count";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.get_group_by_param().add_aggregate_column(str,str,  SUM, is_return), OB_SUCCESS);

  /// composite columns
  c_ptr = "`item_collect_count`*`item_price`";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.get_group_by_param().add_column(str, str,is_return), OB_SUCCESS );

  /// having condtion
  c_ptr = "`item_price` > 10";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.get_group_by_param().add_having_cond(str), OB_SUCCESS );

  /// orderby
  c_ptr = "item_price";
  str.assign((char*)c_ptr, static_cast<int32_t>(strlen(c_ptr)));
  EXPECT_EQ(org_scan_param.add_orderby_column(str),OB_SUCCESS);
}

TEST(ObMsScanParam, columns)
{
  ObSchemaManagerV2 *mgr = new ObSchemaManagerV2();
  tbsys::CConfig config;
  EXPECT_TRUE(mgr->parse_from_file("schema.ini",config));

  ObScanParam scan_param;
  ObScanParam decoded_param;
  ObStringBuf buf;
  init_decode_param(buf,scan_param);
  clear_assis();
  EXPECT_EQ(ob_decode_scan_param(scan_param,*mgr,decoded_param),OB_SUCCESS);
  ObMergerScanParam ms_scan_param;
  EXPECT_EQ(ms_scan_param.set_param(decoded_param), OB_SUCCESS);

  /// select basic columns
  int32_t return_info_idx = 0;
  EXPECT_EQ(scan_param.get_column_name_size(), ms_scan_param.get_cs_param()->get_column_id_size());
  EXPECT_EQ(scan_param.get_column_name_size(), ms_scan_param.get_ms_param()->get_column_id_size());
  for (int32_t i = 0; i < scan_param.get_column_name_size(); i ++, return_info_idx ++)
  {
    EXPECT_EQ(ms_scan_param.get_cs_param()->get_column_id()[i], ms_scan_param.get_ms_param()->get_column_id()[i]);
    EXPECT_TRUE(*ms_scan_param.get_cs_param()->get_return_infos().at(return_info_idx));
    EXPECT_EQ(*scan_param.get_return_infos().at(return_info_idx),
      *ms_scan_param.get_ms_param()->get_return_infos().at(return_info_idx));
  }

  /// select composite columns
  EXPECT_EQ(scan_param.get_composite_columns_size(), ms_scan_param.get_cs_param()->get_composite_columns_size());
  EXPECT_EQ(scan_param.get_composite_columns_size(), ms_scan_param.get_ms_param()->get_composite_columns_size());
  for (int32_t i = 0; i < scan_param.get_composite_columns_size();i ++, return_info_idx++)
  {
    EXPECT_TRUE(*ms_scan_param.get_cs_param()->get_return_infos().at(return_info_idx));
    EXPECT_EQ(*scan_param.get_return_infos().at(return_info_idx),
      *ms_scan_param.get_ms_param()->get_return_infos().at(return_info_idx));
    const ObObj * cs_expr = ms_scan_param.get_cs_param()->get_composite_columns().at(i)->get_postfix_expr();
    const ObObj * ms_expr = ms_scan_param.get_ms_param()->get_composite_columns().at(i)->get_postfix_expr();
    for (int obj_idx = 0;; obj_idx += 2)
    {
      int64_t cs_type  = 0;
      int64_t ms_type  = 0;
      EXPECT_EQ(cs_expr[obj_idx].get_int(cs_type),OB_SUCCESS);
      EXPECT_EQ(ms_expr[obj_idx].get_int(ms_type),OB_SUCCESS);
      EXPECT_EQ(cs_type, ms_type);
      if (ms_type == ObExpression::END)
      {
        break;
      }
      EXPECT_TRUE(cs_expr[obj_idx + 1] == ms_expr[obj_idx + 1]);
    }
  }

  /// where condition
  EXPECT_EQ(ms_scan_param.get_ms_param()->get_filter_info().get_count(), 0);
  EXPECT_EQ(ms_scan_param.get_cs_param()->get_filter_info().get_count(), scan_param.get_filter_info().get_count());

  return_info_idx = 0;
  /// groupby::groupby columns
  EXPECT_EQ(scan_param.get_group_by_param().get_groupby_columns().get_array_index(),
    ms_scan_param.get_cs_param()->get_group_by_param().get_groupby_columns().get_array_index());
  EXPECT_EQ(scan_param.get_group_by_param().get_groupby_columns().get_array_index(),
    ms_scan_param.get_ms_param()->get_group_by_param().get_groupby_columns().get_array_index());

  for (int32_t i = 0; i < scan_param.get_group_by_param().get_groupby_columns().get_array_index(); i ++, return_info_idx ++)
  {
    EXPECT_TRUE(*ms_scan_param.get_ms_param()->get_group_by_param().get_groupby_columns().at(i)  ==
      *ms_scan_param.get_ms_param()->get_group_by_param().get_groupby_columns().at(i));

    EXPECT_TRUE(*ms_scan_param.get_cs_param()->get_group_by_param().get_return_infos().at(return_info_idx));
    EXPECT_EQ(*scan_param.get_group_by_param().get_return_infos().at(return_info_idx),
      *ms_scan_param.get_ms_param()->get_group_by_param().get_return_infos().at(return_info_idx));
  }

  /// groupby::aggregate columns
  EXPECT_EQ(scan_param.get_group_by_param().get_aggregate_columns().get_array_index(),
    ms_scan_param.get_cs_param()->get_group_by_param().get_aggregate_columns().get_array_index());
  EXPECT_EQ(scan_param.get_group_by_param().get_aggregate_columns().get_array_index(),
    ms_scan_param.get_ms_param()->get_group_by_param().get_aggregate_columns().get_array_index());

  for (int32_t i = 0; i < scan_param.get_group_by_param().get_aggregate_columns().get_array_index(); i ++, return_info_idx ++)
  {
    EXPECT_TRUE(*ms_scan_param.get_ms_param()->get_group_by_param().get_aggregate_columns().at(i)  ==
      *ms_scan_param.get_ms_param()->get_group_by_param().get_aggregate_columns().at(i));

    EXPECT_TRUE(*ms_scan_param.get_cs_param()->get_group_by_param().get_return_infos().at(return_info_idx));
    EXPECT_EQ(*scan_param.get_group_by_param().get_return_infos().at(return_info_idx),
      *ms_scan_param.get_ms_param()->get_group_by_param().get_return_infos().at(return_info_idx));
  }
  /// groupby::composite columns columns
  EXPECT_EQ(scan_param.get_group_by_param().get_return_infos().get_array_index(),
    ms_scan_param.get_cs_param()->get_group_by_param().get_return_infos().get_array_index());
  EXPECT_EQ(scan_param.get_group_by_param().get_return_infos().get_array_index(),
    ms_scan_param.get_ms_param()->get_group_by_param().get_return_infos().get_array_index());
  for (int32_t i = 0; i < scan_param.get_group_by_param().get_composite_columns().get_array_index();i ++, return_info_idx++)
  {
    EXPECT_TRUE(*ms_scan_param.get_cs_param()->get_group_by_param().get_return_infos().at(return_info_idx));
    EXPECT_EQ(*scan_param.get_group_by_param().get_return_infos().at(return_info_idx),
      *ms_scan_param.get_ms_param()->get_group_by_param().get_return_infos().at(return_info_idx));
    const ObObj * cs_expr = ms_scan_param.get_cs_param()->get_group_by_param().get_composite_columns().at(i)->get_postfix_expr();
    const ObObj * ms_expr = ms_scan_param.get_ms_param()->get_group_by_param().get_composite_columns().at(i)->get_postfix_expr();
    for (int obj_idx = 0;; obj_idx += 2)
    {
      int64_t cs_type  = 0;
      int64_t ms_type  = 0;
      EXPECT_EQ(cs_expr[obj_idx].get_int(cs_type),OB_SUCCESS);
      EXPECT_EQ(ms_expr[obj_idx].get_int(ms_type),OB_SUCCESS);
      EXPECT_EQ(cs_type, ms_type);
      if (ms_type == ObExpression::END)
      {
        break;
      }
      EXPECT_TRUE(cs_expr[obj_idx + 1] == ms_expr[obj_idx + 1]);
    }
  }
  /// having condition
  EXPECT_EQ(ms_scan_param.get_cs_param()->get_group_by_param().get_having_condition().get_count(),  0);
  EXPECT_EQ(ms_scan_param.get_ms_param()->get_group_by_param().get_having_condition().get_count(),
    scan_param.get_group_by_param().get_having_condition().get_count());

  /// orderby
  EXPECT_EQ(ms_scan_param.get_cs_param()->get_orderby_column_size(),
    ((scan_param.get_group_by_param().get_aggregate_row_width() > 0) && (scan_param.get_orderby_column_size() == 0))?
    1: scan_param.get_orderby_column_size());
  EXPECT_EQ(ms_scan_param.get_ms_param()->get_orderby_column_size(),
    scan_param.get_orderby_column_size());

  /// limit info witout sharding_minimum preicession
  scan_param.set_limit_info(0,100);
  clear_assis();
  EXPECT_EQ(ob_decode_scan_param(scan_param,*mgr,decoded_param),OB_SUCCESS);
  EXPECT_EQ(ms_scan_param.set_param(decoded_param), OB_SUCCESS);

  /// orderby
  EXPECT_EQ(ms_scan_param.get_cs_param()->get_orderby_column_size(),
    ((scan_param.get_group_by_param().get_aggregate_row_width() > 0) && (scan_param.get_orderby_column_size() == 0))?
    1: scan_param.get_orderby_column_size());
  EXPECT_EQ(ms_scan_param.get_ms_param()->get_orderby_column_size(),
    scan_param.get_orderby_column_size());
  int64_t limit_offset;
  int64_t limit_count;
  int64_t sharding_minimum;
  double topk_precision;
  ms_scan_param.get_cs_param()->get_limit_info(limit_offset,limit_count);
  ms_scan_param.get_cs_param()->get_topk_precision(sharding_minimum,topk_precision);
  EXPECT_EQ(limit_offset,0);
  EXPECT_EQ(limit_count,0);
  EXPECT_EQ(sharding_minimum,0);
  EXPECT_EQ(topk_precision,0);

  ms_scan_param.get_ms_param()->get_limit_info(limit_offset,limit_count);
  ms_scan_param.get_ms_param()->get_topk_precision(sharding_minimum,topk_precision);
  EXPECT_EQ(limit_offset,0);
  EXPECT_EQ(limit_count,100);
  EXPECT_EQ(sharding_minimum,0);
  EXPECT_EQ(topk_precision,0);

  /// limit info witout sharding_minimum preicession
  scan_param.set_limit_info(0,100);
  scan_param.set_topk_precision(1000, 0.5);
  clear_assis();
  EXPECT_EQ(ob_decode_scan_param(scan_param,*mgr,decoded_param),OB_SUCCESS);
  EXPECT_EQ(ms_scan_param.set_param(decoded_param), OB_SUCCESS);

  /// orderby
  EXPECT_EQ(ms_scan_param.get_cs_param()->get_orderby_column_size(), scan_param.get_orderby_column_size());
  EXPECT_EQ(ms_scan_param.get_ms_param()->get_orderby_column_size(),
    scan_param.get_orderby_column_size());
  ms_scan_param.get_cs_param()->get_limit_info(limit_offset,limit_count);
  ms_scan_param.get_cs_param()->get_topk_precision(sharding_minimum,topk_precision);
  EXPECT_EQ(limit_offset,0);
  EXPECT_EQ(limit_count,0);
  EXPECT_EQ(sharding_minimum,1000);
  EXPECT_EQ(topk_precision,0.5);

  ms_scan_param.get_ms_param()->get_limit_info(limit_offset,limit_count);
  ms_scan_param.get_ms_param()->get_topk_precision(sharding_minimum,topk_precision);
  EXPECT_EQ(limit_offset,0);
  EXPECT_EQ(limit_count,100);
  EXPECT_EQ(sharding_minimum,0);
  EXPECT_EQ(topk_precision,0);

  delete mgr;
}

int main(int argc, char **argv)
{
  srandom(static_cast<uint32_t>(time(NULL)));
  ob_init_memory_pool(64*1024);
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
