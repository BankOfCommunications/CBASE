/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_adgroup_impfession_count.cpp for define adgroup impfession
 * count query classs. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */

#include "ob_adgroup_impression_count.h"

namespace oceanbase 
{ 
  namespace olapdrive 
  {
    using namespace tbsys;
    using namespace common;
    using namespace client;

    ObAdgroupImpressionCount::ObAdgroupImpressionCount(
      ObClient& client, ObOlapdriveMeta& meta,
      ObOlapdriveSchema& schema, ObOlapdriveParam& param,
      ObOlapdriveStat& stat)
    : ObLzQuery(client, meta, schema, param, stat)
    {

    }

    ObAdgroupImpressionCount::~ObAdgroupImpressionCount()
    {

    }

    const char* ObAdgroupImpressionCount::get_test_name()
    {
      return "ADGROUP_IMPRESSION_COUNT";
    }

    int ObAdgroupImpressionCount::check(
      const ObScanner &scanner, const int64_t time)
    {
      int ret = OB_SUCCESS;
      ObScanInfo scan_info;

      //currently OB doesn't support distinct count
      ret = get_scan_info(scan_info);
      if (OB_SUCCESS == ret)
      {
        ObCampaign& campaign = scan_info.campaign_;
        int64_t org_row_cnt  = 
          (campaign.end_id_ - campaign.start_id_) * 2 * scan_info.day_count_;
        int64_t exp_count = campaign.end_adgroup_id_ - campaign.start_adgroup_id_;

        print_brief_result(get_test_name(), scanner, scan_info, 
                           time, org_row_cnt);

        ret = check_row_and_cell_num(scanner, exp_count, exp_count * 3);
        if (OB_SUCCESS == ret)
        {
          FOREACH_CELL(scanner)
          {
            GET_CELL();
            //ret = check_one_cell(*cell_info, 1, column_index, exp_count, 0);
          }

          //FIXME: for check temporarily,
          if (row_index != exp_count)
          {
            TBSYS_LOG(WARN, "returns wrong count, row_index=%ld, exp_count=%ld",
                      row_index, exp_count);
            ret = OB_ERROR;
          }
        }
      }

      return ret;
    }

    int ObAdgroupImpressionCount::build_basic_column(ObScanParam& scan_param)
    {
      int ret = OB_SUCCESS;

      ret = add_scan_range(scan_param);
      if (OB_SUCCESS == ret)
      {
        set_read_param(scan_param);
        ret = scan_param.add_column(campaign_id);
      }

      if (OB_SUCCESS == ret)
      {
        ret = scan_param.add_column(adgroup_id);
      }

      if (OB_SUCCESS == ret)
      {
        ret = scan_param.add_column(impression);
      }

      if (OB_SUCCESS == ret)
      {
        ret = scan_param.add_column(shop_type);
      }

      return ret;
    }

    int ObAdgroupImpressionCount::add_filter_column(ObScanParam& scan_param)
    {
      int ret             = OB_SUCCESS;

      START_BUILD_EXTRA_INFO(scan_param);
      extra_info->shop_type_ = random() % MAX_SHOP_TYPE;
      filter_len = snprintf(extra_info->filter_str, FILTER_BUF_SIZE, 
                            "(`shop_type` = %ld) AND (`impression` > 0)", 
                            extra_info->shop_type_);
      END_BUILD_EXTRA_INFO(scan_param);

      return ret;
    }

    int ObAdgroupImpressionCount::add_groupby_column(ObScanParam& scan_param)
    {
      int ret = OB_SUCCESS;
      ObGroupByParam& groupby_param = scan_param.get_group_by_param();

      ret = groupby_param.add_groupby_column(campaign_id);
      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_groupby_column(adgroup_id);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_aggregate_column(impression, impression_sum, SUM);
      }

      return ret;
    }

    int ObAdgroupImpressionCount::prepare(ObScanParam &scan_param)
    {
      int ret = OB_SUCCESS;

      ret = build_basic_column(scan_param);
      if (OB_SUCCESS == ret)
      {
        ret = add_filter_column(scan_param);
      }

      if (OB_SUCCESS == ret)
      {
        ret = add_groupby_column(scan_param);
      }

      return ret;
    }
  } // end namespace olapdrive
} // end namespace oceanbase
