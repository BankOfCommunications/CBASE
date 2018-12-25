/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_campaign_impression.cpp for define campaign impression 
 * query classs. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */

#include "ob_campaign_impression.h"

namespace oceanbase 
{ 
  namespace olapdrive 
  {
    using namespace tbsys;
    using namespace common;
    using namespace client;

    ObCampaignImpression::ObCampaignImpression(
      ObClient& client, ObOlapdriveMeta& meta,
      ObOlapdriveSchema& schema, ObOlapdriveParam& param,
      ObOlapdriveStat& stat)
    : ObLzQuery(client, meta, schema, param, stat)
    {

    }

    ObCampaignImpression::~ObCampaignImpression()
    {

    }

    const char* ObCampaignImpression::get_test_name()
    {
      return "CAMPAIGN_IMPRESSION";
    }

    int ObCampaignImpression::check(
      const ObScanner &scanner, const int64_t time)
    {
      int ret = OB_SUCCESS;
      ObScanInfo scan_info;

      ret = get_scan_info(scan_info);
      if (OB_SUCCESS == ret)
      {
        ObCampaign& campaign = scan_info.campaign_;
        int64_t org_row_cnt = 
          (campaign.end_id_ - campaign.start_id_) * 2 * scan_info.day_count_;

        print_brief_result(get_test_name(), scanner, scan_info, 
                           time, org_row_cnt);
        ret = check_row_and_cell_num(scanner, 1, 1);
        if (OB_SUCCESS == ret)
        {
          FOREACH_CELL(scanner)
          {
            GET_CELL();
            ret = check_one_cell(*cell_info, 1, column_index, 
                                 campaign.campaign_id_, 0);
          }
        }
      }

      return ret;
    }

    int ObCampaignImpression::prepare(ObScanParam &scan_param)
    {
      int ret = OB_SUCCESS;
      ObGroupByParam& groupby_param = scan_param.get_group_by_param();

      ret = reset_extra_info();
      if (OB_SUCCESS == ret)
      {
        ret = add_scan_range(scan_param);
      }

      if (OB_SUCCESS == ret)
      {
        set_read_param(scan_param);
        ret = scan_param.add_column(campaign_id);
      }

      if (OB_SUCCESS == ret)
      {
        ret = scan_param.add_column(impression);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_groupby_column(campaign_id);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_aggregate_column(impression, impression, SUM, false);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_having_cond(impression_filter_expr);
      }

      return ret;
    }
  } // end namespace olapdrive
} // end namespace oceanbase
