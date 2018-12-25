/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_campaign_effect.cpp for define campaign effect query 
 * classs. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */

#include "ob_campaign_effect.h"

namespace oceanbase 
{ 
  namespace olapdrive 
  {
    using namespace tbsys;
    using namespace common;
    using namespace client;

    ObCampaignEffect::ObCampaignEffect(
      ObClient& client, ObOlapdriveMeta& meta,
      ObOlapdriveSchema& schema, ObOlapdriveParam& param,
      ObOlapdriveStat& stat)
    : ObLzQuery(client, meta, schema, param, stat)
    {

    }

    ObCampaignEffect::~ObCampaignEffect()
    {

    }

    const char* ObCampaignEffect::get_test_name()
    {
      return "CAMPAIGN_EFFECT";
    }

    int ObCampaignEffect::check(
      const ObScanner &scanner, const int64_t time)
    {
      int ret           = OB_SUCCESS;
      ObScanInfo scan_info;

      ret = get_scan_info(scan_info);
      if (OB_SUCCESS == ret)
      {
        ObCampaign& campaign = scan_info.campaign_;
        int64_t org_row_cnt = 
          (campaign.end_id_ - campaign.start_id_) * 2 * scan_info.day_count_;
        int64_t sum_val = (campaign.end_id_ + campaign.start_id_ - 1) 
          * (campaign.end_id_ - campaign.start_id_) * scan_info.day_count_;

        print_brief_result(get_test_name(), scanner, scan_info, time, org_row_cnt);

        ret = check_row_and_cell_num(scanner, 1, EFFECT_COUNT);
        if (OB_SUCCESS == ret)
        {
          FOREACH_CELL(scanner)
          {
            GET_CELL();
            ret = check_one_cell(*cell_info, 0, column_index, 0, sum_val);
          }
        }
      }

      return ret;
    }

    int ObCampaignEffect::prepare(ObScanParam &scan_param)
    {
      int ret = OB_SUCCESS;

      ret = reset_extra_info();
      if (OB_SUCCESS == ret)
      {
        ret = build_basic_scan_param(scan_param);
      }

      if (OB_SUCCESS == ret)
      {
        ret = build_groupby_param(scan_param);
      }

      return ret;
    }
  } // end namespace olapdrive
} // end namespace oceanbase
