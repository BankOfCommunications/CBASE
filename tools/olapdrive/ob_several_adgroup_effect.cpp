/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_several_adgroup_effect.cpp for define several adgroup 
 * effect query classs. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */

#include "ob_several_adgroup_effect.h"

namespace oceanbase 
{ 
  namespace olapdrive 
  {
    using namespace tbsys;
    using namespace common;
    using namespace client;

    ObSeveralAdgroupEffect::ObSeveralAdgroupEffect(
      ObClient& client, ObOlapdriveMeta& meta,
      ObOlapdriveSchema& schema, ObOlapdriveParam& param,
      ObOlapdriveStat& stat)
    : ObLzQuery(client, meta, schema, param, stat)
    {

    }

    ObSeveralAdgroupEffect::~ObSeveralAdgroupEffect()
    {

    }

    int ObSeveralAdgroupEffect::calc_adgroup_sum_val(
      const ObCampaign& campaign, const ObScanInfo& scan_info, 
      int64_t& sum_val, const int64_t adgroup_id)
    {
      int ret = OB_SUCCESS;
      ObAdgroup adgroup;

      ret = meta_.get_adgroup(campaign.campaign_id_, campaign.campaign_id_, 
                              adgroup_id, adgroup);
      if (OB_SUCCESS == ret)
      {
        sum_val = (adgroup.end_id_ + adgroup.start_id_ - 1) 
          * (adgroup.end_id_ - adgroup.start_id_) / 2 * scan_info.day_count_;
      }

      return ret;
    }

    const char* ObSeveralAdgroupEffect::get_test_name()
    {
      return "SEVERAL_ADGROUP_EFFECT";
    }

    int ObSeveralAdgroupEffect::check(
      const ObScanner &scanner, const int64_t time)
    {
      int ret = OB_SUCCESS;
      ObScanInfo scan_info;

      ret = get_scan_info(scan_info);
      if (OB_SUCCESS == ret)
      {
        ObCampaign& campaign    = scan_info.campaign_;
        int64_t org_row_cnt     = 
          (campaign.end_id_ - campaign.start_id_) * 2 * scan_info.day_count_;
        int64_t groupby_col_cnt = 1;
        int64_t row_count       = scan_info.batch_scan_info_.adgroup_cnt_;
        int64_t column_count    = groupby_col_cnt + EFFECT_COUNT;
        int64_t sum_val         = 0;
        int64_t adgroup_id      = 0;

        print_brief_result(get_test_name(), scanner, scan_info, 
                           time, org_row_cnt);

        ret = check_row_and_cell_num(scanner, row_count, row_count * column_count);
        if (OB_SUCCESS == ret)
        {
          FOREACH_CELL(scanner)
          {
            GET_CELL();
            if (is_row_changed)
            {
              adgroup_id = scan_info.batch_scan_info_.adgroup_id_array_[row_index - 1];
              ret = calc_adgroup_sum_val(campaign, scan_info, sum_val, adgroup_id);
            }
            if (OB_SUCCESS == ret)
            {
              ret = check_one_cell(*cell_info, groupby_col_cnt, column_index, 
                                   adgroup_id, sum_val * 2);
            }
          }
        }
      }

      return ret;
    }

    int ObSeveralAdgroupEffect::add_special_column(ObScanParam& scan_param)
    {
      int ret                       = OB_SUCCESS;
      ObGroupByParam& groupby_param = scan_param.get_group_by_param();
      ObCampaign campaign;

      START_BUILD_EXTRA_INFO(scan_param);
      ret = get_cur_scan_campaign(campaign);
      if (OB_SUCCESS == ret)
      {
        extra_info->batch_scan_info_.adgroup_cnt_ = BATCH_SCAN_COUNT;
        ret = get_random_adgroup_id_array(
          campaign, extra_info->batch_scan_info_.adgroup_id_array_, 
          extra_info->batch_scan_info_.adgroup_cnt_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = build_batch_scan_filter(extra_info->filter_str, FILTER_BUF_SIZE, 
                                      filter_len, extra_info->batch_scan_info_);
      }
      END_BUILD_EXTRA_INFO(scan_param);

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_groupby_column(adgroup_id);
      }

      return ret;
    }
  } // end namespace olapdrive
} // end namespace oceanbase
