/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_several_bidword_effect.cpp for define several bidword 
 * effect query classs. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */

#include "ob_several_bidword_effect.h"

namespace oceanbase 
{ 
  namespace olapdrive 
  {
    using namespace tbsys;
    using namespace common;
    using namespace client;

    ObSeveralBidwordEffect::ObSeveralBidwordEffect(
      ObClient& client, ObOlapdriveMeta& meta,
      ObOlapdriveSchema& schema, ObOlapdriveParam& param,
      ObOlapdriveStat& stat)
    : ObLzQuery(client, meta, schema, param, stat)
    {

    }

    ObSeveralBidwordEffect::~ObSeveralBidwordEffect()
    {

    }

    const char* ObSeveralBidwordEffect::get_test_name()
    {
      return "SEVERAL_BIDWORD_EFFECT";
    }

    int ObSeveralBidwordEffect::check(
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
        int64_t bidword_id      = 0;

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
              bidword_id = scan_info.batch_scan_info_.bidword_id_array_[row_index - 1];
              sum_val = bidword_id * scan_info.day_count_;
            }
            if (OB_SUCCESS == ret)
            {
              ret = check_one_cell(*cell_info, groupby_col_cnt, column_index, 
                                   bidword_id, sum_val);
            }
          }
        }
      }

      return ret;
    }

    int ObSeveralBidwordEffect::add_special_column(ObScanParam& scan_param)
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
        ret = get_random_bidword_id_array(
          campaign, extra_info->batch_scan_info_.adgroup_id_array_, 
          extra_info->batch_scan_info_.adgroup_cnt_,
          extra_info->batch_scan_info_.bidword_id_array_,
          extra_info->batch_scan_info_.bidword_cnt_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = build_batch_scan_filter(extra_info->filter_str, FILTER_BUF_SIZE, 
                                      filter_len, extra_info->batch_scan_info_);
      }
      END_BUILD_EXTRA_INFO(scan_param);

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_groupby_column(bidword_id);
      }

      return ret;
    }
  } // end namespace olapdrive
} // end namespace oceanbase
