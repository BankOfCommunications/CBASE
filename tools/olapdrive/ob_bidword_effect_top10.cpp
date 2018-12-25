/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_bidword_effect_top10.cpp for define bidword effect top 10 
 * query classs.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */

#include "ob_bidword_effect_top10.h"

namespace oceanbase 
{ 
  namespace olapdrive 
  {
    using namespace tbsys;
    using namespace common;
    using namespace client;

    ObBidwordEffectTop10::ObBidwordEffectTop10(
      ObClient& client, ObOlapdriveMeta& meta,
      ObOlapdriveSchema& schema, ObOlapdriveParam& param,
      ObOlapdriveStat& stat)
    : ObLzQuery(client, meta, schema, param, stat)
    {

    }

    ObBidwordEffectTop10::~ObBidwordEffectTop10()
    {

    }

    void ObBidwordEffectTop10::get_next_bidword_id(
      const ObScanInfo& scan_info, const ObCampaign& campaign, 
      int64_t& bidword_id)
    {
      if (is_orderby_equal_val_column(scan_info.orderby_col_idx_)) 
      {
        if (bidword_id < 0)
        {
          bidword_id = campaign.start_id_;
        }
        else
        {
          bidword_id += 1;
        }
        for (int64_t i = bidword_id; i < campaign.end_id_; ++i)
        {
          if (i % MAX_SHOP_TYPE == scan_info.shop_type_)
          {
            bidword_id = i;
            break;
          }
        }
      }
      else
      {
        if (bidword_id < 0)
        {
          bidword_id = campaign.end_id_ - 1;
        }
        else
        {
          bidword_id -= 1;
        }
        for (int64_t i = bidword_id; i >= campaign.start_id_; --i)
        {
          if (i % MAX_SHOP_TYPE == scan_info.shop_type_)
          {
            bidword_id = i;
            break;
          }
        }
      }
    }

    int64_t ObBidwordEffectTop10::calc_result_row_count(
      const ObCampaign& campaign, const int64_t shop_type)
    {
      int64_t start_val = 0;
      int64_t interval  = MAX_SHOP_TYPE;

      for (int64_t i = 0; i < interval; ++i)
      {
        if ((campaign.start_id_ + i) % MAX_SHOP_TYPE == shop_type)
        {
          start_val = campaign.start_id_ + i;
          break;
        }
      }

      return calc_row_count(campaign.end_id_, start_val, interval);
    }

    const char* ObBidwordEffectTop10::get_test_name()
    {
      return "BIDWORD_EFFECT_TOP10";
    }

    int ObBidwordEffectTop10::check(
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
        int64_t max_row_count   = calc_result_row_count(campaign, scan_info.shop_type_);
        int64_t row_count       = max_row_count > 10 ? 10 : max_row_count;
        int64_t column_count    = groupby_col_cnt + EFFECT_COUNT;
        int64_t bidword_id      = -1;

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
              get_next_bidword_id(scan_info, campaign, bidword_id);
            }
            if (OB_SUCCESS == ret)
            {
              ret = check_one_cell(*cell_info, groupby_col_cnt, column_index, 
                                   bidword_id, bidword_id * scan_info.day_count_);
            }
          }
        }
      }

      return ret;
    }

    int ObBidwordEffectTop10::add_special_column(
      ObScanParam& scan_param)
    {
      int ret                       = OB_SUCCESS;
      ObGroupByParam& groupby_param = scan_param.get_group_by_param();

      START_BUILD_EXTRA_INFO(scan_param);
      extra_info->shop_type_ = random() % MAX_SHOP_TYPE;
      extra_info->orderby_col_idx_ = get_not_equl_val_colum_idx();
      extra_info->adgroup_id_ = get_random_adgroup_id();
      filter_len = snprintf(extra_info->filter_str, FILTER_BUF_SIZE, 
                            "(`shop_type` = %ld) AND (`bidword_id` > 0)", 
                            extra_info->shop_type_);
      END_BUILD_EXTRA_INFO(scan_param);

      if (OB_SUCCESS == ret)
      {
        ret = scan_param.add_orderby_column(
          EFFECT_COLUMN_NAME[extra_info->orderby_col_idx_], ObScanParam::DESC);
      }

      if (OB_SUCCESS == ret)
      {
        ret = scan_param.set_topk_precision(1000, 0.05);
      }

      if (OB_SUCCESS == ret)
      {
        ret = scan_param.set_limit_info(0, 10);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_groupby_column(bidword_id);
      }

      return ret;
    }
  } // end namespace olapdrive
} // end namespace oceanbase
