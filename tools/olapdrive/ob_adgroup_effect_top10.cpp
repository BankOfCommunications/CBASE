/**
 * (C) 2010-2011 Taobao Inc. 
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_adgroup_effect_top10.cpp for define adgroup effect top 10 
 * query classs. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */

#include "ob_adgroup_effect_top10.h"

namespace oceanbase 
{ 
  namespace olapdrive 
  {
    using namespace tbsys;
    using namespace common;
    using namespace client;

    ObAdgroupEffectTop10::ObAdgroupEffectTop10(
      ObClient& client, ObOlapdriveMeta& meta,
      ObOlapdriveSchema& schema, ObOlapdriveParam& param,
      ObOlapdriveStat& stat)
    : ObLzQuery(client, meta, schema, param, stat)
    {

    }

    ObAdgroupEffectTop10::~ObAdgroupEffectTop10()
    {

    }

    void ObAdgroupEffectTop10::calc_adgroup_id(
      const ObCampaign& campaign, const ObScanInfo& scan_info,
      int64_t& adgroup_id, int64_t& next_adgroup_id)
    {
      if (is_orderby_equal_val_column(scan_info.orderby_col_idx_))
      {
        if (adgroup_id < 0)
        {
          adgroup_id = campaign.start_adgroup_id_;
        }
        else
        {
          adgroup_id += 1;
        }
        next_adgroup_id = adgroup_id + 1;
      }
      else
      {
        if (adgroup_id < 0)
        {
          adgroup_id = campaign.end_adgroup_id_ - 1;
        }
        else
        {
          adgroup_id -= 1;
        }
        next_adgroup_id = adgroup_id - 1;
      }
    }

    int ObAdgroupEffectTop10::calc_adgroup(
      const ObCampaign& campaign, const ObScanInfo& scan_info, 
      int64_t& sum_val, int64_t& adgroup_id)
    {
      int ret                 = OB_SUCCESS;
      int64_t next_adgroup_id = 0;
      int64_t next_sum_val    = 0;
      ObAdgroup adgroup;
      ObAdgroup next_adgroup;

      calc_adgroup_id(campaign, scan_info, adgroup_id, next_adgroup_id);
      ret = meta_.get_adgroup(campaign.campaign_id_, campaign.campaign_id_, 
                              adgroup_id, adgroup);
      if (OB_SUCCESS == ret)
      {
        sum_val = calc_adgroup_shoptype_sum_val(adgroup, scan_info.day_count_, 
                                                scan_info.shop_type_); 
        if (next_adgroup_id >= campaign.start_adgroup_id_ &&
            next_adgroup_id < campaign.end_adgroup_id_)
        {
          ret = meta_.get_adgroup(campaign.campaign_id_, campaign.campaign_id_, 
                                  next_adgroup_id, next_adgroup);
          if (OB_SUCCESS == ret)
          {
            next_sum_val = calc_adgroup_shoptype_sum_val(
              next_adgroup, scan_info.day_count_, scan_info.shop_type_);
          }
        }
      }

      if (sum_val < next_sum_val)
      {
        sum_val = next_sum_val;
        adgroup_id = next_adgroup_id;
      }

      return ret;
    }

    const char* ObAdgroupEffectTop10::get_test_name()
    {
      return "ADGROUP_EFFECT_TOP10";
    }

    int ObAdgroupEffectTop10::check(
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
        int64_t groupby_col_cnt = 2;
        int64_t max_row_count   = campaign.end_adgroup_id_ - campaign.start_adgroup_id_;
        int64_t row_count       = max_row_count > 10 ? 10 : max_row_count;
        int64_t column_count    = groupby_col_cnt + EFFECT_COUNT;
        int64_t groupby_col_val = 0;
        int64_t sum_val         = 0;
        int64_t adgroup_id      = -1;

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
              ret = calc_adgroup(campaign, scan_info, sum_val, adgroup_id);
            }
            if (OB_SUCCESS == ret)
            {
              groupby_col_val = 
                (0 == column_index) ? campaign.campaign_id_ : adgroup_id;
              ret = check_one_cell(*cell_info, groupby_col_cnt, column_index, 
                                   groupby_col_val, sum_val);
            }
          }
        }
      }

      return ret;
    }

    int ObAdgroupEffectTop10::add_special_column(ObScanParam& scan_param)
    {
      int ret                       = OB_SUCCESS;
      ObGroupByParam& groupby_param = scan_param.get_group_by_param();

      START_BUILD_EXTRA_INFO(scan_param);
      extra_info->shop_type_ = MAX_SHOP_TYPE;
      extra_info->orderby_col_idx_ = get_not_equl_val_colum_idx();
      END_BUILD_EXTRA_INFO(scan_param);

      if (OB_SUCCESS == ret)
      {
        ret = scan_param.add_orderby_column(
          EFFECT_COLUMN_NAME[extra_info->orderby_col_idx_], ObScanParam::DESC);
      }

      if (OB_SUCCESS == ret)
      {
        ret = scan_param.set_topk_precision(1000, 0.1);
      }

      if (OB_SUCCESS == ret)
      {
        ret = scan_param.set_limit_info(0, 10);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_groupby_column(campaign_id);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_groupby_column(adgroup_id);
      }

      return ret;
    }
  } // end namespace olapdrive
} // end namespace oceanbase
