/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_platform_pvtype_effect.cpp for define platform pvtype 
 * effect query classs. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */

#include "ob_platform_pvtype_effect.h"

namespace oceanbase 
{ 
  namespace olapdrive 
  {
    using namespace tbsys;
    using namespace common;
    using namespace client;

    ObPlatformPvtypeEffect::ObPlatformPvtypeEffect(
      ObClient& client, ObOlapdriveMeta& meta,
      ObOlapdriveSchema& schema, ObOlapdriveParam& param,
      ObOlapdriveStat& stat)
    : ObLzQuery(client, meta, schema, param, stat)
    {

    }

    ObPlatformPvtypeEffect::~ObPlatformPvtypeEffect()
    {

    }

    int64_t ObPlatformPvtypeEffect::cacl_column_value(
      const ObCampaign& campaign, const int64_t day_count,
      const int64_t row_index, const int64_t column_index)
    {
      int64_t ret_val   = 0;
      int64_t interval  = MAX_PLATFORM_ID * MAX_PV_TYPE;
      int64_t start_val = campaign.start_id_ + row_index - 1;

      if (0 == column_index) 
      {
        ret_val = campaign.campaign_id_;
      }
      else if (1 == column_index) 
      {
        ret_val = start_val % MAX_PLATFORM_ID + 1;
      } 
      else if (2 == column_index) 
      {
        ret_val = start_val % MAX_PV_TYPE;
      }
      else
      {
        ret_val = calc_sum_val(campaign.end_id_, start_val, interval, day_count);
      }

      return ret_val;
    }

    const char* ObPlatformPvtypeEffect::get_test_name()
    {
      return "PLATFORM_PVTYPE_EFFECT";
    }

    int ObPlatformPvtypeEffect::check(
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
        int64_t groupby_col_cnt = 3;
        int64_t row_count       = MAX_PLATFORM_ID * MAX_PV_TYPE;
        int64_t column_count    = groupby_col_cnt + EFFECT_COUNT;
        int64_t exp_val         = 0;

        print_brief_result(get_test_name(), scanner, scan_info, 
                           time, org_row_cnt);

        ret = check_row_and_cell_num(scanner, row_count, row_count * column_count);
        if (OB_SUCCESS == ret)
        {
          FOREACH_CELL(scanner)
          {
            GET_CELL();
            exp_val = cacl_column_value(campaign, scan_info.day_count_, 
                                        row_index, column_index);
            ret = check_one_cell(*cell_info, groupby_col_cnt, column_index, 
                                 exp_val, exp_val);
          }
        }
      }

      return ret;
    }

    int ObPlatformPvtypeEffect::add_special_column(ObScanParam& scan_param)
    {
      int ret = OB_SUCCESS;
      ObGroupByParam& groupby_param = scan_param.get_group_by_param();
      
      ret = reset_extra_info();
      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_groupby_column(campaign_id);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_groupby_column(platform_id);
      }

      if (OB_SUCCESS == ret)
      {
        ret = groupby_param.add_groupby_column(pvtype_id);
      }

      return ret;
    }
  } // end namespace olapdrive
} // end namespace oceanbase
