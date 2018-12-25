/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_platform_pvtype_effect_daily.cpp for define platform 
 * pvtype effect daily effect query classs. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */

#include "ob_platform_pvtype_effect_daily.h"

namespace oceanbase 
{ 
  namespace olapdrive 
  {
    using namespace tbsys;
    using namespace common;
    using namespace client;

    ObPlatformPvtypeEffectDaily::ObPlatformPvtypeEffectDaily(
      ObClient& client, ObOlapdriveMeta& meta,
      ObOlapdriveSchema& schema, ObOlapdriveParam& param,
      ObOlapdriveStat& stat)
    : ObLzQuery(client, meta, schema, param, stat)
    {

    }

    ObPlatformPvtypeEffectDaily::~ObPlatformPvtypeEffectDaily()
    {

    }

    int64_t ObPlatformPvtypeEffectDaily::calc_pped_sum_val(
      const ObCampaign& campaign, 
      const int64_t platform_id, 
      const int64_t pvtype_id)
    {
      int64_t ret_val   = 0;
      int64_t start_val = 0;
      int64_t interval  = MAX_PLATFORM_ID * MAX_PV_TYPE;

      for (int64_t i = 0; i < interval; ++i)
      {
        if ((campaign.start_id_ + i) % MAX_PLATFORM_ID + 1 == platform_id
            && (campaign.start_id_ + i) % MAX_PV_TYPE == pvtype_id)
        {
          start_val = campaign.start_id_ + i;
          break;
        }
      }

      ret_val = calc_sum_val(campaign.end_id_, start_val, interval, 1);

      return ret_val;
    }

    const char* ObPlatformPvtypeEffectDaily::get_test_name()
    {
      return "PLATFORM_PVTYPE_EFFECT_DAILY";
    }

    int ObPlatformPvtypeEffectDaily::check(
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
        int64_t row_count       = scan_info.day_count_;
        int64_t column_count    = groupby_col_cnt + EFFECT_COUNT;
        int64_t sum_val         = 
          calc_pped_sum_val(campaign, scan_info.platform_id_, 
                            scan_info.pvtype_id_);

        print_brief_result(get_test_name(), scanner, scan_info, 
                           time, org_row_cnt);

        ret = check_row_and_cell_num(scanner, row_count, row_count * column_count);
        if (OB_SUCCESS == ret)
        {
          FOREACH_CELL(scanner)
          {
            GET_CELL();
            ret = check_one_cell(*cell_info, groupby_col_cnt, column_index, 
                                 scan_info.start_date_ + row_count - row_index, 
                                 sum_val);
          }
        }
      }

      return ret;
    }

    int ObPlatformPvtypeEffectDaily::add_special_column(ObScanParam& scan_param)
    {
      int ret = OB_SUCCESS;

      START_BUILD_EXTRA_INFO(scan_param);
      extra_info->platform_id_ = random() % MAX_PLATFORM_ID + 1;
      extra_info->pvtype_id_ = random() % MAX_PV_TYPE;
      filter_len = snprintf(extra_info->filter_str, FILTER_BUF_SIZE, 
                            "(`platform_id` = %ld) AND (`pvtype_id` = %ld)", 
                            extra_info->platform_id_, extra_info->pvtype_id_);
      END_BUILD_EXTRA_INFO(scan_param);
      if (OB_SUCCESS == ret)
      {
        ret = add_thedate_groupby_orderby(scan_param);
      }

      return ret;
    }
  } // end namespace olapdrive
} // end namespace oceanbase
