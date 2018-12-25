/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_bidword_effect_daily.cpp for define bidword effect daily 
 * query classs.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */

#include "ob_bidword_effect_daily.h"

namespace oceanbase 
{ 
  namespace olapdrive 
  {
    using namespace tbsys;
    using namespace common;
    using namespace client;

    ObBidwordEffectDaily::ObBidwordEffectDaily(
      ObClient& client, ObOlapdriveMeta& meta,
      ObOlapdriveSchema& schema, ObOlapdriveParam& param,
      ObOlapdriveStat& stat)
    : ObLzQuery(client, meta, schema, param, stat)
    {

    }

    ObBidwordEffectDaily::~ObBidwordEffectDaily()
    {

    }

    const char* ObBidwordEffectDaily::get_test_name()
    {
      return "BIDWORD_EFFECT_DAILY";
    }

    int ObBidwordEffectDaily::check(
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
                                 scan_info.bidword_id_);
          }
        }
      }

      return ret;
    }

    int ObBidwordEffectDaily::add_special_column(ObScanParam& scan_param)
    {
      int ret = OB_SUCCESS;

      START_BUILD_EXTRA_INFO(scan_param);
      extra_info->shop_type_ = random() % MAX_SHOP_TYPE;
      extra_info->adgroup_id_ = get_random_adgroup_id();
      ret = get_random_bidword_id(extra_info->bidword_id_);
      if (OB_SUCCESS == ret)
      {
        filter_len = snprintf(extra_info->filter_str, FILTER_BUF_SIZE, 
                              "(`shop_type` = %ld) AND (`adgroup_id` = %ld) "
                              "AND (`bidword_id` = %ld)", 
                              extra_info->shop_type_, extra_info->adgroup_id_,
                              extra_info->bidword_id_);
      }
      END_BUILD_EXTRA_INFO(scan_param);

      if (OB_SUCCESS == ret)
      {
        ret = add_thedate_groupby_orderby(scan_param);
      }

      return ret;
    }
  } // end namespace olapdrive
} // end namespace oceanbase
