/**
 * (C) 2010-2011 Taobao Inc. 
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_adgroup_effect_daily.h for define adgroup effect daily 
 * query classs. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_ADGROUP_EFFECT_DAILY_H
#define OCEANBASE_OLAPDRIVE_ADGROUP_EFFECT_DAILY_H

#include "ob_lz_query.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    class ObAdgroupEffectDaily : public ObLzQuery
    {
    public:
      ObAdgroupEffectDaily(client::ObClient& client, ObOlapdriveMeta& meta,
                           ObOlapdriveSchema& schema, ObOlapdriveParam& param,
                           ObOlapdriveStat& stat);
      virtual ~ObAdgroupEffectDaily();

    public:
      virtual int check(const common::ObScanner &scanner, const int64_t time);
      virtual int add_special_column(common::ObScanParam& scan_param);
      virtual const char* get_test_name();

    private:
      int calc_adgroup(const ObCampaign& campaign, const int64_t adgroup_id,
                       const int64_t day_count, const int64_t shop_type, 
                       int64_t& sum_val);      

    private:
      DISALLOW_COPY_AND_ASSIGN(ObAdgroupEffectDaily);
    };
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_ADGROUP_EFFECT_DAILY_H
