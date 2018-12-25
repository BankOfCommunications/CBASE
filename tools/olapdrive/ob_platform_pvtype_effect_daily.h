/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_platform_pvtype_effect_daily.h for define platform pvtype
 * effect daily effect query classs. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_PLATFORM_PVTYPE_EFFECT_DAILY_H
#define OCEANBASE_OLAPDRIVE_PLATFORM_PVTYPE_EFFECT_DAILY_H

#include "ob_lz_query.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    class ObPlatformPvtypeEffectDaily : public ObLzQuery
    {
    public:
      ObPlatformPvtypeEffectDaily(
        client::ObClient& client, ObOlapdriveMeta& meta,
        ObOlapdriveSchema& schema, ObOlapdriveParam& param,
        ObOlapdriveStat& stat);
      virtual ~ObPlatformPvtypeEffectDaily();

    public:
      virtual int check(const common::ObScanner &scanner, const int64_t time);
      virtual int add_special_column(common::ObScanParam& scan_param);
      virtual const char* get_test_name();

    private:
      int64_t calc_pped_sum_val(const ObCampaign& campaign, 
                                const int64_t platform_id, 
                                const int64_t pvtype_id);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObPlatformPvtypeEffectDaily);
    };
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_PLATFORM_PVTYPE_EFFECT_DAILY_H
