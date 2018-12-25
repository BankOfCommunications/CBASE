/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_adgroup_effect_top10.h for define adgroup effect top 10 
 * query classs.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_ADGROUP_EFFECT_TOP10_H
#define OCEANBASE_OLAPDRIVE_ADGROUP_EFFECT_TOP10_H

#include "ob_lz_query.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    class ObAdgroupEffectTop10 : public ObLzQuery
    {
    public:
      ObAdgroupEffectTop10(client::ObClient& client, ObOlapdriveMeta& meta,
                           ObOlapdriveSchema& schema, ObOlapdriveParam& param,
                           ObOlapdriveStat& stat);
      virtual ~ObAdgroupEffectTop10();

    public:
      virtual int check(const common::ObScanner &scanner, const int64_t time);
      virtual int add_special_column(common::ObScanParam& scan_param);
      virtual const char* get_test_name();

    private:
      void calc_adgroup_id(const ObCampaign& campaign, 
                           const ObScanInfo& scan_info,
                           int64_t& adgroup_id, 
                           int64_t& next_adgroup_id);
      int calc_adgroup(const ObCampaign& campaign,
                       const ObScanInfo& scan_info,
                       int64_t& sum_val,
                       int64_t& adgroup_id);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObAdgroupEffectTop10);
    };
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_ADGROUP_EFFECT_TOP10_H
