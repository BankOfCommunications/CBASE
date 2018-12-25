/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_several_adgroup_effect.h for define several adgroup effect
 * query classs. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_SEVERAL_ADGROUP_EFFECT_H
#define OCEANBASE_OLAPDRIVE_SEVERAL_ADGROUP_EFFECT_H

#include "ob_lz_query.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    class ObSeveralAdgroupEffect : public ObLzQuery
    {
    public:
      ObSeveralAdgroupEffect(client::ObClient& client, ObOlapdriveMeta& meta,
                             ObOlapdriveSchema& schema, ObOlapdriveParam& param,
                             ObOlapdriveStat& stat);
      virtual ~ObSeveralAdgroupEffect();

    public:
      virtual int check(const common::ObScanner &scanner, const int64_t time);
      virtual int add_special_column(common::ObScanParam& scan_param);
      virtual const char* get_test_name();

    private:
      int calc_adgroup_sum_val(const ObCampaign& campaign,
                               const ObScanInfo& scan_info,
                               int64_t& sum_val,
                               const int64_t adgroup_id);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObSeveralAdgroupEffect);
    };
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_SEVERAL_ADGROUP_EFFECT_H
