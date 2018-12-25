/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_platform_pvtype_effect.h for define platform pvtype effect
 * query classs. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_PLATFORM_PVTYPE_EFFECT_H
#define OCEANBASE_OLAPDRIVE_PLATFORM_PVTYPE_EFFECT_H

#include "ob_lz_query.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    class ObPlatformPvtypeEffect : public ObLzQuery
    {
    public:
      ObPlatformPvtypeEffect(client::ObClient& client, ObOlapdriveMeta& meta,
                             ObOlapdriveSchema& schema, ObOlapdriveParam& param,
                             ObOlapdriveStat& stat);
      virtual ~ObPlatformPvtypeEffect();

    public:
      virtual int check(const common::ObScanner &scanner, const int64_t time);
      virtual int add_special_column(common::ObScanParam& scan_param);
      virtual const char* get_test_name();

    private:
      int64_t cacl_column_value(
        const ObCampaign& campaign, const int64_t day_count,
        const int64_t row_index, const int64_t column_index);
      

    private:
      DISALLOW_COPY_AND_ASSIGN(ObPlatformPvtypeEffect);
    };
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_PLATFORM_PVTYPE_EFFECT_H
