/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_campaign_effect.h for define campaign effect query classs.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_OLAPDRIVE_CAMPAIGN_EFFECT_H
#define OCEANBASE_OLAPDRIVE_CAMPAIGN_EFFECT_H

#include "ob_lz_query.h"

namespace oceanbase 
{ 
  namespace olapdrive
  {
    class ObCampaignEffect : public ObLzQuery
    {
    public:
      ObCampaignEffect(client::ObClient& client, ObOlapdriveMeta& meta,
                       ObOlapdriveSchema& schema, ObOlapdriveParam& param,
                       ObOlapdriveStat& stat);
    ~ObCampaignEffect();

    public:
      virtual int check(const common::ObScanner &scanner, const int64_t time);
      virtual int prepare(common::ObScanParam &scan_param);
      virtual const char* get_test_name();

    private:
      DISALLOW_COPY_AND_ASSIGN(ObCampaignEffect);
    };
  } // end namespace olapdrive
} // end namespace oceanbase

#endif //OCEANBASE_OLAPDRIVE_CAMPAIGN_EFFECT_H
